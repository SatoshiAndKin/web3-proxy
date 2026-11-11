use std::{
    fs::{self, File, OpenOptions, Permissions},
    io::{self, ErrorKind},
    os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};

use tokio::{
    net::{UnixListener, UnixSocket},
    sync::watch,
    time::sleep,
};
use ulid::Ulid;

use super::LISTEN_BACKLOG;

/// Remove this socket after it drains if a replacement has not published.
pub(super) struct SocketPath {
    path: PathBuf,
    device: u64,
    inode: u64,
    publication_lock: Option<File>,
}

impl Drop for SocketPath {
    fn drop(&mut self) {
        if let Some(lock) = &self.publication_lock {
            // A publisher holding the lock may already have replaced this path.
            if lock.try_lock().is_err() {
                return;
            }
        }
        if fs::symlink_metadata(&self.path)
            .is_ok_and(|m| m.dev() == self.device && m.ino() == self.inode)
        {
            if let Err(error) = fs::remove_file(&self.path) {
                tracing::warn!(?error, path = %self.path.display(), "removing frontend socket");
            }
        }
    }
}

fn bind(path: &Path) -> io::Result<(UnixListener, SocketPath)> {
    if !path.is_absolute() || path.file_name().is_none() {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "frontend Unix socket path must be absolute and include a file name",
        ));
    }
    let directory =
        fs::symlink_metadata(path.parent().expect("absolute socket path has a parent"))?;
    if !directory.is_dir() || directory.mode() & 0o022 != 0 {
        return Err(io::Error::new(
            ErrorKind::PermissionDenied,
            "frontend Unix socket directory must permit writes only by its owner",
        ));
    }
    let mut lock_path = path.as_os_str().to_owned();
    lock_path.push(".lock");
    match fs::symlink_metadata(&lock_path) {
        Ok(metadata) if metadata.is_file() && metadata.uid() == directory.uid() => {}
        Ok(_) => {
            return Err(io::Error::new(
                ErrorKind::AlreadyExists,
                "frontend socket publication lock has an unexpected type or owner",
            ))
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .mode(0o600)
        .open(lock_path)?;
    if lock.metadata()?.uid() != directory.uid() {
        return Err(io::Error::new(
            ErrorKind::PermissionDenied,
            "frontend Unix socket directory must be owned by the proxy",
        ));
    }
    lock.try_lock().map_err(io::Error::from)?;
    match fs::symlink_metadata(path) {
        Ok(old) if old.file_type().is_socket() && old.uid() == directory.uid() => {}
        Ok(_) => {
            return Err(io::Error::new(
                ErrorKind::AlreadyExists,
                "frontend socket path has an unexpected type or owner",
            ))
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    let staged_path = path.with_file_name(format!(".w3p-{}", Ulid::generate()));
    let socket = UnixSocket::new_stream()?;
    socket.bind(&staged_path)?;
    let metadata = fs::symlink_metadata(&staged_path)?;
    let mut guard = SocketPath {
        path: staged_path,
        device: metadata.dev(),
        inode: metadata.ino(),
        publication_lock: None,
    };
    fs::set_permissions(&guard.path, Permissions::from_mode(0o660))?;
    let listener = socket.listen(LISTEN_BACKLOG)?;
    // Publish only after listen succeeds. Connections already accepted by the
    // old listener continue to use it while new dials use the replacement.
    fs::rename(&guard.path, path)?;
    guard.path = path.to_owned();
    let unlocked = lock.unlock();
    guard.publication_lock = Some(lock);
    unlocked?;
    tracing::info!(path = %path.display(), "listening on Unix socket");
    Ok((listener, guard))
}

/// Keep TCP serving if another executor is publishing the Unix socket.
pub(super) async fn bind_when_available(
    path: &Path,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<Option<(UnixListener, SocketPath)>> {
    let acquire = async {
        loop {
            match bind(path) {
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    sleep(Duration::from_millis(50)).await;
                }
                result => return result.map(Some),
            }
        }
    };
    tokio::select! {
        biased;
        _ = shutdown.wait_for(|stopping| *stopping) => Ok(None),
        result = acquire => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::UnixStream,
    };

    #[tokio::test]
    async fn socket_is_private_and_recovers_after_an_unclean_exit() {
        let dir = tempfile::Builder::new()
            .prefix("w3p")
            .tempdir_in("/tmp")
            .unwrap();
        let path = dir.path().join("q");
        drop(UnixListener::bind(&path).unwrap());
        let (listener, guard) = bind(&path).unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o660
        );
        let _peer = UnixStream::connect(&path).await.unwrap();
        assert!(listener.accept().await.is_ok());
        drop(listener);
        drop(guard);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn publication_preserves_old_connections_and_the_replacement_path() {
        let dir = tempfile::Builder::new()
            .prefix("w3p")
            .tempdir_in("/tmp")
            .unwrap();
        let path = dir.path().join("q");
        let (first, first_guard) = bind(&path).unwrap();
        let mut old_peer = UnixStream::connect(&path).await.unwrap();
        let (mut old_connection, _) = first.accept().await.unwrap();

        let (replacement, replacement_guard) = bind(&path).unwrap();
        let mut new_peer = UnixStream::connect(&path).await.unwrap();
        let (mut new_connection, _) = replacement.accept().await.unwrap();
        old_peer.write_all(b"o").await.unwrap();
        new_peer.write_all(b"n").await.unwrap();
        assert_eq!(old_connection.read_u8().await.unwrap(), b'o');
        assert_eq!(new_connection.read_u8().await.unwrap(), b'n');

        drop(first);
        drop(first_guard);
        let _peer = UnixStream::connect(&path).await.unwrap();
        assert!(replacement.accept().await.is_ok());
        drop(replacement);
        drop(replacement_guard);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn socket_does_not_replace_a_file() {
        let dir = tempfile::Builder::new()
            .prefix("w3p")
            .tempdir_in("/tmp")
            .unwrap();
        let path = dir.path().join("q");
        fs::write(&path, "original").unwrap();
        assert!(bind(&path).is_err());
        assert_eq!(fs::read_to_string(path).unwrap(), "original");
    }
}
