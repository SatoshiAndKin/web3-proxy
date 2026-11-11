use std::{
    fs::{self, File, OpenOptions, Permissions},
    io::{self, ErrorKind},
    os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};

use tokio::net::{UnixListener, UnixSocket, UnixStream};

use super::LISTEN_BACKLOG;

/// Keep the lease until the listener and its in-flight requests have stopped.
pub(super) struct SocketPath {
    path: PathBuf,
    device: u64,
    inode: u64,
    _lock: File,
}

impl Drop for SocketPath {
    fn drop(&mut self) {
        if fs::symlink_metadata(&self.path)
            .is_ok_and(|m| m.dev() == self.device && m.ino() == self.inode)
        {
            if let Err(error) = fs::remove_file(&self.path) {
                tracing::warn!(?error, path = %self.path.display(), "removing frontend socket");
            }
        }
    }
}

pub(super) async fn bind(path: &Path) -> io::Result<(UnixListener, SocketPath)> {
    if !path.is_absolute() {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "frontend Unix socket path must be absolute",
        ));
    }
    let mut lock_path = path.as_os_str().to_owned();
    lock_path.push(".lock");
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .mode(0o600)
        .open(lock_path)?;
    lock.try_lock().map_err(io::Error::from)?;
    match fs::symlink_metadata(path) {
        Ok(old) if old.file_type().is_socket() && old.uid() == lock.metadata()?.uid() => {
            // The lease serializes proxy instances. Check for a live listener
            // before removing a socket created by another program.
            let probe = tokio::time::timeout(Duration::from_millis(250), UnixStream::connect(path));
            match probe.await {
                Ok(Err(error)) if error.kind() == ErrorKind::ConnectionRefused => {
                    fs::remove_file(path)?;
                }
                _ => {
                    return Err(io::Error::new(
                        ErrorKind::AddrInUse,
                        "frontend socket is in use",
                    ))
                }
            }
        }
        Ok(_) => {
            return Err(io::Error::new(
                ErrorKind::AlreadyExists,
                "frontend socket path exists",
            ))
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    let socket = UnixSocket::new_stream()?;
    socket.bind(path)?;
    let metadata = fs::symlink_metadata(path)?;
    let guard = SocketPath {
        path: path.to_owned(),
        device: metadata.dev(),
        inode: metadata.ino(),
        _lock: lock,
    };
    fs::set_permissions(path, Permissions::from_mode(0o660))?;
    let listener = socket.listen(LISTEN_BACKLOG)?;
    tracing::info!(path = %path.display(), "listening on Unix socket");
    Ok((listener, guard))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn socket_is_private_and_recovers_after_an_unclean_exit() {
        let dir = tempfile::Builder::new()
            .prefix("w3p")
            .tempdir_in("/tmp")
            .unwrap();
        let path = dir.path().join("q");
        let stale = UnixListener::bind(&path).unwrap();
        drop(stale);

        let (listener, lease) = bind(&path).await.unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o660
        );
        assert!(bind(&path).await.is_err());
        let _peer = UnixStream::connect(&path).await.unwrap();
        assert!(listener.accept().await.is_ok());

        drop(listener);
        drop(lease);
        assert!(!path.exists());
        assert!(bind(&path).await.is_ok());
    }

    #[tokio::test]
    async fn socket_does_not_replace_a_file() {
        let dir = tempfile::Builder::new()
            .prefix("w3p")
            .tempdir_in("/tmp")
            .unwrap();
        let path = dir.path().join("q");
        fs::write(&path, "original").unwrap();
        assert!(bind(&path).await.is_err());
        assert_eq!(fs::read_to_string(path).unwrap(), "original");
    }
}
