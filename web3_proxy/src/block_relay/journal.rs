//! Local crash safety for Engine imports. This is not a cross-host lock.
use alloy::primitives::{keccak256, B256};
use anyhow::{ensure, Result};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::{Path, PathBuf},
    sync::{Arc, Weak},
};

pub(super) struct StateStore {
    directory: PathBuf,
    _lock: File,
    journals: Mutex<BTreeMap<B256, Weak<Journal>>>,
}
impl StateStore {
    /// Call on the blocking pool. Each forwarder must own a separate persistent directory.
    pub fn open(directory: &Path) -> Result<Arc<Self>> {
        ensure!(
            directory.is_absolute(),
            "relay state directory must be absolute"
        );
        let open = || -> std::io::Result<_> {
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(directory)?;
            let lock = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .mode(0o600)
                .open(directory.join(".lock"))?;
            Ok(lock)
        };
        let lock = open().map_err(|_| anyhow::anyhow!("cannot open relay state directory"))?;
        lock.try_lock().map_err(|_| {
            anyhow::anyhow!("relay state directory is already locked or cannot be locked")
        })?;
        Ok(Arc::new(Self {
            directory: directory.to_owned(),
            _lock: lock,
            journals: Mutex::new(BTreeMap::new()),
        }))
    }

    /// Hash the normalized endpoint so credentials never appear in filenames or records.
    pub fn journal(self: &Arc<Self>, endpoint: &str) -> Result<Arc<Journal>> {
        let id = keccak256(super::config::url(endpoint)?.as_str());
        let mut journals = self.journals.lock();
        if let Some(journal) = journals.get(&id).and_then(Weak::upgrade) {
            return Ok(journal);
        }
        let path = self.directory.join(format!("engine-{id:x}.json"));
        let record = match File::open(&path) {
            Ok(file) => {
                let mut bytes = Vec::new();
                file.take(4097)
                    .read_to_end(&mut bytes)
                    .map_err(|_| anyhow::anyhow!("cannot read Engine journal"))?;
                ensure!(bytes.len() <= 4096, "invalid Engine journal");
                sonic_rs::from_slice::<Record>(&bytes)
                    .map_err(|_| anyhow::anyhow!("invalid Engine journal"))?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Record::Idle {},
            Err(_) => anyhow::bail!("cannot read Engine journal"),
        };
        let journal = Arc::new(Journal {
            store: self.clone(),
            path,
            state: Mutex::new(State {
                record,
                failed: false,
            }),
        });
        journals.insert(id, Arc::downgrade(&journal));
        Ok(journal)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Pending {
    pub hash: B256,
    pub number: u64,
}
#[derive(Deserialize, Serialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum Record {
    Idle {},
    Pending { hash: B256, number: u64 },
}
impl Record {
    fn pending(&self) -> Option<Pending> {
        match self {
            Self::Idle {} => None,
            Self::Pending { hash, number } => Some(Pending {
                hash: *hash,
                number: *number,
            }),
        }
    }
}
struct State {
    record: Record,
    failed: bool,
}
pub(super) struct Journal {
    store: Arc<StateStore>,
    path: PathBuf,
    state: Mutex<State>,
}
impl Journal {
    pub fn status(&self) -> (Option<Pending>, bool) {
        let state = self.state.lock();
        (state.record.pending(), state.failed)
    }
    pub async fn begin(self: &Arc<Self>, pending: Pending) -> Result<()> {
        let journal = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut state = journal.state.lock();
            ensure!(
                !state.failed && state.record.pending().is_none(),
                "Engine journal is suspended"
            );
            journal.replace(&mut state, Some(pending))
        })
        .await
        .map_err(|_| anyhow::anyhow!("Engine journal task failed"))?
    }
    pub async fn complete(self: &Arc<Self>, pending: Pending) -> Result<()> {
        let journal = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut state = journal.state.lock();
            ensure!(
                state.record.pending() == Some(pending),
                "Engine journal identity mismatch"
            );
            journal.replace(&mut state, None)
        })
        .await
        .map_err(|_| anyhow::anyhow!("Engine journal task failed"))?
    }
    fn replace(&self, state: &mut State, pending: Option<Pending>) -> Result<()> {
        let record = match pending {
            Some(Pending { hash, number }) => Record::Pending { hash, number },
            None => Record::Idle {},
        };
        let write = || -> Result<()> {
            let bytes = sonic_rs::to_vec(&record)?;
            let temporary = self.path.with_extension("tmp");
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            std::fs::rename(temporary, &self.path)?;
            File::open(&self.store.directory)?.sync_all()?;
            Ok(())
        };
        if write().is_err() {
            state.failed = true;
            anyhow::bail!("cannot persist Engine journal; injection suspended");
        }
        state.record = record;
        state.failed = false;
        Ok(())
    }
}
