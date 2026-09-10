//! Bounded private JSONL measurement storage. Never delete trial evidence automatically.
use super::stats::Sample;
use anyhow::{ensure, Result};
use serde::Serialize;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncWriteExt, BufWriter},
    sync::mpsc,
};

const MAX_TOTAL_BYTES: u64 = 512 * 1024 * 1024;
const MAX_FILE_BYTES: u64 = 16 * 1024 * 1024;
pub(super) type Sender = mpsc::Sender<Record>;

#[derive(Serialize)]
#[serde(tag = "record", rename_all = "snake_case")]
pub(super) enum Record {
    Observation(Sample),
    BlobReady {
        root: alloy::primitives::B256,
        slot: u64,
        mode: super::config::Mode,
        source: String,
        ready_us: u64,
    },
    Announcement {
        root: alloy::primitives::B256,
        slot: u64,
        source: String,
        event: &'static str,
        at_unix_us: u64,
    },
    AcquisitionFailed {
        root: alloy::primitives::B256,
        slot: u64,
        consensus: bool,
    },
}

pub(super) struct Recording {
    directory: PathBuf,
    file: BufWriter<File>,
    file_bytes: u64,
    total_bytes: u64,
}
impl Recording {
    pub async fn open(state_dir: &Path) -> Result<Self> {
        Self::open_inner(state_dir)
            .await
            .map_err(|_| anyhow::anyhow!("cannot open private relay measurement log"))
    }
    async fn open_inner(state_dir: &Path) -> Result<Self> {
        let directory = state_dir.join("observations");
        tokio::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&directory)
            .await?;
        let mut files = tokio::fs::read_dir(&directory).await?;
        let mut total_bytes = 0u64;
        let mut count = 0;
        while let Some(file) = files.next_entry().await? {
            count += 1;
            ensure!(count <= 4096, "too many measurement files");
            total_bytes = total_bytes
                .checked_add(file.metadata().await?.len())
                .ok_or_else(|| anyhow::anyhow!("measurement size overflow"))?;
        }
        ensure!(
            total_bytes < MAX_TOTAL_BYTES,
            "measurement storage limit reached"
        );
        let file = Self::file(&directory).await?;
        Ok(Self {
            directory,
            file,
            file_bytes: 0,
            total_bytes,
        })
    }
    async fn file(directory: &Path) -> Result<BufWriter<File>> {
        let path = directory.join(format!("{}.jsonl", ulid::Ulid::generate()));
        Ok(BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path)
                .await?,
        ))
    }
    async fn append(&mut self, record: &Record) -> Result<()> {
        let mut bytes = sonic_rs::to_vec(record)?;
        bytes.push(b'\n');
        let size = u64::try_from(bytes.len())?;
        ensure!(
            size <= MAX_FILE_BYTES && self.total_bytes + size <= MAX_TOTAL_BYTES,
            "measurement storage limit reached"
        );
        if self.file_bytes + size > MAX_FILE_BYTES {
            self.flush().await?;
            self.file = Self::file(&self.directory).await?;
            self.file_bytes = 0;
        }
        self.file.write_all(&bytes).await?;
        self.file_bytes += size;
        self.total_bytes += size;
        Ok(())
    }
    async fn flush(&mut self) -> Result<()> {
        self.file.flush().await?;
        self.file.get_ref().sync_data().await?;
        Ok(())
    }
    pub async fn run(mut self, mut records: mpsc::Receiver<Record>) -> Result<()> {
        let run = async {
            let mut flush = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = flush.tick() => self.flush().await?,
                    record = records.recv() => match record {
                        Some(record) => self.append(&record).await?,
                        None => return self.flush().await,
                    }
                }
            }
        };
        run.await
            .map_err(|_| anyhow::anyhow!("private relay measurement log failed; check storage"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn records_flush_on_shutdown_and_storage_limits_preserve_existing_evidence() {
        let directory = tempfile::tempdir().unwrap();
        let mut recording = Recording::open(directory.path()).await.unwrap();
        let record = Record::AcquisitionFailed {
            root: alloy::primitives::B256::ZERO,
            slot: 1,
            consensus: false,
        };
        recording.append(&record).await.unwrap();
        recording.file_bytes = MAX_FILE_BYTES;
        recording.append(&record).await.unwrap();
        recording.total_bytes = MAX_TOTAL_BYTES;
        assert!(recording.append(&record).await.is_err());
        let (send, recv) = mpsc::channel(1);
        drop(send);
        recording.run(recv).await.unwrap();
        let mut files = tokio::fs::read_dir(directory.path().join("observations"))
            .await
            .unwrap();
        let expected = format!("{}\n", sonic_rs::to_string(&record).unwrap());
        let mut count = 0;
        while let Some(file) = files.next_entry().await.unwrap() {
            assert_eq!(
                tokio::fs::read_to_string(file.path()).await.unwrap(),
                expected
            );
            count += 1;
        }
        assert_eq!(count, 2);
    }
}
