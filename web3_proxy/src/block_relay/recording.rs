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
pub(super) type Sender = mpsc::Sender<super::telemetry::Envelope>;

#[derive(Serialize)]
#[serde(tag = "record", rename_all = "snake_case")]
pub(super) enum Record {
    Configured {
        sources: Vec<String>,
        execution_targets: Vec<String>,
        consensus_targets: Vec<String>,
        genesis_time: u64,
        seconds_per_slot: u64,
    },
    ModeChanged {
        previous: super::config::Mode,
    },
    Acquired {
        started_mode_epoch: u64,
        root: alloy::primitives::B256,
        hash: alloy::primitives::B256,
        slot: u64,
        number: u64,
        first_seen_us: u64,
        acquired_us: u64,
        source: String,
        blob_count: usize,
    },
    SubmissionStarted {
        attempt_id: u64,
        layer: super::stats::Layer,
        root: alloy::primitives::B256,
        hash: alloy::primitives::B256,
        slot: u64,
        target: String,
        serialized_request_bytes: usize,
        canonical_before_call: Option<super::telemetry::Head>,
    },
    Submission {
        attempt_id: u64,
        started_mode: super::config::Mode,
        started_mode_epoch: u64,
        layer: super::stats::Layer,
        root: alloy::primitives::B256,
        hash: alloy::primitives::B256,
        slot: u64,
        target: String,
        outcome: String,
        elapsed_us: u64,
    },
    Observation(Sample),
    BlobReady {
        root: alloy::primitives::B256,
        slot: u64,
        mode: super::config::Mode,
        mode_epoch: u64,
        blob_count: usize,
        source: String,
        ready_us: u64,
    },
    Announcement {
        root: alloy::primitives::B256,
        slot: u64,
        source: String,
        event: &'static str,
        at_unix_us: u64,
        observed_us: u64,
    },
    Disposition {
        layer: super::stats::Layer,
        root: alloy::primitives::B256,
        hash: alloy::primitives::B256,
        slot: u64,
        target: String,
        reason: &'static str,
    },
    Head {
        target: String,
        head: super::telemetry::Head,
    },
    Resources {
        resources: super::telemetry::Resources,
        started_mode_epoch: u64,
    },
    ModeTotals {
        totals: sonic_rs::Value,
        losses: sonic_rs::Value,
    },
    AcquisitionFailed {
        started_mode_epoch: u64,
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
    async fn append(&mut self, record: &super::telemetry::Envelope) -> Result<()> {
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
    /// Disk work never runs on the forwarding task. Retain files and retry storage at
    /// a bounded rate; consume and count dropped records while storage is unavailable.
    pub async fn supervise(
        state_dir: PathBuf,
        mut records: mpsc::Receiver<super::telemetry::Envelope>,
        stats: super::stats::Shared,
    ) {
        let mut writer: Option<Self> = None;
        let mut pending_records = 0u64;
        let mut retry_at = tokio::time::Instant::now();
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if writer.is_none() && tokio::time::Instant::now() >= retry_at {
                match tokio::time::timeout(Duration::from_secs(2), Self::open(&state_dir)).await {
                    Ok(Ok(opened)) => writer = Some(opened),
                    _ => {
                        stats
                            .lock()
                            .recording
                            .failure("cannot open measurement storage");
                        retry_at = tokio::time::Instant::now() + Duration::from_secs(5);
                    }
                }
            }
            let result = tokio::select! {
                _ = tick.tick() => {
                    if let Some(writer) = writer.as_mut() {
                        match tokio::time::timeout(Duration::from_secs(2), writer.flush()).await {
                            Ok(Ok(())) => {
                                pending_records = 0;
                                stats.lock().recording.success(0, "measurement storage available");
                                Ok(())
                            }
                            _ => Err(()),
                        }
                    } else { Ok(()) }
                }
                record = records.recv() => match record {
                    Some(record) => {
                        if let Some(writer) = writer.as_mut() {
                            pending_records += 1;
                            match tokio::time::timeout(Duration::from_secs(2), writer.append(&record)).await {
                                Ok(Ok(())) => Ok(()),
                                _ => Err(()),
                            }
                        } else { stats.lock().recording_dropped += 1; Ok(()) }
                    }
                    None => {
                        if let Some(writer) = writer.as_mut() {
                            if !matches!(tokio::time::timeout(Duration::from_secs(1), writer.flush()).await, Ok(Ok(()))) {
                                let mut s = stats.lock();
                                s.recording_dropped += pending_records;
                                s.recording.failure("measurement flush failed");
                            }
                        }
                        return;
                    }
                }
            };
            if result.is_err() {
                let mut s = stats.lock();
                s.recording
                    .failure("measurement write failed; storage retry pending");
                s.recording_dropped += pending_records;
                pending_records = 0;
                writer = None;
                retry_at = tokio::time::Instant::now() + Duration::from_secs(5);
            }
        }
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
            started_mode_epoch: 0,
            root: alloy::primitives::B256::ZERO,
            slot: 1,
            consensus: false,
        };
        let record = super::super::telemetry::Envelope {
            context: super::super::telemetry::Telemetry::default().context(Default::default()),
            event: record,
        };
        recording.append(&record).await.unwrap();
        recording.file_bytes = MAX_FILE_BYTES;
        recording.append(&record).await.unwrap();
        recording.total_bytes = MAX_TOTAL_BYTES;
        assert!(recording.append(&record).await.is_err());
        recording.flush().await.unwrap();
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
