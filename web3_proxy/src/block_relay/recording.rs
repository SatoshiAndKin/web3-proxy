//! Bounded private JSONL measurement storage with oldest-first retention.
use super::stats::Sample;
use anyhow::{ensure, Result};
use serde::Serialize;
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncWriteExt, BufWriter},
    sync::mpsc,
};

const MAX_TOTAL_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_FILES: usize = 4096;
const MAX_FILE_BYTES: u64 = 16 * 1024 * 1024;
pub(super) type Sender = mpsc::Sender<super::telemetry::Envelope>;

#[derive(Serialize)]
#[serde(tag = "record", rename_all = "snake_case")]
pub(super) enum Record {
    AcquisitionAttempt(super::telemetry::AcquisitionAttempt),
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

struct LogFile {
    path: PathBuf,
    bytes: u64,
}

struct ActiveFile {
    log: LogFile,
    writer: BufWriter<File>,
}

pub(super) struct Recording {
    directory: PathBuf,
    active: Option<ActiveFile>,
    completed: VecDeque<LogFile>,
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
        let mut entries = tokio::fs::read_dir(&directory).await?;
        let mut files = Vec::new();
        let mut total_bytes = 0u64;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let recording_file = name
                .to_str()
                .and_then(|name| name.strip_suffix(".jsonl"))
                .is_some_and(|stem| stem.parse::<ulid::Ulid>().is_ok());
            if !recording_file || !entry.file_type().await?.is_file() {
                continue;
            }
            let bytes = entry.metadata().await?.len();
            if bytes == 0 {
                tokio::fs::remove_file(entry.path()).await?;
                continue;
            }
            total_bytes = total_bytes
                .checked_add(bytes)
                .ok_or_else(|| anyhow::anyhow!("measurement size overflow"))?;
            files.push(LogFile {
                path: entry.path(),
                bytes,
            });
        }
        // ULID names order completed logs by creation time. Only this writer's
        // active file is excluded from retention until it has been flushed.
        files.sort_by(|left, right| left.path.cmp(&right.path));
        let mut recording = Self {
            directory,
            active: None,
            completed: files.into(),
            total_bytes,
        };
        recording.make_room(0).await?;
        Ok(recording)
    }
    async fn file(directory: &Path) -> Result<ActiveFile> {
        let log = LogFile {
            path: directory.join(format!("{}.jsonl", ulid::Ulid::generate())),
            bytes: 0,
        };
        let writer = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&log.path)
                .await?,
        );
        Ok(ActiveFile { log, writer })
    }
    async fn make_room(&mut self, incoming_bytes: u64) -> Result<()> {
        // Reserve one file for the active writer even before its first record.
        while self.total_bytes + incoming_bytes > MAX_TOTAL_BYTES
            || self.completed.len() >= MAX_FILES
        {
            let oldest = self
                .completed
                .front()
                .ok_or_else(|| anyhow::anyhow!("measurement storage limit reached"))?;
            tokio::fs::remove_file(&oldest.path).await?;
            self.total_bytes -= oldest.bytes;
            self.completed.pop_front();
        }
        Ok(())
    }
    async fn append(&mut self, record: &super::telemetry::Envelope) -> Result<()> {
        let mut bytes = sonic_rs::to_vec(record)?;
        bytes.push(b'\n');
        let size = u64::try_from(bytes.len())?;
        ensure!(
            size <= MAX_FILE_BYTES,
            "measurement record exceeds file limit"
        );
        if self
            .active
            .as_ref()
            .is_some_and(|active| active.log.bytes + size > MAX_FILE_BYTES)
        {
            self.flush().await?;
            if let Some(active) = self.active.take() {
                self.completed.push_back(active.log);
            }
        }
        // Check the serialized size and prune closed logs before opening a file.
        // Idle retries and rejected records therefore cannot create empty files.
        self.make_room(size).await?;
        if self.active.is_none() {
            self.active = Some(Self::file(&self.directory).await?);
        }
        let active = self
            .active
            .as_mut()
            .expect("active recording file was opened");
        active.writer.write_all(&bytes).await?;
        active.log.bytes += size;
        self.total_bytes += size;
        Ok(())
    }
    async fn flush(&mut self) -> Result<()> {
        if let Some(active) = self.active.as_mut() {
            active.writer.flush().await?;
            active.writer.get_ref().sync_data().await?;
        }
        Ok(())
    }
    /// Disk work never runs on the forwarding task. Rotate logs and retry storage at
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

    fn sample() -> super::super::telemetry::Envelope {
        super::super::telemetry::Envelope {
            context: super::super::telemetry::Telemetry::default().context(Default::default()),
            event: Record::AcquisitionFailed {
                started_mode_epoch: 0,
                root: alloy::primitives::B256::ZERO,
                slot: 1,
                consensus: false,
            },
        }
    }

    #[tokio::test]
    async fn opening_and_flushing_idle_storage_creates_no_files() {
        let directory = tempfile::tempdir().unwrap();
        for _ in 0..3 {
            let mut recording = Recording::open(directory.path()).await.unwrap();
            recording.flush().await.unwrap();
        }
        assert_eq!(
            std::fs::read_dir(directory.path().join("observations"))
                .unwrap()
                .count(),
            0
        );
    }

    #[tokio::test]
    async fn startup_removes_empty_recordings_and_preserves_other_entries() {
        let directory = tempfile::tempdir().unwrap();
        let observations = directory.path().join("observations");
        std::fs::create_dir(&observations).unwrap();
        for i in 0..4097 {
            std::fs::write(observations.join(format!("{i:026}.jsonl")), "").unwrap();
        }
        let history = observations.join(format!("{:026}.jsonl", 5000));
        std::fs::write(&history, "{\"history\":true}\n").unwrap();
        let unrelated = observations.join("operator-notes.txt");
        std::fs::write(&unrelated, "preserve").unwrap();
        let outside = directory.path().join("outside");
        std::fs::write(&outside, "preserve outside").unwrap();
        let link = observations.join(format!("{:026}.jsonl", 5001));
        std::os::unix::fs::symlink(&outside, &link).unwrap();
        let nested = observations.join(format!("{:026}.jsonl", 5002));
        std::fs::create_dir(&nested).unwrap();

        let mut recording = Recording::open(directory.path()).await.unwrap();
        assert_eq!(std::fs::read_dir(&observations).unwrap().count(), 4);
        recording.append(&sample()).await.unwrap();
        recording.flush().await.unwrap();
        assert_eq!(std::fs::read_dir(&observations).unwrap().count(), 5);
        assert_eq!(
            std::fs::read_to_string(history).unwrap(),
            "{\"history\":true}\n"
        );
        assert_eq!(std::fs::read_to_string(unrelated).unwrap(), "preserve");
        assert_eq!(
            std::fs::read_to_string(outside).unwrap(),
            "preserve outside"
        );
        assert!(link.symlink_metadata().unwrap().file_type().is_symlink());
        assert!(nested.is_dir());
    }

    #[tokio::test]
    async fn full_storage_prunes_oldest_recording_and_accepts_new_records() {
        let directory = tempfile::tempdir().unwrap();
        let observations = directory.path().join("observations");
        std::fs::create_dir(&observations).unwrap();
        let limit = 1024 * 1024 * 1024;
        for i in 0..limit / MAX_FILE_BYTES {
            std::fs::File::create(observations.join(format!("{i:026}.jsonl")))
                .unwrap()
                .set_len(MAX_FILE_BYTES)
                .unwrap();
        }
        let mut recording = Recording::open(directory.path()).await.unwrap();
        let record = sample();
        recording.append(&record).await.unwrap();
        recording.append(&record).await.unwrap();
        recording.flush().await.unwrap();
        assert!(!observations.join(format!("{:026}.jsonl", 0)).exists());
        for i in 1..limit / MAX_FILE_BYTES {
            assert_eq!(
                std::fs::metadata(observations.join(format!("{i:026}.jsonl")))
                    .unwrap()
                    .len(),
                MAX_FILE_BYTES
            );
        }
        let sizes: Vec<_> = std::fs::read_dir(&observations)
            .unwrap()
            .map(|file| file.unwrap().metadata().unwrap().len())
            .collect();
        assert!(sizes.iter().sum::<u64>() <= limit);
        assert_eq!(sizes.len() as u64, limit / MAX_FILE_BYTES);
        let newest = std::fs::read_dir(&observations)
            .unwrap()
            .map(|file| file.unwrap().path())
            .max()
            .unwrap();
        let expected = format!("{}\n", sonic_rs::to_string(&record).unwrap());
        assert_eq!(std::fs::read_to_string(newest).unwrap(), expected.repeat(2));
    }
    #[tokio::test]
    async fn rotation_flushes_completed_files_and_preserves_their_contents() {
        let directory = tempfile::tempdir().unwrap();
        let mut recording = Recording::open(directory.path()).await.unwrap();
        let mut record = sample();
        record.event = Record::Configured {
            sources: vec!["x".repeat(MAX_FILE_BYTES as usize / 2)],
            execution_targets: vec![],
            consensus_targets: vec![],
            genesis_time: 0,
            seconds_per_slot: 12,
        };
        recording.append(&record).await.unwrap();
        recording.append(&record).await.unwrap();
        recording.flush().await.unwrap();
        let expected = format!("{}\n", sonic_rs::to_string(&record).unwrap());
        let files: Vec<_> = std::fs::read_dir(directory.path().join("observations"))
            .unwrap()
            .map(|file| file.unwrap().path())
            .collect();
        assert_eq!(files.len(), 2);
        for file in files {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(std::fs::read_to_string(&file).unwrap(), expected);
            assert_eq!(
                std::fs::metadata(file).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[tokio::test]
    async fn rejected_oversize_records_do_not_create_files_or_prune_history() {
        let directory = tempfile::tempdir().unwrap();
        let observations = directory.path().join("observations");
        std::fs::create_dir(&observations).unwrap();
        let history = observations.join(format!("{:026}.jsonl", 1));
        std::fs::write(&history, "{}\n").unwrap();
        let mut recording = Recording::open(directory.path()).await.unwrap();
        let mut record = sample();
        record.event = Record::Configured {
            sources: vec!["x".repeat(MAX_FILE_BYTES as usize)],
            execution_targets: vec![],
            consensus_targets: vec![],
            genesis_time: 0,
            seconds_per_slot: 12,
        };
        for _ in 0..3 {
            assert!(recording
                .append(&record)
                .await
                .unwrap_err()
                .to_string()
                .contains("record exceeds file limit"));
        }
        recording.flush().await.unwrap();
        assert_eq!(std::fs::read_dir(&observations).unwrap().count(), 1);
        assert_eq!(std::fs::read_to_string(history).unwrap(), "{}\n");
    }

    #[tokio::test]
    async fn partial_files_are_pruned_oldest_first_to_keep_the_file_count_bounded() {
        let directory = tempfile::tempdir().unwrap();
        let observations = directory.path().join("observations");
        std::fs::create_dir(&observations).unwrap();
        for i in 0..MAX_FILES {
            std::fs::write(observations.join(format!("{i:026}.jsonl")), "{}\n").unwrap();
        }
        let mut recording = Recording::open(directory.path()).await.unwrap();
        assert!(!observations.join(format!("{:026}.jsonl", 0)).exists());
        recording.append(&sample()).await.unwrap();
        recording.flush().await.unwrap();
        assert_eq!(std::fs::read_dir(&observations).unwrap().count(), MAX_FILES);
        for i in 1..MAX_FILES {
            assert_eq!(
                std::fs::read_to_string(observations.join(format!("{i:026}.jsonl"))).unwrap(),
                "{}\n"
            );
        }
    }

    #[tokio::test]
    async fn exact_byte_limit_is_allowed_and_the_next_record_preserves_the_active_file() {
        let directory = tempfile::tempdir().unwrap();
        let observations = directory.path().join("observations");
        std::fs::create_dir(&observations).unwrap();
        let record = sample();
        let size = sonic_rs::to_vec(&record).unwrap().len() as u64 + 1;
        let count = MAX_TOTAL_BYTES / MAX_FILE_BYTES;
        for i in 0..count {
            std::fs::File::create(observations.join(format!("{i:026}.jsonl")))
                .unwrap()
                .set_len(MAX_FILE_BYTES - if i == count - 1 { size } else { 0 })
                .unwrap();
        }
        let mut recording = Recording::open(directory.path()).await.unwrap();
        recording.append(&record).await.unwrap();
        recording.flush().await.unwrap();
        assert!(observations.join(format!("{:026}.jsonl", 0)).exists());
        let total: u64 = std::fs::read_dir(&observations)
            .unwrap()
            .map(|file| file.unwrap().metadata().unwrap().len())
            .sum();
        assert_eq!(total, MAX_TOTAL_BYTES);
        recording.append(&record).await.unwrap();
        recording.flush().await.unwrap();
        assert!(!observations.join(format!("{:026}.jsonl", 0)).exists());
        let newest = std::fs::read_dir(&observations)
            .unwrap()
            .map(|file| file.unwrap().path())
            .max()
            .unwrap();
        let expected = format!("{}\n", sonic_rs::to_string(&record).unwrap());
        assert_eq!(std::fs::read_to_string(newest).unwrap(), expected.repeat(2));
    }
}
