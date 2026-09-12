//! Schema-2 accounting for physical relay transport attempts.
//!
//! This module has no billing API dependency. It records what entered the
//! transport and leaves tariff values to an explicit, static configuration.
use serde::Serialize;
use std::time::SystemTime;

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Lifecycle {
    Queued,
    Shared,
    Dispatched,
    Completed,
    TimedOut,
    Cancelled,
}

#[derive(Clone, Debug, Serialize)]
pub struct TransportRecord {
    pub schema: u32,
    pub logical_request_id: u64,
    pub physical_attempt_id: u64,
    pub lifecycle: Lifecycle,
    pub method: String,
    pub purpose: String,
    pub resource: String,
    pub endpoint_identity: String,
    pub mode: String,
    pub mode_epoch: u64,
    pub acquired_block: Option<String>,
    pub request_bytes: u64,
    pub response_bytes: u64,
    pub started_unix_us: u64,
    pub dispatch_unix_us: Option<u64>,
    pub completed_unix_us: Option<u64>,
    pub http_status: Option<u16>,
    pub error_class: Option<String>,
    pub retry_reason: Option<String>,
    pub entered_transport: bool,
}

impl TransportRecord {
    pub const SCHEMA: u32 = 2;

    pub fn new(logical_request_id: u64, physical_attempt_id: u64, method: &str) -> Self {
        Self {
            schema: Self::SCHEMA,
            logical_request_id,
            physical_attempt_id,
            lifecycle: Lifecycle::Queued,
            method: method.to_owned(),
            purpose: String::new(),
            resource: String::new(),
            endpoint_identity: String::new(),
            mode: String::new(),
            mode_epoch: 0,
            acquired_block: None,
            request_bytes: 0,
            response_bytes: 0,
            started_unix_us: unix_us(),
            dispatch_unix_us: None,
            completed_unix_us: None,
            http_status: None,
            error_class: None,
            retry_reason: None,
            entered_transport: false,
        }
    }

    pub fn dispatched(&mut self) {
        self.lifecycle = Lifecycle::Dispatched;
        self.entered_transport = true;
        self.dispatch_unix_us = Some(unix_us());
    }

    pub fn completed(&mut self, status: Option<u16>, response_bytes: u64) {
        self.lifecycle = Lifecycle::Completed;
        self.http_status = status;
        self.response_bytes = response_bytes;
        self.completed_unix_us = Some(unix_us());
    }

    pub fn failed(&mut self, timed_out: bool, error_class: &str) {
        self.lifecycle = if timed_out {
            Lifecycle::TimedOut
        } else {
            Lifecycle::Cancelled
        };
        self.error_class = Some(error_class.to_owned());
        self.completed_unix_us = Some(unix_us());
    }
}

fn unix_us() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_only_dispatched_attempts_as_transport_entries() {
        let mut queued = TransportRecord::new(1, 1, "GET");
        assert!(!queued.entered_transport);
        queued.lifecycle = Lifecycle::Shared;
        assert_eq!(queued.lifecycle, Lifecycle::Shared);

        let mut dispatched = TransportRecord::new(2, 2, "GET");
        dispatched.dispatched();
        dispatched.completed(Some(200), 12);
        assert!(dispatched.entered_transport);
        assert_eq!(dispatched.lifecycle, Lifecycle::Completed);
        assert_eq!(dispatched.response_bytes, 12);
    }
}
