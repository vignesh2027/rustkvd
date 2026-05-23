use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MVCCValue {
    pub value: Vec<u8>,
    pub version: u64,
    pub deleted: bool,
    pub ttl_secs: Option<u64>,
}

pub struct MVCCStore {
    version_counter: AtomicU64,
}

impl MVCCStore {
    pub fn new() -> Self {
        MVCCStore {
            version_counter: AtomicU64::new(1),
        }
    }

    pub fn next_version(&self) -> u64 {
        self.version_counter.fetch_add(1, Ordering::SeqCst)
    }

    pub fn current_version(&self) -> u64 {
        self.version_counter.load(Ordering::SeqCst)
    }
}

impl Default for MVCCStore {
    fn default() -> Self {
        Self::new()
    }
}
