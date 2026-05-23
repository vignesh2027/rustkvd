use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Default, Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

#[derive(Debug, Default)]
struct MetricsInner {
    pub reads: AtomicU64,
    pub writes: AtomicU64,
    pub deletes: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub raft_elections: AtomicU64,
    pub raft_log_appends: AtomicU64,
    pub compactions: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc_reads(&self) { self.inner.reads.fetch_add(1, Ordering::Relaxed); }
    pub fn inc_writes(&self) { self.inner.writes.fetch_add(1, Ordering::Relaxed); }
    pub fn inc_deletes(&self) { self.inner.deletes.fetch_add(1, Ordering::Relaxed); }
    pub fn inc_cache_hits(&self) { self.inner.cache_hits.fetch_add(1, Ordering::Relaxed); }
    pub fn inc_cache_misses(&self) { self.inner.cache_misses.fetch_add(1, Ordering::Relaxed); }
    pub fn inc_raft_elections(&self) { self.inner.raft_elections.fetch_add(1, Ordering::Relaxed); }
    pub fn inc_raft_log_appends(&self) { self.inner.raft_log_appends.fetch_add(1, Ordering::Relaxed); }
    pub fn inc_compactions(&self) { self.inner.compactions.fetch_add(1, Ordering::Relaxed); }

    pub fn reads(&self) -> u64 { self.inner.reads.load(Ordering::Relaxed) }
    pub fn writes(&self) -> u64 { self.inner.writes.load(Ordering::Relaxed) }
    pub fn deletes(&self) -> u64 { self.inner.deletes.load(Ordering::Relaxed) }
    pub fn cache_hits(&self) -> u64 { self.inner.cache_hits.load(Ordering::Relaxed) }
    pub fn cache_misses(&self) -> u64 { self.inner.cache_misses.load(Ordering::Relaxed) }
    pub fn raft_elections(&self) -> u64 { self.inner.raft_elections.load(Ordering::Relaxed) }
    pub fn compactions(&self) -> u64 { self.inner.compactions.load(Ordering::Relaxed) }
}
