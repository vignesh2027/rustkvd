pub mod bloom_filter;
pub mod compaction;
pub mod memtable;
pub mod mvcc;
pub mod sstable;
pub mod wal;

pub use memtable::MemTable;
pub use mvcc::{MVCCStore, MVCCValue};
pub use wal::WriteAheadLog;

use common::StorageError;
use parking_lot::RwLock;
use std::path::PathBuf;
use std::sync::Arc;

pub struct StorageEngine {
    memtable: Arc<RwLock<MemTable>>,
    wal: Arc<RwLock<WriteAheadLog>>,
    mvcc: Arc<MVCCStore>,
    data_dir: PathBuf,
}

impl StorageEngine {
    pub fn open(data_dir: PathBuf) -> Result<Self, StorageError> {
        std::fs::create_dir_all(&data_dir)?;
        let wal_path = data_dir.join("wal.log");
        let mut wal = WriteAheadLog::open(wal_path)?;
        let mut memtable = MemTable::new();
        // Replay WAL
        for entry in wal.replay()? {
            memtable.apply_wal_entry(entry);
        }
        Ok(StorageEngine {
            memtable: Arc::new(RwLock::new(memtable)),
            wal: Arc::new(RwLock::new(wal)),
            mvcc: Arc::new(MVCCStore::new()),
            data_dir,
        })
    }

    pub fn get(&self, key: &str, version: Option<u64>) -> Result<Option<MVCCValue>, StorageError> {
        let memtable = self.memtable.read();
        Ok(memtable.get(key, version))
    }

    pub fn put(&self, key: String, value: bytes::Bytes, ttl_secs: Option<u64>) -> Result<u64, StorageError> {
        let version = self.mvcc.next_version();
        let value_vec = value.to_vec();
        let entry = wal::WalEntry::Put {
            key: key.clone(),
            value: value_vec.clone(),
            version,
            ttl_secs,
        };
        self.wal.write().append(&entry)?;
        self.memtable.write().put(key, value_vec, version, ttl_secs);
        Ok(version)
    }

    pub fn delete(&self, key: String) -> Result<bool, StorageError> {
        let version = self.mvcc.next_version();
        let entry = wal::WalEntry::Delete { key: key.clone(), version };
        self.wal.write().append(&entry)?;
        let existed = self.memtable.write().delete(key, version);
        Ok(existed)
    }

    pub fn scan(&self, start: &str, end: &str, limit: usize) -> Vec<common::KVPair> {
        self.memtable.read().scan(start, end, limit)
    }
}
