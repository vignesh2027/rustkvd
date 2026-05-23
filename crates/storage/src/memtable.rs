use crate::mvcc::MVCCValue;
use crate::wal::WalEntry;
use common::KVPair;
use std::collections::BTreeMap;

pub struct MemTable {
    data: BTreeMap<String, Vec<MVCCValue>>,
    size_bytes: usize,
}

impl MemTable {
    pub fn new() -> Self {
        MemTable {
            data: BTreeMap::new(),
            size_bytes: 0,
        }
    }

    pub fn put(&mut self, key: String, value: Vec<u8>, version: u64, ttl_secs: Option<u64>) {
        let val_len = value.len();
        let key_len = key.len();
        let mv = MVCCValue { value, version, deleted: false, ttl_secs };
        self.data.entry(key).or_default().push(mv);
        self.size_bytes += key_len + val_len + 16;
    }

    pub fn delete(&mut self, key: String, version: u64) -> bool {
        let existed = self.data.contains_key(&key);
        let key_len = key.len();
        let mv = MVCCValue { value: Vec::new(), version, deleted: true, ttl_secs: None };
        self.data.entry(key).or_default().push(mv);
        self.size_bytes += key_len + 16;
        existed
    }

    pub fn get(&self, key: &str, version: Option<u64>) -> Option<MVCCValue> {
        let versions = self.data.get(key)?;
        let target = version.unwrap_or(u64::MAX);
        versions
            .iter()
            .filter(|v| v.version <= target)
            .max_by_key(|v| v.version)
            .cloned()
    }

    pub fn apply_wal_entry(&mut self, entry: WalEntry) {
        match entry {
            WalEntry::Put { key, value, version, ttl_secs } => {
                self.put(key, value, version, ttl_secs);
            }
            WalEntry::Delete { key, version } => {
                self.delete(key, version);
            }
        }
    }

    pub fn scan(&self, start: &str, end: &str, limit: usize) -> Vec<KVPair> {
        self.data
            .range(start.to_string()..end.to_string())
            .filter_map(|(k, versions)| {
                let latest = versions.iter().max_by_key(|v| v.version)?;
                if latest.deleted {
                    return None;
                }
                Some(KVPair {
                    key: k.clone(),
                    value: latest.value.clone(),
                    version: latest.version,
                })
            })
            .take(limit)
            .collect()
    }

    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Vec<MVCCValue>)> {
        self.data.iter()
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}
