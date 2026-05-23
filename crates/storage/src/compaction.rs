use crate::mvcc::MVCCValue;
use crate::sstable::{SSTable, SSTableWriter};
use common::StorageError;
use std::collections::BTreeMap;
use std::path::PathBuf;

pub struct CompactionEngine {
    data_dir: PathBuf,
    level0: Vec<SSTable>,
    level1: Vec<SSTable>,
    next_seq: u64,
}

impl CompactionEngine {
    pub fn new(data_dir: PathBuf) -> Result<Self, StorageError> {
        std::fs::create_dir_all(&data_dir)?;
        Ok(CompactionEngine {
            data_dir,
            level0: Vec::new(),
            level1: Vec::new(),
            next_seq: 0,
        })
    }

    pub fn add_to_level0(&mut self, table: SSTable) {
        self.level0.push(table);
    }

    pub fn needs_compaction(&self) -> bool {
        self.level0.len() >= 4
    }

    pub fn compact(&mut self) -> Result<(), StorageError> {
        if self.level0.is_empty() {
            return Ok(());
        }

        // Merge all level0 tables into sorted output
        let mut merged: BTreeMap<String, (String, MVCCValue)> = BTreeMap::new();

        for table in &self.level0 {
            for entry in table.iter_entries()? {
                let e = merged.entry(entry.key.clone()).or_insert_with(|| (entry.key.clone(), entry.value.clone()));
                if entry.value.version > e.1.version {
                    *e = (entry.key, entry.value);
                }
            }
        }

        // Also merge existing level1 tables
        for table in &self.level1 {
            for entry in table.iter_entries()? {
                let e = merged.entry(entry.key.clone()).or_insert_with(|| (entry.key.clone(), entry.value.clone()));
                if entry.value.version > e.1.version {
                    *e = (entry.key, entry.value);
                }
            }
        }

        let seq = self.next_seq;
        self.next_seq += 1;
        let new_path = self.data_dir.join(format!("1_{:08}.sst", seq));
        let mut writer = SSTableWriter::new(new_path);
        for (_, (key, value)) in merged {
            writer.add(key, value);
        }
        let new_table = writer.finish()?;

        // Remove old files
        for table in self.level0.drain(..) {
            let _ = std::fs::remove_file(table.path());
        }
        for table in self.level1.drain(..) {
            let _ = std::fs::remove_file(table.path());
        }

        self.level1.push(new_table);
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<MVCCValue>, StorageError> {
        // Search level0 first (newest)
        for table in self.level0.iter().rev() {
            if let Some(v) = table.get(key)? {
                return Ok(Some(v));
            }
        }
        // Then level1
        for table in &self.level1 {
            if let Some(v) = table.get(key)? {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }
}
