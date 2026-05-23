use crate::bloom_filter::BloomFilter;
use crate::mvcc::MVCCValue;
use common::StorageError;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableEntry {
    pub key: String,
    pub value: MVCCValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexEntry {
    first_key: String,
    offset: u64,
    length: u64,
}

pub struct SSTableWriter {
    path: PathBuf,
    entries: Vec<SSTableEntry>,
}

impl SSTableWriter {
    pub fn new(path: PathBuf) -> Self {
        SSTableWriter { path, entries: Vec::new() }
    }

    pub fn add(&mut self, key: String, value: MVCCValue) {
        self.entries.push(SSTableEntry { key, value });
    }

    pub fn finish(mut self) -> Result<SSTable, StorageError> {
        self.entries.sort_by(|a, b| a.key.cmp(&b.key));
        let item_count = self.entries.len().max(1);
        let mut bloom = BloomFilter::new(item_count, 0.01);
        for entry in &self.entries {
            bloom.insert(entry.key.as_bytes());
        }

        let mut writer = BufWriter::new(File::create(&self.path)?);
        let mut index: Vec<IndexEntry> = Vec::new();
        let mut offset = 0u64;

        // Write entries in chunks of 64
        for chunk in self.entries.chunks(64) {
            let block_data = bincode::serialize(chunk)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            let len = block_data.len() as u64;
            let first_key = chunk[0].key.clone();
            index.push(IndexEntry { first_key, offset, length: len });
            writer.write_all(&block_data)?;
            offset += len;
        }

        // Write index
        let index_data = bincode::serialize(&index)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        let index_offset = offset;
        writer.write_all(&index_data)?;

        // Write bloom filter
        let bloom_bytes = bloom.to_bytes();
        let bloom_offset = index_offset + index_data.len() as u64;
        writer.write_all(&bloom_bytes)?;

        // Write footer: index_offset(8) + index_len(8) + bloom_offset(8) + bloom_len(8)
        writer.write_all(&index_offset.to_le_bytes())?;
        writer.write_all(&(index_data.len() as u64).to_le_bytes())?;
        writer.write_all(&bloom_offset.to_le_bytes())?;
        writer.write_all(&(bloom_bytes.len() as u64).to_le_bytes())?;
        writer.flush()?;

        SSTable::open(self.path)
    }
}

pub struct SSTable {
    path: PathBuf,
    index: Vec<IndexEntry>,
    bloom: BloomFilter,
    min_key: String,
    max_key: String,
}

impl SSTable {
    pub fn open(path: PathBuf) -> Result<Self, StorageError> {
        let mut file = File::open(&path)?;
        let file_len = file.metadata()?.len();

        // Read footer (last 32 bytes)
        if file_len < 32 {
            return Err(StorageError::Corruption("SSTable too small".into()));
        }
        file.seek(SeekFrom::End(-32))?;
        let mut footer = [0u8; 32];
        file.read_exact(&mut footer)?;

        let index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let index_len = u64::from_le_bytes(footer[8..16].try_into().unwrap());
        let bloom_offset = u64::from_le_bytes(footer[16..24].try_into().unwrap());
        let bloom_len = u64::from_le_bytes(footer[24..32].try_into().unwrap());

        // Read index
        file.seek(SeekFrom::Start(index_offset))?;
        let mut index_data = vec![0u8; index_len as usize];
        file.read_exact(&mut index_data)?;
        let index: Vec<IndexEntry> = bincode::deserialize(&index_data)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        // Read bloom
        file.seek(SeekFrom::Start(bloom_offset))?;
        let mut bloom_data = vec![0u8; bloom_len as usize];
        file.read_exact(&mut bloom_data)?;
        let bloom = BloomFilter::from_bytes(&bloom_data)
            .ok_or_else(|| StorageError::Corruption("Invalid bloom filter".into()))?;

        let min_key = index.first().map(|e| e.first_key.clone()).unwrap_or_default();
        let max_key = index.last().map(|e| e.first_key.clone()).unwrap_or_default();

        Ok(SSTable { path, index, bloom, min_key, max_key })
    }

    pub fn get(&self, key: &str) -> Result<Option<MVCCValue>, StorageError> {
        if !self.bloom.may_contain(key.as_bytes()) {
            return Ok(None);
        }

        // Binary search index for correct block
        let idx = self.index.partition_point(|e| e.first_key.as_str() <= key);
        if idx == 0 {
            return Ok(None);
        }
        let block_idx = idx - 1;
        let entry = &self.index[block_idx];

        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(entry.offset))?;
        let mut block_data = vec![0u8; entry.length as usize];
        file.read_exact(&mut block_data)?;

        let entries: Vec<SSTableEntry> = bincode::deserialize(&block_data)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        // Find key in block
        Ok(entries.into_iter().find(|e| e.key == key).map(|e| e.value))
    }

    pub fn min_key(&self) -> &str { &self.min_key }
    pub fn max_key(&self) -> &str { &self.max_key }
    pub fn path(&self) -> &PathBuf { &self.path }

    pub fn iter_entries(&self) -> Result<Vec<SSTableEntry>, StorageError> {
        let mut all = Vec::new();
        let mut file = File::open(&self.path)?;
        for entry in &self.index {
            file.seek(SeekFrom::Start(entry.offset))?;
            let mut block_data = vec![0u8; entry.length as usize];
            file.read_exact(&mut block_data)?;
            let entries: Vec<SSTableEntry> = bincode::deserialize(&block_data)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            all.extend(entries);
        }
        Ok(all)
    }
}
