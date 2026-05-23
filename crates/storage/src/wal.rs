use bytes::{BufMut, BytesMut};
use common::StorageError;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEntry {
    Put {
        key: String,
        value: Vec<u8>,
        version: u64,
        ttl_secs: Option<u64>,
    },
    Delete {
        key: String,
        version: u64,
    },
}

pub struct WriteAheadLog {
    path: PathBuf,
    file: File,
}

impl WriteAheadLog {
    pub fn open(path: PathBuf) -> Result<Self, StorageError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        Ok(WriteAheadLog { path, file })
    }

    pub fn append(&mut self, entry: &WalEntry) -> Result<(), StorageError> {
        let data = bincode::serialize(entry)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        let checksum = crc32fast::hash(&data);
        let len = data.len() as u32;

        let mut buf = BytesMut::with_capacity(8 + data.len());
        buf.put_u32(checksum);
        buf.put_u32(len);
        buf.extend_from_slice(&data);

        self.file.write_all(&buf)?;
        self.file.sync_data()?;
        Ok(())
    }

    pub fn replay(&mut self) -> Result<Vec<WalEntry>, StorageError> {
        let mut entries = Vec::new();
        let mut file = BufReader::new(File::open(&self.path)?);
        let mut header = [0u8; 8];

        loop {
            match file.read_exact(&mut header) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(StorageError::Io(e)),
            }

            let checksum = u32::from_be_bytes(header[0..4].try_into().unwrap());
            let len = u32::from_be_bytes(header[4..8].try_into().unwrap()) as usize;

            let mut data = vec![0u8; len];
            file.read_exact(&mut data)?;

            let actual_checksum = crc32fast::hash(&data);
            if actual_checksum != checksum {
                return Err(StorageError::Corruption(format!(
                    "WAL checksum mismatch: expected {}, got {}",
                    checksum, actual_checksum
                )));
            }

            let entry: WalEntry = bincode::deserialize(&data)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            entries.push(entry);
        }

        Ok(entries)
    }
}
