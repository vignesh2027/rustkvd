use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct BloomFilter {
    bits: Vec<u64>,
    num_bits: usize,
    num_hashes: usize,
}

impl BloomFilter {
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        let num_bits = Self::optimal_bits(expected_items, false_positive_rate);
        let num_hashes = Self::optimal_hashes(num_bits, expected_items);
        let words = (num_bits + 63) / 64;
        BloomFilter {
            bits: vec![0u64; words],
            num_bits,
            num_hashes,
        }
    }

    fn optimal_bits(n: usize, p: f64) -> usize {
        let m = -(n as f64 * p.ln()) / (2f64.ln().powi(2));
        (m as usize).max(64)
    }

    fn optimal_hashes(m: usize, n: usize) -> usize {
        if n == 0 {
            return 1;
        }
        let k = (m as f64 / n as f64 * 2f64.ln()) as usize;
        k.clamp(1, 10)
    }

    fn hashes(&self, key: &[u8]) -> Vec<usize> {
        (0..self.num_hashes)
            .map(|i| {
                let mut h = DefaultHasher::new();
                i.hash(&mut h);
                key.hash(&mut h);
                (h.finish() as usize) % self.num_bits
            })
            .collect()
    }

    pub fn insert(&mut self, key: &[u8]) {
        for bit in self.hashes(key) {
            self.bits[bit / 64] |= 1u64 << (bit % 64);
        }
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.hashes(key)
            .iter()
            .all(|&bit| (self.bits[bit / 64] >> (bit % 64)) & 1 == 1)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(16 + self.bits.len() * 8);
        bytes.extend_from_slice(&(self.num_bits as u64).to_le_bytes());
        bytes.extend_from_slice(&(self.num_hashes as u64).to_le_bytes());
        for &word in &self.bits {
            bytes.extend_from_slice(&word.to_le_bytes());
        }
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 16 {
            return None;
        }
        let num_bits = u64::from_le_bytes(bytes[0..8].try_into().ok()?) as usize;
        let num_hashes = u64::from_le_bytes(bytes[8..16].try_into().ok()?) as usize;
        let words = (num_bits + 63) / 64;
        if bytes.len() < 16 + words * 8 {
            return None;
        }
        let mut bits = vec![0u64; words];
        for (i, chunk) in bytes[16..16 + words * 8].chunks(8).enumerate() {
            bits[i] = u64::from_le_bytes(chunk.try_into().ok()?);
        }
        Some(BloomFilter { bits, num_bits, num_hashes })
    }
}
