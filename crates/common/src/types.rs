use std::fmt;
use serde::{Deserialize, Serialize};

pub type Key = String;
pub type Value = bytes::Bytes;
pub type Term = u64;
pub type LogIndex = u64;
pub type Version = u64;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct NodeId(pub String);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Self {
        NodeId(id.into())
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for NodeId {
    fn from(s: String) -> Self {
        NodeId(s)
    }
}

impl From<&str> for NodeId {
    fn from(s: &str) -> Self {
        NodeId(s.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyRange {
    pub start: Key,
    pub end: Key,
}

impl KeyRange {
    pub fn new(start: impl Into<Key>, end: impl Into<Key>) -> Self {
        KeyRange { start: start.into(), end: end.into() }
    }

    pub fn contains(&self, key: &str) -> bool {
        key >= self.start.as_str() && key < self.end.as_str()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KVPair {
    pub key: Key,
    pub value: Vec<u8>,
    pub version: Version,
}
