use crate::messages::{EntryType, LogEntry};
use common::{LogIndex, Term};

#[derive(Debug, Clone)]
pub struct RaftLog {
    entries: Vec<LogEntry>,
    base_index: LogIndex,
    base_term: Term,
}

impl RaftLog {
    pub fn new() -> Self {
        RaftLog {
            entries: Vec::new(),
            base_index: 0,
            base_term: 0,
        }
    }

    pub fn last_index(&self) -> LogIndex {
        self.base_index + self.entries.len() as u64
    }

    pub fn last_term(&self) -> Term {
        self.entries.last().map(|e| e.term).unwrap_or(self.base_term)
    }

    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == self.base_index {
            return Some(self.base_term);
        }
        if index < self.base_index || index > self.last_index() {
            return None;
        }
        let pos = (index - self.base_index - 1) as usize;
        self.entries.get(pos).map(|e| e.term)
    }

    pub fn get(&self, index: LogIndex) -> Option<&LogEntry> {
        if index <= self.base_index || index > self.last_index() {
            return None;
        }
        let pos = (index - self.base_index - 1) as usize;
        self.entries.get(pos)
    }

    pub fn append(&mut self, term: Term, data: Vec<u8>, entry_type: EntryType) -> LogIndex {
        let index = self.last_index() + 1;
        self.entries.push(LogEntry { term, index, data, entry_type });
        index
    }

    pub fn append_entry(&mut self, entry: LogEntry) {
        self.entries.push(entry);
    }

    pub fn truncate_from(&mut self, index: LogIndex) {
        if index <= self.base_index {
            self.entries.clear();
            return;
        }
        let pos = (index - self.base_index - 1) as usize;
        self.entries.truncate(pos);
    }

    pub fn entries_from(&self, from_index: LogIndex) -> &[LogEntry] {
        if from_index <= self.base_index {
            return &[];
        }
        if from_index > self.last_index() + 1 {
            return &[];
        }
        let pos = (from_index - self.base_index - 1) as usize;
        if pos >= self.entries.len() {
            return &[];
        }
        &self.entries[pos..]
    }

    pub fn compact_to(&mut self, index: LogIndex, term: Term) {
        if index <= self.base_index {
            return;
        }
        let remove = (index - self.base_index) as usize;
        if remove >= self.entries.len() {
            self.entries.clear();
        } else {
            self.entries.drain(..remove);
        }
        self.base_index = index;
        self.base_term = term;
    }

    pub fn base_index(&self) -> LogIndex { self.base_index }
    pub fn base_term(&self) -> Term { self.base_term }
}

impl Default for RaftLog {
    fn default() -> Self { Self::new() }
}
