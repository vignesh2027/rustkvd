use crate::messages::{AppendEntriesRequest, AppendEntriesResponse, EntryType};
use crate::node::{RaftNode, RaftRole};
use common::{LogIndex, NodeId};

impl RaftNode {
    pub fn build_append_entries(&self, peer: &NodeId) -> AppendEntriesRequest {
        let next_idx = self.next_index.get(peer).copied().unwrap_or(1);
        let prev_log_index = next_idx - 1;
        let prev_log_term = self.log.term_at(prev_log_index).unwrap_or(0);
        let entries = self.log.entries_from(next_idx).to_vec();

        AppendEntriesRequest {
            term: self.current_term,
            leader_id: self.id.clone(),
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.commit_index,
        }
    }

    pub fn handle_append_entries(&mut self, req: AppendEntriesRequest) -> AppendEntriesResponse {
        if req.term < self.current_term {
            return AppendEntriesResponse {
                term: self.current_term,
                success: false,
                match_index: 0,
                from: self.id.clone(),
            };
        }

        if req.term > self.current_term {
            self.become_follower(req.term);
        }
        self.role = RaftRole::Follower;
        self.leader_id = Some(req.leader_id.clone());
        self.reset_election_timeout();

        if req.prev_log_index > 0 {
            match self.log.term_at(req.prev_log_index) {
                None => {
                    return AppendEntriesResponse {
                        term: self.current_term,
                        success: false,
                        match_index: self.log.last_index(),
                        from: self.id.clone(),
                    };
                }
                Some(t) if t != req.prev_log_term => {
                    self.log.truncate_from(req.prev_log_index);
                    return AppendEntriesResponse {
                        term: self.current_term,
                        success: false,
                        match_index: req.prev_log_index.saturating_sub(1),
                        from: self.id.clone(),
                    };
                }
                _ => {}
            }
        }

        let mut insert_idx = req.prev_log_index + 1;
        for entry in req.entries {
            if let Some(existing_term) = self.log.term_at(insert_idx) {
                if existing_term != entry.term {
                    self.log.truncate_from(insert_idx);
                    self.log.append_entry(entry);
                }
            } else {
                self.log.append_entry(entry);
            }
            insert_idx += 1;
        }

        if req.leader_commit > self.commit_index {
            self.commit_index = req.leader_commit.min(self.log.last_index());
        }

        AppendEntriesResponse {
            term: self.current_term,
            success: true,
            match_index: self.log.last_index(),
            from: self.id.clone(),
        }
    }

    pub fn handle_append_response(&mut self, peer: NodeId, resp: AppendEntriesResponse) {
        if resp.term > self.current_term {
            self.become_follower(resp.term);
            return;
        }
        if self.role != RaftRole::Leader {
            return;
        }
        if resp.success {
            let match_idx = resp.match_index;
            self.match_index.insert(peer.clone(), match_idx);
            self.next_index.insert(peer, match_idx + 1);
            self.advance_commit_index();
        } else {
            let next = self.next_index.get(&peer).copied().unwrap_or(1);
            self.next_index.insert(peer, (next - 1).max(1));
        }
    }

    fn advance_commit_index(&mut self) {
        let mut indices: Vec<LogIndex> = self.match_index.values().copied().collect();
        indices.push(self.log.last_index());
        indices.sort_unstable();

        let quorum = (self.peers.len() + 1) / 2 + 1;
        if let Some(&n) = indices.get(indices.len().saturating_sub(quorum)) {
            if n > self.commit_index {
                if let Some(t) = self.log.term_at(n) {
                    if t == self.current_term {
                        self.commit_index = n;
                    }
                }
            }
        }
    }

    pub fn propose(&mut self, data: Vec<u8>) -> LogIndex {
        self.log.append(self.current_term, data, EntryType::Normal)
    }

    pub fn apply_committed(&mut self) -> Vec<(LogIndex, Vec<u8>)> {
        let mut applied = Vec::new();
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            if let Some(entry) = self.log.get(self.last_applied) {
                applied.push((entry.index, entry.data.clone()));
            }
        }
        applied
    }
}
