use crate::messages::{RequestVoteRequest, RequestVoteResponse};
use crate::node::{RaftNode, RaftRole};
use common::{NodeId, Term};
use std::collections::HashSet;

impl RaftNode {
    pub fn start_election(&mut self) {
        self.current_term += 1;
        self.role = RaftRole::Candidate;
        self.voted_for = Some(self.id.clone());
        self.votes_received = HashSet::new();
        self.votes_received.insert(self.id.clone());
        tracing::info!(term = self.current_term, id = %self.id, "Starting election");
    }

    pub fn build_vote_request(&self) -> RequestVoteRequest {
        RequestVoteRequest {
            term: self.current_term,
            candidate_id: self.id.clone(),
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
        }
    }

    pub fn handle_vote_request(&mut self, req: RequestVoteRequest) -> RequestVoteResponse {
        if req.term < self.current_term {
            return RequestVoteResponse {
                term: self.current_term,
                vote_granted: false,
                from: self.id.clone(),
            };
        }

        if req.term > self.current_term {
            self.become_follower(req.term);
        }

        let can_vote = self.voted_for.is_none()
            || self.voted_for.as_ref() == Some(&req.candidate_id);

        let log_ok = req.last_log_term > self.log.last_term()
            || (req.last_log_term == self.log.last_term()
                && req.last_log_index >= self.log.last_index());

        let vote_granted = can_vote && log_ok;
        if vote_granted {
            self.voted_for = Some(req.candidate_id.clone());
            self.reset_election_timeout();
        }

        RequestVoteResponse {
            term: self.current_term,
            vote_granted,
            from: self.id.clone(),
        }
    }

    pub fn handle_vote_response(&mut self, resp: RequestVoteResponse) -> bool {
        if resp.term > self.current_term {
            self.become_follower(resp.term);
            return false;
        }
        if self.role != RaftRole::Candidate || resp.term != self.current_term {
            return false;
        }
        if resp.vote_granted {
            self.votes_received.insert(resp.from);
            let quorum = (self.peers.len() + 1) / 2 + 1;
            if self.votes_received.len() >= quorum {
                self.become_leader();
                return true;
            }
        }
        false
    }

    pub fn become_leader(&mut self) {
        self.role = RaftRole::Leader;
        self.leader_id = Some(self.id.clone());
        let next_idx = self.log.last_index() + 1;
        for peer in &self.peers {
            self.next_index.insert(peer.clone(), next_idx);
            self.match_index.insert(peer.clone(), 0);
        }
        tracing::info!(term = self.current_term, id = %self.id, "Became leader");
    }

    pub fn become_follower(&mut self, term: Term) {
        self.current_term = term;
        self.role = RaftRole::Follower;
        self.voted_for = None;
        self.votes_received.clear();
        self.reset_election_timeout();
        tracing::debug!(term, id = %self.id, "Became follower");
    }

    pub fn reset_election_timeout(&mut self) {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        self.election_timeout = std::time::Duration::from_millis(rng.gen_range(150..300));
    }
}
