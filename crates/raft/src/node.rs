use crate::log::RaftLog;
use common::{LogIndex, NodeId, Term};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

impl std::fmt::Display for RaftRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RaftRole::Follower => write!(f, "follower"),
            RaftRole::Candidate => write!(f, "candidate"),
            RaftRole::Leader => write!(f, "leader"),
        }
    }
}

pub struct RaftNode {
    pub id: NodeId,
    pub peers: Vec<NodeId>,
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub log: RaftLog,
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
    pub role: RaftRole,
    pub leader_id: Option<NodeId>,
    pub votes_received: HashSet<NodeId>,
    pub next_index: HashMap<NodeId, LogIndex>,
    pub match_index: HashMap<NodeId, LogIndex>,
    pub election_timeout: Duration,
    pub heartbeat_interval: Duration,
}

impl RaftNode {
    pub fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        let mut node = RaftNode {
            id,
            peers,
            current_term: 0,
            voted_for: None,
            log: RaftLog::new(),
            commit_index: 0,
            last_applied: 0,
            role: RaftRole::Follower,
            leader_id: None,
            votes_received: HashSet::new(),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            election_timeout: Duration::from_millis(150),
            heartbeat_interval: Duration::from_millis(50),
        };
        node.reset_election_timeout();
        node
    }

    pub fn is_leader(&self) -> bool {
        self.role == RaftRole::Leader
    }

    pub fn role_str(&self) -> String {
        self.role.to_string()
    }
}
