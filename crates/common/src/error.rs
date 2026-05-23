use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Corruption: {0}")]
    Corruption(String),
    #[error("Key not found: {0}")]
    KeyNotFound(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
}

#[derive(Debug, Error)]
pub enum KVError {
    #[error("Key not found: {0}")]
    KeyNotFound(String),

    #[error("Not leader, redirect to: {leader_hint:?}")]
    NotLeader { leader_hint: Option<String> },

    #[error("Raft error: {0}")]
    RaftError(String),

    #[error("Storage error: {0}")]
    StorageError(#[from] StorageError),

    #[error("Network error: {0}")]
    NetworkError(#[from] tonic::Status),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Cluster not ready: quorum unavailable")]
    NoQuorum,

    #[error("Timeout")]
    Timeout,

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
}

impl From<bincode::Error> for KVError {
    fn from(e: bincode::Error) -> Self {
        KVError::SerializationError(e.to_string())
    }
}

impl From<KVError> for tonic::Status {
    fn from(e: KVError) -> Self {
        match e {
            KVError::KeyNotFound(k) => tonic::Status::not_found(k),
            KVError::NotLeader { leader_hint } => {
                let msg = format!("not_leader:{}", leader_hint.unwrap_or_default());
                tonic::Status::failed_precondition(msg)
            }
            KVError::NoQuorum => tonic::Status::unavailable("no quorum"),
            KVError::Timeout => tonic::Status::deadline_exceeded("timeout"),
            KVError::InvalidArgument(m) => tonic::Status::invalid_argument(m),
            e => tonic::Status::internal(e.to_string()),
        }
    }
}
