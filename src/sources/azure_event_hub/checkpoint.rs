use super::blob_client::BlobStorageClient;
use serde::{Serialize, Deserialize};
use std::error::Error;

#[derive(Debug)]
pub enum CheckpointError {
    BlobError(String),
    SerializationError(String),
    Other(String),
}

impl std::fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlobError(msg) => write!(f, "Blob error: {}", msg),
            Self::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            Self::Other(msg) => write!(f, "Other error: {}", msg),
        }
    }
}

impl Error for CheckpointError {}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CheckpointManager {
    pub blob_client: BlobStorageClient,
}

impl CheckpointManager {
    pub fn new(blob_client: BlobStorageClient) -> Self {
        Self {
            blob_client,
        }
    }

    pub async fn store_checkpoint(&self, offset: &str) -> Result<(), CheckpointError> {
        self.blob_client.upload_checkpoint(offset)
            .await
            .map_err(|e| CheckpointError::BlobError(e.to_string()))
    }
}