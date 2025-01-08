use serde::{Deserialize, Serialize};
use std::error::Error;
use hyper::{Body, Request};

#[derive(Debug)]
pub enum BlobClientError {
    ParseError(String),
    HttpError(String),
    Other(String),
}

impl std::fmt::Display for BlobClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ParseError(msg) => write!(f, "Parse error: {}", msg),
            Self::HttpError(msg) => write!(f, "HTTP error: {}", msg),
            Self::Other(msg) => write!(f, "Other error: {}", msg),
        }
    }
}

impl Error for BlobClientError {}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BlobStorageClient {
    pub blob_storage_account: String,
    pub blob_storage_key: String,
    pub blob_storage_url: String,
}

impl BlobStorageClient {
    pub fn new(account: String, key: String, url: String) -> Self {
        Self {
            blob_storage_account: account,
            blob_storage_key: key,
            blob_storage_url: url,
        }
    }

    pub async fn upload_checkpoint(&self, checkpoint: &str) -> Result<(), BlobClientError> {
        let url = format!("{}/checkpoints", self.blob_storage_url);
        let client = hyper::Client::new();

        let request = Request::post(&url)
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::json!({ "checkpoint": checkpoint }).to_string()))
            .map_err(|e| BlobClientError::HttpError(e.to_string()))?;

        let response = client.request(request)
            .await
            .map_err(|e| BlobClientError::HttpError(e.to_string()))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(BlobClientError::HttpError(response.status().to_string()))
        }
    }
}

