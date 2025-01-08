pub mod config;
pub mod client;
pub mod blob_client;
pub mod checkpoint;

use crate::sources::azure_event_hub::config::AzureEventHubConfig;
use crate::sources::azure_event_hub::client::EventHubClient;
use crate::sources::azure_event_hub::blob_client::BlobStorageClient;
use crate::sources::azure_event_hub::checkpoint::CheckpointManager;
use async_trait::async_trait;
use std::sync::Arc;

#[async_trait]
pub trait SourceRun {
    async fn run(&mut self, topology: Arc<()>) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>;
}

/// Custom error type for AzureEventHubSource
#[derive(Debug)]
pub enum AzureEventHubError {
    RequestBuildError(String),
    RetryError(String),
    Other(String),
}

impl std::fmt::Display for AzureEventHubError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RequestBuildError(msg) => write!(f, "Request build error: {}", msg),
            Self::RetryError(msg) => write!(f, "Retry error: {}", msg),
            Self::Other(msg) => write!(f, "Other error: {}", msg),
        }
    }
}

impl std::error::Error for AzureEventHubError {}

#[allow(dead_code)] // Temporarily allow dead code while implementing
pub struct AzureEventHubSource {
    event_hub_client: EventHubClient,
    blob_client: BlobStorageClient,
    checkpoint_manager: CheckpointManager,
    config: AzureEventHubConfig,
}

impl AzureEventHubSource {
    pub fn new(config: AzureEventHubConfig) -> Self {
        let event_hub_client = EventHubClient::new(config.connection_string.clone());
        let blob_client = BlobStorageClient::new(
            config.blob_storage_account.clone(),
            config.blob_storage_key.clone(),
            config.blob_storage_url.clone(),
        );
        let checkpoint_manager = CheckpointManager::new(blob_client.clone());

        Self {
            event_hub_client,
            blob_client,
            checkpoint_manager,
            config,
        }
    }
}

#[async_trait]
impl SourceRun for AzureEventHubSource {
    async fn run(&mut self, _topology: Arc<()>) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        // Implementation of the run method
        // Replace with actual logic
        Ok(())
    }
}

// Plugin registration has been removed due to unresolved `vector_lib::plugin`.
// Ensure to register the plugin using the appropriate method as per your project's structure.

