pub mod config;
pub mod client;
pub mod blob_client;
pub mod checkpoint;

use crate::config::AzureEventHubConfig;
use crate::client::EventHubClient;
use crate::blob_client::BlobStorageClient;
use crate::checkpoint::CheckpointManager;
use vector_lib::sinks::util::retry::{ExponentialBackoff, RetryControl, RetryLogic};
use vector_lib::stream::{SourceContext, SourceRun};
use vector_lib::{internal_events::SinkRequestBuildError, Result};
use async_trait::async_trait;
use std::sync::Arc;
use tracing::info;

pub struct AzureEventHubSource {
    event_hub_client: EventHubClient,
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
        let checkpoint_manager = CheckpointManager::new(blob_client);

        Self {
            event_hub_client,
            checkpoint_manager,
            config,
        }
    }

    pub async fn run(&self, ctx: SourceContext) -> Result<()> {
        let starting_position = self
            .checkpoint_manager
            .retrieve_checkpoint()
            .await
            .unwrap_or_else(|_| "beginning".to_string());

        let mut stream = self.event_hub_client.get_events_stream(
            starting_position,
            self.config.batch_size.unwrap_or(100),
            self.config.timeout.unwrap_or(30),
        );

        while let Some(batch) = stream.next().await.transpose()? {
            for event_data in batch.events {
                let event = Event::from(event_data.data);
                ctx.emit(event).await?;
            }

            // Manage checkpoints
            if let Some(last_event) = batch.events.last() {
                self.checkpoint_manager
                    .store_checkpoint(&last_event.offset)
                    .await?;
            }

            // Handle rate limiting
            if batch.rate_limited {
                tokio::time::sleep(Duration::from_secs(1)).await; // Consider exponential backoff
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl SourceRun for AzureEventHubSource {
    async fn run(&mut self, ctx: SourceContext, _topology: Arc<Topology>) -> Result<()> {
        self.run(ctx).await
    }
}

inventory::submit! {
    vector_lib::sinks::util::plugin::Plugin::source(
        "azure_event_hub",
        AzureEventHubConfig::schema(),
        |config, cx| {
            let cfg: AzureEventHubConfig = config.deserialize()?;
            info!("Azure Event Hub plugin registered");
            Ok(Box::new(AzureEventHubSource::new(cfg)) as Box<dyn SourceRun>)
        }
    )
}
