use vector_lib::configurable::configurable_component;
use crate::config::{SourceConfig, SourceContext, SourceOutput, DataType};
use vector_lib::source::Source;
use vector_lib::schema::Definition;
use vector_lib::config::LogNamespace;
use std::collections::BTreeSet;
use anyhow::Result;
use std::sync::Arc;
use tracing::{info, error};

// Add these imports
use super::eventhub::{EventHubConnection, EventHubConfig};
use super::checkpoint::CheckpointStore;
use super::processor::{EventHubConsumer, ConsumerConfig};
use super::processor_pipeline::{ProcessingPipeline, ProcessingConfig};

/// Configuration for the Azure Event Hub source.
#[derive(Clone, Debug, Default)]
#[configurable_component(source("azure_event_hub", "Collect logs from Azure Event Hub."))]
pub struct AzureEventHubConfig {
    /// Event Hub configuration.
    #[configurable(metadata(docs::description = "Event Hub configuration."))]
    pub event_hub: EventHubSettings,

    /// Processing configuration.
    #[configurable(metadata(docs::description = "Processing configuration."))]
    pub processing: ProcessingSettings,

    /// Checkpointing configuration.
    #[configurable(metadata(docs::description = "Checkpoint configuration."))]
    pub checkpointing: CheckpointSettings,
}

/// Configuration for connecting to Azure Event Hub.
#[derive(Clone, Debug, Default)]
#[configurable_component]
pub struct EventHubSettings {
    /// The fully qualified namespace of the Event Hub instance.
    #[configurable(metadata(docs::description = "The fully qualified namespace of the Event Hub instance."))]
    pub fully_qualified_namespace: String,

    /// The name of the Event Hub instance.
    #[configurable(metadata(docs::description = "The name of the Event Hub instance."))]
    pub event_hub_name: String,

    /// The consumer group to use.
    #[configurable(metadata(docs::description = "The consumer group to use. Defaults to '$Default'."))]
    #[serde(default = "default_consumer_group")]
    pub consumer_group: String,

    /// The number of partitions in the Event Hub.
    #[serde(default = "default_partition_count")]
    pub partition_count: usize,
}

fn default_consumer_group() -> String {
    "$Default".to_string()
}

fn default_partition_count() -> usize {
    4
}

/// Configuration for event processing settings.
#[derive(Clone, Debug, Default)]
#[configurable_component]
pub struct ProcessingSettings {
    /// The maximum number of events to include in a batch before sending.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// The maximum amount of time to wait before sending a batch of events.
    #[serde(default = "default_batch_timeout")]
    pub batch_timeout_ms: u64,

    /// The number of worker threads.
    #[serde(default = "default_worker_count")]
    pub worker_count: usize,

    /// The size of the processing queue.
    #[serde(default = "default_queue_size")]
    pub queue_size: usize,
}

fn default_batch_size() -> usize { 1000 }
fn default_batch_timeout() -> u64 { 100 }
fn default_worker_count() -> usize { 4 }
fn default_queue_size() -> usize { 10000 }

/// Configuration for Azure Blob Storage checkpointing.
#[derive(Clone, Debug, Default)]
#[configurable_component]
pub struct CheckpointSettings {
    /// Whether checkpointing is enabled.
    #[serde(default)]
    pub enabled: bool,

    /// The name of the storage account to use for checkpointing.
    #[configurable(metadata(docs::description = "The name of the storage account to use for checkpointing."))]
    pub storage_account_name: String,

    /// The name of the container to use for checkpointing.
    #[configurable(metadata(docs::description = "The name of the container to use for checkpointing."))]
    pub container_name: String,

    /// The interval in seconds between checkpoint saves.
    #[serde(default = "default_checkpoint_interval")]
    pub interval_seconds: u64,

    /// The maximum number of retry attempts for checkpoint operations.
    #[serde(default = "default_retry_attempts")]
    pub retry_max_attempts: u32,

    /// The initial retry interval in milliseconds.
    #[serde(default = "default_retry_interval")]
    pub retry_initial_interval_ms: u64,
}

fn default_checkpoint_interval() -> u64 { 30 }
fn default_retry_attempts() -> u32 { 3 }
fn default_retry_interval() -> u64 { 1000 }

impl CheckpointSettings {
    pub fn validate(&self) -> Result<()> {
        if self.enabled {
            if self.storage_account_name.is_empty() {
                return Err(anyhow::anyhow!("Storage account name cannot be empty when checkpointing is enabled"));
            }
            if self.container_name.is_empty() {
                return Err(anyhow::anyhow!("Container name cannot be empty when checkpointing is enabled"));
            }
            if !self.storage_account_name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
                return Err(anyhow::anyhow!("Storage account name contains invalid characters"));
            }
        }
        Ok(())
    }
}

impl_generate_config_from_default!(AzureEventHubConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "azure_event_hub")]
impl SourceConfig for AzureEventHubConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<Source> {
        self.checkpointing.validate()?;

        let event_hub_connection = EventHubConnection::new(EventHubConfig {
            fully_qualified_namespace: self.event_hub.fully_qualified_namespace.clone(),
            event_hub_name: self.event_hub.event_hub_name.clone(),
            consumer_group: self.event_hub.consumer_group.clone(),
        });

        event_hub_connection.connect().await?;

        let (_, client) = event_hub_connection.get_client().await?;
        let (mut consumer, event_receiver) = EventHubConsumer::new(
            client,
            self.event_hub.fully_qualified_namespace.clone(),
            self.event_hub.event_hub_name.clone(),
            self.event_hub.consumer_group.clone(),
            ConsumerConfig {
                max_batch_size: self.processing.batch_size,
                partition_count: self.event_hub.partition_count,
                buffer_size: self.processing.queue_size,
            },
            if self.checkpointing.enabled {
                Some(Arc::new(CheckpointStore::new(
                    &self.checkpointing.storage_account_name,
                    &self.checkpointing.container_name,
                    &self.event_hub.event_hub_name,
                    self.checkpointing.interval_seconds,
                ).await?))
            } else {
                None
            },
        );

        let pipeline = ProcessingPipeline::new(
            ProcessingConfig {
                batch_size: self.processing.batch_size,
                batch_timeout_ms: self.processing.batch_timeout_ms,
                worker_count: self.processing.worker_count,
                queue_size: self.processing.queue_size,
            },
            cx.out.clone(),
        );

        // Start the pipeline first
        pipeline.start(event_receiver).await?;

        // Then start consuming in the background task
        Ok(Box::pin(async move {
            tokio::select! {
                res = consumer.start_consuming() => {
                    if let Err(e) = res {
                        error!("Consumer error: {}", e);
                    }
                },
                _ = cx.shutdown => {
                    info!("Shutdown signal received, stopping Azure Event Hub source");
                    info!("Azure Event Hub source stopped");
                }
            }
            Ok(())
        }))
    }

    fn outputs(&self, global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        let namespaces = BTreeSet::from([global_log_namespace]);
        let schema_definition = Definition::default_for_namespace(&namespaces);
        vec![SourceOutput::new_maybe_logs(DataType::Log, schema_definition)]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}
