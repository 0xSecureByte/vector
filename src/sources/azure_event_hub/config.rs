use vector_lib::configurable::configurable_component;
// use serde::{Deserialize, Serialize};
use crate::config::{SourceConfig, SourceContext, SourceOutput, DataType};
use vector_lib::source::Source;
use vector_lib::schema::Definition;
use vector_lib::config::LogNamespace;
use std::collections::BTreeSet;

/// Configuration for the Azure Event Hub source.
#[configurable_component(source("azure_event_hub", "Collect logs from Azure Event Hub."))]
#[derive(Clone, Debug, Default)]
pub struct AzureEventHubConfig {
    /// Event Hub configuration.
    #[configurable(metadata(docs::description = "Event Hub configuration."))]
    pub event_hub: EventHubConfig,

    /// Processing configuration.
    #[serde(default)]
    #[configurable(metadata(docs::description = "Processing configuration."))]
    pub processing: ProcessingConfig,

    /// Checkpointing configuration.
    #[serde(default)]
    #[configurable(metadata(docs::description = "Checkpointing configuration."))]
    pub checkpointing: CheckpointConfig,
}

/// Configuration for connecting to Azure Event Hub.
#[configurable_component]
#[derive(Clone, Debug, Default)]
pub struct EventHubConfig {
    /// The fully qualified namespace for the Event Hub.
    pub fully_qualified_namespace: String,

    /// The names of the Event Hubs to consume from.
    pub event_hub_names: Vec<String>,

    /// The consumer group to use.
    pub consumer_group: String,

    /// The number of partitions in the Event Hub.
    pub partition_count: u32,
}

/// Configuration for event processing settings.
#[configurable_component]
#[derive(Clone, Debug, Default)]
pub struct ProcessingConfig {
    /// The maximum number of events to include in a batch before sending.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// The maximum amount of time to wait before sending a batch of events.
    #[serde(default = "default_batch_timeout_ms")]
    pub batch_timeout_ms: u64,

    /// The number of worker threads.
    #[serde(default = "default_worker_count")]
    pub worker_count: usize,

    /// The size of the processing queue.
    #[serde(default = "default_queue_size")]
    pub queue_size: usize,
}

/// Configuration for Azure Blob Storage checkpointing.
#[configurable_component]
#[derive(Clone, Debug, Default)]
pub struct CheckpointConfig {
    /// Whether checkpointing is enabled.
    #[serde(default)]
    pub enabled: bool,

    /// The name of the storage account to use for checkpointing.
    pub storage_account_name: String,

    /// The name of the container to use for checkpointing.
    pub container_name: String,

    /// The interval in seconds between checkpoint saves.
    #[serde(default = "default_checkpoint_interval")]
    pub interval_seconds: u64,

    /// The maximum number of retry attempts for checkpoint operations.
    #[serde(default = "default_retry_max_attempts")]
    pub retry_max_attempts: u32,

    /// The initial retry interval in milliseconds.
    #[serde(default = "default_retry_initial_interval")]
    pub retry_initial_interval_ms: u64,
}

fn default_batch_size() -> usize { 100 }
fn default_batch_timeout_ms() -> u64 { 1000 }
fn default_worker_count() -> usize { 4 }
fn default_queue_size() -> usize { 1000 }
fn default_checkpoint_interval() -> u64 { 3 }
fn default_retry_max_attempts() -> u32 { 5 }
fn default_retry_initial_interval() -> u64 { 1000 }

impl_generate_config_from_default!(AzureEventHubConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "azure_event_hub")]
impl SourceConfig for AzureEventHubConfig {
    async fn build(&self, _cx: SourceContext) -> crate::Result<Source> {
        // Implementation will be added later
        todo!()
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
