mod config;
mod eventhub;
mod processor;
mod checkpoint;
mod processor_pipeline;

pub use self::config::AzureEventHubConfig;
pub use self::eventhub::{EventHubConnection, EventHubConfig};
pub use self::checkpoint::CheckpointStore;
pub use self::processor::{EventHubConsumer, ConsumerConfig};
pub use self::processor_pipeline::{ProcessingPipeline, ProcessingConfig};

mod manager;

pub use self::manager::{
    ConfigManager,
    AppConfig,
};

pub use self::processor_pipeline::{
    EventBatch,
    ProcessedEvent,
    EventMetadata,
};