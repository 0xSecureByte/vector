//! Azure Event Hub source for Vector
//! 
//! This module provides the implementation for reading events from Azure Event Hub.

mod config;
mod eventhub;
mod manager;
mod processor;
mod processor_pipeline;
mod checkpoint;

pub use self::config::*;
pub use self::eventhub::*;
pub use self::manager::*;
pub use self::processor::*;
pub use self::processor_pipeline::*;
pub use self::checkpoint::*;

use vector_lib::configurable::configurable_component;
use vector_lib::internal_event::{CountByteSize, InternalEventHandle as _, Protocol};
use vector_lib::{
    config::{DataType, SourceConfig, SourceContext},
    shutdown::ShutdownSignal,
    source::Source,
};

// Re-export the main source config and implementation
pub use crate::config::AzureEventHubSourceConfig;

#[async_trait::async_trait]
impl SourceConfig for AzureEventHubSourceConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<Source> {
        let source = AzureEventHubSource::new(self.clone(), cx.shutdown, cx.out);
        Ok(source.run().boxed())
    }

    fn output_type(&self) -> DataType {
        DataType::Log
    }

    fn source_type(&self) -> &'static str {
        "azure_event_hub"
    }
}
