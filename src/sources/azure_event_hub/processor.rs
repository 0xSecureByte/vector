use anyhow::Result;
use azeventhubs::consumer::{EventHubConsumerClient, EventHubConsumerClientOptions, ReadEventOptions, EventPosition};
use azeventhubs::{BasicRetryPolicy};
use std::sync::Arc;
use tracing::{info, error};
use futures::stream::StreamExt;
use super::checkpoint::CheckpointStore;
use crate::sources::azure_event_hub::MyTokenCredential;

// Define your own Event struct
#[derive(Debug)]
pub struct Event {
    pub partition_id: String,
    pub data: Vec<u8>,
    pub sequence_number: i64,
    pub offset: String,
}

#[derive(Clone, Debug)]
pub struct ConsumerConfig {
    pub max_batch_size: usize,
    pub partition_count: usize,
    pub buffer_size: usize,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 1000,
            partition_count: 4,
            buffer_size: 10000,
        }
    }
}

pub struct EventHubConsumer {
    client: EventHubConsumerClient<BasicRetryPolicy>,
    fully_qualified_namespace: String,
    event_hub_name: String,
    consumer_group: String,
    config: ConsumerConfig,
    checkpoint_store: Option<Arc<CheckpointStore>>,
}

impl EventHubConsumer {
    pub async fn new(
        fully_qualified_namespace: String,
        event_hub_name: String,
        consumer_group: String,
        config: ConsumerConfig,
        checkpoint_store: Option<Arc<CheckpointStore>>,
    ) -> Result<Self> {
        let credential = MyTokenCredential::new();
        let client = EventHubConsumerClient::new_from_credential(
            consumer_group.clone(),
            fully_qualified_namespace.clone(),
            event_hub_name.clone(),
            credential,
            EventHubConsumerClientOptions::default(),
        ).await?;

        Ok(Self {
            client,
            fully_qualified_namespace,
            event_hub_name,
            consumer_group,
            config,
            checkpoint_store,
        })
    }

    pub fn get_namespace(&self) -> &str {
        &self.fully_qualified_namespace
    }

    pub fn get_event_hub_name(&self) -> &str {
        &self.event_hub_name
    }

    pub fn get_consumer_group(&self) -> &str {
        &self.consumer_group
    }

    pub async fn process_events<F>(&mut self, mut callback: F) -> Result<()>
    where
        F: FnMut(Event) -> Result<()> + Send + 'static,
    {
        info!(
            "Starting event processing for Event Hub '{}' in namespace '{}' with consumer group '{}'",
            self.event_hub_name, self.fully_qualified_namespace, self.consumer_group
        );

        let partition_ids = self.client.get_partition_ids().await?;
        info!("Found {} partitions: {:?}", partition_ids.len(), partition_ids);
        
        for partition_id in partition_ids {
            let checkpoint_position = self.get_checkpoint_position(&partition_id).await?;
            let read_options = ReadEventOptions::default()
                .with_prefetch_count(self.config.max_batch_size as u32);

            // Clone the necessary data for checkpointing
            let checkpoint_store = self.checkpoint_store.clone();
            
            // Create stream with a separate reference
            let mut stream = self.client.read_events_from_partition(
                &partition_id,
                checkpoint_position,
                read_options
            ).await?;

            while let Some(event_result) = stream.next().await {
                match event_result {
                    Ok(event_data) => {
                        let event = Event {
                            partition_id: partition_id.clone(),
                            data: event_data.body()?.to_vec(),
                            sequence_number: event_data.sequence_number(),
                            offset: event_data.offset()
                                .ok_or_else(|| anyhow::anyhow!("No offset available"))?
                                .to_string(),
                        };

                        callback(event)?;

                        // Handle checkpointing without borrowing self
                        if let Some(ref store) = checkpoint_store {
                            let offset = event_data.offset()
                                .ok_or_else(|| anyhow::anyhow!("No offset available"))?;
                                
                            store.save_checkpoint(
                                &partition_id,
                                offset,
                                event_data.sequence_number(),
                            ).await?;
                        }
                    }
                    Err(e) => {
                        error!("Error receiving event from partition {}: {}", partition_id, e);
                        continue;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn get_checkpoint_position(&self, _partition_id: &str) -> Result<EventPosition> {
        // Implementation logic to retrieve checkpoint position
        Ok(EventPosition::earliest())
    }
}

pub enum CheckpointPosition {
    Beginning,
    End,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[tokio::test]
    async fn test_config_loading() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let config = AppConfig::default();
        let toml_str = toml::to_string(&config).unwrap();
        write!(temp_file, "{}", toml_str).unwrap();

        let (config_manager, _rx) = ConfigManager::new(temp_file.path().to_str().unwrap().to_string())
            .await
            .unwrap();

        let loaded_config = config_manager.get_config().await;
        assert_eq!(loaded_config.event_hub.partition_count, 4);
    }
}