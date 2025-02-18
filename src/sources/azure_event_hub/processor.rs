use anyhow::Result;
use azeventhubs::consumer::{EventHubConsumerClient, EventHubConsumerClientOptions, EventPosition, ReadEventOptions};
use azeventhubs::BasicRetryPolicy;
use azeventhubs::EventHubsTransportType;
use azeventhubs::MaxRetries;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, error};
use futures::stream::StreamExt;
use super::checkpoint::CheckpointStore;
use azure_identity::DefaultAzureCredential;
use std::time::Duration;


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

#[derive(Clone, Debug)]
pub struct Event {
    pub data: Vec<u8>,
    pub partition_id: String,
    pub sequence_number: i64,
    pub offset: i64,
}

pub struct EventHubConsumer {
    client: EventHubConsumerClient<BasicRetryPolicy>,
    fully_qualified_namespace: String,
    event_hub_name: String,
    consumer_group: String,
    config: ConsumerConfig,
    sender: mpsc::Sender<Event>,
    checkpoint_store: Option<Arc<CheckpointStore>>,
}

impl EventHubConsumer {
    pub fn new(
        client: EventHubConsumerClient<BasicRetryPolicy>,
        fully_qualified_namespace: String,
        event_hub_name: String,
        consumer_group: String,
        config: ConsumerConfig,
        checkpoint_store: Option<Arc<CheckpointStore>>,
    ) -> (Self, mpsc::Receiver<Event>) {
        let (sender, receiver) = mpsc::channel(config.buffer_size);
        
        // Start periodic checkpoint flush only once
        if let Some(store) = &checkpoint_store {
            info!("Starting periodic checkpoint flush...");
            tokio::spawn(store.clone().start_periodic_flush());
        }

        (Self {
            client,
            fully_qualified_namespace,
            event_hub_name,
            consumer_group,
            config,
            sender,
            checkpoint_store,
        }, receiver)
    }

    // Remove duplicate shutdown handler
    pub async fn start_consuming(&mut self) -> Result<()> {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
        let checkpoint_store_clone = self.checkpoint_store.clone();
        
        tokio::spawn(async move {
            if let Ok(_) = shutdown_rx.recv().await {
                if let Some(store) = checkpoint_store_clone {
                    info!("Final checkpoint flush...");
                    let _ = tokio::time::timeout(Duration::from_secs(30), store.flush_checkpoints()).await;
                }
            }
        });

        // Set up signal handler
        let shutdown_sender_clone = shutdown_tx.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.unwrap();
            info!("Received shutdown signal");
            let _ = shutdown_sender_clone.send(());
        });

        let partition_ids = self.client.get_partition_ids().await?;
        info!("Found {} partitions: {:?}", partition_ids.len(), partition_ids);
        
        let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        let checkpoint_store = self.checkpoint_store.clone();
        let sender = self.sender.clone();
        let max_batch_size = self.config.max_batch_size;
        let fully_qualified_namespace = format!(
            "{}.servicebus.windows.net",  // Add the full domain
            self.fully_qualified_namespace
        );
        let event_hub_name = self.event_hub_name.clone();
        let consumer_group = self.consumer_group.clone();

        for partition_id in partition_ids {
            let partition_checkpoint_store = checkpoint_store.clone();
            let sender = sender.clone();
            let partition_id = partition_id.clone();
            let fully_qualified_namespace = fully_qualified_namespace.clone();
            let event_hub_name = event_hub_name.clone();
            let consumer_group = consumer_group.clone();

            let handle = tokio::spawn(async move {
                loop {
                    let credential = DefaultAzureCredential::default();
                    let mut client_options = EventHubConsumerClientOptions::default();
                    client_options.connection_options.connection_idle_timeout = Duration::from_secs(300);
                    client_options.connection_options.transport_type = EventHubsTransportType::AmqpTcp;
                    client_options.retry_options.max_retries = MaxRetries::new(5).unwrap();
                    client_options.retry_options.delay = Duration::from_secs(2);

                    info!("Creating client for partition {} with namespace {}", partition_id, fully_qualified_namespace);
                    let mut client = match EventHubConsumerClient::new_from_credential(
                        consumer_group.clone(),
                        fully_qualified_namespace.clone(),
                        event_hub_name.clone(),
                        credential,
                        client_options,
                    ).await {
                        Ok(client) => {
                            info!("Successfully created client for partition {}", partition_id);
                            client
                        },
                        Err(e) => {
                            error!("Failed to create client for partition {}: {}", partition_id, e);
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            continue;
                        }
                    };

                    let checkpoint_position = if let Some(store) = &partition_checkpoint_store {
                        match store.load_checkpoint(&partition_id).await {
                            Ok(Some(checkpoint)) => {
                                info!("Resuming from checkpoint: offset {} for partition {}", 
                                      checkpoint.offset, partition_id);
                                EventPosition::from_offset(checkpoint.offset, true)
                            }
                            _ => EventPosition::earliest(),
                        }
                    } else {
                        EventPosition::earliest()
                    };

                    let read_options = ReadEventOptions::default()
                        .with_prefetch_count(max_batch_size as u32);

                    let mut stream = match client.read_events_from_partition(
                        &partition_id,
                        checkpoint_position,
                        read_options
                    ).await {
                        Ok(s) => s,
                        Err(e) => {
                            error!("Failed to create stream for partition {}: {}", partition_id, e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                            continue;
                        }
                    };

                    // Remove checkpoint saving from event processing
                    while let Some(event_result) = stream.next().await {
                        match event_result {
                            Ok(event_data) => {
                                let sequence_number = event_data.sequence_number();
                                let offset = event_data.offset().unwrap_or_default();

                                let body = match event_data.body() {
                                    Ok(data) => data.to_vec(),
                                    Err(e) => {
                                        error!("Failed to get event body from partition {}: {}", partition_id, e);
                                        continue;
                                    }
                                };

                                let processed_event = Event {
                                    data: body,
                                    partition_id: partition_id.clone(),
                                    sequence_number,
                                    offset,
                                };

                                if let Err(e) = sender.send(processed_event).await {
                                    error!("Failed to send event from partition {} to channel: {}", partition_id, e);
                                    continue;
                                }

                                // Update in-memory checkpoint without saving to storage
                                if let Some(store) = &partition_checkpoint_store {
                                    let _ = store.update_checkpoint(
                                        &partition_id,
                                        offset,
                                        sequence_number
                                    ).await;
                                }
                            }
                            Err(e) => {
                                error!("Error receiving event from partition {}: {}", partition_id, e);
                                continue;
                            }
                        }
                    }
                    
                    info!("Stream ended for partition {}, attempting to reconnect...", partition_id);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            });
            
            handles.push(handle);
        }

        for handle in handles {
            if let Err(e) = handle.await {
                error!("Task error: {}", e);
            }
        }

        Ok(())
    }
}