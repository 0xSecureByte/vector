use anyhow::Result;
use crossbeam_queue::ArrayQueue;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, warn, info};
use serde::{Serialize, Deserialize};
use std::time::Duration;
use vector_lib::event::LogEvent;
use crate::SourceSender;
use crate::internal_events::StreamClosedError;

use super::processor::Event;

#[derive(Clone, Debug)]
pub struct ProcessingConfig {
    pub batch_size: usize,
    pub batch_timeout_ms: u64,
    pub worker_count: usize,
    pub queue_size: usize,
}

impl Default for ProcessingConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            batch_timeout_ms: 100,
            worker_count: 4,
            queue_size: 10000,
        }
    }
}

#[derive(Debug)]
pub struct EventBatch {
    pub events: Vec<ProcessedEvent>,
    pub partition_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProcessedEvent {
    pub data: Vec<u8>,
    pub metadata: EventMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventMetadata {
    pub partition_id: String,
    pub sequence_number: i64,
    pub offset: String,
    pub timestamp: i64,
}

pub struct ProcessingPipeline {
    config: ProcessingConfig,
    input_queue: Arc<ArrayQueue<Event>>,
    output_sender: SourceSender,
}

impl ProcessingPipeline {
    /// Creates a new processing pipeline
    pub fn new(
        config: ProcessingConfig,
        output_sender: SourceSender,
    ) -> Self {
        Self {
            config: config.clone(),
            input_queue: Arc::new(ArrayQueue::new(config.queue_size)),
            output_sender,
        }
    }

    /// Starts the processing pipeline
    pub async fn start(&self, mut event_receiver: mpsc::Receiver<Event>) -> Result<()> {
        let input_queue = Arc::clone(&self.input_queue);
        
        // Spawn input handler
        tokio::spawn(async move {
            while let Some(event) = event_receiver.recv().await {
                if input_queue.push(event).is_err() {
                    warn!("Input queue full, applying backpressure");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        });

        // Spawn processing workers
        for worker_id in 0..self.config.worker_count {
            self.spawn_worker(worker_id).await?;
        }

        Ok(())
    }

    /// Spawns a processing worker
    async fn spawn_worker(&self, worker_id: usize) -> Result<()> {
        let input_queue = Arc::clone(&self.input_queue);
        let mut output_sender = self.output_sender.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            info!("Starting worker {}", worker_id);
            let mut batch = Vec::with_capacity(config.batch_size);
            let mut current_partition = String::new();
            
            loop {
                // Try to fill a batch
                while batch.len() < config.batch_size {
                    if let Some(event) = input_queue.pop() {
                        if current_partition.is_empty() {
                            current_partition = event.partition_id.clone();
                        }

                        // Convert to Vector LogEvent
                        let mut log_event = LogEvent::default();
                        log_event.insert("data", event.data);
                        log_event.insert("partition_id", event.partition_id);
                        log_event.insert("sequence_number", event.sequence_number);
                        log_event.insert("offset", event.offset.to_string());
                        log_event.insert("timestamp", chrono::Utc::now().timestamp_millis());

                        batch.push(log_event);
                    } else {
                        // No more events available right now
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        break;
                    }
                }

                // Send batch events
                if !batch.is_empty() {
                    let count = batch.len();
                    if let Err(_) = output_sender.send_batch(batch.drain(..).collect::<Vec<LogEvent>>()).await {
                        error!("Failed to send batch to output");
                        emit!(StreamClosedError { count });
                    }
                    current_partition.clear();
                }

                // Small sleep to prevent tight loop
                tokio::time::sleep(Duration::from_millis(config.batch_timeout_ms)).await;
            }
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pipeline_creation() {
        let config = ProcessingConfig::default();
        let (_pipeline, _receiver) = ProcessingPipeline::new(config);
        // Structural test only
    }
}