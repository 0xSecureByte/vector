use anyhow::Result;
use crossbeam_queue::ArrayQueue;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, warn, info};
use serde::{Serialize, Deserialize};
use std::time::Duration;
use vector_lib::event::LogEvent;
use crate::SourceSender;
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use dashmap::DashMap;

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

#[derive(Clone)]
pub struct ProcessingMetrics {
    processed_events: Arc<AtomicU64>,
    processing_errors: Arc<AtomicU64>,
    checkpoint_successes: Arc<AtomicU64>,
    checkpoint_failures: Arc<AtomicU64>,
    per_partition_events: Arc<DashMap<String, AtomicU64>>,
}

pub struct ProcessingPipeline {
    config: ProcessingConfig,
    input_queue: Arc<ArrayQueue<Event>>,
    output_sender: SourceSender,
    metrics: ProcessingMetrics,
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
            metrics: ProcessingMetrics {
                processed_events: Arc::new(AtomicU64::new(0)),
                processing_errors: Arc::new(AtomicU64::new(0)),
                checkpoint_successes: Arc::new(AtomicU64::new(0)),
                checkpoint_failures: Arc::new(AtomicU64::new(0)),
                per_partition_events: Arc::new(DashMap::new()),
            },
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
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            info!("Starting worker {}", worker_id);
            let mut batch = Vec::with_capacity(config.batch_size);
            
            let metrics_interval = Duration::from_secs(60);
            let mut last_metrics = tokio::time::Instant::now();
            
            loop {
                while batch.len() < config.batch_size {
                    if let Some(event) = input_queue.pop() {
                        match String::from_utf8(event.data.clone()) {
                            Ok(json_str) => match serde_json::from_str::<Value>(&json_str) {
                                Ok(json_value) => {
                                    let mut log_event = LogEvent::default();
                                    
                                    if let Some(map) = json_value.as_object() {
                                        for (key, value) in map {
                                            log_event.insert(key.as_str(), value.clone());
                                        }
                                    }

                                    log_event.insert("partition_id", event.partition_id.clone());
                                    log_event.insert("sequence_number", event.sequence_number);
                                    log_event.insert("offset", event.offset.to_string());
                                    log_event.insert("timestamp", chrono::Utc::now().timestamp_millis());

                                    batch.push(log_event);
                                    metrics.processed_events.fetch_add(1, Ordering::Relaxed);
                                    
                                    metrics.per_partition_events
                                        .entry(event.partition_id)
                                        .or_insert_with(|| AtomicU64::new(0))
                                        .fetch_add(1, Ordering::Relaxed);
                                },
                                Err(e) => {
                                    metrics.processing_errors.fetch_add(1, Ordering::Relaxed);
                                    error!("Invalid JSON: {}", e);
                                }
                            },
                            Err(e) => {
                                metrics.processing_errors.fetch_add(1, Ordering::Relaxed);
                                error!("Invalid UTF-8: {}", e);
                            }
                        }
                    } else {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        break;
                    }
                }

                if !batch.is_empty() {
                    let events: Vec<LogEvent> = batch.drain(..).collect();
                    if let Err(e) = output_sender.send_batch(events).await {
                        error!("Failed to send batch: {}", e);
                        metrics.processing_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }

                if last_metrics.elapsed() >= metrics_interval {
                    let total = metrics.processed_events.load(Ordering::Relaxed);
                    let errors = metrics.processing_errors.load(Ordering::Relaxed);
                    let checkpoints = metrics.checkpoint_successes.load(Ordering::Relaxed);
                    let failures = metrics.checkpoint_failures.load(Ordering::Relaxed);
                    
                    info!(
                        "Processing metrics - Total: {}, Errors: {}, Checkpoints: {}/{}, Rate: {:.2} evt/s",
                        total, errors, checkpoints, failures,
                        total as f64 / 60.0
                    );

                    for entry in metrics.per_partition_events.iter() {
                        info!(
                            "Partition {} - Events: {}", 
                            entry.key(), entry.value().load(Ordering::Relaxed)
                        );
                    }
                    
                    last_metrics = tokio::time::Instant::now();
                }

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