use super::*;

const PARTITION_INGRESS_BATCH_RECORDS: usize = 32;
const PARTITION_INGRESS_BATCH_BYTES: usize = 1024 * 1024;
const PARTITION_INGRESS_BATCH_DELAY_MS: u64 = 2;
const MAX_PARTITION_INGRESS_BATCH_RECORDS: usize = 256;
const MAX_PARTITION_INGRESS_BATCH_BYTES: usize = 8 * 1024 * 1024;
const MAX_PARTITION_INGRESS_BATCH_DELAY_MS: u64 = 100;
const MAX_PARTITION_INGRESS_QUEUES: usize = 4096;
const MAX_CONFIGURED_PARTITION_INGRESS_QUEUES: usize = 65_536;
const PARTITION_INGRESS_QUEUE_BYTES: usize = 64 * 1024 * 1024;
const MAX_PARTITION_INGRESS_QUEUE_BYTES: usize = 256 * 1024 * 1024;

fn partition_ingress_queue_limit() -> usize {
    std::env::var("MORROW_PARTITION_INGRESS_QUEUE_LIMIT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(MAX_PARTITION_INGRESS_QUEUES)
        .clamp(1, MAX_CONFIGURED_PARTITION_INGRESS_QUEUES)
}

fn partition_ingress_batch_records() -> usize {
    std::env::var("MORROW_PARTITION_INGRESS_BATCH_RECORDS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(PARTITION_INGRESS_BATCH_RECORDS)
        .clamp(1, MAX_PARTITION_INGRESS_BATCH_RECORDS)
}

fn partition_ingress_batch_bytes() -> usize {
    std::env::var("MORROW_PARTITION_INGRESS_BATCH_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(PARTITION_INGRESS_BATCH_BYTES)
        .clamp(1, MAX_PARTITION_INGRESS_BATCH_BYTES)
}

fn partition_ingress_batch_delay_ms() -> u64 {
    std::env::var("MORROW_PARTITION_INGRESS_BATCH_DELAY_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(PARTITION_INGRESS_BATCH_DELAY_MS)
        .clamp(1, MAX_PARTITION_INGRESS_BATCH_DELAY_MS)
}

fn partition_ingress_queue_bytes() -> usize {
    std::env::var("MORROW_PARTITION_INGRESS_QUEUE_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(PARTITION_INGRESS_QUEUE_BYTES)
        .clamp(1, MAX_PARTITION_INGRESS_QUEUE_BYTES)
}

pub(super) struct PartitionIngressQueue {
    sender: tokio::sync::mpsc::Sender<PartitionIngressItem>,
    bytes: Arc<tokio::sync::Semaphore>,
}

#[derive(Default)]
pub(crate) struct PartitionIngressMetrics {
    pub(super) queue_records: AtomicU64,
    pub(super) batches_total: AtomicU64,
    pub(super) records_total: AtomicU64,
    pub(super) bytes_total: AtomicU64,
    pub(super) max_batch_records: AtomicU64,
    pub(super) max_batch_bytes: AtomicU64,
    pub(super) batch_wait_us_max: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct PartitionIngressMetricsSnapshot {
    pub(crate) queue_records: u64,
    pub(crate) batches_total: u64,
    pub(crate) records_total: u64,
    pub(crate) bytes_total: u64,
    pub(crate) max_batch_records: u64,
    pub(crate) max_batch_bytes: u64,
    pub(crate) batch_wait_us_max: u64,
}

impl PartitionIngressMetrics {
    pub(crate) fn snapshot(&self) -> PartitionIngressMetricsSnapshot {
        PartitionIngressMetricsSnapshot {
            queue_records: self.queue_records.load(Ordering::Relaxed),
            batches_total: self.batches_total.load(Ordering::Relaxed),
            records_total: self.records_total.load(Ordering::Relaxed),
            bytes_total: self.bytes_total.load(Ordering::Relaxed),
            max_batch_records: self.max_batch_records.load(Ordering::Relaxed),
            max_batch_bytes: self.max_batch_bytes.load(Ordering::Relaxed),
            batch_wait_us_max: self.batch_wait_us_max.load(Ordering::Relaxed),
        }
    }
}

pub(super) struct PartitionIngressItem {
    envelope: crate::partition_log::MessageEnvelope,
    fsync: bool,
    cluster_durable: bool,
    response: tokio::sync::oneshot::Sender<Result<crate::partition_log::MessageEnvelope>>,
    _byte_permit: tokio::sync::OwnedSemaphorePermit,
    enqueued_at: tokio::time::Instant,
}

impl RaftRuntime {
    async fn ensure_partition_metadata_ready(&self) -> Result<()> {
        if self
            .state_machine
            .durable_state()
            .stream_definitions
            .is_empty()
        {
            let _gate = self.metadata_bootstrap_gate.lock().await;
            if self
                .state_machine
                .durable_state()
                .stream_definitions
                .is_empty()
                && self.raft.current_leader().await == Some(self.node_id)
            {
                self.ensure_metadata_ready().await?;
            }
        }
        Ok(())
    }

    pub async fn replicate_partition(
        &self,
        envelope: crate::partition_log::MessageEnvelope,
        fsync: bool,
        cluster_durable: bool,
    ) -> Result<crate::partition_log::MessageEnvelope> {
        self.ensure_partition_metadata_ready().await?;
        let key = partition_key(envelope.stream.as_str(), envelope.partition.0);
        let (response, receiver) = tokio::sync::oneshot::channel();
        let envelope_bytes = data_append_envelope_bytes(&envelope);
        crate::broker_ensure!(
            envelope_bytes <= partition_ingress_queue_bytes(),
            "partition ingress envelope exceeds byte budget"
        );
        let permits = u32::try_from(envelope_bytes)
            .map_err(|_| BrokerError::msg("partition ingress envelope is too large"))?;
        let sender = {
            let mut queues = self.partition_ingress_queues.lock().await;
            if let Some(queue) = queues.get(&key) {
                PartitionIngressQueue {
                    sender: queue.sender.clone(),
                    bytes: queue.bytes.clone(),
                }
            } else {
                crate::broker_ensure!(
                    queues.len() < partition_ingress_queue_limit(),
                    "partition ingress queue budget exhausted"
                );
                let (sender, receiver) = tokio::sync::mpsc::channel(1024);
                let bytes = Arc::new(tokio::sync::Semaphore::new(partition_ingress_queue_bytes()));
                queues.insert(
                    key.clone(),
                    PartitionIngressQueue {
                        sender: sender.clone(),
                        bytes: bytes.clone(),
                    },
                );
                let runtime = self.clone();
                tokio::spawn(async move {
                    run_partition_ingress_queue(runtime, key, receiver).await;
                });
                PartitionIngressQueue { sender, bytes }
            }
        };
        let byte_permit = sender
            .bytes
            .clone()
            .acquire_many_owned(permits.max(1))
            .await
            .map_err(|_| BrokerError::msg("partition ingress byte budget unavailable"))?;
        let item = PartitionIngressItem {
            envelope,
            fsync,
            cluster_durable,
            response,
            _byte_permit: byte_permit,
            enqueued_at: tokio::time::Instant::now(),
        };
        self.partition_ingress_metrics
            .queue_records
            .fetch_add(1, Ordering::Relaxed);
        if sender.sender.send(item).await.is_err() {
            self.partition_ingress_metrics
                .queue_records
                .fetch_sub(1, Ordering::Relaxed);
            return Err(BrokerError::msg("partition ingress queue is unavailable"));
        }
        receiver
            .await
            .map_err(|_| BrokerError::msg("partition ingress response was canceled"))?
    }

    /// Replicate an ordered range of records in one partition batch.
    pub async fn replicate_partition_batch(
        &self,
        mut envelopes: Vec<crate::partition_log::MessageEnvelope>,
        fsync: bool,
        cluster_durable: bool,
    ) -> Result<Vec<crate::partition_log::MessageEnvelope>> {
        self.ensure_partition_metadata_ready().await?;
        let (max_records, max_bytes) = data_append_batch_limits();
        crate::broker_ensure!(
            !envelopes.is_empty() && envelopes.len() <= max_records,
            "partition replication batch is outside the supported bound"
        );
        let encoded_bytes = envelopes
            .iter()
            .map(data_append_envelope_bytes)
            .sum::<usize>();
        crate::broker_ensure!(
            encoded_bytes <= max_bytes,
            "partition replication batch bytes exceed the supported bound"
        );
        let first = envelopes
            .first()
            .ok_or_else(|| BrokerError::msg("partition replication batch was empty"))?;
        let key = partition_key(first.stream.as_str(), first.partition.0);
        crate::broker_ensure!(
            envelopes.iter().all(|envelope| {
                partition_key(envelope.stream.as_str(), envelope.partition.0) == key
            }),
            "partition replication batch contains multiple partitions"
        );
        let write_gate = self
            .partition_write_gates
            .lock()
            .await
            .entry(key.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _write_guard = write_gate.lock().await;
        let metadata = self.durable_state();
        crate::broker_ensure!(
            self.partition_data
                .lock()
                .unwrap()
                .has_committed_prefix(&metadata),
            "no safe replica available"
        );
        let assignment = metadata
            .partition_assignments
            .get(&key)
            .cloned()
            .ok_or_else(|| BrokerError::msg("partition has no metadata assignment"))?;
        crate::broker_ensure!(
            assignment.leader_id == self.node_id,
            "partition leader assignment is not committed"
        );
        let local_commit = self
            .partition_data
            .lock()
            .unwrap()
            .commit_metadata(first.stream.as_str(), first.partition);
        let previous = local_commit
            .as_ref()
            .or_else(|| metadata.partition_commits.get(&key));
        if metadata.feature_gates.contains("partition-local-commit-v1") {
            crate::broker_ensure!(
                previous.is_none_or(|commit| {
                    commit.replica_set_generation == assignment.replica_set_generation
                }),
                "partition commit generation is not safe"
            );
        }
        let first_offset = previous.map_or(0, |commit| commit.high_watermark.saturating_add(1));
        let leader_epoch = assignment.leader_epoch;
        let mut checksums = Vec::with_capacity(envelopes.len());
        for (index, envelope) in envelopes.iter_mut().enumerate() {
            envelope.offset = first_offset.saturating_add(index as u64);
            envelope.leader_epoch = leader_epoch;
            checksums.push(crate::partition_log::committed_envelope_checksum(envelope)?);
        }
        let requests = envelopes
            .iter()
            .enumerate()
            .map(|(index, envelope)| DataAppendRequest {
                leader_id: self.node_id,
                leader_epoch,
                replica_set_generation: assignment.replica_set_generation,
                fsync,
                committed_high_watermark: previous.map(|commit| commit.high_watermark),
                predecessor_offset: if index == 0 {
                    previous.map(|commit| commit.high_watermark)
                } else {
                    Some(envelope.offset.saturating_sub(1))
                },
                predecessor_checksum: if index == 0 {
                    previous.map(|commit| commit.checksum)
                } else {
                    Some(checksums[index - 1])
                },
                batch_digest: checksums[index],
                durability: if fsync {
                    DurabilityBoundary::LocalFlush
                } else {
                    DurabilityBoundary::Memory
                },
                envelope: envelope.clone(),
            })
            .collect::<Vec<_>>();
        let required_nodes = if cluster_durable {
            assignment.replicas.clone()
        } else {
            assignment.active_members().clone()
        };
        crate::broker_ensure!(
            required_nodes.contains(&self.node_id),
            "partition leader is outside the required commit set"
        );
        let required_count = required_nodes.len();
        let final_offset = requests
            .last()
            .map(|request| request.envelope.offset)
            .ok_or_else(|| BrokerError::msg("partition replication batch was empty"))?;
        let mut replicated = 1usize;
        let mut flushed = usize::from(fsync);
        let mut joins = tokio::task::JoinSet::new();
        let metadata = Arc::new(metadata);
        for node_id in self.nodes.keys() {
            if *node_id == self.node_id || !assignment.replicas.contains(node_id) {
                continue;
            }
            let client = self.data_client(*node_id).await?;
            let requests = requests.clone();
            let partition_data = self.partition_data.clone();
            let metadata = metadata.clone();
            let work_scheduler = self.work_scheduler.clone();
            let required = required_nodes.contains(node_id);
            let task = async move {
                let progress = send_data_progress_on_client(
                    &client,
                    DataProgressRequest {
                        stream: requests[0].envelope.stream.as_str().to_string(),
                        partition: requests[0].envelope.partition,
                    },
                )
                .await?;
                let committed_records = super::runtime::load_partition_delta(
                    partition_data,
                    metadata,
                    requests[0].envelope.stream.as_str().to_string(),
                    requests[0].envelope.partition,
                    progress,
                )
                .await?;
                let catch_up_records = committed_records.len() as u64;
                let catch_up_bytes = committed_records
                    .iter()
                    .map(data_append_envelope_bytes)
                    .sum::<usize>() as u64;
                let work_class = if required {
                    crate::work_scheduler::WorkClass::Control
                } else {
                    crate::work_scheduler::WorkClass::CatchUp
                };
                let reservation = if catch_up_records == 0 {
                    None
                } else {
                    let reservation = crate::work_scheduler::WorkReservation::try_acquire(
                        work_scheduler.clone(),
                        work_class,
                        catch_up_records,
                        catch_up_bytes,
                    )
                    .await;
                    if reservation.is_none() {
                        return Err(BrokerError::msg(if required {
                            "quorum replication work budget exhausted"
                        } else {
                            "catch-up work budget exhausted"
                        }));
                    }
                    reservation
                };
                let result = async {
                    let mut append_batch = Vec::with_capacity(max_records);
                    let mut append_batch_bytes = 0usize;
                    let mut predecessor_checksum = requests[0].predecessor_checksum;
                    let mut last_response = None;
                    for record in committed_records {
                        let checksum = crate::partition_log::committed_envelope_checksum(&record)?;
                        let record_bytes = data_append_envelope_bytes(&record);
                        if !append_batch.is_empty()
                            && (append_batch.len() == max_records
                                || append_batch_bytes.saturating_add(record_bytes) > max_bytes)
                        {
                            last_response = send_data_append_batch_on_client(
                                &client,
                                std::mem::take(&mut append_batch),
                            )
                            .await?
                            .into_iter()
                            .last();
                            append_batch_bytes = 0;
                        }
                        append_batch.push(DataAppendRequest {
                            leader_id: requests[0].leader_id,
                            leader_epoch: requests[0].leader_epoch,
                            replica_set_generation: requests[0].replica_set_generation,
                            fsync: requests[0].fsync,
                            committed_high_watermark: requests[0].committed_high_watermark,
                            predecessor_offset: record.offset.checked_sub(1),
                            predecessor_checksum,
                            batch_digest: checksum,
                            durability: requests[0].durability,
                            envelope: record,
                        });
                        append_batch_bytes = append_batch_bytes.saturating_add(record_bytes);
                        predecessor_checksum = Some(checksum);
                    }
                    for request in requests {
                        let record_bytes = data_append_envelope_bytes(&request.envelope);
                        if !append_batch.is_empty()
                            && (append_batch.len() == max_records
                                || append_batch_bytes.saturating_add(record_bytes) > max_bytes)
                        {
                            last_response = send_data_append_batch_on_client(
                                &client,
                                std::mem::take(&mut append_batch),
                            )
                            .await?
                            .into_iter()
                            .last();
                            append_batch_bytes = 0;
                        }
                        append_batch.push(request);
                        append_batch_bytes = append_batch_bytes.saturating_add(record_bytes);
                    }
                    if !append_batch.is_empty() {
                        last_response = send_data_append_batch_on_client(&client, append_batch)
                            .await?
                            .into_iter()
                            .last();
                    }
                    last_response
                        .ok_or_else(|| BrokerError::msg("partition append batch was empty"))
                }
                .await;
                drop(reservation);
                result
            };
            if required {
                let node_id = *node_id;
                joins.spawn(async move { (node_id, task.await) });
            } else {
                tokio::spawn(async move {
                    if let Err(err) = task.await {
                        tracing::warn!(error = ?err, "observer partition replication failed");
                    }
                });
            }
        }
        let mut first_replica_error = None;
        while let Some(result) = joins.join_next().await {
            match result {
                Ok((_, Ok(response))) => {
                    if response.match_offset == final_offset {
                        replicated += 1;
                    }
                    if response.flushed_offset == Some(final_offset) {
                        flushed += 1;
                    }
                }
                Ok((node_id, Err(error))) => {
                    first_replica_error.get_or_insert_with(|| {
                        BrokerError::msg(format!(
                            "partition replica {node_id} unavailable: {error}"
                        ))
                    });
                }
                Err(error) => {
                    first_replica_error.get_or_insert_with(|| {
                        BrokerError::with_source("partition replication worker failed", error)
                    });
                }
            }
        }
        if replicated < required_count {
            return Err(first_replica_error
                .unwrap_or_else(|| BrokerError::msg("partition quorum unavailable")));
        }
        if fsync {
            crate::broker_ensure!(
                flushed >= required_count,
                "partition fsync quorum unavailable"
            );
        }
        self.partition_data
            .lock()
            .unwrap()
            .append_batch(&requests)?;
        let final_envelope = envelopes
            .last()
            .cloned()
            .ok_or_else(|| BrokerError::msg("partition replication batch was empty"))?;
        let commit_checksum = checksums
            .last()
            .copied()
            .ok_or_else(|| BrokerError::msg("partition replication batch was empty"))?;
        if metadata.feature_gates.contains("partition-local-commit-v1") {
            let commit_request = DataCommitRequest {
                leader_id: self.node_id,
                leader_epoch,
                replica_set_generation: assignment.replica_set_generation,
                stream: final_envelope.stream.as_str().to_string(),
                partition: final_envelope.partition,
                high_watermark: final_envelope.offset,
                checksum: commit_checksum,
                fsync,
            };
            let commit_clients = self
                .nodes
                .keys()
                .filter(|node_id| **node_id != self.node_id && required_nodes.contains(node_id))
                .map(|node_id| async {
                    Ok::<_, BrokerError>((
                        self.data_client(*node_id).await?,
                        commit_request.clone(),
                    ))
                });
            let mut commit_clients = futures_util::future::try_join_all(commit_clients).await?;
            crate::raft::commit_on_replicas(std::mem::take(&mut commit_clients)).await?;
            self.partition_data
                .lock()
                .unwrap()
                .commit(&commit_request)?;
        } else {
            for envelope in &envelopes {
                let response = self
                    .client_write(BrokerCommand::PartitionCommit {
                        stream: envelope.stream.as_str().to_string(),
                        partition: envelope.partition.0,
                        offset: envelope.offset,
                        checksum: crate::partition_log::committed_envelope_checksum(envelope)?,
                        leader_id: self.node_id,
                        leader_epoch,
                    })
                    .await?;
                crate::broker_ensure!(
                    matches!(
                        response,
                        BrokerResponse::PartitionCommit {
                            high_watermark,
                            leader_epoch: committed_epoch,
                        } if high_watermark == envelope.offset && committed_epoch == leader_epoch
                    ),
                    "partition metadata commit rejected"
                );
            }
        }
        Ok(envelopes)
    }
}

async fn run_partition_ingress_queue(
    runtime: RaftRuntime,
    _key: String,
    mut receiver: tokio::sync::mpsc::Receiver<PartitionIngressItem>,
) {
    let mut pending = None;
    loop {
        let first = if let Some(item) = pending.take() {
            item
        } else if let Some(item) = receiver.recv().await {
            item
        } else {
            return;
        };
        let fsync = first.fsync;
        let cluster_durable = first.cluster_durable;
        let max_records = partition_ingress_batch_records();
        let max_bytes = partition_ingress_batch_bytes();
        let delay = tokio::time::Duration::from_millis(partition_ingress_batch_delay_ms());
        let deadline = tokio::time::Instant::now() + delay;
        let mut bytes = data_append_envelope_bytes(&first.envelope);
        let enqueued_at = first.enqueued_at;
        let mut items = vec![first];
        while items.len() < max_records {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let Ok(Some(candidate)) = tokio::time::timeout(remaining, receiver.recv()).await else {
                break;
            };
            let candidate_bytes = data_append_envelope_bytes(&candidate.envelope);
            if candidate.fsync != fsync
                || candidate.cluster_durable != cluster_durable
                || bytes.saturating_add(candidate_bytes) > max_bytes
            {
                pending = Some(candidate);
                break;
            }
            bytes = bytes.saturating_add(candidate_bytes);
            items.push(candidate);
        }
        let envelopes = items
            .iter()
            .map(|item| item.envelope.clone())
            .collect::<Vec<_>>();
        let batch_bytes = bytes;
        let batch_wait_us = enqueued_at.elapsed().as_micros() as u64;
        runtime
            .partition_ingress_metrics
            .queue_records
            .fetch_sub(items.len() as u64, Ordering::Relaxed);
        runtime
            .partition_ingress_metrics
            .batches_total
            .fetch_add(1, Ordering::Relaxed);
        runtime
            .partition_ingress_metrics
            .records_total
            .fetch_add(envelopes.len() as u64, Ordering::Relaxed);
        runtime
            .partition_ingress_metrics
            .bytes_total
            .fetch_add(batch_bytes as u64, Ordering::Relaxed);
        runtime
            .partition_ingress_metrics
            .max_batch_records
            .fetch_max(envelopes.len() as u64, Ordering::Relaxed);
        runtime
            .partition_ingress_metrics
            .max_batch_bytes
            .fetch_max(batch_bytes as u64, Ordering::Relaxed);
        runtime
            .partition_ingress_metrics
            .batch_wait_us_max
            .fetch_max(batch_wait_us, Ordering::Relaxed);
        let result = runtime
            .replicate_partition_batch(envelopes, fsync, cluster_durable)
            .await;
        match result {
            Ok(envelopes) => {
                for (item, envelope) in items.into_iter().zip(envelopes) {
                    let _ = item.response.send(Ok(envelope));
                }
            }
            Err(error) => {
                let message = error.to_string();
                for item in items {
                    let _ = item.response.send(Err(BrokerError::msg(&message)));
                }
            }
        }
    }
}
