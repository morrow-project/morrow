use super::*;

const PARTITION_INGRESS_BATCH_RECORDS: usize = 32;
const PARTITION_INGRESS_BATCH_BYTES: usize = 1024 * 1024;
const PARTITION_INGRESS_BATCH_DELAY_MS: u64 = 2;
const MAX_PARTITION_INGRESS_QUEUES: usize = 4096;
const MAX_CONFIGURED_PARTITION_INGRESS_QUEUES: usize = 65_536;

fn partition_ingress_queue_limit() -> usize {
    std::env::var("MORROW_PARTITION_INGRESS_QUEUE_LIMIT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(MAX_PARTITION_INGRESS_QUEUES)
        .clamp(1, MAX_CONFIGURED_PARTITION_INGRESS_QUEUES)
}

pub(super) struct PartitionIngressItem {
    envelope: crate::partition_log::MessageEnvelope,
    fsync: bool,
    cluster_durable: bool,
    response: tokio::sync::oneshot::Sender<Result<crate::partition_log::MessageEnvelope>>,
}

impl RaftRuntime {
    pub async fn replicate_partition(
        &self,
        envelope: crate::partition_log::MessageEnvelope,
        fsync: bool,
        cluster_durable: bool,
    ) -> Result<crate::partition_log::MessageEnvelope> {
        let key = partition_key(envelope.stream.as_str(), envelope.partition.0);
        let (response, receiver) = tokio::sync::oneshot::channel();
        let item = PartitionIngressItem {
            envelope,
            fsync,
            cluster_durable,
            response,
        };
        let sender = {
            let mut queues = self.partition_ingress_queues.lock().await;
            if let Some(sender) = queues.get(&key) {
                sender.clone()
            } else {
                crate::broker_ensure!(
                    queues.len() < partition_ingress_queue_limit(),
                    "partition ingress queue budget exhausted"
                );
                let (sender, receiver) = tokio::sync::mpsc::channel(1024);
                queues.insert(key.clone(), sender.clone());
                let runtime = self.clone();
                tokio::spawn(async move {
                    run_partition_ingress_queue(runtime, key, receiver).await;
                });
                sender
            }
        };
        sender
            .send(item)
            .await
            .map_err(|_| BrokerError::msg("partition ingress queue is unavailable"))?;
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
        let previous = metadata.partition_commits.get(&key);
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
            if *node_id == self.node_id {
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
                let reserved = if catch_up_records == 0 {
                    false
                } else {
                    let mut scheduler = work_scheduler.lock().await;
                    if !scheduler.try_reserve(work_class, catch_up_records, catch_up_bytes) {
                        return Err(BrokerError::msg(if required {
                            "quorum replication work budget exhausted"
                        } else {
                            "catch-up work budget exhausted"
                        }));
                    }
                    true
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
                if reserved {
                    work_scheduler.lock().await.release(
                        work_class,
                        catch_up_records,
                        catch_up_bytes,
                    );
                }
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
        let mut bytes = data_append_envelope_bytes(&first.envelope);
        let mut items = vec![first];
        while items.len() < PARTITION_INGRESS_BATCH_RECORDS {
            let Ok(Some(candidate)) = tokio::time::timeout(
                tokio::time::Duration::from_millis(PARTITION_INGRESS_BATCH_DELAY_MS),
                receiver.recv(),
            )
            .await
            else {
                break;
            };
            let candidate_bytes = data_append_envelope_bytes(&candidate.envelope);
            if candidate.fsync != fsync
                || candidate.cluster_durable != cluster_durable
                || bytes.saturating_add(candidate_bytes) > PARTITION_INGRESS_BATCH_BYTES
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
