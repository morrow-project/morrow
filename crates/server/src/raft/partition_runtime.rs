use super::*;

impl RaftRuntime {
    pub async fn replicate_partition(
        &self,
        mut envelope: crate::partition_log::MessageEnvelope,
        fsync: bool,
        cluster_durable: bool,
    ) -> Result<crate::partition_log::MessageEnvelope> {
        let key = partition_key(envelope.stream.as_str(), envelope.partition.0);
        let write_gate = self
            .partition_write_gates
            .get(&key)
            .ok_or_else(|| BrokerError::msg("partition has no write coordinator"))?;
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
        envelope.offset = previous.map_or(0, |commit| commit.high_watermark.saturating_add(1));
        let leader_epoch = assignment.leader_epoch;
        envelope.leader_epoch = leader_epoch;
        let request = DataAppendRequest {
            leader_id: self.node_id,
            leader_epoch,
            replica_set_generation: assignment.replica_set_generation,
            fsync,
            committed_high_watermark: previous.map(|commit| commit.high_watermark),
            predecessor_offset: previous.map(|commit| commit.high_watermark),
            predecessor_checksum: previous.map(|commit| commit.checksum),
            batch_digest: crate::partition_log::committed_envelope_checksum(&envelope)?,
            durability: if fsync {
                DurabilityBoundary::LocalFlush
            } else {
                DurabilityBoundary::Memory
            },
            envelope: envelope.clone(),
        };
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
        let mut replicated = 1usize;
        let mut flushed = usize::from(fsync);
        let mut joins = tokio::task::JoinSet::new();
        let metadata = Arc::new(metadata);
        for node_id in self.nodes.keys() {
            if *node_id == self.node_id {
                continue;
            }
            let client = self.data_client(*node_id).await?;
            let request = request.clone();
            let partition_data = self.partition_data.clone();
            let metadata = metadata.clone();
            let required = required_nodes.contains(node_id);
            let task = async move {
                let progress = send_data_progress_on_client(
                    &client,
                    DataProgressRequest {
                        stream: request.envelope.stream.as_str().to_string(),
                        partition: request.envelope.partition,
                    },
                )
                .await?;
                let committed_records = super::runtime::load_partition_delta(
                    partition_data,
                    metadata,
                    request.envelope.stream.as_str().to_string(),
                    request.envelope.partition,
                    progress,
                )
                .await?;
                let mut predecessor_checksum = request.predecessor_checksum;
                let mut append_batch = Vec::with_capacity(committed_records.len() + 1);
                for record in committed_records {
                    let record_checksum =
                        crate::partition_log::committed_envelope_checksum(&record)?;
                    append_batch.push(DataAppendRequest {
                        leader_id: request.leader_id,
                        leader_epoch: request.leader_epoch,
                        replica_set_generation: request.replica_set_generation,
                        fsync: request.fsync,
                        committed_high_watermark: request.committed_high_watermark,
                        predecessor_offset: record.offset.checked_sub(1),
                        predecessor_checksum,
                        batch_digest: record_checksum,
                        durability: request.durability,
                        envelope: record,
                    });
                    predecessor_checksum = Some(record_checksum);
                }
                append_batch.push(request);
                let responses = send_data_append_batch_on_client(&client, append_batch).await?;
                responses
                    .into_iter()
                    .last()
                    .ok_or_else(|| BrokerError::msg("partition append batch was empty"))
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
                    if response.match_offset == envelope.offset {
                        replicated += 1;
                    }
                    if response.flushed_offset == Some(envelope.offset) {
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
        self.partition_data.lock().unwrap().append(&request)?;
        if metadata.feature_gates.contains("partition-local-commit-v1") {
            let checksum = crate::partition_log::committed_envelope_checksum(&envelope)?;
            let commit_request = DataCommitRequest {
                leader_id: self.node_id,
                leader_epoch,
                replica_set_generation: assignment.replica_set_generation,
                stream: envelope.stream.as_str().to_string(),
                partition: envelope.partition,
                high_watermark: envelope.offset,
                checksum,
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
            return Ok(envelope);
        }
        let response = self
            .client_write(BrokerCommand::PartitionCommit {
                stream: envelope.stream.as_str().to_string(),
                partition: envelope.partition.0,
                offset: envelope.offset,
                checksum: crate::partition_log::committed_envelope_checksum(&envelope)?,
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
        Ok(envelope)
    }
}
