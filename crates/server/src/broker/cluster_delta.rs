use super::*;

impl Morrow {
    pub(super) async fn sync_local_partition_commits(
        &self,
        cluster: &ClusterRuntime,
    ) -> Result<()> {
        let pending = {
            let applied = self.local_partition_applied.lock().await;
            cluster
                .local_committed_records()?
                .into_iter()
                .filter(|envelope| {
                    let key =
                        crate::raft::partition_key(envelope.stream.as_str(), envelope.partition.0);
                    !applied
                        .get(&key)
                        .is_some_and(|offset| *offset >= envelope.offset)
                })
                .collect::<Vec<_>>()
        };
        for envelope in pending {
            let key = crate::raft::partition_key(envelope.stream.as_str(), envelope.partition.0);
            let offset = envelope.offset;
            self.apply_cluster_partition(envelope).await?;
            self.local_partition_applied
                .lock()
                .await
                .insert(key, offset);
            self.cluster_application_metrics
                .delta_applications
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    pub(super) async fn sync_cluster_deltas(&self, cluster: &ClusterRuntime) -> Result<()> {
        let _delta_application = self.cluster_delta_gate.lock().await;
        let Some(batch) = cluster.deltas_after(self.cluster_applied_log_index()) else {
            return Ok(());
        };
        match batch {
            DeltaBatch::FullReconciliation => self.sync_from_cluster(cluster).await,
            DeltaBatch::Incremental(deltas) => {
                for delta in deltas {
                    if !self.apply_committed_delta(cluster, delta).await? {
                        break;
                    }
                }
                Ok(())
            }
        }
    }

    pub(super) async fn apply_cluster_partition(&self, envelope: MessageEnvelope) -> Result<()> {
        self.apply_cluster_partition_ordered(envelope).await
    }

    async fn apply_cluster_partition_ordered(&self, envelope: MessageEnvelope) -> Result<()> {
        let key = crate::raft::partition_key(envelope.stream.as_str(), envelope.partition.0);
        let partition_gate = {
            let mut gates = self.cluster_partition_apply_gates.lock().await;
            gates
                .entry(key)
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let _partition_apply_guard = partition_gate.lock().await;
        let _storage_operation = self.storage_gate.read().await;
        if self.partition_logs.is_before_retention_floor(
            envelope.stream.as_str(),
            envelope.partition,
            envelope.offset,
        ) {
            return Ok(());
        }
        let partition_logs = self.partition_logs.clone();
        let persisted = envelope.clone();
        let permit = self
            .storage_permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| BrokerError::msg("storage worker pool closed"))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            partition_logs.append_committed(persisted)
        })
        .await
        .map_err(|err| BrokerError::with_source("cluster partition worker failed", err))??;
        // Disk and worker-pool waits happen before taking the state shard. The
        // shard protects only the ordered in-memory index update below.
        let shard = crate::state_shards::shard_for(
            crate::state_shards::StateShardKey::Partition {
                stream: envelope.stream.as_str(),
                partition: envelope.partition.0,
            },
            self.state_shard_gates.len(),
        );
        let shard_wait_started = Instant::now();
        let _state_shard_guard = self.state_shard_gates[shard].lock().await;
        self.metrics
            .state_shard_wait_us
            .observe(shard_wait_started.elapsed());
        let shard_hold_started = Instant::now();
        self.inner
            .lock()
            .await
            .apply_committed_partition(envelope, &self.config.streams)?;
        self.metrics
            .state_shard_hold_us
            .observe(shard_hold_started.elapsed());
        Ok(())
    }

    pub(super) async fn apply_cluster_command(
        &self,
        command: BrokerCommand,
        response: &BrokerResponse,
    ) -> Result<()> {
        let policy = match &command {
            BrokerCommand::PolicyReplace { snapshot } => Some(snapshot.clone()),
            _ => None,
        };
        let group = match &command {
            BrokerCommand::GroupUpsert { group, record } => Some((group.clone(), record.clone())),
            _ => None,
        };
        self.inner
            .lock()
            .await
            .apply_cluster_command(command, response, &self.config.streams)?;
        if let Some((group, record)) = group {
            self.groups.lock().await.insert(
                group,
                crate::consumer_group::GroupCoordinator::from_replicated_record(record)
                    .with_context(|| "applying replicated consumer-group state".to_string())?,
            );
        }
        if let (Some(snapshot), BrokerResponse::PolicyReplace { .. }) = (policy, response) {
            let generation = snapshot.generation;
            self.policy.replace(snapshot)?;
            self.record_audit_event(crate::tenancy::AuditEvent {
                sequence: 0,
                timestamp_ms: self.hooks.clock.now_ms(),
                actor: "cluster".to_string(),
                tenant: None,
                action: "policy.replace".to_string(),
                resource: "cluster/policy".to_string(),
                outcome: "replicated".to_string(),
                details: [("generation".to_string(), generation.to_string())]
                    .into_iter()
                    .collect(),
            })?;
        }
        Ok(())
    }

    pub(super) fn cluster_applied_log_index(&self) -> Option<u64> {
        self.cluster_applied_index
            .load(Ordering::Acquire)
            .checked_sub(1)
    }

    pub(super) fn set_cluster_applied_log_index(&self, index: Option<u64>) {
        self.cluster_applied_index.fetch_max(
            index.map_or(0, |index| index.saturating_add(1)),
            Ordering::AcqRel,
        );
    }

    async fn apply_committed_delta(
        &self,
        cluster: &ClusterRuntime,
        delta: CommittedDelta,
    ) -> Result<bool> {
        if let Some(BrokerCommand::PartitionCommit {
            stream,
            partition,
            offset,
            ..
        }) = delta.command.clone()
            && matches!(
                delta.response,
                BrokerResponse::PartitionCommit { high_watermark, .. } if high_watermark == offset
            )
        {
            if let Some(envelope) = cluster.partition_record(&stream, partition, offset) {
                self.apply_cluster_partition_ordered(envelope).await?;
            } else if cluster.is_local_partition_replica(&stream, partition) {
                return Ok(false);
            }
        } else if let Some(command) = delta.command {
            self.apply_cluster_command(command, &delta.response).await?;
        }
        self.set_cluster_applied_log_index(Some(delta.log_id.index));
        self.cluster_application_metrics
            .delta_applications
            .fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }
}

impl DurableBrokerState {
    pub(super) fn apply_cluster_command(
        &mut self,
        command: BrokerCommand,
        response: &BrokerResponse,
        catalog: &crate::stream::StreamCatalog,
    ) -> Result<()> {
        match (command, response) {
            (
                BrokerCommand::CursorConsumerUpsert { record, cursors },
                BrokerResponse::ConsumerUpsert,
            ) => self.apply_consumer_upsert(record, Some(cursors), catalog),
            (BrokerCommand::ConsumerUpsert { record }, BrokerResponse::ConsumerUpsert) => {
                self.apply_consumer_upsert(record, None, catalog)
            }
            (BrokerCommand::ConsumerDelete { consumer_id }, BrokerResponse::ConsumerDelete) => {
                if let Some(consumer) = self.consumers.remove(&consumer_id) {
                    self.consumer_interest_index
                        .remove(&consumer.record.filter_subject, &consumer_id);
                }
                self.ready_consumers.remove(&consumer_id);
            }
            _ => {}
        }
        Ok(())
    }

    pub(super) fn apply_committed_partition(
        &mut self,
        envelope: MessageEnvelope,
        catalog: &crate::stream::StreamCatalog,
    ) -> Result<()> {
        let record = PublishRecord::from(envelope.clone());
        let resident_record = record.clone().into_resident_metadata();
        let key = (
            envelope.stream.as_str().to_string(),
            envelope.partition.0,
            envelope.offset,
        );
        if let Some(existing_seq) = self.partition_sequences.get(&key) {
            crate::broker_ensure!(
                self.messages.get(existing_seq) == Some(&resident_record),
                "committed partition delta conflicts with local state"
            );
            return Ok(());
        }
        self.wal.append_partition_append(&PartitionAppendRecord {
            seq: record.seq,
            stream: key.0.clone(),
            partition: key.1,
            offset: key.2,
            subject: record.subject.clone(),
        })?;
        self.partition_sequences.insert(key, record.seq);
        let subject = record.subject.clone();
        let seq = record.seq;
        self.messages.insert(seq, resident_record);
        self.observe_published_record(&record);
        self.mark_subject_ready(&subject);
        self.apply_record_compaction(seq, catalog);
        Ok(())
    }

    fn apply_consumer_upsert(
        &mut self,
        record: ConsumerRecord,
        committed_cursors: Option<crate::consumer_cursor::ConsumerCursorSet>,
        catalog: &crate::stream::StreamCatalog,
    ) {
        let consumer_id = record.consumer_id.clone();
        let existed = self.consumers.contains_key(&consumer_id);
        let consumer = self.upsert_consumer(record, catalog);
        if !existed && let Some(cursors) = committed_cursors {
            consumer.cursors = cursors;
        }
        self.ready_consumers.insert(consumer_id);
    }
}
