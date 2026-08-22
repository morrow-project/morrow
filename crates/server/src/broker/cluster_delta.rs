use super::*;

impl Morrow {
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
        let _delta_application = self.cluster_delta_gate.lock().await;
        self.apply_cluster_partition_ordered(envelope).await
    }

    async fn apply_cluster_partition_ordered(&self, envelope: MessageEnvelope) -> Result<()> {
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
        self.inner
            .lock()
            .await
            .apply_committed_partition(envelope, &self.config.streams)?;
        Ok(())
    }

    pub(super) async fn apply_cluster_command(
        &self,
        command: BrokerCommand,
        response: &BrokerResponse,
    ) -> Result<()> {
        self.inner
            .lock()
            .await
            .apply_cluster_command(command, response, &self.config.streams)
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
        let key = (
            envelope.stream.as_str().to_string(),
            envelope.partition.0,
            envelope.offset,
        );
        if let Some(existing_seq) = self.partition_sequences.get(&key) {
            crate::broker_ensure!(
                self.messages.get(existing_seq) == Some(&record),
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
        self.messages.insert(seq, record.into_resident_metadata());
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
