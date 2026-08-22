use super::*;

#[tokio::test]
async fn randomized_incremental_application_matches_full_reconciliation() {
    let scenario = Scenario::new_fake_cluster(3);
    let expected = Scenario::new();
    let cluster = scenario.broker().cluster_runtime().await.unwrap();
    let mut random = 0x5eed_u64;
    let mut next_seq = 1_u64;

    for step in 0..80_u64 {
        random = random
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        match random % 3 {
            0 => {
                let consumer_id = format!("consumer-{}", random % 8);
                let record = ConsumerRecord {
                    consumer_id: consumer_id.clone(),
                    filter_subject: "orders.>".into(),
                    queue_group: None,
                    ack_timeout_ms: 1_000 + step,
                    max_in_flight: 16,
                    start_position: protocol::StartPosition::Earliest,
                };
                let cursors = crate::consumer_cursor::ConsumerCursorSet::new(
                    &record.filter_subject,
                    record.start_position,
                    record.max_in_flight,
                    &scenario.broker().config.streams,
                    &scenario.broker().inner.lock().await.messages,
                );
                scenario
                    .broker()
                    .cluster_write(
                        &cluster,
                        BrokerCommand::CursorConsumerUpsert { record, cursors },
                    )
                    .await
                    .unwrap();
            }
            1 => {
                scenario
                    .broker()
                    .cluster_write(
                        &cluster,
                        BrokerCommand::ConsumerDelete {
                            consumer_id: format!("consumer-{}", random % 8),
                        },
                    )
                    .await
                    .unwrap();
            }
            _ => {
                let envelope = MessageEnvelope {
                    namespace: DEFAULT_NAMESPACE.into(),
                    stream: crate::stream::StreamId::new("orders").unwrap(),
                    partition: crate::stream::PartitionId(0),
                    offset: 0,
                    subject: "orders.created".into(),
                    key: None,
                    headers: vec![],
                    timestamp_ms: step,
                    reply_to: None,
                    payload: step.to_le_bytes().to_vec(),
                    partitioning_epoch: 0,
                    leader_epoch: 0,
                    legacy_seq: next_seq,
                };
                next_seq += 1;
                let committed = cluster.replicate_partition(envelope, false).await.unwrap();
                scenario
                    .broker()
                    .apply_cluster_partition(committed)
                    .await
                    .unwrap();
            }
        }

        let full_state = scenario.fake_cluster().durable_state();
        expected
            .broker()
            .inner
            .lock()
            .await
            .sync_durable_state(
                &expected.broker().partition_logs,
                full_state,
                &expected.broker().config.streams,
            )
            .unwrap();
        assert_equivalent(scenario.broker(), expected.broker()).await;
    }

    assert_eq!(
        scenario
            .broker()
            .cluster_application_metrics
            .full_reconciliations
            .load(Ordering::Relaxed),
        0
    );
    assert!(
        scenario
            .broker()
            .cluster_application_metrics
            .delta_applications
            .load(Ordering::Relaxed)
            > 0
    );
}

#[tokio::test]
async fn duplicate_consumer_delta_is_idempotent_and_does_not_regress_cursors() {
    let scenario = Scenario::new();
    let record = ConsumerRecord {
        consumer_id: "consumer".into(),
        filter_subject: "orders.>".into(),
        queue_group: None,
        ack_timeout_ms: 1_000,
        max_in_flight: 16,
        start_position: protocol::StartPosition::Earliest,
    };
    let cursors = crate::consumer_cursor::ConsumerCursorSet::new(
        &record.filter_subject,
        record.start_position,
        record.max_in_flight,
        &scenario.broker().config.streams,
        &HashMap::new(),
    );
    let command = BrokerCommand::CursorConsumerUpsert {
        record,
        cursors: cursors.clone(),
    };
    scenario
        .broker()
        .apply_cluster_command(command.clone(), &BrokerResponse::ConsumerUpsert)
        .await
        .unwrap();
    scenario
        .broker()
        .inner
        .lock()
        .await
        .consumers
        .get_mut("consumer")
        .unwrap()
        .cursors
        .partitions
        .values_mut()
        .for_each(|cursor| cursor.committed_offset = 7);
    scenario
        .broker()
        .apply_cluster_command(command, &BrokerResponse::ConsumerUpsert)
        .await
        .unwrap();
    assert!(
        scenario.broker().inner.lock().await.consumers["consumer"]
            .cursors
            .partitions
            .values()
            .all(|cursor| cursor.committed_offset == 7)
    );
}

async fn assert_equivalent(incremental: &Broker, reconciled: &Broker) {
    let incremental = incremental.inner.lock().await;
    let reconciled = reconciled.inner.lock().await;
    assert_eq!(incremental.messages, reconciled.messages);
    assert_eq!(
        incremental.partition_sequences,
        reconciled.partition_sequences
    );
    assert_eq!(incremental.consumers.len(), reconciled.consumers.len());
    for (consumer_id, consumer) in &incremental.consumers {
        let expected = &reconciled.consumers[consumer_id];
        assert_eq!(consumer.record, expected.record);
        assert_eq!(consumer.cursors, expected.cursors);
    }
}
