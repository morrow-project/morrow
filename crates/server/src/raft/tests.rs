use super::*;

fn nodes() -> BTreeMap<u64, BasicNode> {
    [(1, BasicNode::new("127.0.0.1:5221"))]
        .into_iter()
        .collect()
}

#[test]
fn applies_publish_attempt_and_ack() {
    let mut state = DurableState::new(nodes());
    let record = ConsumerRecord {
        consumer_id: "durable-client-sid".into(),
        filter_subject: "orders.*".into(),
        queue_group: None,
        ack_timeout_ms: 30_000,
        max_in_flight: 1024,
    };
    assert_eq!(
        state.apply_command(BrokerCommand::ConsumerUpsert { record }),
        BrokerResponse::ConsumerUpsert
    );
    assert_eq!(
        state.apply_command(BrokerCommand::Publish {
            subject: "orders.created".into(),
            reply_to: None,
            payload: b"ok".to_vec(),
        }),
        BrokerResponse::Publish {
            seq: Some(1),
            retained: true
        }
    );
    assert!(state.consumers["durable-client-sid"].pending.contains(&1));

    let response = state.apply_command(BrokerCommand::DeliveryAttempt {
        seq: 1,
        consumer_id: "durable-client-sid".into(),
        deadline_ms: 10,
        attempt: 1,
    });
    let BrokerResponse::DeliveryAttempt {
        record: Some(attempt),
    } = response
    else {
        panic!("expected delivery attempt");
    };
    assert_eq!(attempt.delivery_id, 1);
    assert!(
        state.consumers["durable-client-sid"]
            .in_flight
            .contains_key(&1)
    );

    assert_eq!(
        state.apply_command(BrokerCommand::Ack {
            seq: 1,
            consumer_id: "durable-client-sid".into(),
            delivery_id: 1,
        }),
        BrokerResponse::Ack { accepted: true }
    );
    assert!(state.messages.is_empty());
}

#[test]
fn publish_without_matching_consumer_is_not_retained() {
    let mut state = DurableState::new(nodes());
    assert_eq!(
        state.apply_command(BrokerCommand::Publish {
            subject: "orders.created".into(),
            reply_to: None,
            payload: b"ok".to_vec(),
        }),
        BrokerResponse::Publish {
            seq: None,
            retained: false
        }
    );
    assert!(state.messages.is_empty());
}

#[test]
fn delivery_attempts_allocate_monotonic_delivery_ids() {
    let mut state = DurableState::new(nodes());
    state.apply_command(BrokerCommand::ConsumerUpsert {
        record: ConsumerRecord {
            consumer_id: "durable-client-sid".into(),
            filter_subject: "orders.*".into(),
            queue_group: None,
            ack_timeout_ms: 30_000,
            max_in_flight: 1024,
        },
    });
    state.apply_command(BrokerCommand::Publish {
        subject: "orders.created".into(),
        reply_to: None,
        payload: b"one".to_vec(),
    });
    state.apply_command(BrokerCommand::Publish {
        subject: "orders.updated".into(),
        reply_to: None,
        payload: b"two".to_vec(),
    });

    let BrokerResponse::DeliveryAttempt {
        record: Some(first),
    } = state.apply_command(BrokerCommand::DeliveryAttempt {
        seq: 1,
        consumer_id: "durable-client-sid".into(),
        deadline_ms: 10,
        attempt: 1,
    })
    else {
        panic!("expected first delivery attempt");
    };
    let BrokerResponse::DeliveryAttempt {
        record: Some(second),
    } = state.apply_command(BrokerCommand::DeliveryAttempt {
        seq: 2,
        consumer_id: "durable-client-sid".into(),
        deadline_ms: 20,
        attempt: 1,
    })
    else {
        panic!("expected second delivery attempt");
    };

    assert_eq!(first.delivery_id, 1);
    assert_eq!(second.delivery_id, 2);
}

#[test]
fn ack_rejects_stale_delivery_id() {
    let mut state = DurableState::new(nodes());
    state.apply_command(BrokerCommand::ConsumerUpsert {
        record: ConsumerRecord {
            consumer_id: "durable-client-sid".into(),
            filter_subject: "orders.*".into(),
            queue_group: None,
            ack_timeout_ms: 30_000,
            max_in_flight: 1024,
        },
    });
    state.apply_command(BrokerCommand::Publish {
        subject: "orders.created".into(),
        reply_to: None,
        payload: b"one".to_vec(),
    });
    state.apply_command(BrokerCommand::DeliveryAttempt {
        seq: 1,
        consumer_id: "durable-client-sid".into(),
        deadline_ms: 10,
        attempt: 1,
    });

    assert_eq!(
        state.apply_command(BrokerCommand::Ack {
            seq: 1,
            consumer_id: "durable-client-sid".into(),
            delivery_id: 2,
        }),
        BrokerResponse::Ack { accepted: false }
    );
    assert!(state.messages.contains_key(&1));
    assert!(
        state.consumers["durable-client-sid"]
            .in_flight
            .contains_key(&1)
    );
}

#[test]
fn cleanup_waits_for_all_interested_consumers_to_ack() {
    let mut state = DurableState::new(nodes());
    for consumer_id in ["durable-a-sid", "durable-b-sid"] {
        state.apply_command(BrokerCommand::ConsumerUpsert {
            record: ConsumerRecord {
                consumer_id: consumer_id.into(),
                filter_subject: "orders.*".into(),
                queue_group: None,
                ack_timeout_ms: 30_000,
                max_in_flight: 1024,
            },
        });
    }
    state.apply_command(BrokerCommand::Publish {
        subject: "orders.created".into(),
        reply_to: None,
        payload: b"one".to_vec(),
    });
    state.apply_command(BrokerCommand::DeliveryAttempt {
        seq: 1,
        consumer_id: "durable-a-sid".into(),
        deadline_ms: 10,
        attempt: 1,
    });
    state.apply_command(BrokerCommand::DeliveryAttempt {
        seq: 1,
        consumer_id: "durable-b-sid".into(),
        deadline_ms: 10,
        attempt: 1,
    });

    assert_eq!(
        state.apply_command(BrokerCommand::Ack {
            seq: 1,
            consumer_id: "durable-a-sid".into(),
            delivery_id: 1,
        }),
        BrokerResponse::Ack { accepted: true }
    );
    assert!(state.messages.contains_key(&1));

    assert_eq!(
        state.apply_command(BrokerCommand::Ack {
            seq: 1,
            consumer_id: "durable-b-sid".into(),
            delivery_id: 2,
        }),
        BrokerResponse::Ack { accepted: true }
    );
    assert!(state.messages.is_empty());
}
