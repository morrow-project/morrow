use super::*;

#[test]
fn source_batch_limits_reject_misbehaving_tasks() {
    let record = SourceRecord {
        source_offset: "1".to_string(),
        subject: "orders.created".to_string(),
        key: None,
        payload: vec![0; 5],
    };
    assert!(
        validate_batch(
            &[record.clone(), record.clone()],
            &BrokerSourceConfig {
                max_records: 1,
                max_bytes: 10,
            },
        )
        .is_err()
    );
    assert!(
        validate_batch(
            &[record],
            &BrokerSourceConfig {
                max_records: 1,
                max_bytes: 4,
            },
        )
        .is_err()
    );
}
