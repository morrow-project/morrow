use super::*;

fn stream(name: &str, subjects: &[&str]) -> StreamDefinition {
    StreamDefinition {
        name: StreamId::new(name).unwrap(),
        subjects: subjects.iter().map(|subject| subject.to_string()).collect(),
        partitions: 8,
        partitioning: PartitioningPolicy::default(),
        storage: StoragePolicy::default(),
        retention: RetentionPolicy::default(),
    }
}

#[test]
fn resolves_exact_single_and_tail_wildcard_bindings() {
    let catalog = StreamCatalog::new(vec![
        stream("exact", &["orders.created"]),
        stream("telemetry", &["telemetry.*.cpu"]),
        stream("events", &["events.>"]),
    ])
    .unwrap();

    assert_eq!(
        catalog
            .resolve_primary("orders.created")
            .unwrap()
            .name
            .as_str(),
        "exact"
    );
    assert_eq!(
        catalog
            .resolve_primary("telemetry.mumbai.cpu")
            .unwrap()
            .name
            .as_str(),
        "telemetry"
    );
    assert_eq!(
        catalog
            .resolve_primary("events.orders.created")
            .unwrap()
            .name
            .as_str(),
        "events"
    );
    assert!(catalog.resolve_primary("other.subject").is_none());
}

#[test]
fn rejects_overlapping_primary_bindings() {
    let error = StreamCatalog::new(vec![
        stream("orders", &["orders.>"]),
        stream("created", &["orders.*.created"]),
    ])
    .unwrap_err();

    assert!(error.to_string().contains("ambiguous stream bindings"));
}

#[test]
fn rejects_zero_partitions() {
    let mut definition = stream("orders", &["orders.>"]);
    definition.partitions = 0;

    let error = StreamCatalog::new(vec![definition]).unwrap_err();

    assert!(error.to_string().contains("partitions"));
}

#[test]
fn rejects_bindings_that_capture_inboxes() {
    let error = StreamCatalog::new(vec![stream("everything", &[">"])]).unwrap_err();

    assert!(error.to_string().contains("reserved inbox"));
}

#[test]
fn never_resolves_inbox_subjects() {
    let catalog = StreamCatalog::new(vec![stream("orders", &["orders.>"])]).unwrap();

    assert!(catalog.resolve_primary("_INBOX.client.1").is_none());
}
