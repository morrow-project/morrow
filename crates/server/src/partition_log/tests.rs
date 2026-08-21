use super::*;
use crate::stream::{
    PartitionFallback, PartitioningPolicy, PartitioningStrategy, RetentionPolicy, StoragePolicy,
};
use std::{fs::OpenOptions, io::Write};
use tempfile::TempDir;

fn definition(partitions: u32) -> StreamDefinition {
    StreamDefinition {
        name: StreamId::new("orders").unwrap(),
        subjects: vec!["orders.>".into()],
        partitions,
        partitioning: PartitioningPolicy {
            strategy: PartitioningStrategy::Key,
            fallback: PartitionFallback::SubjectHash,
            epoch: 7,
        },
        storage: StoragePolicy::default(),
        retention: RetentionPolicy::default(),
    }
}

fn catalog(partitions: u32) -> StreamCatalog {
    StreamCatalog::new(vec![definition(partitions)]).unwrap()
}

fn request<'a>(
    stream: &'a StreamDefinition,
    key: Option<&'a [u8]>,
    headers: &'a [MessageHeader],
) -> AppendRequest<'a> {
    AppendRequest {
        namespace: "tenant-a",
        stream,
        subject: "orders.created",
        key,
        partition_hint: None,
        headers,
        timestamp_ms: 42,
        reply_to: Some("reply.orders"),
        payload: b"hello",
        leader_epoch: 3,
        legacy_seq: None,
    }
}

fn request_for_subject<'a>(stream: &'a StreamDefinition, subject: &'a str) -> AppendRequest<'a> {
    let mut request = request(stream, None, &[]);
    request.subject = subject;
    request
}

#[test]
fn keyed_partition_is_stable_during_epoch() {
    let stream = definition(11);
    let first = select_partition(&stream, "orders.created", Some(b"customer-7"), 0);
    let second = select_partition(&stream, "orders.updated", Some(b"customer-7"), 9);
    assert_eq!(first, second);
}

#[test]
fn envelopes_round_trip_across_partitions_and_restart() {
    let dir = TempDir::new().unwrap();
    let catalog = catalog(4);
    let stream = &catalog.definitions()[0];
    let (mut logs, replay) = PartitionLogSet::open(dir.path(), &catalog, 220).unwrap();
    assert!(replay.is_empty());

    let headers = [MessageHeader {
        name: "Trace-Id".into(),
        value: "trace-1".into(),
    }];
    let first = logs.append(request(stream, Some(b"a"), &headers)).unwrap();
    let second = logs.append(request(stream, Some(b"b"), &headers)).unwrap();
    assert_ne!(first.partition, second.partition);
    assert_eq!(first.offset, 0);
    assert_eq!(second.offset, 0);
    logs.flush().unwrap();
    drop(logs);

    let (_, replay) = PartitionLogSet::open(dir.path(), &catalog, 220).unwrap();
    assert_eq!(replay.len(), 2);
    assert!(replay.contains(&first));
    assert!(replay.contains(&second));
    for envelope in replay {
        assert_eq!(envelope.namespace, "tenant-a");
        assert_eq!(envelope.headers[0].name, "Trace-Id");
        assert_eq!(envelope.timestamp_ms, 42);
        assert_eq!(envelope.reply_to.as_deref(), Some("reply.orders"));
        assert_eq!(envelope.payload, b"hello");
        assert_eq!(envelope.partitioning_epoch, 7);
        assert_eq!(envelope.leader_epoch, 3);
    }
}

#[test]
fn torn_active_tail_is_truncated_and_index_is_rebuilt() {
    let dir = TempDir::new().unwrap();
    let catalog = catalog(1);
    let stream = &catalog.definitions()[0];
    let (mut logs, _) = PartitionLogSet::open(dir.path(), &catalog, 4096).unwrap();
    let expected = logs.append(request(stream, None, &[])).unwrap();
    logs.flush().unwrap();
    drop(logs);

    let segment = dir
        .path()
        .join("streams/orders/partition-00000/00000000000000000001.plog");
    let index = segment.with_extension("idx");
    std::fs::remove_file(&index).unwrap();
    OpenOptions::new()
        .append(true)
        .open(&segment)
        .unwrap()
        .write_all(&[4, 0])
        .unwrap();

    let (logs, replay) = PartitionLogSet::open(dir.path(), &catalog, 4096).unwrap();
    assert_eq!(logs.truncations, 1);
    assert_eq!(replay, vec![expected]);
    assert!(index.exists());
}

#[test]
fn sealed_checksum_corruption_is_reported() {
    let dir = TempDir::new().unwrap();
    let catalog = catalog(1);
    let stream = &catalog.definitions()[0];
    let (mut logs, _) = PartitionLogSet::open(dir.path(), &catalog, 1).unwrap();
    logs.append(request(stream, None, &[])).unwrap();
    logs.append(request(stream, None, &[])).unwrap();
    logs.flush().unwrap();
    drop(logs);

    let sealed = dir
        .path()
        .join("streams/orders/partition-00000/00000000000000000001.plog");
    let mut bytes = std::fs::read(&sealed).unwrap();
    *bytes.last_mut().unwrap() ^= 0xff;
    std::fs::write(&sealed, bytes).unwrap();

    let error = PartitionLogSet::open(dir.path(), &catalog, 1).unwrap_err();
    assert!(error.to_string().contains("corrupt partition-log segment"));
}

#[test]
fn committed_envelopes_are_immutable() {
    let dir = TempDir::new().unwrap();
    let catalog = catalog(1);
    let stream = &catalog.definitions()[0];
    let (mut logs, _) = PartitionLogSet::open(dir.path(), &catalog, 4096).unwrap();
    let committed = logs.append(request(stream, None, &[])).unwrap();
    logs.append_committed(committed.clone()).unwrap();

    let mut changed = committed;
    changed.payload = b"changed".to_vec();
    let error = logs.append_committed(changed).unwrap_err();
    assert!(error.to_string().contains("immutable committed record"));
}

#[test]
fn rewriting_a_partition_removes_an_uncommitted_suffix() {
    let dir = TempDir::new().unwrap();
    let catalog = catalog(1);
    let stream = &catalog.definitions()[0];
    let (mut logs, _) = PartitionLogSet::open(dir.path(), &catalog, 4096).unwrap();
    let first = logs.append(request(stream, None, &[])).unwrap();
    let mut second_request = request(stream, None, &[]);
    second_request.payload = b"uncommitted";
    logs.append(second_request).unwrap();

    logs.rewrite_partition("orders", PartitionId(0), std::slice::from_ref(&first))
        .unwrap();
    drop(logs);
    let (_, replay) = PartitionLogSet::open(dir.path(), &catalog, 4096).unwrap();
    assert_eq!(replay, vec![first]);
}

#[test]
fn sealed_subject_index_matches_ordered_full_scan_results() {
    let dir = TempDir::new().unwrap();
    let catalog = catalog(1);
    let stream = &catalog.definitions()[0];
    let (mut logs, _) = PartitionLogSet::open(dir.path(), &catalog, 1).unwrap();
    let subjects = [
        "orders.created",
        "orders.eu.created",
        "orders.updated",
        "orders.eu.deleted",
    ];
    for subject in subjects {
        logs.append(request_for_subject(stream, subject)).unwrap();
    }
    for filter in ["orders.created", "orders.*", "orders.>"] {
        let query = logs
            .matching_offsets("orders", PartitionId(0), filter)
            .unwrap();
        let expected = subjects
            .iter()
            .enumerate()
            .filter(|(_, concrete)| protocol::subject::matches(filter, concrete))
            .map(|(offset, _)| offset as u64)
            .collect::<Vec<_>>();
        assert_eq!(query.offsets, expected);
        if filter == "orders.created" {
            assert!(query.used_index);
        }
    }
}

#[test]
fn missing_and_corrupt_subject_indexes_fall_back_and_rebuild() {
    let dir = TempDir::new().unwrap();
    let catalog = catalog(1);
    let stream = &catalog.definitions()[0];
    let (mut logs, _) = PartitionLogSet::open(dir.path(), &catalog, 1).unwrap();
    logs.append(request_for_subject(stream, "orders.created"))
        .unwrap();
    logs.append(request_for_subject(stream, "orders.updated"))
        .unwrap();
    let sealed = dir
        .path()
        .join("streams/orders/partition-00000/00000000000000000001.sidx");
    drop(logs);

    std::fs::remove_file(&sealed).unwrap();
    let (logs, _) = PartitionLogSet::open(dir.path(), &catalog, 1).unwrap();
    let missing = logs
        .matching_offsets("orders", PartitionId(0), "orders.created")
        .unwrap();
    assert_eq!(missing.offsets, vec![0]);
    assert!(missing.used_index);
    drop(logs);

    std::fs::write(&sealed, b"corrupt index").unwrap();
    let (logs, _) = PartitionLogSet::open(dir.path(), &catalog, 1).unwrap();
    let corrupt = logs
        .matching_offsets("orders", PartitionId(0), "orders.created")
        .unwrap();
    assert_eq!(corrupt.offsets, vec![0]);
    assert!(corrupt.used_index);
}

#[test]
#[ignore = "manual sealed-subject-index microbenchmark"]
fn benchmark_sealed_subject_index_exact_star_and_tail_filters() {
    let dir = TempDir::new().unwrap();
    let catalog = catalog(1);
    let stream = &catalog.definitions()[0];
    let (mut logs, _) = PartitionLogSet::open(dir.path(), &catalog, 512 * 1024).unwrap();
    let subjects = (0..10_000)
        .map(|id| format!("orders.{}.event", id % 1_000))
        .collect::<Vec<_>>();
    for subject in &subjects {
        logs.append(request_for_subject(stream, subject)).unwrap();
    }
    for filter in ["orders.42.event", "orders.*.event", "orders.>"] {
        let started = std::time::Instant::now();
        for _ in 0..100 {
            std::hint::black_box(
                logs.matching_offsets("orders", PartitionId(0), filter)
                    .unwrap(),
            );
        }
        let indexed = started.elapsed();
        let started = std::time::Instant::now();
        for _ in 0..100 {
            std::hint::black_box(
                subjects
                    .iter()
                    .filter(|subject| protocol::subject::matches(filter, subject))
                    .count(),
            );
        }
        eprintln!(
            "filter={filter} index={indexed:?} scan={:?}",
            started.elapsed()
        );
    }
}
