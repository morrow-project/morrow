use super::*;
use crate::stream::{
    PartitionFallback, PartitioningPolicy, PartitioningStrategy, RetentionPolicy, StoragePolicy,
};
use std::collections::BTreeSet;
use std::{fs::OpenOptions, io::Write, sync::Arc};
use tempfile::TempDir;

fn definition(partitions: u32) -> StreamDefinition {
    StreamDefinition {
        name: StreamId::new("orders").unwrap(),
        subjects: vec!["orders/**".into()],
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

#[test]
fn assigned_partition_recovery_opens_only_local_partitions() {
    let dir = TempDir::new().unwrap();
    let catalog = catalog(4);
    let assigned = BTreeSet::from([("orders".to_string(), 1), ("orders".to_string(), 3)]);

    let (logs, replay) = PartitionLogSet::open_with_encryption_for_partitions(
        dir.path(),
        &catalog,
        64 * 1024,
        None,
        Some(&assigned),
    )
    .unwrap();

    assert!(replay.is_empty());
    assert_eq!(logs.recovery_status().total_partitions, 2);
}

fn request<'a>(
    stream: &'a StreamDefinition,
    key: Option<&'a [u8]>,
    headers: &'a [MessageHeader],
) -> AppendRequest<'a> {
    AppendRequest {
        namespace: "tenant-a",
        stream,
        subject: "orders/created",
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

#[test]
fn encrypted_partition_logs_replay_and_load_payloads_after_restart() {
    let dir = TempDir::new().unwrap();
    let catalog = catalog(1);
    let provider = Arc::new(crate::encryption::MemoryKeyProvider::default());
    provider.insert(crate::encryption::KeyVersion::new(1), [6u8; 32]);
    let keys = Arc::new(
        crate::encryption::KeyRing::new(provider, crate::encryption::KeyVersion::new(1)).unwrap(),
    );
    let (logs, _) =
        PartitionLogSet::open_with_encryption(dir.path(), &catalog, 4096, Some(keys.clone()))
            .unwrap();
    logs.append(request(&catalog.definitions()[0], None, &[]))
        .unwrap();
    logs.flush().unwrap();
    let segment = dir
        .path()
        .join("streams/orders/partition-00000/00000000000000000001.plog");
    let raw = std::fs::read(segment).unwrap();
    assert!(!raw.windows(b"hello".len()).any(|window| window == b"hello"));
    drop(logs);
    let (reopened, _) =
        PartitionLogSet::open_with_encryption(dir.path(), &catalog, 4096, Some(keys)).unwrap();
    assert_eq!(
        reopened
            .load_envelope("orders", PartitionId(0), 0)
            .unwrap()
            .unwrap()
            .payload,
        b"hello"
    );
}

#[test]
fn decoded_metadata_cache_is_populated_by_appends_and_reads() {
    let dir = TempDir::new().unwrap();
    let catalog = catalog(1);
    let logs = PartitionLogSet::open(dir.path(), &catalog, 4096).unwrap().0;
    let stream = &catalog.definitions()[0];
    let envelope = logs
        .append(AppendRequest {
            partition_hint: Some(PartitionId(0)),
            ..request(stream, None, &[])
        })
        .unwrap();
    assert_eq!(logs.metadata_cache_stats(), (1, 0));
    assert_eq!(
        logs.load_envelope("orders", PartitionId(0), envelope.offset)
            .unwrap()
            .unwrap()
            .payload,
        b"hello"
    );
    assert_eq!(logs.metadata_cache_stats(), (1, 0));
}

#[test]
fn partition_resource_is_released_at_flush_and_reopened_for_append() {
    let dir = TempDir::new().unwrap();
    let catalog = catalog(1);
    let logs = PartitionLogSet::open(dir.path(), &catalog, 4096).unwrap().0;
    let stream = &catalog.definitions()[0];
    assert_eq!(logs.active_resource_count(), 1);
    logs.append(AppendRequest {
        partition_hint: Some(PartitionId(0)),
        ..request(stream, None, &[])
    })
    .unwrap();
    logs.flush().unwrap();
    assert_eq!(logs.active_resource_count(), 0);
    let appended = logs
        .append(AppendRequest {
            partition_hint: Some(PartitionId(0)),
            ..request(stream, None, &[])
        })
        .unwrap();
    assert_eq!(appended.offset, 1);
    assert_eq!(logs.active_resource_count(), 1);
}

fn request_for_subject<'a>(stream: &'a StreamDefinition, subject: &'a str) -> AppendRequest<'a> {
    let mut request = request(stream, None, &[]);
    request.subject = subject;
    request
}

#[test]
fn keyed_partition_is_stable_during_epoch() {
    let stream = definition(11);
    let first = select_partition(&stream, "orders/created", Some(b"customer-7"), 0);
    let second = select_partition(&stream, "orders/updated", Some(b"customer-7"), 9);
    assert_eq!(first, second);
}

#[test]
fn envelopes_round_trip_across_partitions_and_restart() {
    let dir = TempDir::new().unwrap();
    let catalog = catalog(4);
    let stream = &catalog.definitions()[0];
    let (logs, replay) = PartitionLogSet::open(dir.path(), &catalog, 220).unwrap();
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

    let (logs, replay) = PartitionLogSet::open(dir.path(), &catalog, 220).unwrap();
    assert_eq!(replay.len(), 2);
    assert!(replay.iter().all(|envelope| envelope.payload.is_empty()));
    for envelope in replay {
        let envelope = logs
            .load_envelope(
                envelope.stream.as_str(),
                envelope.partition,
                envelope.offset,
            )
            .unwrap()
            .unwrap();
        assert!(envelope == first || envelope == second);
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
fn blocked_partition_does_not_serialize_an_independent_partition() {
    let dir = TempDir::new().unwrap();
    let catalog = catalog(2);
    let stream = &catalog.definitions()[0];
    let (logs, _) = PartitionLogSet::open(dir.path(), &catalog, 4096).unwrap();
    let logs = std::sync::Arc::new(logs);
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let blocked = logs.clone();
    let holder = std::thread::spawn(move || {
        blocked.with_partition_lock_for_test("orders", PartitionId(0), || {
            ready_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
    });
    ready_rx.recv().unwrap();

    let mut independent = request(stream, None, &[]);
    independent.partition_hint = Some(PartitionId(1));
    let appended = logs.append(independent).unwrap();
    assert_eq!(appended.partition, PartitionId(1));
    assert_eq!(appended.offset, 0);

    release_tx.send(()).unwrap();
    holder.join().unwrap();
}

#[test]
fn torn_active_tail_is_truncated_and_index_is_rebuilt() {
    let dir = TempDir::new().unwrap();
    let catalog = catalog(1);
    let stream = &catalog.definitions()[0];
    let (logs, _) = PartitionLogSet::open(dir.path(), &catalog, 4096).unwrap();
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
    assert_eq!(replay, vec![expected.clone().into_resident_metadata()]);
    assert_eq!(
        logs.load_envelope("orders", PartitionId(0), 0).unwrap(),
        Some(expected)
    );
    assert!(index.exists());
}

#[test]
fn sealed_checksum_corruption_is_reported() {
    let dir = TempDir::new().unwrap();
    let catalog = catalog(1);
    let stream = &catalog.definitions()[0];
    let (logs, _) = PartitionLogSet::open(dir.path(), &catalog, 1).unwrap();
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
    let (logs, _) = PartitionLogSet::open(dir.path(), &catalog, 4096).unwrap();
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
    let (logs, _) = PartitionLogSet::open(dir.path(), &catalog, 4096).unwrap();
    let first = logs.append(request(stream, None, &[])).unwrap();
    let mut second_request = request(stream, None, &[]);
    second_request.payload = b"uncommitted";
    logs.append(second_request).unwrap();

    logs.rewrite_partition("orders", PartitionId(0), std::slice::from_ref(&first))
        .unwrap();
    drop(logs);
    let (logs, replay) = PartitionLogSet::open(dir.path(), &catalog, 4096).unwrap();
    assert_eq!(replay, vec![first.clone().into_resident_metadata()]);
    assert_eq!(
        logs.load_envelope("orders", PartitionId(0), 0).unwrap(),
        Some(first)
    );
}

#[test]
fn interrupted_sparse_rewrite_installs_atomically_on_restart() {
    let dir = TempDir::new().unwrap();
    let catalog = catalog(1);
    let stream = &catalog.definitions()[0];
    let (logs, _) = PartitionLogSet::open(dir.path(), &catalog, 4096).unwrap();
    let first = logs.append(request(stream, None, &[])).unwrap();
    let mut second_request = request(stream, None, &[]);
    second_request.payload = b"superseded";
    logs.append(second_request).unwrap();
    let mut third_request = request(stream, Some(b"other"), &[]);
    third_request.payload = b"latest";
    let third = logs.append(third_request).unwrap();
    logs.flush().unwrap();

    logs.stage_partition_rewrite_for_test(
        "orders",
        PartitionId(0),
        &[first.clone(), third.clone()],
        3,
    )
    .unwrap();
    drop(logs);

    let (logs, replay) = PartitionLogSet::open(dir.path(), &catalog, 4096).unwrap();
    assert_eq!(
        replay
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        vec![0, 2]
    );
    assert_eq!(
        logs.load_envelope("orders", PartitionId(0), 2).unwrap(),
        Some(third)
    );
    assert_eq!(logs.append(request(stream, None, &[])).unwrap().offset, 3);
}

#[test]
fn sealed_subject_index_matches_ordered_full_scan_results() {
    let dir = TempDir::new().unwrap();
    let catalog = catalog(1);
    let stream = &catalog.definitions()[0];
    let (logs, _) = PartitionLogSet::open(dir.path(), &catalog, 1).unwrap();
    let subjects = [
        "orders/created",
        "orders/eu.created",
        "orders/updated",
        "orders/eu.deleted",
    ];
    for subject in subjects {
        logs.append(request_for_subject(stream, subject)).unwrap();
    }
    for filter in ["orders/created", "orders/*", "orders/**"] {
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
        if filter == "orders/created" {
            assert!(query.used_index);
        }
    }
}

#[test]
fn missing_and_corrupt_subject_indexes_fall_back_and_rebuild() {
    let dir = TempDir::new().unwrap();
    let catalog = catalog(1);
    let stream = &catalog.definitions()[0];
    let (logs, _) = PartitionLogSet::open(dir.path(), &catalog, 1).unwrap();
    logs.append(request_for_subject(stream, "orders/created"))
        .unwrap();
    logs.append(request_for_subject(stream, "orders/updated"))
        .unwrap();
    let sealed = dir
        .path()
        .join("streams/orders/partition-00000/00000000000000000001.sidx");
    drop(logs);

    std::fs::remove_file(&sealed).unwrap();
    let (logs, _) = PartitionLogSet::open(dir.path(), &catalog, 1).unwrap();
    let missing = logs
        .matching_offsets("orders", PartitionId(0), "orders/created")
        .unwrap();
    assert_eq!(missing.offsets, vec![0]);
    assert!(missing.used_index);
    drop(logs);

    std::fs::write(&sealed, b"corrupt index").unwrap();
    let (logs, _) = PartitionLogSet::open(dir.path(), &catalog, 1).unwrap();
    let corrupt = logs
        .matching_offsets("orders", PartitionId(0), "orders/created")
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
    let (logs, _) = PartitionLogSet::open(dir.path(), &catalog, 512 * 1024).unwrap();
    let subjects = (0..10_000)
        .map(|id| format!("orders/{}.event", id % 1_000))
        .collect::<Vec<_>>();
    for subject in &subjects {
        logs.append(request_for_subject(stream, subject)).unwrap();
    }
    for filter in ["orders/42.event", "orders/*/event", "orders/**"] {
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

#[test]
fn age_retention_rewrites_prefix_and_preserves_next_offset() {
    let dir = TempDir::new().unwrap();
    let mut definition = definition(1);
    definition.retention.max_age_ms = Some(10);
    let catalog = StreamCatalog::new(vec![definition]).unwrap();
    let stream = &catalog.definitions()[0];
    let (logs, _) = PartitionLogSet::open(dir.path(), &catalog, 256).unwrap();
    let mut envelopes = Vec::new();
    for timestamp_ms in [0, 10, 20] {
        let mut request = request(stream, None, &[]);
        request.timestamp_ms = timestamp_ms;
        envelopes.push(logs.append(request).unwrap());
    }

    let changes = logs
        .enforce_retention(&mut envelopes, &catalog, 21)
        .unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].earliest_offset, 2);
    assert_eq!(envelopes.len(), 1);
    assert_eq!(envelopes[0].offset, 2);
    drop(logs);

    let (logs, replay) = PartitionLogSet::open(dir.path(), &catalog, 256).unwrap();
    assert_eq!(
        replay,
        envelopes
            .iter()
            .cloned()
            .map(MessageEnvelope::into_resident_metadata)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        logs.load_envelope("orders", PartitionId(0), 2).unwrap(),
        Some(envelopes[0].clone())
    );
    let appended = logs.append(request(stream, None, &[])).unwrap();
    assert_eq!(appended.offset, 3);
}

#[test]
fn byte_retention_keeps_newest_records_within_limit() {
    let dir = TempDir::new().unwrap();
    let base_catalog = catalog(1);
    let stream = &base_catalog.definitions()[0];
    let (logs, _) = PartitionLogSet::open(dir.path(), &base_catalog, 4096).unwrap();
    let mut envelopes = Vec::new();
    for payload in [b"first".as_slice(), b"second", b"third"] {
        let mut request = request(stream, None, &[]);
        request.payload = payload;
        envelopes.push(logs.append(request).unwrap());
    }
    let newest_bytes = codec::encode_batch_with_len(&envelopes[1]).unwrap().len
        + codec::encode_batch_with_len(&envelopes[2]).unwrap().len;
    let mut retained_definition = definition(1);
    retained_definition.retention.max_bytes = Some(newest_bytes);
    let retained_catalog = StreamCatalog::new(vec![retained_definition]).unwrap();

    logs.enforce_retention(&mut envelopes, &retained_catalog, 42)
        .unwrap();
    assert_eq!(
        envelopes
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    let status = logs.retention_status("orders", PartitionId(0)).unwrap();
    assert_eq!(status.earliest_offset, 1);
    assert_eq!(status.retained_messages, 2);
    assert!(status.retained_bytes <= newest_bytes);
}

#[test]
fn combined_retention_can_remove_every_record_without_reusing_offsets() {
    let dir = TempDir::new().unwrap();
    let mut definition = definition(1);
    definition.retention.max_age_ms = Some(1_000);
    definition.retention.max_bytes = Some(1);
    let catalog = StreamCatalog::new(vec![definition]).unwrap();
    let stream = &catalog.definitions()[0];
    let (logs, _) = PartitionLogSet::open(dir.path(), &catalog, 4096).unwrap();
    let first = logs.append(request(stream, None, &[])).unwrap();
    let mut envelopes = vec![first];

    logs.enforce_retention(&mut envelopes, &catalog, 42)
        .unwrap();
    assert!(envelopes.is_empty());
    drop(logs);

    let (logs, replay) = PartitionLogSet::open(dir.path(), &catalog, 4096).unwrap();
    assert!(replay.is_empty());
    assert_eq!(logs.append(request(stream, None, &[])).unwrap().offset, 1);
}

#[test]
fn repeated_byte_retention_reaches_a_bounded_physical_steady_state() {
    let dir = TempDir::new().unwrap();
    let mut definition = definition(1);
    definition.retention.max_bytes = Some(512);
    let catalog = StreamCatalog::new(vec![definition]).unwrap();
    let stream = &catalog.definitions()[0];
    let (logs, _) = PartitionLogSet::open(dir.path(), &catalog, 256).unwrap();
    let mut envelopes = Vec::new();
    let payload = [b'x'; 128];
    for id in 0..100_u64 {
        let mut request = request(stream, None, &[]);
        request.timestamp_ms = id;
        request.payload = &payload;
        envelopes.push(logs.append(request).unwrap());
        logs.enforce_retention(&mut envelopes, &catalog, id)
            .unwrap();
        let status = logs.retention_status("orders", PartitionId(0)).unwrap();
        assert!(status.retained_bytes <= 512);
    }
    logs.flush().unwrap();
    drop(logs);

    let partition = dir.path().join("streams/orders/partition-00000");
    let physical_log_bytes = std::fs::read_dir(&partition)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().and_then(|extension| extension.to_str()) == Some("plog"))
        .map(|path| path.metadata().unwrap().len())
        .sum::<u64>();
    assert!(physical_log_bytes <= codec::SEGMENT_HEADER_LEN + 512);
    let (_, replay) = PartitionLogSet::open(dir.path(), &catalog, 256).unwrap();
    let expected_metadata = envelopes
        .iter()
        .cloned()
        .map(MessageEnvelope::into_resident_metadata)
        .collect::<Vec<_>>();
    assert_eq!(replay, expected_metadata);
}

#[test]
fn large_history_recovery_keeps_payloads_on_disk_and_loads_them_lazily() {
    let dir = TempDir::new().unwrap();
    let catalog = catalog(4);
    let stream = &catalog.definitions()[0];
    let payload = vec![0x5a; 32 * 1024];
    let mut expected = Vec::new();
    let (logs, _) = PartitionLogSet::open(dir.path(), &catalog, 64 * 1024).unwrap();
    for seq in 0..256_u64 {
        let mut append = request(stream, None, &[]);
        append.payload = &payload;
        append.legacy_seq = Some(seq + 1);
        expected.push(logs.append(append).unwrap());
    }
    logs.flush().unwrap();
    drop(logs);

    let (logs, replay) = PartitionLogSet::open(dir.path(), &catalog, 64 * 1024).unwrap();
    let recovery = logs.recovery_status();
    assert_eq!(recovery.completed_partitions, 4);
    assert_eq!(recovery.records_scanned, expected.len());
    assert!(
        replay
            .iter()
            .all(|envelope| envelope.payload.capacity() == 0)
    );
    assert!(recovery.resident_metadata_bytes < payload.len() * expected.len() / 8);

    for wanted in [0, 127, 255] {
        let metadata = &replay[wanted];
        let loaded = logs
            .load_envelope(
                metadata.stream.as_str(),
                metadata.partition,
                metadata.offset,
            )
            .unwrap()
            .unwrap();
        assert_eq!(loaded, expected[wanted]);
    }
}

#[test]
#[ignore = "manual increasing-history recovery benchmark"]
fn benchmark_recovery_across_increasing_histories() {
    for records in [100_usize, 1_000, 10_000] {
        let dir = TempDir::new().unwrap();
        let catalog = catalog(8);
        let stream = &catalog.definitions()[0];
        let payload = vec![0x5a; 1024];
        let (logs, _) = PartitionLogSet::open(dir.path(), &catalog, 64 * 1024).unwrap();
        for seq in 0..records {
            let mut append = request(stream, None, &[]);
            append.payload = &payload;
            append.legacy_seq = Some(seq as u64 + 1);
            logs.append(append).unwrap();
        }
        logs.flush().unwrap();
        drop(logs);

        let started = std::time::Instant::now();
        let (logs, replay) = PartitionLogSet::open(dir.path(), &catalog, 64 * 1024).unwrap();
        assert_eq!(replay.len(), records);
        eprintln!(
            "records={records} elapsed={:?} metadata_bytes={}",
            started.elapsed(),
            logs.recovery_status().resident_metadata_bytes
        );
    }
}
