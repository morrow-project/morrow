use super::*;
use openraft::CommittedLeaderId;
use std::io::Write;

fn log_id(index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(1, 1), index)
}

fn blank_entry(index: u64) -> Entry<BrokerRaftConfig> {
    Entry {
        log_id: log_id(index),
        payload: EntryPayload::Blank,
    }
}

fn nodes() -> BTreeMap<u64, BasicNode> {
    [(1, BasicNode::new("127.0.0.1:5221"))]
        .into_iter()
        .collect()
}

#[test]
fn journal_repairs_a_torn_tail_without_losing_synced_records() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("test.journal");
    append_journal(&path, &11_u64).unwrap();
    append_journal(&path, &22_u64).unwrap();
    let complete_len = path.metadata().unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(complete_len - 3)
        .unwrap();

    assert_eq!(read_journal::<u64>(&path).unwrap(), vec![11]);
    let repaired_len = path.metadata().unwrap().len();
    assert!(repaired_len < complete_len);
    assert_eq!(read_journal::<u64>(&path).unwrap(), vec![11]);
}

#[tokio::test]
async fn legacy_log_json_is_migrated_once_and_preserves_consensus_metadata() {
    let dir = tempfile::TempDir::new().unwrap();
    let legacy = dir.path().join(LEGACY_LOG_FILE);
    let journal = dir.path().join(LOG_FILE);
    let legacy_data = LogStoreData {
        vote: Some(Vote::new_committed(3, 1)),
        committed: Some(log_id(7)),
        last_purged_log_id: Some(log_id(4)),
        logs: BTreeMap::new(),
        journal_records: 0,
        journal_bytes: 0,
    };
    write_json_atomically(&legacy, &legacy_data).unwrap();

    let mut store = LogStore::open(journal.clone(), legacy.clone()).unwrap();
    assert_eq!(store.read_committed().await.unwrap(), Some(log_id(7)));
    assert_eq!(
        store.read_vote().await.unwrap(),
        Some(Vote::new_committed(3, 1))
    );
    assert!(journal.exists());
    assert!(!legacy.exists());
    assert!(legacy.with_extension("json.migrated").exists());

    let mut reopened = LogStore::open(journal, legacy).unwrap();
    assert_eq!(reopened.read_committed().await.unwrap(), Some(log_id(7)));
}

#[tokio::test]
async fn synced_vote_and_commit_index_survive_reopen() {
    let dir = tempfile::TempDir::new().unwrap();
    let journal = dir.path().join(LOG_FILE);
    let legacy = dir.path().join(LEGACY_LOG_FILE);
    let mut store = LogStore::open(journal.clone(), legacy.clone()).unwrap();
    store.save_vote(&Vote::new_committed(5, 1)).await.unwrap();
    store.save_committed(Some(log_id(12))).await.unwrap();
    drop(store);

    let mut reopened = LogStore::open(journal, legacy).unwrap();
    assert_eq!(
        reopened.read_vote().await.unwrap(),
        Some(Vote::new_committed(5, 1))
    );
    assert_eq!(reopened.read_committed().await.unwrap(), Some(log_id(12)));
}

#[tokio::test]
async fn legacy_state_json_is_migrated_and_preserves_applied_membership() {
    let dir = tempfile::TempDir::new().unwrap();
    let legacy = dir.path().join(LEGACY_STATE_FILE);
    let journal = dir.path().join(STATE_FILE);
    let snapshot = dir.path().join(SNAPSHOT_FILE);
    let legacy_snapshot = dir.path().join(LEGACY_SNAPSHOT_FILE);
    let mut state = DurableState::new(nodes());
    state.last_applied = Some(log_id(9));
    let legacy_data = StateMachineData {
        state,
        snapshot: None,
    };
    write_json_atomically(&legacy, &legacy_data).unwrap();

    let mut store = StateMachineStore::open(
        journal.clone(),
        snapshot,
        legacy.clone(),
        legacy_snapshot,
        nodes(),
    )
    .unwrap();
    let (last_applied, membership) = store.applied_state().await.unwrap();
    assert_eq!(last_applied, Some(log_id(9)));
    assert_eq!(membership.membership().nodes().count(), 1);
    assert!(journal.exists());
    assert!(!legacy.exists());
    assert!(legacy.with_extension("json.migrated").exists());
}

#[tokio::test]
async fn interrupted_snapshot_rotation_recovers_the_atomic_snapshot() {
    let dir = tempfile::TempDir::new().unwrap();
    let journal = dir.path().join(STATE_FILE);
    let snapshot = dir.path().join(SNAPSHOT_FILE);
    let legacy = dir.path().join(LEGACY_STATE_FILE);
    let legacy_snapshot = dir.path().join(LEGACY_SNAPSHOT_FILE);
    let mut store = StateMachineStore::open(
        journal.clone(),
        snapshot.clone(),
        legacy.clone(),
        legacy_snapshot.clone(),
        nodes(),
    )
    .unwrap();
    store.apply(vec![blank_entry(1)]).await.unwrap();
    store.build_snapshot().await.unwrap();

    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&journal)
        .unwrap();
    file.write_all(&[9, 8, 7]).unwrap();
    file.sync_data().unwrap();
    drop(store);

    let mut reopened =
        StateMachineStore::open(journal, snapshot, legacy, legacy_snapshot, nodes()).unwrap();
    let (last_applied, membership) = reopened.applied_state().await.unwrap();
    assert_eq!(last_applied, Some(log_id(1)));
    assert_eq!(membership.membership().nodes().count(), 1);
}

#[tokio::test]
async fn concurrent_apply_and_snapshot_rotation_recover_the_latest_apply() {
    let dir = tempfile::TempDir::new().unwrap();
    let journal = dir.path().join(STATE_FILE);
    let snapshot = dir.path().join(SNAPSHOT_FILE);
    let legacy = dir.path().join(LEGACY_STATE_FILE);
    let legacy_snapshot = dir.path().join(LEGACY_SNAPSHOT_FILE);
    let store = StateMachineStore::open(
        journal.clone(),
        snapshot.clone(),
        legacy.clone(),
        legacy_snapshot.clone(),
        nodes(),
    )
    .unwrap();
    let mut applier = store.clone();
    let mut snapshotter = store.clone();
    let applies = tokio::spawn(async move {
        for index in 1..=40 {
            applier.apply(vec![blank_entry(index)]).await.unwrap();
        }
    });
    let snapshots = tokio::spawn(async move {
        for _ in 0..20 {
            snapshotter.build_snapshot().await.unwrap();
            tokio::task::yield_now().await;
        }
    });
    applies.await.unwrap();
    snapshots.await.unwrap();
    drop(store);

    let mut reopened =
        StateMachineStore::open(journal, snapshot, legacy, legacy_snapshot, nodes()).unwrap();
    assert_eq!(reopened.applied_state().await.unwrap().0, Some(log_id(40)));
}

#[tokio::test]
async fn delta_stream_advances_by_log_index_and_snapshot_install_forces_reconciliation() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut store = StateMachineStore::open(
        dir.path().join(STATE_FILE),
        dir.path().join(SNAPSHOT_FILE),
        dir.path().join(LEGACY_STATE_FILE),
        dir.path().join(LEGACY_SNAPSHOT_FILE),
        nodes(),
    )
    .unwrap();
    store
        .apply(vec![blank_entry(1), blank_entry(2)])
        .await
        .unwrap();
    let DeltaBatch::Incremental(deltas) = store.deltas_after(Some(1)) else {
        panic!("contiguous committed entry should be incremental");
    };
    assert_eq!(deltas.len(), 1);
    assert_eq!(deltas[0].log_id, log_id(2));

    let mut installed = DurableState::new(nodes());
    installed.last_applied = Some(log_id(5));
    let snapshot = serde_json::to_vec(&installed).unwrap();
    let meta = SnapshotMeta {
        last_log_id: Some(log_id(5)),
        last_membership: installed.last_membership.clone(),
        snapshot_id: "installed-5".into(),
    };
    store
        .install_snapshot(&meta, Box::new(snapshot))
        .await
        .unwrap();
    assert!(matches!(
        store.deltas_after(Some(2)),
        DeltaBatch::FullReconciliation
    ));
}

#[test]
fn journal_append_size_is_independent_of_retained_history() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("growth.journal");
    let first = append_journal(&path, &Vote::new_committed(1, 1)).unwrap();
    for term in 2..130 {
        append_journal(&path, &Vote::new_committed(term, 1)).unwrap();
    }
    let last = append_journal(&path, &Vote::new_committed(130, 1)).unwrap();
    assert!(last <= first + 8, "record cost grew with retained history");
}
