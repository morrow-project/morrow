use super::*;
use tempfile::tempdir;

fn update(offset: u64, key: &str, value: Option<&[u8]>) -> ViewUpdate {
    ViewUpdate {
        key: key.into(),
        value: value.map(ToOwned::to_owned),
        position: ViewPosition {
            stream: "orders".into(),
            partition: PartitionId(0),
            offset,
        },
    }
}

fn limits() -> ViewLimits {
    ViewLimits {
        max_entries: 4,
        max_value_bytes: 32,
        watch_capacity: 2,
    }
}

#[test]
fn view_rebuild_snapshot_and_point_reads_are_deterministic() {
    let history = vec![
        update(1, "a", Some(b"new")),
        update(0, "a", Some(b"old")),
        update(2, "b", Some(b"two")),
        update(3, "a", None),
    ];
    let mut first = MaterializedView::new("tenant-a", "orders-by-id", limits()).unwrap();
    first.rebuild(&history).unwrap();
    assert_eq!(first.point_read("a"), None);
    assert_eq!(first.point_read("b"), Some(&b"two"[..]));
    let snapshot = first.snapshot();
    let mut second = MaterializedView::new("tenant-a", "orders-by-id", limits()).unwrap();
    second.restore_snapshot(snapshot).unwrap();
    assert_eq!(first.snapshot(), second.snapshot());
    assert_eq!(first.consistency_positions()["orders:0000000000"], 3);
}

#[test]
fn view_persists_restart_watch_bounds_and_enforces_limits() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("view.json");
    let mut view = MaterializedView::open(&path, "tenant-a", "orders", limits()).unwrap();
    view.apply(update(0, "a", Some(b"one"))).unwrap();
    view.apply(update(1, "b", Some(b"two"))).unwrap();
    view.apply(update(2, "c", Some(b"three"))).unwrap();
    assert!(view.watch_from(0).is_err());
    assert_eq!(view.watch_from(2).unwrap().len(), 1);
    drop(view);
    let mut reopened = MaterializedView::open(path, "tenant-a", "orders", limits()).unwrap();
    assert_eq!(reopened.point_read("b"), Some(&b"two"[..]));
    assert!(reopened.apply(update(3, "d", Some(&[0; 33]))).is_err());
}

#[test]
fn view_updates_are_idempotent_and_reject_stale_positions() {
    let mut view = MaterializedView::new("tenant-a", "orders", limits()).unwrap();
    assert!(view.apply(update(4, "a", Some(b"value"))).unwrap());
    assert!(view.apply(update(4, "a", Some(b"value"))).unwrap());
    assert!(!view.apply(update(3, "a", Some(b"stale"))).unwrap());
    assert_eq!(view.point_read("a"), Some(&b"value"[..]));
}
