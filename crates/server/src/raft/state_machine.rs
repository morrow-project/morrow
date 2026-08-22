use super::*;

#[derive(Clone)]
pub struct StateMachineStore {
    path: PathBuf,
    snapshot_path: PathBuf,
    inner: Arc<Mutex<StateMachineData>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct StateMachineData {
    pub(super) state: DurableState,
    pub(super) snapshot: Option<StoredSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct StoredSnapshot {
    meta: SnapshotMeta<u64, BasicNode>,
    data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum StateRecord {
    Checkpoint(StateMachineData),
    Apply(Vec<Entry<BrokerRaftConfig>>),
}

impl StateMachineStore {
    pub fn open(
        path: PathBuf,
        snapshot_path: PathBuf,
        legacy_path: PathBuf,
        legacy_snapshot_path: PathBuf,
        nodes: BTreeMap<u64, BasicNode>,
    ) -> Result<Self> {
        migrate_legacy_json::<StateMachineData, StateRecord>(&legacy_path, &path, |data| {
            vec![StateRecord::Checkpoint(data)]
        })?;
        let mut data = StateMachineData {
            state: DurableState::new(nodes),
            snapshot: None,
        };
        if snapshot_path.exists() {
            let snapshot = read_json::<StoredSnapshot>(&snapshot_path)?.expect("snapshot exists");
            data.state = decode_snapshot(&snapshot)?;
            data.snapshot = Some(snapshot);
        } else if legacy_snapshot_path.exists() {
            let snapshot = read_json::<StoredSnapshot>(&legacy_snapshot_path)?
                .expect("legacy snapshot exists");
            write_json_atomically(&snapshot_path, &snapshot).with_context(|| {
                format!(
                    "migrating legacy snapshot {}",
                    legacy_snapshot_path.display()
                )
            })?;
            std::fs::rename(
                &legacy_snapshot_path,
                legacy_snapshot_path.with_extension("json.migrated"),
            )
            .with_context(|| {
                format!(
                    "marking legacy snapshot {} as migrated",
                    legacy_snapshot_path.display()
                )
            })?;
            data.state = decode_snapshot(&snapshot)?;
            data.snapshot = Some(snapshot);
        }
        for record in read_journal::<StateRecord>(&path)? {
            match record {
                StateRecord::Checkpoint(checkpoint) => {
                    if checkpoint.state.last_applied > data.state.last_applied {
                        data = checkpoint;
                    } else if data.snapshot.is_none() {
                        data.snapshot = checkpoint.snapshot;
                    }
                }
                StateRecord::Apply(entries) => {
                    let last_applied = data.state.last_applied;
                    apply_entries_after(&mut data.state, entries, last_applied);
                }
            }
        }
        Ok(Self {
            path,
            snapshot_path,
            inner: Arc::new(Mutex::new(data)),
        })
    }

    pub fn durable_state(&self) -> DurableState {
        self.inner.lock().unwrap().state.clone()
    }

    async fn run_io<T>(
        &self,
        operation: impl FnOnce(StateMachineStore) -> io::Result<T> + Send + 'static,
        subject: ErrorSubject<u64>,
        verb: ErrorVerb,
    ) -> std::result::Result<T, StorageError<u64>>
    where
        T: Send + 'static,
    {
        let store = self.clone();
        tokio::task::spawn_blocking(move || operation(store))
            .await
            .map_err(|err| {
                storage_error(
                    subject.clone(),
                    verb,
                    io::Error::other(format!("Raft state worker failed: {err}")),
                )
            })?
            .map_err(|err| storage_error(subject, verb, err))
    }
}

fn decode_snapshot(snapshot: &StoredSnapshot) -> Result<DurableState> {
    serde_json::from_slice(&snapshot.data).context("decoding stored Raft snapshot")
}

fn apply_entries_after(
    state: &mut DurableState,
    entries: Vec<Entry<BrokerRaftConfig>>,
    after: Option<LogId<u64>>,
) -> Vec<BrokerResponse> {
    entries
        .into_iter()
        .filter(|entry| after.is_none_or(|last| entry.log_id.index > last.index))
        .map(|entry| apply_entry(state, entry))
        .collect()
}

fn apply_entry(state: &mut DurableState, entry: Entry<BrokerRaftConfig>) -> BrokerResponse {
    state.last_applied = Some(entry.log_id);
    if let Some(membership) = entry.payload.get_membership() {
        state.last_membership = StoredMembership::new(Some(entry.log_id), membership.clone());
        return BrokerResponse::Noop;
    }
    match entry.payload {
        EntryPayload::Blank => BrokerResponse::Noop,
        EntryPayload::Normal(command) => state.apply_command(command),
        EntryPayload::Membership(_) => unreachable!(),
    }
}

impl RaftStateMachine<BrokerRaftConfig> for StateMachineStore {
    type SnapshotBuilder = StateMachineStore;

    async fn applied_state(
        &mut self,
    ) -> std::result::Result<
        (Option<LogId<u64>>, StoredMembership<u64, BasicNode>),
        StorageError<u64>,
    > {
        let data = self.inner.lock().unwrap();
        Ok((data.state.last_applied, data.state.last_membership.clone()))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> std::result::Result<Vec<BrokerResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<BrokerRaftConfig>> + Send,
        I::IntoIter: Send,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        self.run_io(
            move |store| {
                let mut data = store.inner.lock().unwrap();
                let mut next = data.state.clone();
                let responses = apply_entries_after(&mut next, entries.clone(), None);
                append_journal(&store.path, &StateRecord::Apply(entries))?;
                data.state = next;
                Ok(responses)
            },
            ErrorSubject::StateMachine,
            ErrorVerb::Write,
        )
        .await
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> std::result::Result<Box<Vec<u8>>, StorageError<u64>> {
        Ok(Box::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Vec<u8>>,
    ) -> std::result::Result<(), StorageError<u64>> {
        let state: DurableState = serde_json::from_slice(&snapshot).map_err(|err| {
            storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, json_io(err))
        })?;
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: *snapshot,
        };
        self.run_io(
            move |store| {
                let mut data = store.inner.lock().unwrap();
                write_json_atomically(&store.snapshot_path, &stored)?;
                rewrite_journal::<StateRecord>(&store.path, &[])?;
                data.state = state;
                data.snapshot = Some(stored);
                Ok(())
            },
            ErrorSubject::Snapshot(None),
            ErrorVerb::Write,
        )
        .await
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> std::result::Result<Option<Snapshot<BrokerRaftConfig>>, StorageError<u64>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .snapshot
            .as_ref()
            .map(|snapshot| Snapshot {
                meta: snapshot.meta.clone(),
                snapshot: Box::new(snapshot.data.clone()),
            }))
    }
}

impl RaftSnapshotBuilder<BrokerRaftConfig> for StateMachineStore {
    async fn build_snapshot(
        &mut self,
    ) -> std::result::Result<Snapshot<BrokerRaftConfig>, StorageError<u64>> {
        self.run_io(
            move |store| {
                let mut data = store.inner.lock().unwrap();
                let snapshot_data = serde_json::to_vec(&data.state).map_err(json_io)?;
                let last_applied = data.state.last_applied;
                let meta = SnapshotMeta {
                    last_log_id: last_applied,
                    last_membership: data.state.last_membership.clone(),
                    snapshot_id: last_applied.map_or_else(
                        || "empty".to_string(),
                        |log_id| format!("{}-{}", log_id.leader_id, log_id.index),
                    ),
                };
                let stored = StoredSnapshot {
                    meta: meta.clone(),
                    data: snapshot_data.clone(),
                };
                write_json_atomically(&store.snapshot_path, &stored)?;
                rewrite_journal::<StateRecord>(&store.path, &[])?;
                data.snapshot = Some(stored);
                Ok(Snapshot {
                    meta,
                    snapshot: Box::new(snapshot_data),
                })
            },
            ErrorSubject::Snapshot(None),
            ErrorVerb::Write,
        )
        .await
    }
}
