use super::*;

const MAX_COMMITTED_DELTAS: usize = 4_096;

#[derive(Debug, Clone)]
pub(crate) struct CommittedDelta {
    pub(crate) log_id: LogId<u64>,
    pub(crate) command: Option<BrokerCommand>,
    pub(crate) response: BrokerResponse,
}

pub(crate) enum DeltaBatch {
    Incremental(Vec<CommittedDelta>),
    FullReconciliation,
}

#[derive(Clone)]
pub struct StateMachineStore {
    path: PathBuf,
    metadata_path: PathBuf,
    snapshot_path: PathBuf,
    inner: Arc<Mutex<StateMachineData>>,
    deltas: Arc<Mutex<VecDeque<CommittedDelta>>>,
    applied_index: Arc<AtomicU64>,
    metadata_override: Arc<Mutex<Option<MetadataSnapshot>>>,
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
        let metadata_path = path.with_extension("controller-metadata.json");
        let persisted_metadata = if metadata_path.exists() {
            read_json::<MetadataSnapshot>(&metadata_path)?
        } else {
            None
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
                    let _ = apply_entries_after(&mut data.state, entries, last_applied);
                }
            }
        }
        let applied_index = data
            .state
            .last_applied
            .map_or(0, |log_id| log_id.index.saturating_add(1));
        Ok(Self {
            path,
            metadata_path,
            snapshot_path,
            inner: Arc::new(Mutex::new(data)),
            deltas: Arc::new(Mutex::new(VecDeque::new())),
            applied_index: Arc::new(AtomicU64::new(applied_index)),
            metadata_override: Arc::new(Mutex::new(persisted_metadata)),
        })
    }

    pub fn durable_state(&self) -> DurableState {
        let mut state = self.inner.lock().unwrap().state.clone();
        if let Some(metadata) = self.metadata_override.lock().unwrap().as_ref() {
            state.stream_definitions = metadata.stream_definitions.clone();
            state.partition_assignments = metadata.partition_assignments.clone();
            state.security_references = metadata.security_references.clone();
            state.feature_gates = metadata.feature_gates.clone();
            state.partition_commits = metadata.partition_commits.clone();
        }
        state
    }

    pub(crate) fn install_metadata(&self, metadata: MetadataSnapshot) {
        if let Err(error) = write_json_atomically(&self.metadata_path, &metadata) {
            tracing::warn!(?error, "persisting controller metadata snapshot failed");
        }
        *self.metadata_override.lock().unwrap() = Some(metadata);
    }

    pub(crate) fn deltas_after(&self, after: Option<u64>) -> DeltaBatch {
        let Some(current) = self.applied_index.load(Ordering::Acquire).checked_sub(1) else {
            return DeltaBatch::Incremental(Vec::new());
        };
        if after.is_some_and(|after| after >= current) {
            return DeltaBatch::Incremental(Vec::new());
        }
        let deltas = self.deltas.lock().unwrap();
        let incremental = deltas
            .iter()
            .filter(|delta| after.is_none_or(|after| delta.log_id.index > after))
            .cloned()
            .collect::<Vec<_>>();
        let expected = after.map_or(0, |after| after.saturating_add(1));
        if incremental
            .first()
            .is_none_or(|delta| delta.log_id.index != expected)
        {
            return DeltaBatch::FullReconciliation;
        }
        DeltaBatch::Incremental(incremental)
    }

    pub(crate) fn is_partition_replica(&self, node_id: u64, stream: &str, partition: u32) -> bool {
        self.inner
            .lock()
            .unwrap()
            .state
            .partition_assignments
            .get(&partition_key(stream, partition))
            .is_some_and(|assignment| assignment.replicas.contains(&node_id))
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
) -> (Vec<BrokerResponse>, Vec<CommittedDelta>) {
    let mut responses = Vec::new();
    let mut deltas = Vec::new();
    for entry in entries
        .into_iter()
        .filter(|entry| after.is_none_or(|last| entry.log_id.index > last.index))
    {
        let log_id = entry.log_id;
        let command = match &entry.payload {
            EntryPayload::Normal(command) => Some(command.clone()),
            EntryPayload::Blank | EntryPayload::Membership(_) => None,
        };
        let response = apply_entry(state, entry);
        responses.push(response.clone());
        deltas.push(CommittedDelta {
            log_id,
            command,
            response,
        });
    }
    (responses, deltas)
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
                let (responses, committed) = apply_entries_after(&mut next, entries.clone(), None);
                append_journal(&store.path, &StateRecord::Apply(entries))?;
                data.state = next;
                let mut deltas = store.deltas.lock().unwrap();
                deltas.extend(committed);
                while deltas.len() > MAX_COMMITTED_DELTAS {
                    deltas.pop_front();
                }
                if let Some(last_applied) = data.state.last_applied {
                    store
                        .applied_index
                        .store(last_applied.index.saturating_add(1), Ordering::Release);
                }
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
                store.deltas.lock().unwrap().clear();
                store.applied_index.store(
                    data.state
                        .last_applied
                        .map_or(0, |log_id| log_id.index.saturating_add(1)),
                    Ordering::Release,
                );
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
