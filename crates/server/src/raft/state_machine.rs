use super::*;

#[derive(Clone)]
pub struct StateMachineStore {
    path: PathBuf,
    snapshot_path: PathBuf,
    inner: Arc<Mutex<StateMachineData>>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct StateMachineData {
    state: DurableState,
    snapshot: Option<StoredSnapshot>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct StoredSnapshot {
    meta: SnapshotMeta<u64, BasicNode>,
    data: Vec<u8>,
}
impl StateMachineStore {
    pub fn open(
        path: PathBuf,
        snapshot_path: PathBuf,
        nodes: BTreeMap<u64, BasicNode>,
    ) -> Result<Self> {
        let data = match read_json::<StateMachineData>(&path)? {
            Some(data) => data,
            None => StateMachineData {
                state: DurableState::new(nodes),
                snapshot: None,
            },
        };
        Ok(Self {
            path,
            snapshot_path,
            inner: Arc::new(Mutex::new(data)),
        })
    }

    pub fn durable_state(&self) -> DurableState {
        self.inner.lock().unwrap().state.clone()
    }

    fn persist(&self, data: &StateMachineData) -> io::Result<()> {
        write_json_atomically(&self.path, data)
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
        let mut responses = Vec::new();
        {
            let mut data = self.inner.lock().unwrap();
            for entry in entries {
                data.state.last_applied = Some(entry.log_id);
                if let Some(membership) = entry.payload.get_membership() {
                    data.state.last_membership =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                    responses.push(BrokerResponse::Noop);
                    continue;
                }
                match entry.payload {
                    EntryPayload::Blank => responses.push(BrokerResponse::Noop),
                    EntryPayload::Normal(command) => {
                        responses.push(data.state.apply_command(command));
                    }
                    EntryPayload::Membership(_) => unreachable!(),
                }
            }
            self.persist(&data)
                .map_err(|err| storage_error(ErrorSubject::StateMachine, ErrorVerb::Write, err))?;
        }
        Ok(responses)
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
        let snapshot: Snapshot<BrokerRaftConfig> = Snapshot {
            meta: meta.clone(),
            snapshot,
        };
        let mut data = self.inner.lock().unwrap();
        data.state = state;
        let snapshot_bytes = <Vec<u8> as Clone>::clone(&*snapshot.snapshot);
        data.snapshot = Some(StoredSnapshot {
            meta: snapshot.meta.clone(),
            data: snapshot_bytes,
        });
        self.persist(&data)
            .map_err(|err| storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, err))?;
        write_json_atomically(&self.snapshot_path, data.snapshot.as_ref().unwrap())
            .map_err(|err| storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, err))
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> std::result::Result<Option<Snapshot<BrokerRaftConfig>>, StorageError<u64>> {
        if let Some(snapshot) = &self.inner.lock().unwrap().snapshot {
            return Ok(Some(Snapshot {
                meta: snapshot.meta.clone(),
                snapshot: Box::new(snapshot.data.clone()),
            }));
        }
        let Some(snapshot): Option<StoredSnapshot> =
            read_json(&self.snapshot_path).map_err(|err| {
                storage_error(
                    ErrorSubject::Snapshot(None),
                    ErrorVerb::Read,
                    io::Error::new(io::ErrorKind::InvalidData, err.to_string()),
                )
            })?
        else {
            return Ok(None);
        };
        Ok(Some(Snapshot {
            meta: snapshot.meta,
            snapshot: Box::new(snapshot.data),
        }))
    }
}
impl RaftSnapshotBuilder<BrokerRaftConfig> for StateMachineStore {
    async fn build_snapshot(
        &mut self,
    ) -> std::result::Result<Snapshot<BrokerRaftConfig>, StorageError<u64>> {
        let (state, last_applied, last_membership) = {
            let data = self.inner.lock().unwrap();
            (
                data.state.clone(),
                data.state.last_applied,
                data.state.last_membership.clone(),
            )
        };
        let snapshot = serde_json::to_vec(&state).map_err(|err| {
            storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, json_io(err))
        })?;
        let snapshot_id = match last_applied {
            Some(log_id) => format!("{}-{}", log_id.leader_id, log_id.index),
            None => "empty".to_string(),
        };
        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id,
        };
        let snapshot: Snapshot<BrokerRaftConfig> = Snapshot {
            meta,
            snapshot: Box::new(snapshot),
        };
        {
            let mut data = self.inner.lock().unwrap();
            let snapshot_bytes = <Vec<u8> as Clone>::clone(&*snapshot.snapshot);
            data.snapshot = Some(StoredSnapshot {
                meta: snapshot.meta.clone(),
                data: snapshot_bytes,
            });
            self.persist(&data).map_err(|err| {
                storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, err)
            })?;
        }
        let snapshot_bytes = <Vec<u8> as Clone>::clone(&*snapshot.snapshot);
        write_json_atomically(
            &self.snapshot_path,
            &StoredSnapshot {
                meta: snapshot.meta.clone(),
                data: snapshot_bytes,
            },
        )
        .map_err(|err| storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, err))?;
        Ok(snapshot)
    }
}
