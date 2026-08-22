use super::*;

const COMPACT_AFTER_RECORDS: u64 = 1_024;
const COMPACT_AFTER_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Clone)]
pub struct LogStore {
    path: PathBuf,
    inner: Arc<Mutex<LogStoreData>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(super) struct LogStoreData {
    pub(super) vote: Option<Vote<u64>>,
    pub(super) committed: Option<LogId<u64>>,
    pub(super) last_purged_log_id: Option<LogId<u64>>,
    pub(super) logs: BTreeMap<u64, Entry<BrokerRaftConfig>>,
    #[serde(skip)]
    pub(super) journal_records: u64,
    #[serde(skip)]
    pub(super) journal_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum LogRecord {
    Checkpoint(LogStoreData),
    Vote(Vote<u64>),
    Committed(Option<LogId<u64>>),
    Append(Vec<Entry<BrokerRaftConfig>>),
    Truncate(LogId<u64>),
    Purge(LogId<u64>),
}

impl LogStore {
    pub fn open(path: PathBuf, legacy_path: PathBuf) -> Result<Self> {
        migrate_legacy_json::<LogStoreData, LogRecord>(&legacy_path, &path, |data| {
            vec![LogRecord::Checkpoint(data)]
        })?;
        let records = read_journal::<LogRecord>(&path)?;
        let journal_bytes = path.metadata().map(|meta| meta.len()).unwrap_or_default();
        let journal_records = records.len() as u64;
        let mut data = LogStoreData::default();
        for record in records {
            apply_record(&mut data, record);
        }
        data.journal_bytes = journal_bytes;
        data.journal_records = journal_records;
        Ok(Self {
            path,
            inner: Arc::new(Mutex::new(data)),
        })
    }

    fn commit_record(&self, data: &mut LogStoreData, record: LogRecord) -> io::Result<()> {
        let bytes = append_journal(&self.path, &record)?;
        data.journal_bytes = data.journal_bytes.saturating_add(bytes);
        data.journal_records = data.journal_records.saturating_add(1);
        apply_record(data, record);
        if data.journal_records >= COMPACT_AFTER_RECORDS
            || data.journal_bytes >= COMPACT_AFTER_BYTES
        {
            let checkpoint = LogRecord::Checkpoint(data.clone());
            rewrite_journal(&self.path, &[checkpoint])?;
            data.journal_bytes = self.path.metadata()?.len();
            data.journal_records = 1;
        }
        Ok(())
    }

    async fn run_io<T>(
        &self,
        operation: impl FnOnce(LogStore) -> io::Result<T> + Send + 'static,
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
                    io::Error::other(format!("Raft storage worker failed: {err}")),
                )
            })?
            .map_err(|err| storage_error(subject, verb, err))
    }
}

fn apply_record(data: &mut LogStoreData, record: LogRecord) {
    match record {
        LogRecord::Checkpoint(checkpoint) => *data = checkpoint,
        LogRecord::Vote(vote) => data.vote = Some(vote),
        LogRecord::Committed(committed) => data.committed = committed,
        LogRecord::Append(entries) => {
            for entry in entries {
                data.logs.insert(entry.log_id.index, entry);
            }
        }
        LogRecord::Truncate(log_id) => {
            data.logs.split_off(&log_id.index);
        }
        LogRecord::Purge(log_id) => {
            data.logs = data.logs.split_off(&log_id.index.saturating_add(1));
            data.last_purged_log_id = Some(log_id);
        }
    }
}

impl RaftLogReader<BrokerRaftConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> std::result::Result<Vec<Entry<BrokerRaftConfig>>, StorageError<u64>> {
        let data = self.inner.lock().unwrap();
        Ok(data
            .logs
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect())
    }
}

impl RaftLogStorage<BrokerRaftConfig> for LogStore {
    type LogReader = LogStore;

    async fn get_log_state(
        &mut self,
    ) -> std::result::Result<LogState<BrokerRaftConfig>, StorageError<u64>> {
        let data = self.inner.lock().unwrap();
        let last_log_id = data
            .logs
            .values()
            .next_back()
            .map(|entry| entry.log_id)
            .or(data.last_purged_log_id);
        Ok(LogState {
            last_purged_log_id: data.last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> std::result::Result<(), StorageError<u64>> {
        let vote = *vote;
        self.run_io(
            move |store| {
                let mut data = store.inner.lock().unwrap();
                store.commit_record(&mut data, LogRecord::Vote(vote))
            },
            ErrorSubject::Vote,
            ErrorVerb::Write,
        )
        .await
    }

    async fn read_vote(&mut self) -> std::result::Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.inner.lock().unwrap().vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> std::result::Result<(), StorageError<u64>> {
        self.run_io(
            move |store| {
                let mut data = store.inner.lock().unwrap();
                store.commit_record(&mut data, LogRecord::Committed(committed))
            },
            ErrorSubject::Logs,
            ErrorVerb::Write,
        )
        .await
    }

    async fn read_committed(
        &mut self,
    ) -> std::result::Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.inner.lock().unwrap().committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<BrokerRaftConfig>,
    ) -> std::result::Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<BrokerRaftConfig>> + Send,
        I::IntoIter: Send,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        let result = self
            .run_io(
                move |store| {
                    let mut data = store.inner.lock().unwrap();
                    store.commit_record(&mut data, LogRecord::Append(entries))
                },
                ErrorSubject::Logs,
                ErrorVerb::Write,
            )
            .await;
        match &result {
            Ok(()) => callback.log_io_completed(Ok(())),
            Err(err) => callback.log_io_completed(Err(io::Error::other(err.to_string()))),
        }
        result
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> std::result::Result<(), StorageError<u64>> {
        self.run_io(
            move |store| {
                let mut data = store.inner.lock().unwrap();
                store.commit_record(&mut data, LogRecord::Truncate(log_id))
            },
            ErrorSubject::Log(log_id),
            ErrorVerb::Delete,
        )
        .await
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> std::result::Result<(), StorageError<u64>> {
        self.run_io(
            move |store| {
                let mut data = store.inner.lock().unwrap();
                store.commit_record(&mut data, LogRecord::Purge(log_id))
            },
            ErrorSubject::Log(log_id),
            ErrorVerb::Delete,
        )
        .await
    }
}
