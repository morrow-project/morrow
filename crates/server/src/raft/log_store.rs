use super::*;

#[derive(Clone)]
pub struct LogStore {
    path: PathBuf,
    inner: Arc<Mutex<LogStoreData>>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(super) struct LogStoreData {
    vote: Option<Vote<u64>>,
    committed: Option<LogId<u64>>,
    last_purged_log_id: Option<LogId<u64>>,
    logs: BTreeMap<u64, Entry<BrokerRaftConfig>>,
}
impl LogStore {
    pub fn open(path: PathBuf) -> Result<Self> {
        let data = read_json_or_default(&path)?;
        Ok(Self {
            path,
            inner: Arc::new(Mutex::new(data)),
        })
    }

    fn persist(&self, data: &LogStoreData) -> io::Result<()> {
        write_json_atomically(&self.path, data)
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
        let mut data = self.inner.lock().unwrap();
        data.vote = Some(*vote);
        self.persist(&data)
            .map_err(|err| storage_error(ErrorSubject::Vote, ErrorVerb::Write, err))
    }

    async fn read_vote(&mut self) -> std::result::Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.inner.lock().unwrap().vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> std::result::Result<(), StorageError<u64>> {
        let mut data = self.inner.lock().unwrap();
        data.committed = committed;
        self.persist(&data)
            .map_err(|err| storage_error(ErrorSubject::Logs, ErrorVerb::Write, err))
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
        let result = {
            let mut data = self.inner.lock().unwrap();
            for entry in entries {
                data.logs.insert(entry.log_id.index, entry);
            }
            self.persist(&data)
        };
        match result {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(err) => {
                callback.log_io_completed(Err(io::Error::new(err.kind(), err.to_string())));
                Err(storage_error(ErrorSubject::Logs, ErrorVerb::Write, err))
            }
        }
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> std::result::Result<(), StorageError<u64>> {
        let mut data = self.inner.lock().unwrap();
        data.logs.split_off(&log_id.index);
        self.persist(&data)
            .map_err(|err| storage_error(ErrorSubject::Log(log_id), ErrorVerb::Delete, err))
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> std::result::Result<(), StorageError<u64>> {
        let mut data = self.inner.lock().unwrap();
        let keep = data.logs.split_off(&(log_id.index + 1));
        data.logs = keep;
        data.last_purged_log_id = Some(log_id);
        self.persist(&data)
            .map_err(|err| storage_error(ErrorSubject::Log(log_id), ErrorVerb::Delete, err))
    }
}
