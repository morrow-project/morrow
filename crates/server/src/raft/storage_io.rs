use super::*;

pub(super) fn read_json_or_default<T>(path: &Path) -> Result<T>
where
    T: for<'de> Deserialize<'de> + Default,
{
    Ok(read_json(path)?.unwrap_or_default())
}

pub(super) fn read_json<T>(path: &Path) -> Result<Option<T>>
where
    T: for<'de> Deserialize<'de>,
{
    if !path.exists() {
        return Ok(None);
    }
    let contents =
        std::fs::read(path).with_context(|| format!("reading Raft file {}", path.display()))?;
    let value = serde_json::from_slice(&contents)
        .with_context(|| format!("parsing Raft file {}", path.display()))?;
    Ok(Some(value))
}

pub(super) fn write_json_atomically<T>(path: &Path, value: &T) -> io::Result<()>
where
    T: Serialize,
{
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    let body = serde_json::to_vec(value).map_err(json_io)?;
    std::fs::write(&tmp, body)?;
    let file = std::fs::OpenOptions::new().read(true).open(&tmp)?;
    file.sync_data()?;
    std::fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        let dir = std::fs::File::open(parent)?;
        dir.sync_data()?;
    }
    Ok(())
}

pub(super) fn storage_error(
    subject: ErrorSubject<u64>,
    verb: ErrorVerb,
    err: io::Error,
) -> StorageError<u64> {
    StorageError::from_io_error(subject, verb, err)
}

pub(super) fn json_io(err: serde_json::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}

pub(super) fn network_error(
    message: impl Into<String>,
) -> RPCError<u64, BasicNode, openraft::error::RaftError<u64>> {
    RPCError::Network(NetworkError::new(&SimpleError(message.into())))
}

#[derive(Debug)]
pub(super) struct SimpleError(pub(super) String);

impl fmt::Display for SimpleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for SimpleError {}
