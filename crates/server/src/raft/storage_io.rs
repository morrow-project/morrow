use super::*;
use crc32fast::Hasher;
use serde::de::DeserializeOwned;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

const JOURNAL_MAGIC: &[u8; 8] = b"BRJNL001";
const RECORD_HEADER_BYTES: usize = 8;
const MAX_JOURNAL_RECORD_BYTES: usize = 64 * 1024 * 1024;

pub(super) fn read_journal<T>(path: &Path) -> Result<Vec<T>>
where
    T: DeserializeOwned,
{
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("opening Raft journal {}", path.display()))?;
    if file.metadata()?.len() < JOURNAL_MAGIC.len() as u64 {
        repair_journal_tail(&mut file, 0, path)?;
        return Ok(Vec::new());
    }
    let mut magic = [0_u8; JOURNAL_MAGIC.len()];
    file.read_exact(&mut magic)
        .with_context(|| format!("reading Raft journal header {}", path.display()))?;
    crate::broker_ensure!(
        &magic == JOURNAL_MAGIC,
        "unsupported Raft journal format {}",
        path.display()
    );

    let mut records = Vec::new();
    let mut valid_len = JOURNAL_MAGIC.len() as u64;
    loop {
        let mut header = [0_u8; RECORD_HEADER_BYTES];
        let read = file
            .read(&mut header[..1])
            .with_context(|| format!("reading Raft journal {}", path.display()))?;
        if read == 0 {
            break;
        }
        if file.read_exact(&mut header[1..]).is_err() {
            repair_journal_tail(&mut file, valid_len, path)?;
            break;
        }
        let body_len = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
        crate::broker_ensure!(
            body_len <= MAX_JOURNAL_RECORD_BYTES,
            "Raft journal record exceeds maximum in {}",
            path.display()
        );
        let expected_crc = u32::from_le_bytes(header[4..].try_into().unwrap());
        let mut body = vec![0_u8; body_len];
        if file.read_exact(&mut body).is_err() {
            repair_journal_tail(&mut file, valid_len, path)?;
            break;
        }
        let actual_crc = crc(&body);
        crate::broker_ensure!(
            actual_crc == expected_crc,
            "Raft journal checksum mismatch in {} at byte {}",
            path.display(),
            valid_len
        );
        let record = serde_json::from_slice(&body)
            .with_context(|| format!("decoding Raft journal {}", path.display()))?;
        records.push(record);
        valid_len = valid_len.saturating_add((RECORD_HEADER_BYTES + body_len) as u64);
    }
    Ok(records)
}

pub(super) fn append_journal<T>(path: &Path, value: &T) -> io::Result<u64>
where
    T: Serialize,
{
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let body = serde_json::to_vec(value).map_err(json_io)?;
    if body.len() > MAX_JOURNAL_RECORD_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Raft journal record exceeds maximum",
        ));
    }
    let is_new = !path.exists() || path.metadata()?.len() == 0;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    if is_new {
        file.write_all(JOURNAL_MAGIC)?;
    }
    file.write_all(&(body.len() as u32).to_le_bytes())?;
    file.write_all(&crc(&body).to_le_bytes())?;
    file.write_all(&body)?;
    file.sync_data()?;
    if is_new {
        sync_parent(path)?;
    }
    Ok((RECORD_HEADER_BYTES + body.len()) as u64)
}

pub(super) fn rewrite_journal<T>(path: &Path, values: &[T]) -> io::Result<()>
where
    T: Serialize,
{
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("journal.tmp");
    let mut file = File::create(&tmp)?;
    file.write_all(JOURNAL_MAGIC)?;
    for value in values {
        let body = serde_json::to_vec(value).map_err(json_io)?;
        if body.len() > MAX_JOURNAL_RECORD_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Raft journal record exceeds maximum",
            ));
        }
        file.write_all(&(body.len() as u32).to_le_bytes())?;
        file.write_all(&crc(&body).to_le_bytes())?;
        file.write_all(&body)?;
    }
    file.sync_data()?;
    std::fs::rename(&tmp, path)?;
    sync_parent(path)
}

pub(super) fn migrate_legacy_json<T, R>(
    legacy_path: &Path,
    journal_path: &Path,
    convert: impl FnOnce(T) -> Vec<R>,
) -> Result<()>
where
    T: DeserializeOwned,
    R: Serialize,
{
    if journal_path.exists() || !legacy_path.exists() {
        return Ok(());
    }
    let Some(value) = read_json::<T>(legacy_path)? else {
        return Ok(());
    };
    rewrite_journal(journal_path, &convert(value))
        .with_context(|| format!("migrating legacy Raft file {}", legacy_path.display()))?;
    let migrated = legacy_path.with_extension("json.migrated");
    std::fs::rename(legacy_path, &migrated).with_context(|| {
        format!(
            "marking legacy Raft file {} as migrated",
            legacy_path.display()
        )
    })?;
    sync_parent(legacy_path)?;
    Ok(())
}

fn repair_journal_tail(file: &mut File, valid_len: u64, path: &Path) -> Result<()> {
    file.set_len(valid_len)
        .with_context(|| format!("repairing Raft journal {}", path.display()))?;
    file.seek(SeekFrom::Start(valid_len))
        .with_context(|| format!("seeking repaired Raft journal {}", path.display()))?;
    file.sync_data()
        .with_context(|| format!("syncing repaired Raft journal {}", path.display()))?;
    Ok(())
}

fn crc(body: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(body);
    hasher.finalize()
}

fn sync_parent(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        crate::storage::sync_dir(parent)?;
    }
    Ok(())
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
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&tmp)?;
    file.sync_data()?;
    std::fs::rename(&tmp, path)?;
    sync_parent(path)
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
