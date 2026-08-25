use server::{
    backup::BackupEngine, backup::BackupManifest, backup::FileObjectStore, backup::ObjectStore,
};
use std::{
    env,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

type Result<T> = server::error::Result<T>;

fn usage() -> &'static str {
    "Usage:\n  morrow-backup create-full SOURCE OBJECT_STORE BACKUP_ID SOURCE_CLUSTER_ID\n  morrow-backup list OBJECT_STORE\n  morrow-backup inspect OBJECT_STORE BACKUP_ID\n  morrow-backup verify OBJECT_STORE BACKUP_ID\n  morrow-backup restore OBJECT_STORE BACKUP_ID DESTINATION NEW_CLUSTER_ID"
}

fn store(path: &str) -> Result<FileObjectStore> {
    FileObjectStore::new(path)
}

fn manifest(store: &FileObjectStore, backup_id: &str) -> Result<BackupManifest> {
    let bytes = store.get(&format!("backups/{backup_id}/manifest.json"))?;
    serde_json::from_slice(&bytes).map_err(|error| {
        server::error::BrokerError::msg(format!("invalid backup manifest: {error}"))
    })
}

fn verify(store: &FileObjectStore, manifest: &BackupManifest) -> Result<u64> {
    let mut bytes: u64 = 0;
    for object in &manifest.objects {
        let content = store.get(&object.key)?;
        if content.len() as u64 != object.bytes || server::backup::sha256(&content) != object.sha256
        {
            return Err(server::error::BrokerError::msg(format!(
                "backup object verification failed: {}",
                object.key
            )));
        }
        bytes = bytes.saturating_add(content.len() as u64);
    }
    Ok(bytes)
}

fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    let Some(command) = args.next() else {
        println!("{}", usage());
        return Ok(());
    };
    if command == "--help" || command == "-h" {
        println!("{}", usage());
        return Ok(());
    }

    match command.as_str() {
        "create-full" => {
            let source = args
                .next()
                .ok_or_else(|| server::error::BrokerError::msg(usage()))?;
            let object_store = args
                .next()
                .ok_or_else(|| server::error::BrokerError::msg(usage()))?;
            let backup_id = args
                .next()
                .ok_or_else(|| server::error::BrokerError::msg(usage()))?;
            let cluster_id = args
                .next()
                .ok_or_else(|| server::error::BrokerError::msg(usage()))?;
            let created_at_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|error| server::error::BrokerError::msg(error.to_string()))?
                .as_millis() as u64;
            let store = std::sync::Arc::new(store(&object_store)?);
            let manifest = BackupEngine::new(store).create_full(
                Path::new(&source),
                Vec::new(),
                &cluster_id,
                &backup_id,
                created_at_ms,
            )?;
            println!("{}", serde_json::to_string_pretty(&manifest).unwrap());
        }
        "list" => {
            let object_store = args
                .next()
                .ok_or_else(|| server::error::BrokerError::msg(usage()))?;
            for key in store(&object_store)?.list("backups/")? {
                if key.ends_with("/manifest.json") {
                    println!("{key}");
                }
            }
        }
        "inspect" => {
            let object_store = args
                .next()
                .ok_or_else(|| server::error::BrokerError::msg(usage()))?;
            let backup_id = args
                .next()
                .ok_or_else(|| server::error::BrokerError::msg(usage()))?;
            println!(
                "{}",
                serde_json::to_string_pretty(&manifest(&store(&object_store)?, &backup_id)?)
                    .unwrap()
            );
        }
        "verify" => {
            let object_store = args
                .next()
                .ok_or_else(|| server::error::BrokerError::msg(usage()))?;
            let backup_id = args
                .next()
                .ok_or_else(|| server::error::BrokerError::msg(usage()))?;
            let store = store(&object_store)?;
            let manifest = manifest(&store, &backup_id)?;
            let bytes = verify(&store, &manifest)?;
            println!(
                "verified backup_id={} objects={} bytes={}",
                backup_id,
                manifest.objects.len(),
                bytes
            );
        }
        "restore" => {
            let object_store = args
                .next()
                .ok_or_else(|| server::error::BrokerError::msg(usage()))?;
            let backup_id = args
                .next()
                .ok_or_else(|| server::error::BrokerError::msg(usage()))?;
            let destination = args
                .next()
                .ok_or_else(|| server::error::BrokerError::msg(usage()))?;
            let new_cluster_id = args
                .next()
                .ok_or_else(|| server::error::BrokerError::msg(usage()))?;
            let store = std::sync::Arc::new(store(&object_store)?);
            let manifest = manifest(&store, &backup_id)?;
            verify(&store, &manifest)?;
            BackupEngine::new(store).restore(
                &manifest,
                Path::new(&destination),
                &new_cluster_id,
            )?;
            println!(
                "restored backup_id={} destination={}",
                backup_id, destination
            );
        }
        _ => return Err(server::error::BrokerError::msg(usage())),
    }
    Ok(())
}
