//! Versioned envelope encryption for data stored outside the live process.
//!
//! The encrypted representation carries only a key version, nonce, and
//! ciphertext. Key bytes are supplied by a [`KeyProvider`] and never become
//! part of the serialized envelope or its debug representation.

use std::{
    collections::BTreeMap,
    fmt, fs,
    path::PathBuf,
    sync::{Arc, RwLock},
};

use aws_lc_rs::aead::{AES_256_GCM, Aad, Nonce, RandomizedNonceKey};
use serde::{Deserialize, Serialize};

use crate::error::{BrokerError, Result};

const KEY_LEN: usize = 32;
const NONCE_LEN: usize = 12;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, Serialize, PartialOrd)]
pub struct KeyVersion(u32);

impl KeyVersion {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }
    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct KeyMetadata {
    pub active_version: KeyVersion,
    pub retained_versions: Vec<KeyVersion>,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct EncryptedBlob {
    pub key_version: KeyVersion,
    pub nonce: Vec<u8>,
    pub ciphertext: Vec<u8>,
}

impl fmt::Debug for EncryptedBlob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncryptedBlob")
            .field("key_version", &self.key_version)
            .field("nonce_len", &self.nonce.len())
            .field("ciphertext_len", &self.ciphertext.len())
            .finish()
    }
}

/// KMS boundary used by the storage encryption layer.
pub trait KeyProvider: Send + Sync {
    fn load_key(&self, version: KeyVersion) -> Result<[u8; KEY_LEN]>;
}

/// In-memory KMS emulator for unit and integration tests.
#[derive(Default)]
pub struct MemoryKeyProvider {
    keys: RwLock<BTreeMap<KeyVersion, [u8; KEY_LEN]>>,
}

impl MemoryKeyProvider {
    pub fn insert(&self, version: KeyVersion, key: [u8; KEY_LEN]) {
        self.keys
            .write()
            .expect("key provider lock poisoned")
            .insert(version, key);
    }

    pub fn revoke(&self, version: KeyVersion) {
        self.keys
            .write()
            .expect("key provider lock poisoned")
            .remove(&version);
    }
}

impl KeyProvider for MemoryKeyProvider {
    fn load_key(&self, version: KeyVersion) -> Result<[u8; KEY_LEN]> {
        self.keys
            .read()
            .expect("key provider lock poisoned")
            .get(&version)
            .copied()
            .ok_or_else(|| BrokerError::msg("encryption key version unavailable"))
    }
}

/// File-backed KMS adapter. The directory contains one exact 32-byte file per
/// version (`key-<version>.bin`); the path and failures are safe to include in
/// diagnostics, but key bytes are never returned in an error.
pub struct FileKeyProvider {
    directory: PathBuf,
}

impl FileKeyProvider {
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
        }
    }
}

impl fmt::Debug for FileKeyProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileKeyProvider")
            .field("directory", &self.directory)
            .finish()
    }
}

impl KeyProvider for FileKeyProvider {
    fn load_key(&self, version: KeyVersion) -> Result<[u8; KEY_LEN]> {
        let path = self.directory.join(format!("key-{}.bin", version.get()));
        let bytes = fs::read(&path).map_err(|_| BrokerError::msg("encryption key unavailable"))?;
        let key: [u8; KEY_LEN] = bytes
            .try_into()
            .map_err(|_| BrokerError::msg("encryption key has invalid length"))?;
        Ok(key)
    }
}

struct KeyRingState {
    active: KeyVersion,
    retained: std::collections::BTreeSet<KeyVersion>,
}

/// Envelope encryption with online key rotation.
///
/// Rotation changes the active version only; existing data remains readable
/// through its embedded version until the provider revokes that key.
pub struct KeyRing {
    provider: Arc<dyn KeyProvider>,
    state: RwLock<KeyRingState>,
}

impl fmt::Debug for KeyRing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyRing")
            .field("active", &self.active_version())
            .finish_non_exhaustive()
    }
}

impl KeyRing {
    pub fn new(provider: Arc<dyn KeyProvider>, active: KeyVersion) -> Result<Self> {
        provider.load_key(active)?;
        Ok(Self {
            provider,
            state: RwLock::new(KeyRingState {
                active,
                retained: [active].into_iter().collect(),
            }),
        })
    }

    pub fn active_version(&self) -> KeyVersion {
        self.state.read().expect("key ring lock poisoned").active
    }

    pub fn metadata(&self) -> KeyMetadata {
        let state = self.state.read().expect("key ring lock poisoned");
        KeyMetadata {
            active_version: state.active,
            retained_versions: state.retained.iter().copied().collect(),
        }
    }

    pub fn restore_active(&self, metadata: &KeyMetadata) -> Result<()> {
        self.rotate(metadata.active_version)
    }

    pub fn rotate(&self, version: KeyVersion) -> Result<()> {
        self.provider.load_key(version)?;
        let mut state = self.state.write().expect("key ring lock poisoned");
        let previous = state.active;
        state.retained.insert(previous);
        state.retained.insert(version);
        state.active = version;
        Ok(())
    }

    pub fn encrypt(&self, plaintext: &[u8], aad: &[u8]) -> Result<EncryptedBlob> {
        let version = self.active_version();
        let key_bytes = self.provider.load_key(version)?;
        let key = RandomizedNonceKey::new(&AES_256_GCM, &key_bytes)
            .map_err(|_| BrokerError::msg("encryption key initialization failed"))?;
        let mut ciphertext = plaintext.to_vec();
        let nonce = key
            .seal_in_place_append_tag(Aad::from(aad), &mut ciphertext)
            .map_err(|_| BrokerError::msg("encryption failed"))?;
        Ok(EncryptedBlob {
            key_version: version,
            nonce: nonce.as_ref().to_vec(),
            ciphertext,
        })
    }

    pub fn decrypt(&self, blob: &EncryptedBlob, aad: &[u8]) -> Result<Vec<u8>> {
        if blob.nonce.len() != NONCE_LEN {
            return Err(BrokerError::msg("invalid encryption nonce"));
        }
        let key_bytes = self.provider.load_key(blob.key_version)?;
        let key = RandomizedNonceKey::new(&AES_256_GCM, &key_bytes)
            .map_err(|_| BrokerError::msg("encryption key initialization failed"))?;
        let nonce = Nonce::try_assume_unique_for_key(&blob.nonce)
            .map_err(|_| BrokerError::msg("invalid encryption nonce"))?;
        let mut plaintext = blob.ciphertext.clone();
        let plaintext = key
            .open_in_place(nonce, Aad::from(aad), &mut plaintext)
            .map_err(|_| BrokerError::msg("encrypted data authentication failed"))?;
        Ok(plaintext.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::tempdir;

    struct FlakyProvider {
        key: [u8; KEY_LEN],
        failures: AtomicUsize,
    }

    impl KeyProvider for FlakyProvider {
        fn load_key(&self, _version: KeyVersion) -> Result<[u8; KEY_LEN]> {
            let remaining = self.failures.load(Ordering::Relaxed);
            if remaining > 0 {
                self.failures.fetch_sub(1, Ordering::Relaxed);
                return Err(BrokerError::msg("KMS temporarily unavailable"));
            }
            Ok(self.key)
        }
    }

    fn provider() -> Arc<MemoryKeyProvider> {
        let provider = Arc::new(MemoryKeyProvider::default());
        provider.insert(KeyVersion::new(1), [1u8; KEY_LEN]);
        provider.insert(KeyVersion::new(2), [2u8; KEY_LEN]);
        provider
    }

    #[test]
    fn round_trip_authenticates_aad_without_logging_plaintext() {
        let provider = provider();
        let ring = KeyRing::new(provider, KeyVersion::new(1)).unwrap();
        let blob = ring
            .encrypt(b"partition payload", b"wal:tenant-a/stream-a")
            .unwrap();
        assert_eq!(
            ring.decrypt(&blob, b"wal:tenant-a/stream-a").unwrap(),
            b"partition payload"
        );
        assert!(!format!("{blob:?}").contains("partition payload"));
        assert!(
            !serde_json::to_string(&blob)
                .unwrap()
                .contains("partition payload")
        );
    }

    #[test]
    fn rotation_keeps_old_data_readable_and_new_data_on_new_key() {
        let provider = provider();
        let ring = KeyRing::new(provider.clone(), KeyVersion::new(1)).unwrap();
        let old = ring.encrypt(b"old", b"checkpoint").unwrap();
        ring.rotate(KeyVersion::new(2)).unwrap();
        let new = ring.encrypt(b"new", b"checkpoint").unwrap();
        assert_eq!(old.key_version, KeyVersion::new(1));
        assert_eq!(new.key_version, KeyVersion::new(2));
        assert_eq!(ring.decrypt(&old, b"checkpoint").unwrap(), b"old");
        assert_eq!(ring.decrypt(&new, b"checkpoint").unwrap(), b"new");
        let metadata = ring.metadata();
        assert_eq!(metadata.active_version, KeyVersion::new(2));
        ring.restore_active(&KeyMetadata {
            active_version: KeyVersion::new(1),
            retained_versions: vec![KeyVersion::new(2)],
        })
        .unwrap();
        assert_eq!(ring.active_version(), KeyVersion::new(1));
    }

    #[test]
    fn tampering_wrong_aad_and_revoked_keys_fail() {
        let provider = provider();
        let ring = KeyRing::new(provider.clone(), KeyVersion::new(1)).unwrap();
        let mut blob = ring.encrypt(b"secret", b"object-key").unwrap();
        blob.ciphertext[0] ^= 1;
        assert!(ring.decrypt(&blob, b"object-key").is_err());
        let blob = ring.encrypt(b"secret", b"object-key").unwrap();
        assert!(ring.decrypt(&blob, b"other-key").is_err());
        provider.revoke(KeyVersion::new(1));
        assert!(ring.decrypt(&blob, b"object-key").is_err());
        assert!(ring.rotate(KeyVersion::new(1)).is_err());
    }

    #[test]
    fn file_key_provider_has_exact_length_and_versioned_key_files() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("key-7.bin"), [7u8; KEY_LEN]).unwrap();
        let provider = FileKeyProvider::new(dir.path());
        assert_eq!(
            provider.load_key(KeyVersion::new(7)).unwrap(),
            [7u8; KEY_LEN]
        );
        fs::write(dir.path().join("key-8.bin"), [8u8; KEY_LEN - 1]).unwrap();
        assert!(provider.load_key(KeyVersion::new(8)).is_err());
        assert!(provider.load_key(KeyVersion::new(9)).is_err());
    }

    #[test]
    fn kms_outage_and_throttling_fail_closed_then_recover_without_stale_keys() {
        let provider = Arc::new(FlakyProvider {
            key: [3u8; KEY_LEN],
            failures: AtomicUsize::new(2),
        });
        assert!(KeyRing::new(provider.clone(), KeyVersion::new(1)).is_err());
        let provider = Arc::new(FlakyProvider {
            key: [3u8; KEY_LEN],
            failures: AtomicUsize::new(0),
        });
        let ring = KeyRing::new(provider.clone(), KeyVersion::new(1)).unwrap();
        let blob = ring.encrypt(b"kms", b"object").unwrap();
        provider.failures.store(1, Ordering::Relaxed);
        assert!(ring.decrypt(&blob, b"object").is_err());
        assert_eq!(ring.decrypt(&blob, b"object").unwrap(), b"kms");
    }
}
