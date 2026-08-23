//! Versioned envelope encryption for data stored outside the live process.
//!
//! The encrypted representation carries only a key version, nonce, and
//! ciphertext. Key bytes are supplied by a [`KeyProvider`] and never become
//! part of the serialized envelope or its debug representation.

use std::{
    collections::BTreeMap,
    fmt,
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

struct KeyRingState {
    active: KeyVersion,
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
            state: RwLock::new(KeyRingState { active }),
        })
    }

    pub fn active_version(&self) -> KeyVersion {
        self.state.read().expect("key ring lock poisoned").active
    }

    pub fn rotate(&self, version: KeyVersion) -> Result<()> {
        self.provider.load_key(version)?;
        self.state.write().expect("key ring lock poisoned").active = version;
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
}
