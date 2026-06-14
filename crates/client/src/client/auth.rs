use super::*;

impl ClientAuth {
    pub fn new(client_id: impl Into<String>, signing_key: SigningKey) -> Self {
        Self {
            client_id: client_id.into(),
            signing_key,
        }
    }

    pub fn from_seed(client_id: impl Into<String>, seed: [u8; 32]) -> Self {
        Self::new(client_id, SigningKey::from_bytes(&seed))
    }

    pub fn from_seed_hex(client_id: impl Into<String>, seed_hex: &str) -> Result<Self> {
        Ok(Self::from_seed(
            client_id,
            decode_fixed::<32>(seed_hex, "private_key_seed_hex")?,
        ))
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    pub fn public_key_hex(&self) -> String {
        hex(self.signing_key.verifying_key().as_bytes())
    }

    pub(super) fn sign_nonce(&self, nonce: &str) -> String {
        hex(&self.signing_key.sign(nonce.as_bytes()).to_bytes())
    }
}
