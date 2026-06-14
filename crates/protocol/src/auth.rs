use ed25519_dalek::{Signature, Verifier, VerifyingKey};

use crate::protocol::ConnectAuth;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthError(pub String);

pub type Result<T> = std::result::Result<T, AuthError>;

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for AuthError {}

pub fn nonce() -> Result<String> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes)
        .map_err(|err| AuthError(format!("generating auth nonce: {err}")))?;
    Ok(hex(&bytes))
}

pub fn verify(auth: &ConnectAuth, nonce: &str, public_key: &str) -> Result<String> {
    let public_key_bytes = decode_fixed::<32>(public_key, "public_key")?;
    let signature = decode_fixed::<64>(&auth.signature, "signature")?;
    let verifying_key = VerifyingKey::from_bytes(&public_key_bytes)
        .map_err(|err| AuthError(format!("invalid public key: {err}")))?;
    let signature = Signature::from_bytes(&signature);
    verifying_key
        .verify(nonce.as_bytes(), &signature)
        .map_err(|err| AuthError(format!("invalid public key signature: {err}")))?;
    Ok(auth.client_id.clone())
}

fn decode_fixed<const N: usize>(value: &str, field: &str) -> Result<[u8; N]> {
    let value = value.trim();
    if value.len() != N * 2 {
        return Err(AuthError(format!(
            "{field} must be {} hex characters",
            N * 2
        )));
    }
    let mut out = [0_u8; N];
    for (idx, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        out[idx] = (hex_value(chunk[0], field)? << 4) | hex_value(chunk[1], field)?;
    }
    Ok(out)
}

fn hex_value(byte: u8, field: &str) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(AuthError(format!("{field} must be hex encoded"))),
    }
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::{Signer, SigningKey};

    use super::*;

    #[test]
    fn verifies_signed_nonce() {
        let signing_key = SigningKey::from_bytes(&[7; 32]);
        let public_key = hex(signing_key.verifying_key().as_bytes());
        let nonce = "nonce";
        let signature = hex(&signing_key.sign(nonce.as_bytes()).to_bytes());
        let auth = ConnectAuth {
            client_id: "client1".into(),
            signature,
        };

        assert_eq!(verify(&auth, nonce, &public_key).unwrap(), "client1");
    }

    #[test]
    fn rejects_unsigned_nonce() {
        let signing_key = SigningKey::from_bytes(&[7; 32]);
        let public_key = hex(signing_key.verifying_key().as_bytes());
        let signature = hex(&signing_key.sign(b"other").to_bytes());
        let auth = ConnectAuth {
            client_id: "client1".into(),
            signature,
        };

        assert!(verify(&auth, "nonce", &public_key).is_err());
    }
}
