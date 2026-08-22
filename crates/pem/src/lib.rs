use rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs1KeyDer, PrivatePkcs8KeyDer, PrivateSec1KeyDer,
    pem::{PemObject, SectionKind},
};
use std::{fmt, path::Path};

#[derive(Debug)]
pub struct PemError(String);

pub fn load_certificates(path: impl AsRef<Path>) -> Result<Vec<CertificateDer<'static>>, PemError> {
    let bytes = read(path.as_ref())?;
    certificates_from_slice(&bytes)
}

pub fn load_private_key(path: impl AsRef<Path>) -> Result<PrivateKeyDer<'static>, PemError> {
    let bytes = read(path.as_ref())?;
    private_key_from_slice(&bytes)
}

pub fn certificates_from_slice(bytes: &[u8]) -> Result<Vec<CertificateDer<'static>>, PemError> {
    validate_envelope(bytes)?;
    let sections = sections(bytes)?;
    if sections.is_empty() {
        return Err(PemError("PEM contains no certificates".to_string()));
    }
    sections
        .into_iter()
        .map(|(kind, bytes)| match kind {
            SectionKind::Certificate => Ok(CertificateDer::from(bytes)),
            _ => Err(PemError(
                "certificate PEM contains unsupported material".to_string(),
            )),
        })
        .collect()
}

pub fn private_key_from_slice(bytes: &[u8]) -> Result<PrivateKeyDer<'static>, PemError> {
    validate_envelope(bytes)?;
    let mut sections = sections(bytes)?;
    if sections.len() != 1 {
        return Err(PemError(
            "private-key PEM must contain exactly one key".to_string(),
        ));
    }
    let (kind, bytes) = sections.pop().unwrap();
    match kind {
        SectionKind::PrivateKey => Ok(PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(bytes))),
        SectionKind::RsaPrivateKey => Ok(PrivateKeyDer::Pkcs1(PrivatePkcs1KeyDer::from(bytes))),
        SectionKind::EcPrivateKey => Ok(PrivateKeyDer::Sec1(PrivateSec1KeyDer::from(bytes))),
        _ => Err(PemError(
            "private-key PEM contains unsupported material".to_string(),
        )),
    }
}

impl fmt::Display for PemError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for PemError {}

fn read(path: &Path) -> Result<Vec<u8>, PemError> {
    std::fs::read(path).map_err(|err| PemError(format!("reading {}: {err}", path.display())))
}

fn sections(bytes: &[u8]) -> Result<Vec<(SectionKind, Vec<u8>)>, PemError> {
    <(SectionKind, Vec<u8>)>::pem_slice_iter(bytes)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| PemError(format!("invalid PEM: {err}")))
}

fn validate_envelope(bytes: &[u8]) -> Result<(), PemError> {
    let text = std::str::from_utf8(bytes).map_err(|_| PemError("PEM is not UTF-8".to_string()))?;
    let mut open_label: Option<&str> = None;
    for raw_line in text.lines() {
        let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
        match open_label {
            None if line.trim().is_empty() => {}
            None => {
                open_label = Some(
                    boundary_label(line, "-----BEGIN ")
                        .ok_or_else(|| PemError("PEM contains trailing material".to_string()))?,
                );
            }
            Some(label) => {
                if let Some(end_label) = boundary_label(line, "-----END ") {
                    if end_label != label {
                        return Err(PemError("PEM boundaries do not match".to_string()));
                    }
                    open_label = None;
                } else if line.starts_with("-----BEGIN ") || line.starts_with("-----END ") {
                    return Err(PemError("PEM contains nested boundaries".to_string()));
                }
            }
        }
    }
    if open_label.is_some() {
        return Err(PemError("PEM section is not terminated".to_string()));
    }
    Ok(())
}

fn boundary_label<'a>(line: &'a str, prefix: &str) -> Option<&'a str> {
    line.strip_prefix(prefix)
        .and_then(|line| line.strip_suffix("-----"))
        .filter(|label| !label.is_empty())
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
