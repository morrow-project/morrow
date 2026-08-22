use std::sync::Arc;

use rustls::{
    RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer},
};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::{
    config::{InternalTlsConfig, TlsConfig},
    error::{BrokerError, Result, ResultExt},
};

pub fn load_acceptor(config: &TlsConfig) -> Result<TlsAcceptor> {
    let certs = load_certs(config)?;
    let key = load_key(config)?;
    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("building TLS server config")?;
    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

pub fn load_internal_acceptor(config: &InternalTlsConfig) -> Result<TlsAcceptor> {
    let verifier =
        rustls::server::WebPkiClientVerifier::builder(Arc::new(load_roots(&config.ca_cert_file)?))
            .build()
            .context("building internal TLS client verifier")?;
    let server_config = rustls::ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(
            load_certs_from(&config.cert_file)?,
            load_key_from(&config.key_file)?,
        )
        .context("building internal TLS server config")?;
    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

pub fn load_internal_connector(config: &InternalTlsConfig) -> Result<TlsConnector> {
    let client_config = rustls::ClientConfig::builder()
        .with_root_certificates(load_roots(&config.ca_cert_file)?)
        .with_client_auth_cert(
            load_certs_from(&config.cert_file)?,
            load_key_from(&config.key_file)?,
        )
        .context("building internal TLS client config")?;
    Ok(TlsConnector::from(Arc::new(client_config)))
}

pub fn load_peer_certificates(
    nodes: &[crate::config::ClusterNodeConfig],
) -> Result<std::collections::HashMap<Vec<u8>, u64>> {
    let mut identities = std::collections::HashMap::new();
    for node in nodes {
        for path in &node.tls_cert_files {
            let leaf = load_certs_from(path)?
                .into_iter()
                .next()
                .ok_or_else(|| BrokerError::msg("peer TLS certificate file is empty"))?;
            crate::broker_ensure!(
                identities
                    .insert(leaf.as_ref().to_vec(), node.node_id)
                    .is_none(),
                "peer TLS certificate is assigned to multiple node IDs"
            );
        }
    }
    Ok(identities)
}

pub fn identify_peer(
    certificates: Option<&[CertificateDer<'_>]>,
    identities: &std::collections::HashMap<Vec<u8>, u64>,
) -> Result<u64> {
    let leaf = certificates
        .and_then(|certificates| certificates.first())
        .ok_or_else(|| BrokerError::msg("internal TLS peer did not provide a certificate"))?;
    identities
        .get(leaf.as_ref())
        .copied()
        .ok_or_else(|| BrokerError::msg("internal TLS peer certificate has no configured node ID"))
}

fn load_certs(config: &TlsConfig) -> Result<Vec<CertificateDer<'static>>> {
    load_certs_from(&config.cert_file)
}

fn load_certs_from(path: &std::path::Path) -> Result<Vec<CertificateDer<'static>>> {
    let certs = broker_pem::load_certificates(path).context("reading TLS certificate PEM")?;
    crate::broker_ensure!(
        !certs.is_empty(),
        "TLS certificate file contains no certificates"
    );
    Ok(certs)
}

fn load_key(config: &TlsConfig) -> Result<PrivateKeyDer<'static>> {
    load_key_from(&config.key_file)
}

fn load_key_from(path: &std::path::Path) -> Result<PrivateKeyDer<'static>> {
    broker_pem::load_private_key(path).context("reading TLS private key PEM")
}

fn load_roots(path: &std::path::Path) -> Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    let (added, ignored) = roots.add_parsable_certificates(load_certs_from(path)?);
    crate::broker_ensure!(added > 0 && ignored == 0, "internal TLS CA file is invalid");
    Ok(roots)
}
