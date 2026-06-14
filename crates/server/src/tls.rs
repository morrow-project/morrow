use std::{fs::File, io::BufReader, sync::Arc};

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;

use crate::{
    config::TlsConfig,
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

fn load_certs(config: &TlsConfig) -> Result<Vec<CertificateDer<'static>>> {
    let file = File::open(&config.cert_file)
        .with_context(|| format!("opening TLS certificate {}", config.cert_file.display()))?;
    let mut reader = BufReader::new(file);
    let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<_, _>>()
        .context("reading TLS certificate PEM")?;
    crate::broker_ensure!(
        !certs.is_empty(),
        "TLS certificate file contains no certificates"
    );
    Ok(certs)
}

fn load_key(config: &TlsConfig) -> Result<PrivateKeyDer<'static>> {
    let file = File::open(&config.key_file)
        .with_context(|| format!("opening TLS private key {}", config.key_file.display()))?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .context("reading TLS private key PEM")?
        .ok_or_else(|| BrokerError::msg("TLS private key file contains no private key"))
}
