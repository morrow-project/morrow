use super::*;
use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
use std::{fs::File, io::BufReader, sync::Arc};
use tokio_rustls::TlsConnector;

#[tokio::test]
async fn secure_cluster_uses_mtls_and_admin_tls() {
    let harness = ClusterHarness::start_three_secure().await;
    harness.wait_for_full_route_mesh().await;
    harness.wait_for_leader().await;

    assert_plaintext_rejected(harness.nodes[0].raft_addr).await;
    assert_plaintext_rejected(harness.nodes[0].route_addr).await;

    let response = admin_tls_request(harness.nodes[0].http_addr, "test-admin-token").await;
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));

    harness.shutdown().await;
}

async fn assert_plaintext_rejected(addr: SocketAddr) {
    let mut plaintext = TcpStream::connect(addr).await.unwrap();
    plaintext.write_all(&[0, 0, 0, 0]).await.unwrap();
    let mut byte = [0_u8; 1];
    let rejected = tokio::time::timeout(Duration::from_secs(1), plaintext.read(&mut byte)).await;
    assert!(rejected.is_ok(), "plaintext connection remained open");
}

#[tokio::test]
async fn admin_tls_rejects_unknown_ca_and_plaintext() {
    let harness = ClusterHarness::start_three_secure().await;
    harness.wait_for_admin_tls(0).await;
    let mut plaintext = TcpStream::connect(harness.nodes[0].http_addr)
        .await
        .unwrap();
    plaintext
        .write_all(b"GET /cluster HTTP/1.1\r\nhost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut bytes = Vec::new();
    let result =
        tokio::time::timeout(Duration::from_secs(1), plaintext.read_to_end(&mut bytes)).await;
    assert!(result.is_ok());
    assert!(!bytes.starts_with(b"HTTP/1.1"));

    let stream = TcpStream::connect(harness.nodes[0].http_addr)
        .await
        .unwrap();
    let connector = tls_connector(internal_fixture("ca-cert.pem"));
    let err = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("certificate") || err.to_string().contains("issuer"));
    harness.shutdown().await;
}

#[tokio::test]
async fn internal_mtls_rejects_unknown_and_expired_peers() {
    let harness = ClusterHarness::start_three_secure().await;
    harness.wait_for_admin_tls(0).await;

    for (cert, key) in [
        (tls_cert_file(), tls_key_file()),
        (
            internal_fixture("expired-cert.pem"),
            internal_fixture("expired-key.pem"),
        ),
    ] {
        let stream = TcpStream::connect(harness.nodes[0].route_addr)
            .await
            .unwrap();
        let connected = internal_client_connector(cert, key)
            .connect(ServerName::try_from("node-1").unwrap(), stream)
            .await;
        if let Ok(mut stream) = connected {
            stream.write_all(&[0]).await.ok();
            let mut response = Vec::new();
            let closed =
                tokio::time::timeout(Duration::from_secs(1), stream.read_to_end(&mut response))
                    .await;
            assert!(closed.is_ok(), "untrusted mTLS peer remained connected");
        }
    }

    let stream = TcpStream::connect(harness.nodes[0].route_addr)
        .await
        .unwrap();
    let err = internal_client_connector(
        internal_fixture("node-2-cert.pem"),
        internal_fixture("node-2-key.pem"),
    )
    .connect(ServerName::try_from("node-2").unwrap(), stream)
    .await
    .unwrap_err();
    assert!(err.to_string().contains("valid for"));
    harness.shutdown().await;
}

#[tokio::test]
async fn route_mtls_rejects_a_hello_for_another_node_id() {
    let harness = ClusterHarness::start_three_secure().await;
    harness.wait_for_admin_tls(0).await;
    let stream = TcpStream::connect(harness.nodes[0].route_addr)
        .await
        .unwrap();
    let mut stream = internal_client_connector(
        internal_fixture("node-2-cert.pem"),
        internal_fixture("node-2-key.pem"),
    )
    .connect(ServerName::try_from("node-1").unwrap(), stream)
    .await
    .unwrap();
    let payload = serde_json::to_vec(&serde_json::json!({
        "auth_token": "test-cluster-token",
        "frame": {
            "type": "hello",
            "node_id": 3,
            "route_addr": "127.0.0.1:1",
            "client_addr": "127.0.0.1:2"
        }
    }))
    .unwrap();
    stream
        .write_all(&(payload.len() as u32).to_be_bytes())
        .await
        .unwrap();
    stream.write_all(&payload).await.unwrap();
    let mut response = Vec::new();
    let closed =
        tokio::time::timeout(Duration::from_secs(1), stream.read_to_end(&mut response)).await;
    assert!(closed.is_ok(), "route accepted a mismatched node identity");
    harness.shutdown().await;
}

async fn admin_tls_request(addr: SocketAddr, token: &str) -> String {
    let stream = TcpStream::connect(addr).await.unwrap();
    let mut stream = tls_connector(tls_ca_cert_file())
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    stream
        .write_all(
            format!(
                "GET /cluster HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer {token}\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    String::from_utf8(response).unwrap()
}

fn tls_connector(ca_file: PathBuf) -> TlsConnector {
    let mut reader = BufReader::new(File::open(ca_file).unwrap());
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let mut roots = RootCertStore::empty();
    roots.add_parsable_certificates(certs);
    TlsConnector::from(Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ))
}

fn internal_client_connector(cert_file: PathBuf, key_file: PathBuf) -> TlsConnector {
    let mut ca_reader = BufReader::new(File::open(internal_fixture("ca-cert.pem")).unwrap());
    let ca_certs = rustls_pemfile::certs(&mut ca_reader)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let mut roots = RootCertStore::empty();
    roots.add_parsable_certificates(ca_certs);
    let mut cert_reader = BufReader::new(File::open(cert_file).unwrap());
    let certs = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let mut key_reader = BufReader::new(File::open(key_file).unwrap());
    let key = rustls_pemfile::private_key(&mut key_reader)
        .unwrap()
        .unwrap();
    TlsConnector::from(Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(certs, key)
            .unwrap(),
    ))
}

fn internal_fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/internal")
        .join(name)
}
