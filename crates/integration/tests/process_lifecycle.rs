#![cfg(unix)]

use std::{
    env, fs,
    net::{TcpListener, TcpStream},
    path::Path,
    process::{Command, ExitStatus},
    thread,
    time::{Duration, Instant},
};
use tempfile::TempDir;

fn reserved_addr() -> String {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .to_string()
}

fn config(dir: &Path, listen: &str) -> String {
    serde_json::json!({
        "listen": listen,
        "websocket": null,
        "http_listen": null,
        "quotas": {
            "max_connections": 100,
            "max_connections_per_identity": 10,
            "max_transient_subscriptions": 100,
            "max_transient_subscriptions_per_identity": 10,
            "max_durable_consumers": 100,
            "max_durable_consumers_per_identity": 10,
            "max_outbound_bytes_per_connection": 1048576,
            "max_http_connections": 16,
            "max_raft_connections": 16,
            "max_route_connections": 16,
            "client_idle_timeout_ms": 5000,
            "http_header_timeout_ms": 1000
        },
        "wal_dir": dir.join("wal"),
        "wal_segment_bytes": 1048576,
        "fsync_interval_ms": 1,
        "max_payload": 1048576,
        "max_control_line": 8192,
        "max_ack_timeout_ms": 300000,
        "max_in_flight": 4096,
        "max_fetch_messages": 128,
        "max_fetch_bytes": 1048576,
        "max_encoded_batch_bytes": 1048576,
        "verbose": false,
        "tls": null,
        "auth": {"enabled": false, "clients": []},
        "cluster": null
    })
    .to_string()
}

fn wait_for_listener(addr: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("server did not listen on {addr} before the deadline");
}

fn terminate(pid: u32, signal: &str) {
    let status = Command::new("kill")
        .args([format!("-{signal}"), pid.to_string()])
        .status()
        .expect("invoke kill");
    assert!(status.success(), "kill -{signal} {pid} failed: {status}");
}

fn run_signal_case(signal: &str) -> ExitStatus {
    let binary = env::var_os("MORROW_SERVER_BIN")
        .expect("set MORROW_SERVER_BIN to the built morrow-server binary");
    let binary = std::path::PathBuf::from(binary);
    let binary = if binary.is_absolute() || binary.exists() {
        binary
    } else {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(binary)
    };
    let temp = TempDir::new().unwrap();
    let listen = reserved_addr();
    let config_path = temp.path().join("morrow.json");
    fs::write(&config_path, config(temp.path(), &listen)).unwrap();
    let mut child = Command::new(binary)
        .arg(&config_path)
        .env("OTEL_SDK_DISABLED", "true")
        .spawn()
        .expect("start morrow-server");
    wait_for_listener(&listen, Duration::from_secs(10));
    terminate(child.id(), signal);
    child.wait().expect("wait for morrow-server")
}

#[test]
#[ignore = "requires a built binary supplied through MORROW_SERVER_BIN"]
fn real_process_sigterm_gracefully_recovers_storage() {
    assert!(run_signal_case("TERM").success());
    assert!(run_signal_case("INT").success());
}
