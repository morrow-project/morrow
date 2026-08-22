#[path = "client_server/support.rs"]
mod support;
use support::*;

#[path = "client_server/client_server_tests.rs"]
mod client_server_tests;

#[path = "client_server/qos_tests.rs"]
mod qos_tests;

#[path = "client_server/pull_tests.rs"]
mod pull_tests;

#[path = "client_server/internal_tls_tests.rs"]
mod internal_tls_tests;
