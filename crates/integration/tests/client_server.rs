#[path = "client_server/port_allocator.rs"]
mod port_allocator;

#[path = "client_server/support.rs"]
mod support;
use support::*;

#[path = "client_server/client_server_tests.rs"]
mod client_server_tests;

#[path = "client_server/route_advertisement_tests.rs"]
mod route_advertisement_tests;

#[path = "client_server/authorization_middleware_tests.rs"]
mod authorization_middleware_tests;

#[path = "client_server/cluster_delta_tests.rs"]
mod cluster_delta_tests;

#[path = "client_server/cluster_publish_concurrency_tests.rs"]
mod cluster_publish_concurrency_tests;

#[path = "client_server/qos_tests.rs"]
mod qos_tests;

#[path = "client_server/pull_tests.rs"]
mod pull_tests;

#[path = "client_server/internal_tls_tests.rs"]
mod internal_tls_tests;

#[path = "client_server/raft_storage_benchmark.rs"]
mod raft_storage_benchmark;
