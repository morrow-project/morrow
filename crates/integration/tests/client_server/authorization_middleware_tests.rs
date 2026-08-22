use super::*;
use server::middleware::{FailurePolicy, MiddlewareManifest, MiddlewareStage, ResourceBudget};
use std::collections::BTreeSet;

#[tokio::test]
async fn clustered_follower_authorizes_before_leader_middleware() {
    let publisher_auth = ClientAuth::from_seed("publisher1", [8; 32]);
    let harness = ClusterHarness::start_three_with_auth(auth_config_with_permissions(vec![(
        &publisher_auth,
        Some(vec!["orders.*".to_string()]),
        None,
    )]))
    .await;
    let leader = harness.wait_for_leader().await;
    let leader_index = harness
        .nodes
        .iter()
        .position(|node| node.node_id == leader)
        .unwrap();
    harness.brokers[leader_index]
        .middleware_runtime()
        .install(vec![(
            MiddlewareManifest {
                name: "must-not-run".to_string(),
                subject: "events.denied".to_string(),
                stage: MiddlewareStage::Ingress,
                capabilities: BTreeSet::new(),
                failure_policy: FailurePolicy::FailClosed,
                budget: ResourceBudget::default(),
                named_kv: BTreeSet::new(),
                secrets: BTreeSet::new(),
                http_allow_lists: BTreeSet::new(),
            },
            wat::parse_str(
                "(module (func (export \"process\") (param i32) (result i32) unreachable))",
            )
            .unwrap(),
        )])
        .unwrap();
    let follower = harness
        .nodes
        .iter()
        .find(|node| node.node_id != leader)
        .unwrap();
    harness
        .wait_until_follower_knows_leader(follower.node_id, leader)
        .await;

    let mut publisher = Client::connect(follower.client_addr, harness.max_payload)
        .await
        .unwrap();
    let info = publisher.read_info().await.unwrap();
    publisher
        .connect_authenticated(&info, &publisher_auth, false, 5_000, 16)
        .await
        .unwrap();
    publisher.publish("events.denied", b"secret").await.unwrap();
    match publisher.next_frame().await.unwrap().unwrap() {
        ServerFrame::Err(error) => assert!(error.contains("publish not authorized")),
        frame => panic!("expected publish auth error, got {frame:?}"),
    }

    harness.shutdown().await;
}
