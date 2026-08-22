use super::*;
use crate::middleware::{
    Capability, FailurePolicy, MiddlewareManifest, MiddlewareStage, ResourceBudget,
};

#[tokio::test]
async fn ingress_subject_mutation_precedes_routing_and_partition_append() {
    let scenario = Scenario::new();
    scenario
        .broker()
        .middleware_runtime()
        .install(vec![(
            MiddlewareManifest {
                name: "rewrite".to_string(),
                subject: "orders/created".to_string(),
                stage: MiddlewareStage::Ingress,
                capabilities: [Capability::WriteSubject].into_iter().collect(),
                failure_policy: FailurePolicy::FailClosed,
                budget: ResourceBudget::default(),
                named_kv: BTreeSet::new(),
                secrets: BTreeSet::new(),
                http_allow_lists: BTreeSet::new(),
            },
            wat::parse_str(
                "(module
                    (import \"broker\" \"set-field\" (func $set (param i32 i32 i32) (result i32)))
                    (memory (export \"memory\") 1)
                    (data (i32.const 0) \"orders/updated\")
                    (func (export \"process\") (param i32) (result i32)
                      i32.const 0 i32.const 0 i32.const 14 call $set drop i32.const 0))",
            )
            .unwrap(),
        )])
        .unwrap();
    let mut subscriber = scenario.connect_durable("subscriber", 1_000).await;
    let mut publisher = scenario.connect_durable("publisher", 1_000).await;
    subscriber.subscribe("orders/updated", "sid1").await;
    subscriber.ping_roundtrip().await;

    publisher.publish("orders/created", b"hello").await;
    let delivery = subscriber.expect_msg().await;
    assert!(delivery.starts_with("DELIVER orders/updated sid1 "));
    assert_eq!(
        scenario.broker().inner.lock().await.messages[&1].subject,
        "orders/updated"
    );
}

#[tokio::test]
async fn before_deliver_mutation_is_a_projection_not_a_stored_record_change() {
    let scenario = Scenario::new();
    scenario
        .broker()
        .middleware_runtime()
        .install(vec![(
            MiddlewareManifest {
                name: "delivery-projection".to_string(),
                subject: "orders/**".to_string(),
                stage: MiddlewareStage::BeforeDeliver,
                capabilities: [Capability::WritePayload].into_iter().collect(),
                failure_policy: FailurePolicy::FailClosed,
                budget: ResourceBudget::default(),
                named_kv: BTreeSet::new(),
                secrets: BTreeSet::new(),
                http_allow_lists: BTreeSet::new(),
            },
            wat::parse_str(
                "(module
                    (import \"broker\" \"set-field\" (func $set (param i32 i32 i32) (result i32)))
                    (memory (export \"memory\") 1)
                    (data (i32.const 0) \"changed\")
                    (func (export \"process\") (param i32) (result i32)
                      i32.const 3 i32.const 0 i32.const 7 call $set drop i32.const 0))",
            )
            .unwrap(),
        )])
        .unwrap();
    let mut subscriber = scenario.connect_durable("subscriber", 1_000).await;
    let mut publisher = scenario.connect_durable("publisher", 1_000).await;
    subscriber.subscribe("orders/*", "sid1").await;
    subscriber.ping_roundtrip().await;

    publisher.publish("orders/created", b"hello").await;
    assert!(subscriber.expect_msg().await.ends_with("7\r\nchanged\r\n"));
    let metadata = scenario.broker().inner.lock().await.messages[&1].clone();
    assert_eq!(
        scenario
            .broker()
            .partition_logs
            .load_record(&metadata)
            .unwrap()
            .payload,
        b"hello"
    );
}
