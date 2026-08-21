use super::*;
use std::collections::BTreeSet;

fn manifest(capabilities: impl IntoIterator<Item = Capability>) -> MiddlewareManifest {
    MiddlewareManifest {
        name: "test-policy".to_string(),
        subject: "orders.>".to_string(),
        stage: MiddlewareStage::Ingress,
        capabilities: capabilities.into_iter().collect(),
        failure_policy: FailurePolicy::FailClosed,
        budget: ResourceBudget::default(),
        named_kv: BTreeSet::new(),
        secrets: BTreeSet::new(),
        http_allow_lists: BTreeSet::new(),
    }
}

fn message() -> MiddlewareMessage {
    MiddlewareMessage {
        subject: "orders.created".to_string(),
        key: None,
        headers: Vec::new(),
        payload: b"hello".to_vec(),
        reply_to: None,
    }
}

fn wasm(source: &str) -> Vec<u8> {
    wat::parse_str(source).unwrap()
}

#[test]
fn traps_and_instruction_deadlines_are_interrupted() {
    let runtime = MiddlewareRuntime::new().unwrap();
    runtime
        .install(vec![(
            manifest([]),
            wasm("(module (func (export \"process\") (param i32) (result i32) unreachable))"),
        )])
        .unwrap();
    assert!(
        runtime
            .process(MiddlewareStage::Ingress, message(), 0)
            .is_err()
    );

    let mut fuel = manifest([]);
    fuel.budget.max_fuel = 100;
    runtime
        .install(vec![(
            fuel,
            wasm("(module (func (export \"process\") (param i32) (result i32) (loop br 0) i32.const 0))"),
        )])
        .unwrap();
    assert!(
        runtime
            .process(MiddlewareStage::Ingress, message(), 0)
            .is_err()
    );

    let mut deadline = manifest([]);
    deadline.budget.max_fuel = u64::MAX;
    deadline.budget.deadline = std::time::Duration::from_millis(2);
    runtime
        .install(vec![(
            deadline,
            wasm("(module (func (export \"process\") (param i32) (result i32) (loop br 0) i32.const 0))"),
        )])
        .unwrap();
    let started = std::time::Instant::now();
    assert!(
        runtime
            .process(MiddlewareStage::Ingress, message(), 0)
            .is_err()
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(1));
}

#[test]
fn memory_growth_is_bounded() {
    let runtime = MiddlewareRuntime::new().unwrap();
    let mut limited = manifest([]);
    limited.budget.max_memory_bytes = 64 * 1024;
    runtime
        .install(vec![(
            limited,
            wasm(
                "(module
                    (memory (export \"memory\") 1 100)
                    (func (export \"process\") (param i32) (result i32)
                      i32.const 1 memory.grow i32.const -1 i32.ne if unreachable end
                      i32.const 0))",
            ),
        )])
        .unwrap();
    assert_eq!(
        runtime
            .process(MiddlewareStage::Ingress, message(), 0)
            .unwrap()
            .decision,
        MiddlewareDecision::Continue
    );
}

#[test]
fn denied_host_calls_and_stage_mutations_fail_closed() {
    let runtime = MiddlewareRuntime::new().unwrap();
    runtime
        .install(vec![(
            manifest([]),
            wasm(
                "(module
                    (import \"broker\" \"host-call\" (func $host (param i32) (result i32)))
                    (func (export \"process\") (param i32) (result i32)
                      i32.const 3 call $host drop i32.const 0))",
            ),
        )])
        .unwrap();
    assert!(
        runtime
            .process(MiddlewareStage::Ingress, message(), 0)
            .unwrap_err()
            .to_string()
            .contains("undeclared capability")
    );

    runtime
        .install(vec![(
            manifest([]),
            wasm(
                "(module
                    (import \"broker\" \"get-field\" (func $get (param i32 i32 i32) (result i32)))
                    (memory (export \"memory\") 1)
                    (func (export \"process\") (param i32) (result i32)
                      i32.const 0 i32.const 0 i32.const 32 call $get drop i32.const 0))",
            ),
        )])
        .unwrap();
    assert!(
        runtime
            .process(MiddlewareStage::Ingress, message(), 0)
            .unwrap_err()
            .to_string()
            .contains("ReadMessage")
    );

    let mut after_commit = manifest([Capability::WriteSubject]);
    after_commit.stage = MiddlewareStage::AfterCommit;
    runtime
        .install(vec![(
            after_commit,
            wasm(
                "(module
                    (import \"broker\" \"set-field\" (func $set (param i32 i32 i32) (result i32)))
                    (memory (export \"memory\") 1)
                    (data (i32.const 0) \"orders.changed\")
                    (func (export \"process\") (param i32) (result i32)
                      i32.const 0 i32.const 0 i32.const 14 call $set drop i32.const 0))",
            ),
        )])
        .unwrap();
    assert!(
        runtime
            .process(MiddlewareStage::AfterCommit, message(), 0)
            .is_err()
    );

    let mut named = manifest([Capability::NamedKv]);
    named.named_kv.insert("orders-cache".to_string());
    runtime
        .install(vec![(
            named,
            wasm(
                "(module
                    (import \"broker\" \"named-host-call\" (func $host (param i32 i32 i32) (result i32)))
                    (memory (export \"memory\") 1)
                    (data (i32.const 0) \"other-cache\")
                    (func (export \"process\") (param i32) (result i32)
                      i32.const 0 i32.const 0 i32.const 11 call $host drop i32.const 0))",
            ),
        )])
        .unwrap();
    assert!(
        runtime
            .process(MiddlewareStage::Ingress, message(), 0)
            .unwrap_err()
            .to_string()
            .contains("not allow-listed")
    );
}

#[test]
fn emission_recursion_and_output_growth_are_bounded() {
    let runtime = MiddlewareRuntime::new().unwrap();
    let mut emitting = manifest([Capability::SecondaryPublish]);
    emitting.budget.max_emitted_messages = 0;
    runtime
        .install(vec![(
            emitting,
            wasm(
                "(module
                    (import \"broker\" \"emit\" (func $emit (param i32 i32 i32 i32) (result i32)))
                    (memory (export \"memory\") 1)
                    (data (i32.const 0) \"events.outx\")
                    (func (export \"process\") (param i32) (result i32)
                      i32.const 0 i32.const 10 i32.const 10 i32.const 1 call $emit drop i32.const 0))",
            ),
        )])
        .unwrap();
    assert!(
        runtime
            .process(MiddlewareStage::Ingress, message(), 0)
            .is_err()
    );

    let mut growth = manifest([Capability::WritePayload]);
    growth.budget.max_output_growth_bytes = 1;
    runtime
        .install(vec![(
            growth,
            wasm(
                "(module
                    (import \"broker\" \"set-field\" (func $set (param i32 i32 i32) (result i32)))
                    (memory (export \"memory\") 1)
                    (data (i32.const 0) \"0123456789\")
                    (func (export \"process\") (param i32) (result i32)
                      i32.const 3 i32.const 0 i32.const 10 call $set drop i32.const 0))",
            ),
        )])
        .unwrap();
    assert!(
        runtime
            .process(MiddlewareStage::Ingress, message(), 0)
            .unwrap_err()
            .to_string()
            .contains("output-growth")
    );

    let mut allocation = manifest([Capability::WritePayload]);
    allocation.budget.max_host_allocation_bytes = 4;
    runtime
        .install(vec![(
            allocation,
            wasm(
                "(module
                    (import \"broker\" \"set-field\" (func $set (param i32 i32 i32) (result i32)))
                    (memory (export \"memory\") 1)
                    (data (i32.const 0) \"0123456789\")
                    (func (export \"process\") (param i32) (result i32)
                      i32.const 3 i32.const 0 i32.const 10 call $set drop i32.const 0))",
            ),
        )])
        .unwrap();
    assert!(
        runtime
            .process(MiddlewareStage::Ingress, message(), 0)
            .unwrap_err()
            .to_string()
            .contains("host allocation")
    );
    assert!(
        runtime
            .process(MiddlewareStage::Ingress, message(), 3)
            .is_err()
    );
}

#[test]
fn hot_upgrade_and_rollback_switch_complete_generations() {
    let runtime = MiddlewareRuntime::new().unwrap();
    let first = runtime
        .install(vec![(
            manifest([]),
            wasm("(module (func (export \"process\") (param i32) (result i32) i32.const 0))"),
        )])
        .unwrap();
    let second = runtime
        .install(vec![(
            manifest([]),
            wasm("(module (func (export \"process\") (param i32) (result i32) i32.const 1))"),
        )])
        .unwrap();
    assert!(second > first);
    assert_eq!(
        runtime
            .process(MiddlewareStage::Ingress, message(), 0)
            .unwrap()
            .decision,
        MiddlewareDecision::Drop
    );
    assert_eq!(runtime.rollback().unwrap(), first);
    assert_eq!(
        runtime
            .process(MiddlewareStage::Ingress, message(), 0)
            .unwrap()
            .decision,
        MiddlewareDecision::Continue
    );
}

#[test]
#[ignore = "manual no-op middleware benchmark"]
fn benchmark_noop_middleware_overhead() {
    let runtime = MiddlewareRuntime::new().unwrap();
    runtime
        .install(vec![(
            manifest([]),
            wasm("(module (func (export \"process\") (param i32) (result i32) i32.const 0))"),
        )])
        .unwrap();
    let started = std::time::Instant::now();
    let mut latencies = Vec::with_capacity(10_000);
    for _ in 0..10_000 {
        let call_started = std::time::Instant::now();
        std::hint::black_box(
            runtime
                .process(MiddlewareStage::Ingress, message(), 0)
                .unwrap(),
        );
        latencies.push(call_started.elapsed());
    }
    let elapsed = started.elapsed();
    latencies.sort();
    eprintln!(
        "noop_10k={elapsed:?} throughput_per_second={:.0} p50={:?} p99={:?}",
        10_000.0 / elapsed.as_secs_f64(),
        latencies[5_000],
        latencies[9_900],
    );
}
