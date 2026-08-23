use super::*;
use tempfile::tempdir;

fn registration(format: SchemaFormat, definition: &str) -> SchemaRegistration {
    SchemaRegistration {
        tenant: "tenant-a".into(),
        subject: "orders".into(),
        format,
        definition: definition.into(),
        references: Vec::new(),
    }
}

#[test]
fn json_schema_compatibility_is_deterministic_and_persisted() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schemas.json");
    let mut registry = SchemaRegistry::open(&path).unwrap();
    registry
        .set_compatibility("tenant-a", "orders", Compatibility::Backward)
        .unwrap();
    let first = registry
        .register(registration(
            SchemaFormat::JsonSchema,
            r#"{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}"#,
        ))
        .unwrap();
    let second = registry
        .register(registration(
            SchemaFormat::JsonSchema,
            r#"{"type":"object","properties":{"id":{"type":"string"},"region":{"type":"string","default":"west"}},"required":["id","region"]}"#,
        ))
        .unwrap();
    assert_eq!(first.version, 1);
    assert_eq!(second.version, 2);
    drop(registry);
    let reopened = SchemaRegistry::open(path).unwrap();
    assert_eq!(reopened.by_id(second.id).unwrap().version, 2);
}

#[test]
fn protobuf_and_avro_compatibility_reject_breaking_changes() {
    let mut registry = SchemaRegistry::new();
    registry
        .set_compatibility("tenant-a", "orders", Compatibility::Backward)
        .unwrap();
    registry
        .register(registration(
            SchemaFormat::Protobuf,
            "message Event { string id = 1; }",
        ))
        .unwrap();
    assert!(
        registry
            .register(registration(
                SchemaFormat::Protobuf,
                "message Event { int64 id = 1; }",
            ))
            .is_err()
    );

    let mut avro = SchemaRegistry::new();
    avro.set_compatibility("tenant-a", "orders", Compatibility::Backward)
        .unwrap();
    avro.register(registration(
        SchemaFormat::Avro,
        r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"string"}]}"#,
    ))
    .unwrap();
    assert!(
        avro.register(registration(
            SchemaFormat::Avro,
            r#"{"type":"record","name":"Event","fields":[]}"#,
        ))
        .is_err()
    );
}

#[test]
fn references_deletion_and_rollback_are_tenant_scoped() {
    let mut registry = SchemaRegistry::new();
    registry
        .register(registration(
            SchemaFormat::JsonSchema,
            r#"{"type":"object","properties":{"id":{"type":"string"}}}"#,
        ))
        .unwrap();
    registry
        .register(SchemaRegistration {
            tenant: "tenant-a".into(),
            subject: "orders-v2".into(),
            format: SchemaFormat::JsonSchema,
            definition: r#"{"type":"object"}"#.into(),
            references: vec![SchemaReference {
                subject: "orders".into(),
                version: 1,
            }],
        })
        .unwrap();
    assert!(
        registry
            .register(SchemaRegistration {
                tenant: "tenant-b".into(),
                subject: "orders-v2".into(),
                format: SchemaFormat::JsonSchema,
                definition: r#"{"type":"object"}"#.into(),
                references: vec![SchemaReference {
                    subject: "orders".into(),
                    version: 1,
                }],
            })
            .is_err()
    );
    registry.delete("tenant-a", "orders", 1).unwrap();
    assert!(registry.get("tenant-a", "orders", 1).is_none());
    assert_eq!(
        registry.rollback("tenant-a", "orders", 1).unwrap().version,
        1
    );
    assert!(registry.get("tenant-a", "orders", 1).is_some());
}
