//! Tenant-scoped schema governance without payload deserialization on the
//! normal routing path.

use crate::error::{BrokerError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SchemaFormat {
    JsonSchema,
    Protobuf,
    Avro,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Compatibility {
    None,
    Backward,
    Forward,
    Full,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaReference {
    pub subject: String,
    pub version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaVersion {
    pub id: u64,
    pub tenant: String,
    pub subject: String,
    pub version: u32,
    pub format: SchemaFormat,
    pub definition: String,
    pub references: Vec<SchemaReference>,
    pub deleted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaRegistration {
    pub tenant: String,
    pub subject: String,
    pub format: SchemaFormat,
    pub definition: String,
    pub references: Vec<SchemaReference>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RegistryState {
    next_id: u64,
    subjects: BTreeMap<String, Vec<SchemaVersion>>,
}

#[derive(Debug)]
pub struct SchemaRegistry {
    path: Option<PathBuf>,
    compatibility: BTreeMap<String, Compatibility>,
    state: RegistryState,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self {
            path: None,
            compatibility: BTreeMap::new(),
            state: RegistryState {
                next_id: 1,
                ..Default::default()
            },
        }
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let state = if path.exists() {
            serde_json::from_slice(&fs::read(&path)?)
                .map_err(|error| BrokerError::with_source("decoding schema registry", error))?
        } else {
            RegistryState {
                next_id: 1,
                ..Default::default()
            }
        };
        Ok(Self {
            path: Some(path),
            compatibility: BTreeMap::new(),
            state,
        })
    }

    pub fn set_compatibility(
        &mut self,
        tenant: &str,
        subject: &str,
        compatibility: Compatibility,
    ) -> Result<()> {
        validate_name(tenant, "tenant")?;
        validate_name(subject, "subject")?;
        self.compatibility
            .insert(key(tenant, subject), compatibility);
        Ok(())
    }

    pub fn compatibility(&self, tenant: &str, subject: &str) -> Compatibility {
        self.compatibility
            .get(&key(tenant, subject))
            .copied()
            .unwrap_or(Compatibility::Backward)
    }

    pub fn register(&mut self, registration: SchemaRegistration) -> Result<SchemaVersion> {
        validate_name(&registration.tenant, "tenant")?;
        validate_name(&registration.subject, "subject")?;
        validate_definition(registration.format, &registration.definition)?;
        self.validate_references(&registration.tenant, &registration.references)?;
        let subject_key = key(&registration.tenant, &registration.subject);
        let versions = self.state.subjects.entry(subject_key.clone()).or_default();
        let previous = versions.iter().rev().find(|version| !version.deleted);
        if let Some(previous) = previous {
            let policy = self
                .compatibility
                .get(&subject_key)
                .copied()
                .unwrap_or(Compatibility::Backward);
            crate::broker_ensure!(
                compatible(
                    policy,
                    previous.format,
                    &previous.definition,
                    registration.format,
                    &registration.definition,
                ),
                "schema is incompatible with the active version"
            );
        }
        let version = SchemaVersion {
            id: self.state.next_id,
            tenant: registration.tenant,
            subject: registration.subject,
            version: versions.len() as u32 + 1,
            format: registration.format,
            definition: registration.definition,
            references: registration.references,
            deleted: false,
        };
        self.state.next_id = self.state.next_id.saturating_add(1);
        versions.push(version.clone());
        self.persist()?;
        Ok(version)
    }

    pub fn get(&self, tenant: &str, subject: &str, version: u32) -> Option<&SchemaVersion> {
        self.state
            .subjects
            .get(&key(tenant, subject))?
            .iter()
            .find(|schema| schema.version == version && !schema.deleted)
    }

    pub fn by_id(&self, id: u64) -> Option<&SchemaVersion> {
        self.state
            .subjects
            .values()
            .flat_map(|versions| versions.iter())
            .find(|schema| schema.id == id && !schema.deleted)
    }

    pub fn delete(&mut self, tenant: &str, subject: &str, version: u32) -> Result<()> {
        let schema = self
            .state
            .subjects
            .get_mut(&key(tenant, subject))
            .and_then(|versions| versions.iter_mut().find(|schema| schema.version == version))
            .ok_or_else(|| BrokerError::msg("schema version not found"))?;
        schema.deleted = true;
        self.persist()
    }

    pub fn rollback(&mut self, tenant: &str, subject: &str, version: u32) -> Result<SchemaVersion> {
        let schema = self
            .state
            .subjects
            .get_mut(&key(tenant, subject))
            .and_then(|versions| versions.iter_mut().find(|schema| schema.version == version))
            .ok_or_else(|| BrokerError::msg("schema version not found"))?;
        schema.deleted = false;
        let schema = schema.clone();
        self.persist()?;
        Ok(schema)
    }

    pub fn versions(&self, tenant: &str, subject: &str) -> Vec<SchemaVersion> {
        self.state
            .subjects
            .get(&key(tenant, subject))
            .into_iter()
            .flat_map(|versions| versions.iter().filter(|schema| !schema.deleted).cloned())
            .collect()
    }

    fn validate_references(&self, tenant: &str, references: &[SchemaReference]) -> Result<()> {
        let mut seen = BTreeSet::new();
        for reference in references {
            crate::broker_ensure!(
                seen.insert((reference.subject.clone(), reference.version)),
                "duplicate schema reference"
            );
            crate::broker_ensure!(
                self.get(tenant, &reference.subject, reference.version)
                    .is_some(),
                "schema reference does not exist"
            );
        }
        Ok(())
    }

    fn persist(&self) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = path.with_extension("tmp");
        let body = serde_json::to_vec(&self.state)
            .map_err(|error| BrokerError::with_source("encoding schema registry", error))?;
        fs::write(&temporary, body)?;
        fs::rename(temporary, path)?;
        Ok(())
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

fn key(tenant: &str, subject: &str) -> String {
    format!("{tenant}/{subject}")
}

fn validate_name(value: &str, field: &str) -> Result<()> {
    crate::broker_ensure!(
        !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')),
        "invalid schema {field}"
    );
    Ok(())
}

fn validate_definition(format: SchemaFormat, definition: &str) -> Result<()> {
    crate::broker_ensure!(!definition.trim().is_empty(), "schema definition is empty");
    match format {
        SchemaFormat::JsonSchema => {
            let value: Value = serde_json::from_str(definition)
                .map_err(|error| BrokerError::with_source("invalid JSON Schema", error))?;
            crate::broker_ensure!(value.is_object(), "JSON Schema must be an object");
        }
        SchemaFormat::Protobuf => {
            crate::broker_ensure!(
                definition.contains("message "),
                "Protobuf schema has no message"
            );
        }
        SchemaFormat::Avro => {
            let value: Value = serde_json::from_str(definition)
                .map_err(|error| BrokerError::with_source("invalid Avro schema", error))?;
            crate::broker_ensure!(value["type"] == "record", "Avro schema must be a record");
            crate::broker_ensure!(value["fields"].is_array(), "Avro schema fields are missing");
        }
    }
    Ok(())
}

fn compatible(
    policy: Compatibility,
    old_format: SchemaFormat,
    old_definition: &str,
    new_format: SchemaFormat,
    new_definition: &str,
) -> bool {
    if policy == Compatibility::None {
        return true;
    }
    if old_format != new_format {
        return false;
    }
    let backward = format_compatible(old_format, old_definition, new_definition);
    let forward = format_compatible(old_format, new_definition, old_definition);
    match policy {
        Compatibility::None => true,
        Compatibility::Backward => backward,
        Compatibility::Forward => forward,
        Compatibility::Full => backward && forward,
    }
}

fn format_compatible(format: SchemaFormat, old: &str, new: &str) -> bool {
    match format {
        SchemaFormat::JsonSchema => json_backward(old, new),
        SchemaFormat::Avro => avro_backward(old, new),
        SchemaFormat::Protobuf => protobuf_backward(old, new),
    }
}

fn json_backward(old: &str, new: &str) -> bool {
    let Ok(old) = serde_json::from_str::<Value>(old) else {
        return false;
    };
    let Ok(new) = serde_json::from_str::<Value>(new) else {
        return false;
    };
    let old_required = old["required"].as_array().cloned().unwrap_or_default();
    let new_required = new["required"].as_array().cloned().unwrap_or_default();
    old_required.iter().all(|field| {
        new["properties"]
            .get(field.as_str().unwrap_or(""))
            .is_some()
    }) && new_required.iter().all(|field| {
        old_required.contains(field)
            || new["properties"]
                .get(field.as_str().unwrap_or(""))
                .and_then(|value| value.get("default"))
                .is_some()
    })
}

fn avro_backward(old: &str, new: &str) -> bool {
    let Ok(old) = serde_json::from_str::<Value>(old) else {
        return false;
    };
    let Ok(new) = serde_json::from_str::<Value>(new) else {
        return false;
    };
    let old_fields = field_names(&old);
    let new_fields = field_names(&new);
    old_fields.iter().all(|field| new_fields.contains(field))
        && new_fields.iter().all(|field| {
            old_fields.contains(field)
                || new["fields"].as_array().is_some_and(|fields| {
                    fields
                        .iter()
                        .any(|entry| entry["name"] == *field && entry.get("default").is_some())
                })
        })
}

fn field_names(schema: &Value) -> BTreeSet<String> {
    schema["fields"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|field| field["name"].as_str().map(str::to_string))
        .collect()
}

fn protobuf_backward(old: &str, new: &str) -> bool {
    let old_fields = protobuf_fields(old);
    let new_fields = protobuf_fields(new);
    old_fields
        .iter()
        .all(|(number, ty)| new_fields.get(number) == Some(ty))
}

fn protobuf_fields(definition: &str) -> BTreeMap<u32, String> {
    definition
        .split(';')
        .filter_map(|field| {
            let (left, number) = field.rsplit_once('=')?;
            let number = number.trim().parse().ok()?;
            let tokens = left.split_whitespace().collect::<Vec<_>>();
            let field_name = tokens.last()?;
            let field_type = tokens.get(tokens.len().checked_sub(2)?)?;
            Some((number, format!("{field_type} {field_name}")))
        })
        .collect()
}

#[cfg(test)]
mod tests;
