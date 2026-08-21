use crate::error::Result;
use protocol::subject;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct StreamId(String);

impl StreamId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        crate::broker_ensure!(
            !value.is_empty()
                && !value.starts_with('_')
                && !value.contains('.')
                && !value.chars().any(char::is_whitespace),
            "stream name must be non-empty, contain no '.', contain no whitespace, and not start with '_'"
        );
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct PartitionId(pub u32);

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "strategy", rename_all = "snake_case")]
pub enum PartitioningStrategy {
    Key,
    SubjectToken { token: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionFallback {
    Sticky,
    SubjectHash,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct PartitioningPolicy {
    pub strategy: PartitioningStrategy,
    pub fallback: PartitionFallback,
    pub epoch: u64,
}

impl Default for PartitioningPolicy {
    fn default() -> Self {
        Self {
            strategy: PartitioningStrategy::Key,
            fallback: PartitionFallback::Sticky,
            epoch: 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageMode {
    Local,
    Quorum,
    QuorumFsync,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct StoragePolicy {
    pub mode: StorageMode,
    pub replicas: u32,
    pub min_ack_replicas: u32,
}

impl Default for StoragePolicy {
    fn default() -> Self {
        Self {
            mode: StorageMode::Local,
            replicas: 1,
            min_ack_replicas: 1,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct RetentionPolicy {
    pub max_age_ms: Option<u64>,
    pub max_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct StreamDefinition {
    pub name: StreamId,
    pub subjects: Vec<String>,
    pub partitions: u32,
    pub partitioning: PartitioningPolicy,
    pub storage: StoragePolicy,
    pub retention: RetentionPolicy,
}

impl StreamDefinition {
    fn validate(&self) -> Result<()> {
        crate::broker_ensure!(
            self.partitions > 0,
            "stream partitions must be greater than zero"
        );
        crate::broker_ensure!(
            self.partitioning.epoch > 0,
            "stream partitioning epoch must be greater than zero"
        );
        crate::broker_ensure!(
            !self.subjects.is_empty(),
            "stream subjects must not be empty"
        );
        crate::broker_ensure!(
            self.storage.replicas > 0,
            "stream storage replicas must be greater than zero"
        );
        crate::broker_ensure!(
            self.storage.min_ack_replicas > 0
                && self.storage.min_ack_replicas <= self.storage.replicas,
            "stream min_ack_replicas must be between one and replicas"
        );
        if self.storage.mode == StorageMode::Local {
            crate::broker_ensure!(
                self.storage.replicas == 1 && self.storage.min_ack_replicas == 1,
                "local stream storage requires replicas=1 and min_ack_replicas=1"
            );
        }
        for binding in &self.subjects {
            crate::broker_ensure!(
                subject::validate_subscription(binding),
                "stream contains invalid subject binding {binding}"
            );
            crate::broker_ensure!(
                !patterns_overlap(binding, "_INBOX.>"),
                "stream binding {binding} captures reserved inbox subjects"
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct StreamCatalog {
    streams: Vec<StreamDefinition>,
}

impl StreamCatalog {
    pub fn new(mut streams: Vec<StreamDefinition>) -> Result<Self> {
        for stream in &streams {
            stream.validate()?;
        }
        streams.sort_by(|left, right| left.name.as_str().cmp(right.name.as_str()));
        for pair in streams.windows(2) {
            crate::broker_ensure!(pair[0].name != pair[1].name, "duplicate stream name");
        }
        for left_index in 0..streams.len() {
            for right_index in (left_index + 1)..streams.len() {
                for left in &streams[left_index].subjects {
                    for right in &streams[right_index].subjects {
                        crate::broker_ensure!(
                            !patterns_overlap(left, right),
                            "ambiguous stream bindings {left} ({}) and {right} ({})",
                            streams[left_index].name.as_str(),
                            streams[right_index].name.as_str()
                        );
                    }
                }
            }
        }
        Ok(Self { streams })
    }

    pub fn definitions(&self) -> &[StreamDefinition] {
        &self.streams
    }

    pub fn resolve_primary(&self, concrete_subject: &str) -> Option<&StreamDefinition> {
        if !subject::validate_subject(concrete_subject) || concrete_subject.starts_with("_INBOX.") {
            return None;
        }
        self.streams.iter().find(|stream| {
            stream
                .subjects
                .iter()
                .any(|binding| subject::matches(binding, concrete_subject))
        })
    }
}

fn patterns_overlap(left: &str, right: &str) -> bool {
    let left = left.split('.').collect::<Vec<_>>();
    let right = right.split('.').collect::<Vec<_>>();
    patterns_overlap_from(&left, 0, &right, 0)
}

fn patterns_overlap_from(left: &[&str], left_at: usize, right: &[&str], right_at: usize) -> bool {
    match (left.get(left_at), right.get(right_at)) {
        (Some(&">"), _) | (_, Some(&">")) => true,
        (None, None) => true,
        (Some(left_token), Some(right_token))
            if left_token == right_token || *left_token == "*" || *right_token == "*" =>
        {
            patterns_overlap_from(left, left_at + 1, right, right_at + 1)
        }
        _ => false,
    }
}

#[cfg(test)]
#[path = "stream/tests.rs"]
mod tests;
