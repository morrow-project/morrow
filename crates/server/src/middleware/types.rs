use std::{collections::BTreeSet, time::Duration};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MiddlewareStage {
    Ingress,
    Route,
    BeforeAppend,
    AfterCommit,
    BeforeDeliver,
    AfterAck,
}

impl MiddlewareStage {
    pub(super) fn code(self) -> i32 {
        match self {
            Self::Ingress => 0,
            Self::Route => 1,
            Self::BeforeAppend => 2,
            Self::AfterCommit => 3,
            Self::BeforeDeliver => 4,
            Self::AfterAck => 5,
        }
    }

    pub(super) fn may_mutate_subject(self) -> bool {
        matches!(self, Self::Ingress | Self::Route)
    }

    pub(super) fn may_mutate_persisted_fields(self) -> bool {
        matches!(
            self,
            Self::Ingress | Self::Route | Self::BeforeAppend | Self::BeforeDeliver
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Capability {
    ReadMessage,
    WriteSubject,
    WriteKey,
    WriteHeaders,
    WritePayload,
    SecondaryPublish,
    NamedKv,
    Secrets,
    AllowListedHttp,
    Clock,
    Random,
    Telemetry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailurePolicy {
    FailOpen,
    FailClosed,
    Drop,
}

#[derive(Debug, Clone)]
pub struct ResourceBudget {
    pub max_memory_bytes: usize,
    pub max_fuel: u64,
    pub deadline: Duration,
    pub max_host_allocation_bytes: usize,
    pub max_output_growth_bytes: usize,
    pub max_emitted_messages: usize,
    pub max_recursion_depth: usize,
}

impl Default for ResourceBudget {
    fn default() -> Self {
        Self {
            max_memory_bytes: 16 * 1024 * 1024,
            max_fuel: 1_000_000,
            deadline: Duration::from_millis(25),
            max_host_allocation_bytes: 1024 * 1024,
            max_output_growth_bytes: 1024 * 1024,
            max_emitted_messages: 8,
            max_recursion_depth: 2,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MiddlewareManifest {
    pub name: String,
    pub subject: String,
    pub stage: MiddlewareStage,
    pub capabilities: BTreeSet<Capability>,
    pub failure_policy: FailurePolicy,
    pub budget: ResourceBudget,
    pub named_kv: BTreeSet<String>,
    pub secrets: BTreeSet<String>,
    pub http_allow_lists: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MiddlewareMessage {
    pub subject: String,
    pub key: Option<Vec<u8>>,
    pub headers: Vec<(String, String)>,
    pub payload: Vec<u8>,
    pub reply_to: Option<String>,
}

impl MiddlewareMessage {
    pub(super) fn size(&self) -> usize {
        self.subject.len()
            + self.key.as_ref().map_or(0, Vec::len)
            + self
                .headers
                .iter()
                .map(|(name, value)| name.len() + value.len())
                .sum::<usize>()
            + self.payload.len()
            + self.reply_to.as_ref().map_or(0, String::len)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmittedMessage {
    pub subject: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MiddlewareDecision {
    Continue,
    Drop,
    Reject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MiddlewareOutcome {
    pub generation: u64,
    pub decision: MiddlewareDecision,
    pub message: MiddlewareMessage,
    pub emitted: Vec<EmittedMessage>,
}

#[derive(Debug)]
pub struct MiddlewareError(pub String);

impl std::fmt::Display for MiddlewareError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for MiddlewareError {}
