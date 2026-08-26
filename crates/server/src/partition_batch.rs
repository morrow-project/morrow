//! Bounded partition ingress batching primitives.

use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchLimits {
    pub max_records: usize,
    pub max_bytes: usize,
    pub max_delay: Duration,
}

impl BatchLimits {
    pub fn validate(self) -> bool {
        self.max_records > 0 && self.max_bytes > 0 && !self.max_delay.is_zero()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionBatch<T> {
    pub items: Vec<T>,
    pub bytes: usize,
}

#[derive(Debug)]
pub struct PartitionBatcher<T> {
    limits: BatchLimits,
    items: Vec<T>,
    bytes: usize,
    opened_at: Option<Instant>,
}

impl<T> PartitionBatcher<T> {
    pub fn new(limits: BatchLimits) -> Option<Self> {
        limits.validate().then_some(Self {
            limits,
            items: Vec::new(),
            bytes: 0,
            opened_at: None,
        })
    }

    pub fn push(&mut self, item: T, bytes: usize) -> Option<PartitionBatch<T>> {
        if self.items.is_empty() {
            self.opened_at = Some(Instant::now());
        }
        self.bytes = self.bytes.saturating_add(bytes);
        self.items.push(item);
        (self.items.len() >= self.limits.max_records || self.bytes >= self.limits.max_bytes)
            .then(|| self.take())
    }

    pub fn flush_due(&self) -> bool {
        self.opened_at
            .is_some_and(|opened| opened.elapsed() >= self.limits.max_delay)
    }

    pub fn flush(&mut self) -> Option<PartitionBatch<T>> {
        (!self.items.is_empty()).then(|| self.take())
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    fn take(&mut self) -> PartitionBatch<T> {
        self.opened_at = None;
        PartitionBatch {
            items: std::mem::take(&mut self.items),
            bytes: std::mem::take(&mut self.bytes),
        }
    }
}
