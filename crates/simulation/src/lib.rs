//! Deterministic building blocks for simulation tests.
//!
//! The primitives in this crate do not implement broker behavior. They make
//! clocks, scheduling, event delivery, storage faults, and failure traces
//! controllable so tests can exercise the production state machines without
//! relying on wall-clock timing or operating-system scheduling.

use std::{
    collections::BTreeMap,
    fmt,
    sync::atomic::{AtomicU64, Ordering},
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Default)]
pub struct VirtualClock {
    now_ms: AtomicU64,
}

impl VirtualClock {
    pub const fn new(now_ms: u64) -> Self {
        Self {
            now_ms: AtomicU64::new(now_ms),
        }
    }

    pub fn now_ms(&self) -> u64 {
        self.now_ms.load(Ordering::Acquire)
    }

    pub fn set_ms(&self, now_ms: u64) {
        self.now_ms.store(now_ms, Ordering::Release);
    }

    pub fn advance_ms(&self, millis: u64) -> u64 {
        self.now_ms.fetch_add(millis, Ordering::AcqRel) + millis
    }
}

#[derive(Debug)]
pub struct DeterministicScheduler<E> {
    next_sequence: u64,
    events: BTreeMap<(u64, u64), E>,
}

impl<E> Default for DeterministicScheduler<E> {
    fn default() -> Self {
        Self {
            next_sequence: 0,
            events: BTreeMap::new(),
        }
    }
}

impl<E> DeterministicScheduler<E> {
    pub fn schedule_at(&mut self, at_ms: u64, event: E) -> u64 {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.events.insert((at_ms, sequence), event);
        sequence
    }

    pub fn next_due_at(&self) -> Option<u64> {
        self.events.keys().next().map(|(at_ms, _)| *at_ms)
    }

    pub fn pop_due(&mut self, now_ms: u64) -> Option<E> {
        let key = self
            .events
            .keys()
            .next()
            .copied()
            .filter(|(at_ms, _)| *at_ms <= now_ms)?;
        self.events.remove(&key)
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SimulationError {
    StepLimitExceeded { limit: usize },
}

impl fmt::Display for SimulationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StepLimitExceeded { limit } => {
                write!(formatter, "simulation exceeded step limit {limit}")
            }
        }
    }
}

impl std::error::Error for SimulationError {}

#[derive(Debug)]
pub struct Simulation<E> {
    pub clock: VirtualClock,
    pub scheduler: DeterministicScheduler<E>,
    pub rng: DeterministicRng,
    pub trace: EventTrace,
    step_limit: usize,
    steps: usize,
}

impl<E> Simulation<E> {
    pub fn new(seed: u64, start_ms: u64) -> Self {
        Self {
            clock: VirtualClock::new(start_ms),
            scheduler: DeterministicScheduler::default(),
            rng: DeterministicRng::new(seed),
            trace: EventTrace::new(seed),
            step_limit: 100_000,
            steps: 0,
        }
    }

    pub fn set_step_limit(&mut self, limit: usize) {
        self.step_limit = limit;
    }

    pub fn schedule_after(&mut self, delay_ms: u64, event: E) -> u64 {
        self.scheduler
            .schedule_at(self.clock.now_ms().saturating_add(delay_ms), event)
    }

    pub fn schedule_at(&mut self, at_ms: u64, event: E) -> u64 {
        self.scheduler.schedule_at(at_ms, event)
    }

    pub fn step(&mut self) -> Result<Option<E>, SimulationError> {
        if self.steps >= self.step_limit {
            return Err(SimulationError::StepLimitExceeded {
                limit: self.step_limit,
            });
        }
        let Some(at_ms) = self.scheduler.next_due_at() else {
            return Ok(None);
        };
        self.clock.set_ms(at_ms);
        let event = self.scheduler.pop_due(at_ms);
        self.steps += 1;
        self.trace.record(at_ms, "scheduler.step");
        Ok(event)
    }

    pub fn run_until_idle<F>(&mut self, mut apply: F) -> Result<usize, SimulationError>
    where
        F: FnMut(&mut Self, E),
    {
        while let Some(event) = self.step()? {
            apply(self, event);
        }
        Ok(self.steps)
    }

    pub fn steps(&self) -> usize {
        self.steps
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DeterministicRng {
    seed: u64,
    state: u64,
}

impl DeterministicRng {
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            state: if seed == 0 {
                0x9e37_79b9_7f4a_7c15
            } else {
                seed
            },
        }
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut value = self.state;
        value ^= value >> 12;
        value ^= value << 25;
        value ^= value >> 27;
        self.state = value;
        value.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    pub fn range(&mut self, range: std::ops::Range<u64>) -> u64 {
        assert!(range.start < range.end, "random range must be non-empty");
        range.start + self.next_u64() % (range.end - range.start)
    }

    pub fn coin_flip(&mut self) -> bool {
        self.next_u64() & 1 == 1
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TraceEvent {
    pub time_ms: u64,
    pub kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventTrace {
    pub seed: u64,
    pub events: Vec<TraceEvent>,
}

impl EventTrace {
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            events: Vec::new(),
        }
    }

    pub fn record(&mut self, time_ms: u64, kind: impl Into<String>) {
        self.events.push(TraceEvent {
            time_ms,
            kind: kind.into(),
        });
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    pub fn from_json(value: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinkConfig {
    pub delay_ms: u64,
    pub blocked: bool,
    pub duplicate: bool,
    pub reorder: bool,
}

impl Default for LinkConfig {
    fn default() -> Self {
        Self {
            delay_ms: 0,
            blocked: false,
            duplicate: false,
            reorder: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveredMessage<M> {
    pub from: u64,
    pub to: u64,
    pub payload: M,
}

#[derive(Debug, Clone)]
struct QueuedMessage<M> {
    at_ms: u64,
    order: u64,
    message: DeliveredMessage<M>,
}

#[derive(Debug)]
pub struct SimulatedTransport<M> {
    links: BTreeMap<(u64, u64), LinkConfig>,
    queue: Vec<QueuedMessage<M>>,
    next_order: u64,
}

impl<M: Clone> Default for SimulatedTransport<M> {
    fn default() -> Self {
        Self {
            links: BTreeMap::new(),
            queue: Vec::new(),
            next_order: 0,
        }
    }
}

impl<M: Clone> SimulatedTransport<M> {
    pub fn set_link(&mut self, from: u64, to: u64, config: LinkConfig) {
        self.links.insert((from, to), config);
    }

    pub fn partition(&mut self, left: &[u64], right: &[u64], blocked: bool) {
        for &from in left {
            for &to in right {
                let mut forward = self.link(from, to);
                forward.blocked = blocked;
                self.set_link(from, to, forward);
                let mut reverse = self.link(to, from);
                reverse.blocked = blocked;
                self.set_link(to, from, reverse);
            }
        }
    }

    pub fn disconnect(&mut self, from: u64, to: u64, disconnected: bool) {
        let mut config = self.link(from, to);
        config.blocked = disconnected;
        self.set_link(from, to, config);
    }

    pub fn link(&self, from: u64, to: u64) -> LinkConfig {
        self.links.get(&(from, to)).copied().unwrap_or_default()
    }

    pub fn send(&mut self, now_ms: u64, from: u64, to: u64, payload: M) -> bool {
        let config = self.link(from, to);
        if config.blocked {
            return false;
        }
        self.enqueue(now_ms, from, to, payload.clone(), config);
        if config.duplicate {
            self.enqueue(now_ms, from, to, payload, config);
        }
        true
    }

    pub fn deliver_ready(&mut self, now_ms: u64) -> Vec<DeliveredMessage<M>> {
        let mut delivered = Vec::new();
        while let Some(index) = self
            .queue
            .iter()
            .enumerate()
            .filter(|(_, message)| message.at_ms <= now_ms)
            .min_by_key(|(_, message)| (message.at_ms, message.order))
            .map(|(index, _)| index)
        {
            delivered.push(self.queue.swap_remove(index).message);
        }
        delivered
    }

    pub fn queued_messages(&self) -> usize {
        self.queue.len()
    }

    fn enqueue(&mut self, now_ms: u64, from: u64, to: u64, payload: M, config: LinkConfig) {
        let order = self.next_order;
        self.next_order = self.next_order.saturating_add(1);
        let order = if config.reorder {
            u64::MAX - order
        } else {
            order
        };
        self.queue.push(QueuedMessage {
            at_ms: now_ms.saturating_add(config.delay_ms),
            order,
            message: DeliveredMessage { from, to, payload },
        });
    }
}

#[derive(Debug)]
pub struct SimulatedCluster {
    members: BTreeMap<u64, bool>,
    membership_epoch: u64,
    transport: SimulatedTransport<Vec<u8>>,
}

impl Default for SimulatedCluster {
    fn default() -> Self {
        Self {
            members: BTreeMap::new(),
            membership_epoch: 0,
            transport: SimulatedTransport::default(),
        }
    }
}

impl SimulatedCluster {
    pub fn bootstrap(&mut self, node_id: u64) -> bool {
        if !self.members.is_empty() {
            return false;
        }
        self.members.insert(node_id, true);
        self.membership_epoch = self.membership_epoch.saturating_add(1);
        true
    }

    pub fn join(&mut self, node_id: u64) -> bool {
        if self.members.contains_key(&node_id) || self.members.is_empty() {
            return false;
        }
        self.members.insert(node_id, true);
        self.membership_epoch = self.membership_epoch.saturating_add(1);
        true
    }

    pub fn leave(&mut self, node_id: u64) -> bool {
        if self.members.remove(&node_id).is_none() {
            return false;
        }
        self.membership_epoch = self.membership_epoch.saturating_add(1);
        true
    }

    pub fn crash(&mut self, node_id: u64) -> bool {
        let Some(online) = self.members.get_mut(&node_id) else {
            return false;
        };
        *online = false;
        true
    }

    pub fn restart(&mut self, node_id: u64) -> bool {
        let Some(online) = self.members.get_mut(&node_id) else {
            return false;
        };
        *online = true;
        true
    }

    pub fn members(&self) -> impl Iterator<Item = u64> + '_ {
        self.members.keys().copied()
    }

    pub fn online_members(&self) -> impl Iterator<Item = u64> + '_ {
        self.members
            .iter()
            .filter_map(|(node_id, online)| online.then_some(*node_id))
    }

    pub fn is_member(&self, node_id: u64) -> bool {
        self.members.contains_key(&node_id)
    }

    pub fn is_online(&self, node_id: u64) -> bool {
        self.members.get(&node_id).copied().unwrap_or(false)
    }

    pub fn membership_epoch(&self) -> u64 {
        self.membership_epoch
    }

    pub fn quorum_size(&self) -> usize {
        self.members.len() / 2 + 1
    }

    pub fn quorum_available(&self) -> bool {
        self.online_members().count() >= self.quorum_size()
    }

    pub fn set_link(&mut self, from: u64, to: u64, config: LinkConfig) {
        self.transport.set_link(from, to, config);
    }

    pub fn partition(&mut self, left: &[u64], right: &[u64], blocked: bool) {
        self.transport.partition(left, right, blocked);
    }

    pub fn send(&mut self, now_ms: u64, from: u64, to: u64, payload: Vec<u8>) -> bool {
        if !self.is_online(from) || !self.is_online(to) {
            return false;
        }
        self.transport.send(now_ms, from, to, payload)
    }

    pub fn deliver_ready(&mut self, now_ms: u64) -> Vec<DeliveredMessage<Vec<u8>>> {
        self.transport.deliver_ready(now_ms)
    }

    pub fn queued_messages(&self) -> usize {
        self.transport.queued_messages()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    Failed,
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Failed => formatter.write_str("simulated storage failure"),
        }
    }
}

impl std::error::Error for StorageError {}

#[derive(Debug, Default, Clone)]
pub struct SimulatedStorage {
    persisted: BTreeMap<String, Vec<u8>>,
    pending: Vec<PendingWrite>,
    fail_writes: bool,
    partial_write_bytes: Option<usize>,
    write_delay_ms: u64,
}

#[derive(Debug, Clone)]
struct PendingWrite {
    ready_at_ms: u64,
    key: String,
    value: Vec<u8>,
}

impl SimulatedStorage {
    pub fn set_fail_writes(&mut self, fail: bool) {
        self.fail_writes = fail;
    }

    pub fn set_partial_write_bytes(&mut self, bytes: Option<usize>) {
        self.partial_write_bytes = bytes;
    }

    pub fn set_write_delay_ms(&mut self, delay_ms: u64) {
        self.write_delay_ms = delay_ms;
    }

    pub fn write(&mut self, key: impl Into<String>, value: &[u8]) -> Result<(), StorageError> {
        self.write_at(0, key, value)
    }

    pub fn write_at(
        &mut self,
        now_ms: u64,
        key: impl Into<String>,
        value: &[u8],
    ) -> Result<(), StorageError> {
        if self.fail_writes {
            return Err(StorageError::Failed);
        }
        let value = self.partial_write_bytes.map_or_else(
            || value.to_vec(),
            |limit| value[..value.len().min(limit)].to_vec(),
        );
        let key = key.into();
        if self.write_delay_ms == 0 {
            self.persisted.insert(key, value);
        } else {
            self.pending.push(PendingWrite {
                ready_at_ms: now_ms.saturating_add(self.write_delay_ms),
                key,
                value,
            });
        }
        Ok(())
    }

    pub fn flush(&mut self, now_ms: u64) -> usize {
        let mut flushed = 0;
        let mut pending = Vec::with_capacity(self.pending.len());
        for write in self.pending.drain(..) {
            if write.ready_at_ms <= now_ms {
                self.persisted.insert(write.key, write.value);
                flushed += 1;
            } else {
                pending.push(write);
            }
        }
        self.pending = pending;
        flushed
    }

    pub fn pending_writes(&self) -> usize {
        self.pending.len()
    }

    pub fn read(&self, key: &str) -> Option<&[u8]> {
        self.persisted.get(key).map(Vec::as_slice)
    }

    pub fn restart(&self) -> Self {
        Self {
            persisted: self.persisted.clone(),
            pending: Vec::new(),
            fail_writes: false,
            partial_write_bytes: None,
            write_delay_ms: 0,
        }
    }

    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.persisted.keys().map(String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduler_orders_same_time_events_deterministically() {
        let mut scheduler = DeterministicScheduler::default();
        scheduler.schedule_at(10, "first");
        scheduler.schedule_at(10, "second");
        assert_eq!(scheduler.pop_due(9), None);
        assert_eq!(scheduler.pop_due(10), Some("first"));
        assert_eq!(scheduler.pop_due(10), Some("second"));
    }

    #[test]
    fn transport_models_delay_duplication_reordering_and_partitions() {
        let mut transport = SimulatedTransport::default();
        transport.set_link(
            1,
            2,
            LinkConfig {
                delay_ms: 10,
                duplicate: true,
                reorder: false,
                blocked: false,
            },
        );
        assert!(transport.send(0, 1, 2, "hello"));
        assert!(transport.deliver_ready(9).is_empty());
        assert_eq!(transport.deliver_ready(10).len(), 2);
        transport.partition(&[1], &[2], true);
        assert!(!transport.send(10, 1, 2, "dropped"));
    }

    #[test]
    fn storage_restart_preserves_partial_state_and_clears_faults() {
        let mut storage = SimulatedStorage::default();
        storage.set_partial_write_bytes(Some(2));
        storage.write("wal", b"abcd").unwrap();
        assert_eq!(storage.read("wal"), Some(&b"ab"[..]));
        let restarted = storage.restart();
        assert_eq!(restarted.read("wal"), Some(&b"ab"[..]));
    }

    #[test]
    fn delayed_storage_flushes_only_after_virtual_time_advances() {
        let mut storage = SimulatedStorage::default();
        storage.set_write_delay_ms(10);
        storage.write_at(0, "wal", b"record").unwrap();
        assert_eq!(storage.pending_writes(), 1);
        assert_eq!(storage.flush(9), 0);
        assert_eq!(storage.read("wal"), None);
        assert_eq!(storage.flush(10), 1);
        assert_eq!(storage.read("wal"), Some(&b"record"[..]));
    }

    #[test]
    fn traces_round_trip_for_replay() {
        let mut trace = EventTrace::new(42);
        trace.record(0, "bootstrap");
        trace.record(10, "partition:1-2");
        let replay = EventTrace::from_json(&trace.to_json().unwrap()).unwrap();
        assert_eq!(replay, trace);
    }

    #[test]
    fn simulation_advances_virtual_time_and_replays_scheduled_events() {
        let mut simulation = Simulation::new(7, 100);
        simulation.schedule_after(20, "first");
        simulation.schedule_at(100, "second");
        let mut events = Vec::new();
        simulation
            .run_until_idle(|simulation, event| {
                events.push((simulation.clock.now_ms(), event));
            })
            .unwrap();
        assert_eq!(events, [(100, "second"), (120, "first")]);
        assert_eq!(simulation.steps(), 2);
        assert_eq!(simulation.trace.seed, 7);
    }

    #[test]
    fn cluster_models_membership_lifecycle_quorum_and_network_faults() {
        let mut cluster = SimulatedCluster::default();
        assert!(cluster.bootstrap(1));
        assert!(cluster.join(2));
        assert!(cluster.join(3));
        assert_eq!(cluster.membership_epoch(), 3);
        assert!(cluster.quorum_available());

        cluster.set_link(
            1,
            2,
            LinkConfig {
                delay_ms: 5,
                ..LinkConfig::default()
            },
        );
        assert!(cluster.send(0, 1, 2, b"join-catch-up".to_vec()));
        assert!(cluster.crash(3));
        assert!(cluster.quorum_available());
        cluster.partition(&[1], &[2], true);
        assert!(!cluster.send(0, 1, 2, b"partitioned".to_vec()));
        cluster.partition(&[1], &[2], false);
        assert_eq!(cluster.deliver_ready(4), Vec::new());
        assert_eq!(cluster.deliver_ready(5)[0].payload, b"join-catch-up");
        assert!(cluster.restart(3));
        assert!(cluster.leave(2));
        assert!(cluster.crash(3));
        assert!(!cluster.quorum_available());
    }
}
