use super::*;
use std::sync::Mutex as StdMutex;

const MAX_WAITERS_PER_CONNECTION: usize = 1;
const MAX_WAITERS_PER_CONSUMER: usize = 64;

#[derive(Clone, Default)]
pub(super) struct PullWaiterRegistry {
    state: Arc<StdMutex<PullWaiterState>>,
    #[cfg(test)]
    fetch_checks: Arc<AtomicU64>,
}

#[derive(Default)]
struct PullWaiterState {
    next_id: u64,
    waiters: HashMap<u64, WaiterRecord>,
    availability: HashMap<String, Arc<Notify>>,
    shutdown: bool,
}

struct WaiterRecord {
    connection_id: u64,
    consumer_id: String,
    filter_subject: String,
    cancellation: Arc<Cancellation>,
}

pub(super) struct PullWaiter {
    id: u64,
    consumer_id: String,
    registry: PullWaiterRegistry,
    availability: Arc<Notify>,
    cancellation: Arc<Cancellation>,
}

struct Cancellation {
    cancelled: AtomicBool,
    notify: Notify,
}

impl PullWaiterRegistry {
    pub(super) fn register(
        &self,
        connection_id: u64,
        consumer_id: &str,
        filter_subject: &str,
    ) -> Result<PullWaiter> {
        let mut state = self.state.lock().unwrap();
        crate::broker_ensure!(!state.shutdown, "broker is shutting down");
        let connection_waiters = state
            .waiters
            .values()
            .filter(|waiter| waiter.connection_id == connection_id)
            .count();
        crate::broker_ensure!(
            connection_waiters < MAX_WAITERS_PER_CONNECTION,
            "FETCH waiter limit reached for connection"
        );
        let consumer_waiters = state
            .waiters
            .values()
            .filter(|waiter| waiter.consumer_id == consumer_id)
            .count();
        crate::broker_ensure!(
            consumer_waiters < MAX_WAITERS_PER_CONSUMER,
            "FETCH waiter limit reached for consumer"
        );

        state.next_id = state
            .next_id
            .checked_add(1)
            .ok_or_else(|| BrokerError::msg("FETCH waiter identifier exhausted"))?;
        let id = state.next_id;
        let availability = state
            .availability
            .entry(consumer_id.to_string())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone();
        let cancellation = Arc::new(Cancellation {
            cancelled: AtomicBool::new(false),
            notify: Notify::new(),
        });
        state.waiters.insert(
            id,
            WaiterRecord {
                connection_id,
                consumer_id: consumer_id.to_string(),
                filter_subject: filter_subject.to_string(),
                cancellation: cancellation.clone(),
            },
        );
        Ok(PullWaiter {
            id,
            consumer_id: consumer_id.to_string(),
            registry: self.clone(),
            availability,
            cancellation,
        })
    }

    pub(super) fn notify_subject(&self, published_subject: &str) {
        let notifications = {
            let state = self.state.lock().unwrap();
            let mut consumers = HashSet::new();
            state
                .waiters
                .values()
                .filter(|waiter| subject::matches(&waiter.filter_subject, published_subject))
                .filter(|waiter| consumers.insert(waiter.consumer_id.as_str()))
                .filter_map(|waiter| state.availability.get(&waiter.consumer_id))
                .cloned()
                .collect::<Vec<_>>()
        };
        for notification in notifications {
            notification.notify_waiters();
        }
    }

    pub(super) fn notify_consumer(&self, consumer_id: &str) {
        let notification = self
            .state
            .lock()
            .unwrap()
            .availability
            .get(consumer_id)
            .cloned();
        if let Some(notification) = notification {
            notification.notify_waiters();
        }
    }

    pub(super) fn notify_all(&self) {
        let notifications = self
            .state
            .lock()
            .unwrap()
            .availability
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for notification in notifications {
            notification.notify_waiters();
        }
    }

    pub(super) fn cancel_consumer(&self, consumer_id: &str) {
        self.cancel_matching(|waiter| waiter.consumer_id == consumer_id);
    }

    pub(super) fn cancel_connection(&self, connection_id: u64) {
        self.cancel_matching(|waiter| waiter.connection_id == connection_id);
    }

    pub(super) fn shutdown(&self) {
        let cancellations = {
            let mut state = self.state.lock().unwrap();
            state.shutdown = true;
            state
                .waiters
                .values()
                .map(|waiter| waiter.cancellation.clone())
                .collect::<Vec<_>>()
        };
        cancel(cancellations);
    }

    fn cancel_matching(&self, matches: impl Fn(&WaiterRecord) -> bool) {
        let cancellations = self
            .state
            .lock()
            .unwrap()
            .waiters
            .values()
            .filter(|waiter| matches(waiter))
            .map(|waiter| waiter.cancellation.clone())
            .collect::<Vec<_>>();
        cancel(cancellations);
    }

    #[cfg(test)]
    pub(super) fn waiter_count(&self) -> usize {
        self.state.lock().unwrap().waiters.len()
    }

    #[cfg(test)]
    pub(super) fn record_fetch_check(&self) {
        self.fetch_checks.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(super) fn fetch_check_count(&self) -> u64 {
        self.fetch_checks.load(Ordering::Relaxed)
    }
}

impl PullWaiter {
    pub(super) fn availability(&self) -> &Notify {
        &self.availability
    }

    pub(super) fn cancellation(&self) -> &Notify {
        &self.cancellation.notify
    }

    pub(super) fn is_cancelled(&self) -> bool {
        self.cancellation.cancelled.load(Ordering::Acquire)
    }
}

impl Drop for PullWaiter {
    fn drop(&mut self) {
        let mut state = self.registry.state.lock().unwrap();
        state.waiters.remove(&self.id);
        if !state
            .waiters
            .values()
            .any(|waiter| waiter.consumer_id == self.consumer_id)
        {
            state.availability.remove(&self.consumer_id);
        }
    }
}

fn cancel(cancellations: Vec<Arc<Cancellation>>) {
    for cancellation in cancellations {
        cancellation.cancelled.store(true, Ordering::Release);
        cancellation.notify.notify_waiters();
    }
}
