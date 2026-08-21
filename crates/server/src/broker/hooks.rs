use super::*;

#[derive(Clone)]
pub(crate) struct BrokerHooks {
    pub(super) clock: Arc<dyn Clock>,
    pub(super) start_redelivery_loop: bool,
    pub(super) durable_publish_flush_mode: DurablePublishFlushMode,
    pub(super) middleware: MiddlewareRuntime,
    #[cfg(test)]
    pub(super) initial_cluster: Option<ClusterRuntime>,
}

pub(crate) trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DurablePublishFlushMode {
    SleepThenFlush,
    #[cfg(test)]
    FlushImmediately,
}

pub(super) struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        now_ms()
    }
}

impl Default for BrokerHooks {
    fn default() -> Self {
        Self {
            clock: Arc::new(SystemClock),
            start_redelivery_loop: true,
            durable_publish_flush_mode: DurablePublishFlushMode::SleepThenFlush,
            middleware: MiddlewareRuntime::default(),
            #[cfg(test)]
            initial_cluster: None,
        }
    }
}
