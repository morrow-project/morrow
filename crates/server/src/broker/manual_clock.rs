#[cfg(test)]
use super::*;

#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct ManualClock {
    now_ms: AtomicU64,
}

#[cfg(test)]
impl ManualClock {
    pub(super) fn new(now_ms: u64) -> Self {
        Self {
            now_ms: AtomicU64::new(now_ms),
        }
    }

    pub(super) fn advance_ms(&self, millis: u64) {
        self.now_ms.fetch_add(millis, Ordering::Relaxed);
    }
}

#[cfg(test)]
impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.now_ms.load(Ordering::Relaxed)
    }
}
