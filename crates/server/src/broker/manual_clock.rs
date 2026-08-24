#[cfg(test)]
use super::Clock;

#[cfg(test)]
pub(crate) type ManualClock = simulation::VirtualClock;

#[cfg(test)]
impl Clock for simulation::VirtualClock {
    fn now_ms(&self) -> u64 {
        simulation::VirtualClock::now_ms(self)
    }
}
