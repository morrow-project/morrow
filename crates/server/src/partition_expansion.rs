//! Epoch-fenced online partition expansion state machine.

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExpansionPlan {
    pub from_partitions: u32,
    pub to_partitions: u32,
    pub epoch: u64,
    pub prepared_partitions: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpochDecision {
    Current,
    RefreshRequired { current_epoch: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionExpansion {
    current_partitions: u32,
    current_epoch: u64,
    pending: Option<ExpansionPlan>,
}

impl PartitionExpansion {
    pub fn new(partitions: u32, epoch: u64) -> Option<Self> {
        (partitions > 0).then_some(Self {
            current_partitions: partitions,
            current_epoch: epoch,
            pending: None,
        })
    }

    pub fn begin(&mut self, partitions: u32) -> Option<&ExpansionPlan> {
        if self
            .pending
            .as_ref()
            .is_some_and(|pending| pending.to_partitions == partitions)
        {
            return self.pending.as_ref();
        }
        if self.pending.is_some() {
            return None;
        }
        if partitions <= self.current_partitions {
            return None;
        }
        self.pending = Some(ExpansionPlan {
            from_partitions: self.current_partitions,
            to_partitions: partitions,
            epoch: self.current_epoch.saturating_add(1),
            prepared_partitions: 0,
        });
        self.pending.as_ref()
    }

    pub fn mark_prepared(&mut self, count: u32) -> bool {
        let Some(plan) = self.pending.as_mut() else {
            return false;
        };
        if count > plan.to_partitions {
            return false;
        }
        plan.prepared_partitions = plan.prepared_partitions.max(count);
        true
    }

    pub fn activate(&mut self) -> bool {
        let Some(plan) = self.pending.take() else {
            return false;
        };
        if plan.prepared_partitions != plan.to_partitions {
            self.pending = Some(plan);
            return false;
        }
        self.current_partitions = plan.to_partitions;
        self.current_epoch = plan.epoch;
        true
    }

    pub fn decide(&self, epoch: u64) -> EpochDecision {
        if epoch == self.current_epoch {
            EpochDecision::Current
        } else {
            EpochDecision::RefreshRequired {
                current_epoch: self.current_epoch,
            }
        }
    }

    pub fn current(&self) -> (u32, u64) {
        (self.current_partitions, self.current_epoch)
    }

    pub fn pending(&self) -> Option<&ExpansionPlan> {
        self.pending.as_ref()
    }
}
