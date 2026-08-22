use super::*;

#[derive(Default)]
pub(super) struct RouteInterestChanges {
    pub(super) added: Vec<String>,
    pub(super) removed: Vec<String>,
}

impl RouteInterestChanges {
    pub(super) fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty()
    }

    pub(super) fn merge(&mut self, mut other: Self) {
        self.added.append(&mut other.added);
        self.removed.append(&mut other.removed);
    }
}

impl TransientState {
    pub(super) fn upsert_subscription(
        &mut self,
        key: (u64, String),
        subscription: TransientSubscription,
    ) -> RouteInterestChanges {
        if self
            .subscriptions
            .get(&key)
            .is_some_and(|existing| existing.subject == subscription.subject)
        {
            self.subscriptions.insert(key, subscription);
            return RouteInterestChanges::default();
        }
        let mut changes = RouteInterestChanges::default();
        if let Some(existing) = self.subscriptions.remove(&key) {
            self.interest_index.remove(&existing.subject, &key);
            self.decrement_route_interest(&existing.subject, &mut changes);
        }
        self.interest_index
            .insert(&subscription.subject, key.clone());
        self.increment_route_interest(&subscription.subject, &mut changes);
        self.subscriptions.insert(key, subscription);
        changes
    }

    pub(super) fn remove_subscription(&mut self, key: &(u64, String)) -> RouteInterestChanges {
        let mut changes = RouteInterestChanges::default();
        if let Some(subscription) = self.subscriptions.remove(key) {
            self.interest_index.remove(&subscription.subject, key);
            self.decrement_route_interest(&subscription.subject, &mut changes);
        }
        changes
    }

    pub(super) fn remove_connection_interests(
        &mut self,
        connection_id: u64,
    ) -> RouteInterestChanges {
        let keys = self
            .subscriptions
            .keys()
            .filter(|(client_id, _)| *client_id == connection_id)
            .cloned()
            .collect::<Vec<_>>();
        let mut changes = RouteInterestChanges::default();
        for key in keys {
            changes.merge(self.remove_subscription(&key));
        }
        changes
    }

    pub(super) fn decrement_subscription(
        &mut self,
        connection_id: u64,
        sid: &str,
    ) -> RouteInterestChanges {
        let key = (connection_id, sid.to_string());
        let should_remove = self
            .subscriptions
            .get_mut(&key)
            .and_then(|subscription| decrement_remaining(&mut subscription.remaining_deliveries))
            .unwrap_or(false);
        if should_remove {
            self.remove_subscription(&key)
        } else {
            RouteInterestChanges::default()
        }
    }

    pub(super) fn route_interests(&self) -> Vec<String> {
        self.route_interest_counts.keys().cloned().collect()
    }

    fn increment_route_interest(&mut self, subject: &str, changes: &mut RouteInterestChanges) {
        let count = self
            .route_interest_counts
            .entry(subject.to_string())
            .or_default();
        if *count == 0 {
            changes.added.push(subject.to_string());
        }
        *count = count.saturating_add(1);
    }

    fn decrement_route_interest(&mut self, subject: &str, changes: &mut RouteInterestChanges) {
        let Some(count) = self.route_interest_counts.get_mut(subject) else {
            return;
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            self.route_interest_counts.remove(subject);
            changes.removed.push(subject.to_string());
        }
    }
}
