pub mod sliding_window;

use std::sync::Arc;
use arc_swap::ArcSwap;
use dashmap::DashMap;

use crate::config::{BudgetLimit, TenantLimits};
use crate::error::WeirError;
use sliding_window::SlidingWindowCounter;

pub struct BudgetRegistry {
    limits: Arc<ArcSwap<TenantLimits>>,
    counters: DashMap<String, Arc<SlidingWindowCounter>>,
}

impl BudgetRegistry {
    pub fn new(limits: Arc<ArcSwap<TenantLimits>>) -> Self {
        Self { limits, counters: DashMap::new() }
    }

    fn limit_for(&self, tenant: &str) -> Result<BudgetLimit, WeirError> {
        self.limits
            .load()
            .get(tenant)
            .copied()
            .ok_or(WeirError::UnknownTenant)
    }

    fn counter_for(&self, tenant: &str, limit: BudgetLimit) -> Arc<SlidingWindowCounter> {
        self.counters
            .entry(tenant.to_string())
            .or_insert_with(|| Arc::new(SlidingWindowCounter::new(limit.window)))
            .clone()
    }

    pub fn is_within_budget(&self, tenant: &str, now_ms: i64) -> Result<bool, WeirError> {
        let limit = self.limit_for(tenant)?;
        let counter = self.counter_for(tenant, limit);
        Ok(counter.estimate(now_ms) < limit.max_tokens)
    }

    pub fn record(&self, tenant: &str, amount: u64, now_ms: i64) -> Result<bool, WeirError> {
        let limit = self.limit_for(tenant)?;
        let counter = self.counter_for(tenant, limit);
        let total = counter.add(amount, now_ms);
        Ok(total <= limit.max_tokens)
    }
}

#[cfg(test)]
mod registry_tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;

    fn registry_with(tenant: &str, max_tokens: u64, window_secs: u64) -> BudgetRegistry {
        let mut limits = HashMap::new();
        limits.insert(
            tenant.to_string(),
            BudgetLimit { max_tokens, window: Duration::from_secs(window_secs) },
        );
        BudgetRegistry::new(Arc::new(ArcSwap::from_pointee(limits)))
    }

    #[test]
    fn unknown_tenant_is_rejected() {
        let registry = registry_with("acct_1", 1000, 60);
        let result = registry.is_within_budget("acct_unknown", 0);
        assert!(matches!(result, Err(WeirError::UnknownTenant)));
    }

    #[test]
    fn records_and_trips_at_ceiling() {
        let registry = registry_with("acct_1", 100, 60);
        assert!(registry.record("acct_1", 60, 0).unwrap());
        assert!(registry.record("acct_1", 30, 0).unwrap()); // 90, still within
        assert!(!registry.record("acct_1", 20, 0).unwrap()); // 110, over
    }
}
