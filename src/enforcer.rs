use std::sync::Arc;
use bytes::Bytes;
use futures::{Stream, StreamExt};

use crate::budget::BudgetRegistry;
use crate::error::WeirError;
use crate::provider::ProviderAdapter;

const BUDGET_EXCEEDED_EVENT: &[u8] =
    b"event: error\ndata: {\"error\":\"budget_exceeded\"}\n\n";

/// Wraps an upstream SSE byte stream, enforcing the tenant's token budget
/// chunk by chunk. Each chunk's cost is recorded against the budget BEFORE
/// it is yielded; a chunk that would breach the ceiling is never forwarded.
/// Instead, a terminal SSE error event is yielded and the stream ends.
pub fn enforce(
    tenant: String,
    mut upstream: impl Stream<Item = reqwest::Result<Bytes>> + Unpin + Send + 'static,
    mut adapter: Box<dyn ProviderAdapter>,
    budget: Arc<BudgetRegistry>,
    now_ms: impl Fn() -> i64 + Send + 'static,
) -> impl Stream<Item = Result<Bytes, WeirError>> {
    async_stream::stream! {
        let mut recorded_so_far: u64 = 0;

        while let Some(chunk_res) = upstream.next().await {
            let raw = match chunk_res {
                Ok(raw) => raw,
                Err(e) => {
                    yield Err(WeirError::Upstream(e));
                    return;
                }
            };

            let cost = adapter.chunk_cost(&raw);
            let delta = match cost.authoritative_total {
                Some(total) => {
                    let delta = total.saturating_sub(recorded_so_far);
                    recorded_so_far = total;
                    delta
                }
                None => {
                    recorded_so_far += cost.estimated_tokens;
                    cost.estimated_tokens
                }
            };

            let within_budget = match budget.record(&tenant, delta, now_ms()) {
                Ok(v) => v,
                Err(e) => {
                    yield Err(e);
                    return;
                }
            };

            if !within_budget {
                yield Ok(Bytes::from_static(BUDGET_EXCEEDED_EVENT));
                return;
            }

            yield Ok(raw);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BudgetLimit, TenantLimits};
    use crate::provider::{ChunkCost, ProviderAdapter};
    use arc_swap::ArcSwap;
    use std::collections::HashMap;
    use std::time::Duration;

    struct FixedCostAdapter {
        cost_per_chunk: u64,
    }

    impl ProviderAdapter for FixedCostAdapter {
        fn chunk_cost(&mut self, _raw: &Bytes) -> ChunkCost {
            ChunkCost { estimated_tokens: self.cost_per_chunk, authoritative_total: None }
        }
    }

    struct AuthoritativeCostAdapter {
        totals: std::collections::VecDeque<u64>,
    }

    impl ProviderAdapter for AuthoritativeCostAdapter {
        fn chunk_cost(&mut self, _raw: &Bytes) -> ChunkCost {
            let total = self.totals.pop_front().unwrap_or(0);
            ChunkCost { estimated_tokens: 0, authoritative_total: Some(total) }
        }
    }

    fn budget_with(tenant: &str, max_tokens: u64) -> Arc<BudgetRegistry> {
        let mut limits: TenantLimits = HashMap::new();
        limits.insert(
            tenant.to_string(),
            BudgetLimit { max_tokens, window: Duration::from_secs(60) },
        );
        Arc::new(BudgetRegistry::new(Arc::new(ArcSwap::from_pointee(limits))))
    }

    #[tokio::test]
    async fn forwards_chunks_within_budget() {
        let upstream = futures::stream::iter(vec![
            Ok(Bytes::from_static(b"chunk1")),
            Ok(Bytes::from_static(b"chunk2")),
        ]);
        let adapter: Box<dyn ProviderAdapter> = Box::new(FixedCostAdapter { cost_per_chunk: 10 });
        let budget = budget_with("acct_1", 1000);

        let out: Vec<_> = enforce("acct_1".into(), upstream, adapter, budget, || 0)
            .collect()
            .await;

        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|r| r.is_ok()));
    }

    #[tokio::test]
    async fn trips_before_forwarding_over_budget_chunk() {
        let upstream = futures::stream::iter(vec![
            Ok(Bytes::from_static(b"chunk1")),
            Ok(Bytes::from_static(b"chunk2")), // pushes total to 20, over the 15 ceiling
            Ok(Bytes::from_static(b"chunk3")), // must never be forwarded
        ]);
        let adapter: Box<dyn ProviderAdapter> = Box::new(FixedCostAdapter { cost_per_chunk: 10 });
        let budget = budget_with("acct_1", 15);

        let out: Vec<_> = enforce("acct_1".into(), upstream, adapter, budget, || 0)
            .collect()
            .await;

        assert_eq!(out.len(), 2, "chunk1 forwarded, then trip event — chunk3 never reached");
        assert_eq!(out[0].as_ref().unwrap(), &Bytes::from_static(b"chunk1"));
        let trip_event = out[1].as_ref().unwrap();
        assert!(String::from_utf8_lossy(trip_event).contains("budget_exceeded"));
    }

    #[tokio::test]
    async fn authoritative_total_reconciles_via_delta_not_double_count() {
        let upstream = futures::stream::iter(vec![
            Ok(Bytes::from_static(b"chunk1")),
            Ok(Bytes::from_static(b"chunk2")),
            Ok(Bytes::from_static(b"chunk3")),
        ]);
        let adapter: Box<dyn ProviderAdapter> = Box::new(AuthoritativeCostAdapter {
            totals: std::collections::VecDeque::from(vec![30, 70, 70]),
        });
        let budget = budget_with("acct_1", 100);

        let out: Vec<_> = enforce("acct_1".into(), upstream, adapter, budget, || 0)
            .collect()
            .await;

        // Chunk1 reports total=30 (delta 30, recorded=30). Chunk2 reports
        // total=70 (delta 40, recorded=70). Chunk3 reports the SAME total=70
        // again (delta 0, recorded stays 70). All three should forward
        // normally, staying under the 100 ceiling throughout.
        //
        // If the reconciliation regressed to recording the full `total` each
        // time instead of the delta versus what was already recorded, the
        // registry would see 30 + 70 + 70 = 170 — over the 100 ceiling — and
        // chunk3 would incorrectly trip instead of being forwarded. This test
        // exists specifically to catch that regression.
        assert_eq!(out.len(), 3);
        assert!(out.iter().all(|r| r.is_ok()));
        assert_eq!(out[2].as_ref().unwrap(), &Bytes::from_static(b"chunk3"));
    }
}
