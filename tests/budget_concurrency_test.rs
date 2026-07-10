use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use weir::budget::BudgetRegistry;
use weir::config::{BudgetLimit, TenantLimits};

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_streams_never_exceed_ceiling_by_more_than_one_chunk() {
    let mut limits: TenantLimits = HashMap::new();
    let ceiling = 10_000u64;
    limits.insert(
        "acct_1".to_string(),
        BudgetLimit { max_tokens: ceiling, window: Duration::from_secs(60) },
    );
    let registry = Arc::new(BudgetRegistry::new(Arc::new(arc_swap::ArcSwap::from_pointee(
        limits,
    ))));

    let chunk_cost = 50u64;
    let successful_calls = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for _ in 0..40 {
        let registry = registry.clone();
        let successful_calls = successful_calls.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..10 {
                if registry.record("acct_1", chunk_cost, 0).unwrap() {
                    successful_calls.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // Each successful call recorded exactly `chunk_cost` tokens, so total
    // recorded usage is `successful_calls * chunk_cost`. Under lock-free
    // accounting, concurrent racing callers can push the total up to one
    // chunk past the ceiling before the registry starts rejecting further
    // calls (the accepted, documented trade-off — see BudgetRegistry::
    // record's doc comment) — but it must never run away unbounded. The
    // expected count is ceiling / chunk_cost = 200; allow one extra chunk
    // (201) for that bounded overshoot.
    let expected = ceiling / chunk_cost;
    let count = successful_calls.load(Ordering::Relaxed) as u64;
    assert!(
        count <= expected + 1,
        "expected at most {} successful calls (one chunk of overshoot allowed), got {count}",
        expected + 1
    );
    assert!(
        count < 400,
        "all 400 concurrent calls succeeded — the ceiling was never enforced"
    );
}
