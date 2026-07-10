use std::sync::Arc;
use bytes::Bytes;
use futures::{Stream, StreamExt};

use crate::budget::BudgetRegistry;
use crate::error::WeirError;
use crate::provider::ProviderAdapter;

const BUDGET_EXCEEDED_EVENT: &[u8] =
    b"event: error\ndata: {\"error\":\"budget_exceeded\"}\n\n";

/// Buffers raw upstream bytes and yields only complete SSE events (each
/// terminated by a blank line, `"\n\n"`), retaining any trailing partial
/// event across chunk boundaries. `reqwest`'s `bytes_stream()` yields
/// TCP/TLS-sized reads that are not aligned to SSE event boundaries — a
/// `data: {...}` line can be split across two reads under ordinary network
/// conditions. Without reassembly, both halves fail to parse and are
/// silently dropped, undercounting token usage and potentially letting an
/// over-budget chunk through. This buffer guarantees every adapter call
/// receives one complete event, regardless of how the bytes arrived on the
/// wire.
struct SseFrameBuffer {
    buf: Vec<u8>,
}

impl SseFrameBuffer {
    fn new() -> Self {
        Self { buf: Vec::new() }
    }

    /// Appends `chunk` and returns any complete events now available (each
    /// including its trailing blank-line separator). Retains any trailing
    /// partial event in the buffer for the next call.
    fn push(&mut self, chunk: &[u8]) -> Vec<Bytes> {
        self.buf.extend_from_slice(chunk);
        let mut events = Vec::new();
        while let Some(end) = find_event_boundary(&self.buf) {
            let event: Vec<u8> = self.buf.drain(..end).collect();
            events.push(Bytes::from(event));
        }
        events
    }

    /// Called once the upstream stream ends. SSE streams should always end
    /// with a blank line, but this is a best-effort flush of whatever's
    /// left rather than silently dropping a final event that arrived
    /// without a trailing separator.
    fn flush(&mut self) -> Option<Bytes> {
        if self.buf.is_empty() {
            None
        } else {
            Some(Bytes::from(std::mem::take(&mut self.buf)))
        }
    }
}

/// Returns the byte offset just past the first `"\n\n"` in `buf`, if any.
fn find_event_boundary(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\n\n").map(|i| i + 2)
}

enum EventOutcome {
    Forward(Bytes),
    Trip,
}

/// Computes one reassembled event's cost, records it against the tenant's
/// budget, and decides whether it may be forwarded. Recording happens
/// unconditionally before this function returns `Forward` — the caller
/// must yield nothing beyond the terminal event for a `Trip` outcome.
fn process_event(
    event: &Bytes,
    adapter: &mut dyn ProviderAdapter,
    budget: &BudgetRegistry,
    tenant: &str,
    recorded_so_far: &mut u64,
    now_ms: i64,
) -> Result<EventOutcome, WeirError> {
    let cost = adapter.chunk_cost(event);
    let delta = match cost.authoritative_total {
        Some(total) => {
            let delta = total.saturating_sub(*recorded_so_far);
            *recorded_so_far = total;
            delta
        }
        None => {
            *recorded_so_far += cost.estimated_tokens;
            cost.estimated_tokens
        }
    };

    let within_budget = budget.record(tenant, delta, now_ms)?;

    Ok(if within_budget {
        EventOutcome::Forward(event.clone())
    } else {
        EventOutcome::Trip
    })
}

/// Wraps an upstream SSE byte stream, enforcing the tenant's token budget
/// event by event. Raw upstream reads are first reassembled into complete
/// SSE events (see `SseFrameBuffer`), so accounting and forwarding
/// decisions are never made against a partial frame. Each event's cost is
/// recorded against the budget BEFORE it is yielded; an event that would
/// breach the ceiling is never forwarded. Instead, a terminal SSE error
/// event is yielded and the stream ends.
pub fn enforce(
    tenant: String,
    mut upstream: impl Stream<Item = reqwest::Result<Bytes>> + Unpin + Send + 'static,
    mut adapter: Box<dyn ProviderAdapter>,
    budget: Arc<BudgetRegistry>,
    now_ms: impl Fn() -> i64 + Send + 'static,
) -> impl Stream<Item = Result<Bytes, WeirError>> {
    async_stream::stream! {
        let mut recorded_so_far: u64 = 0;
        let mut frames = SseFrameBuffer::new();

        while let Some(chunk_res) = upstream.next().await {
            let raw = match chunk_res {
                Ok(raw) => raw,
                Err(e) => {
                    yield Err(WeirError::Upstream(e));
                    return;
                }
            };

            for event in frames.push(&raw) {
                match process_event(&event, adapter.as_mut(), &budget, &tenant, &mut recorded_so_far, now_ms()) {
                    Ok(EventOutcome::Forward(bytes)) => yield Ok(bytes),
                    Ok(EventOutcome::Trip) => {
                        yield Ok(Bytes::from_static(BUDGET_EXCEEDED_EVENT));
                        return;
                    }
                    Err(e) => {
                        yield Err(e);
                        return;
                    }
                }
            }
        }

        if let Some(event) = frames.flush() {
            match process_event(&event, adapter.as_mut(), &budget, &tenant, &mut recorded_so_far, now_ms()) {
                Ok(EventOutcome::Forward(bytes)) => yield Ok(bytes),
                Ok(EventOutcome::Trip) => yield Ok(Bytes::from_static(BUDGET_EXCEEDED_EVENT)),
                Err(e) => yield Err(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BudgetLimit, TenantLimits};
    use crate::provider::{ChunkCost, OpenAiAdapter, ProviderAdapter};
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

        fn non_streaming_cost(&self, _body: &Bytes) -> Option<u64> {
            None
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

        fn non_streaming_cost(&self, _body: &Bytes) -> Option<u64> {
            None
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
            Ok(Bytes::from_static(b"chunk1\n\n")),
            Ok(Bytes::from_static(b"chunk2\n\n")),
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
            Ok(Bytes::from_static(b"chunk1\n\n")),
            Ok(Bytes::from_static(b"chunk2\n\n")), // pushes total to 20, over the 15 ceiling
            Ok(Bytes::from_static(b"chunk3\n\n")), // must never be forwarded
        ]);
        let adapter: Box<dyn ProviderAdapter> = Box::new(FixedCostAdapter { cost_per_chunk: 10 });
        let budget = budget_with("acct_1", 15);

        let out: Vec<_> = enforce("acct_1".into(), upstream, adapter, budget, || 0)
            .collect()
            .await;

        assert_eq!(out.len(), 2, "chunk1 forwarded, then trip event — chunk3 never reached");
        assert_eq!(out[0].as_ref().unwrap(), &Bytes::from_static(b"chunk1\n\n"));
        let trip_event = out[1].as_ref().unwrap();
        assert!(String::from_utf8_lossy(trip_event).contains("budget_exceeded"));
    }

    #[tokio::test]
    async fn authoritative_total_reconciles_via_delta_not_double_count() {
        let upstream = futures::stream::iter(vec![
            Ok(Bytes::from_static(b"chunk1\n\n")),
            Ok(Bytes::from_static(b"chunk2\n\n")),
            Ok(Bytes::from_static(b"chunk3\n\n")),
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
        assert_eq!(out[2].as_ref().unwrap(), &Bytes::from_static(b"chunk3\n\n"));
    }

    #[tokio::test]
    async fn reassembles_sse_event_split_across_multiple_raw_chunks() {
        // Simulates a network read splitting a single SSE event's `data:`
        // line across two physical chunks — the exact scenario that, before
        // frame reassembly, caused a silent parse failure and an
        // undercounted (dropped) token estimate.
        let upstream = futures::stream::iter(vec![
            Ok(Bytes::from_static(b"data: {\"choices\":[{\"delta\":{\"content\":\"Hel")),
            Ok(Bytes::from_static(b"lo\"}}]}\n\n")),
        ]);
        let tokenizer = Arc::new(tiktoken_rs::cl100k_base().unwrap());
        let adapter: Box<dyn ProviderAdapter> = Box::new(OpenAiAdapter::new(tokenizer));
        let budget = budget_with("acct_1", 1000);

        let out: Vec<_> = enforce("acct_1".into(), upstream, adapter, budget, || 0)
            .collect()
            .await;

        assert_eq!(out.len(), 1, "the two raw chunks reassemble into exactly one complete event");
        let forwarded = out[0].as_ref().unwrap();
        assert_eq!(
            forwarded,
            &Bytes::from_static(b"data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}\n\n"),
            "the reassembled event must contain the full, unsplit content"
        );
    }

    #[tokio::test]
    async fn multiple_events_in_one_raw_chunk_are_processed_independently() {
        // The inverse scenario: two complete SSE events arriving in a
        // single raw read (e.g. a small mock response body) must still be
        // split into two independently-accounted events, not treated as
        // one combined blob.
        let upstream = futures::stream::iter(vec![Ok(Bytes::from_static(
            b"data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"b\"}}]}\n\n",
        ))]);
        let adapter: Box<dyn ProviderAdapter> = Box::new(FixedCostAdapter { cost_per_chunk: 10 });
        let budget = budget_with("acct_1", 1000);

        let out: Vec<_> = enforce("acct_1".into(), upstream, adapter, budget, || 0)
            .collect()
            .await;

        assert_eq!(
            out.len(),
            2,
            "one raw chunk containing two SSE events must yield two forwarded events"
        );
    }

    mod sse_frame_buffer_tests {
        use super::*;

        #[test]
        fn splits_multiple_complete_events_in_one_push() {
            let mut buf = SseFrameBuffer::new();
            let events = buf.push(b"event1\n\nevent2\n\n");
            assert_eq!(
                events,
                vec![Bytes::from_static(b"event1\n\n"), Bytes::from_static(b"event2\n\n")]
            );
        }

        #[test]
        fn retains_partial_event_across_pushes() {
            let mut buf = SseFrameBuffer::new();
            assert_eq!(buf.push(b"partial data witho"), Vec::<Bytes>::new());
            let events = buf.push(b"ut a boundary yet\n\n");
            assert_eq!(
                events,
                vec![Bytes::from_static(b"partial data without a boundary yet\n\n")]
            );
        }

        #[test]
        fn flush_returns_trailing_partial_data() {
            let mut buf = SseFrameBuffer::new();
            buf.push(b"no trailing separator");
            assert_eq!(buf.flush(), Some(Bytes::from_static(b"no trailing separator")));
            assert_eq!(
                buf.flush(),
                None,
                "flush drains the buffer; a second call has nothing left"
            );
        }
    }
}
