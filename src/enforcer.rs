use std::sync::Arc;
use bytes::Bytes;
use futures::{Stream, StreamExt};

use crate::budget::BudgetRegistry;
use crate::error::WeirError;
use crate::provider::{Provider, ProviderAdapter};
use crate::telemetry::{EventLog, UsageEvent, UsageOutcome};

const BUDGET_EXCEEDED_EVENT: &[u8] =
    b"event: error\ndata: {\"error\":\"budget_exceeded\"}\n\n";

fn policy_violation_event(tool: &str) -> Bytes {
    Bytes::from(format!(
        "event: error\ndata: {{\"error\":\"policy_violation\",\"tool\":\"{tool}\"}}\n\n"
    ))
}

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

    fn push(&mut self, chunk: &[u8]) -> Vec<Bytes> {
        self.buf.extend_from_slice(chunk);
        let mut events = Vec::new();
        while let Some(end) = find_event_boundary(&self.buf) {
            let event: Vec<u8> = self.buf.drain(..end).collect();
            events.push(Bytes::from(event));
        }
        events
    }

    fn flush(&mut self) -> Option<Bytes> {
        if self.buf.is_empty() {
            None
        } else {
            Some(Bytes::from(std::mem::take(&mut self.buf)))
        }
    }
}

fn find_event_boundary(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\n\n").map(|i| i + 2)
}

enum EventOutcome {
    Forward(Bytes),
    BudgetTrip,
    PolicyTrip(String),
}

struct EventAccounting<'a> {
    adapter: &'a mut dyn ProviderAdapter,
    budget: &'a BudgetRegistry,
    tenant: &'a str,
    blocked_tools: &'a [String],
    recorded_so_far: &'a mut u64,
    tools_seen: &'a mut Vec<String>,
}

fn process_event(acc: &mut EventAccounting, event: &Bytes, now_ms: i64) -> Result<EventOutcome, WeirError> {
    let cost = acc.adapter.chunk_cost(event);

    for tool in &cost.tool_calls {
        if !acc.tools_seen.contains(tool) {
            acc.tools_seen.push(tool.clone());
        }
        if acc.blocked_tools.contains(tool) {
            return Ok(EventOutcome::PolicyTrip(tool.clone()));
        }
    }

    let delta = match cost.authoritative_total {
        Some(total) => {
            let delta = total.saturating_sub(*acc.recorded_so_far);
            *acc.recorded_so_far = total;
            delta
        }
        None => {
            *acc.recorded_so_far += cost.estimated_tokens;
            cost.estimated_tokens
        }
    };

    let within_budget = acc.budget.record(acc.tenant, delta, now_ms)?;

    Ok(if within_budget {
        EventOutcome::Forward(event.clone())
    } else {
        EventOutcome::BudgetTrip
    })
}

/// Owns the per-stream telemetry state and guarantees exactly one
/// `UsageEvent` is emitted for the request. Terminal paths call `emit(...)`
/// explicitly; if the stream generator is dropped before any terminal
/// (e.g. the client disconnects mid-stream), `Drop` emits an `Incomplete`
/// event so no request is ever invisible in telemetry.
struct StreamTelemetry {
    event_log: Arc<EventLog>,
    tenant: String,
    provider: Provider,
    model: Option<String>,
    now_ms: Box<dyn Fn() -> i64 + Send>,
    tools_seen: Vec<String>,
    recorded_so_far: u64,
    emitted: bool,
}

impl StreamTelemetry {
    fn emit(&mut self, outcome: UsageOutcome, rule: Option<String>) {
        if self.emitted {
            return;
        }
        self.emitted = true;
        self.event_log.push(UsageEvent {
            id: 0,
            tenant: self.tenant.clone(),
            provider: self.provider,
            model: self.model.clone(),
            tools_called: self.tools_seen.clone(),
            tokens: self.recorded_so_far,
            outcome,
            rule,
            timestamp_ms: (self.now_ms)(),
        });
    }
}

impl Drop for StreamTelemetry {
    fn drop(&mut self) {
        // Reached only if no terminal path emitted first — i.e. the
        // generator was dropped early (client disconnect). Record the
        // partial request as Incomplete rather than losing it entirely.
        if !self.emitted {
            self.emit(UsageOutcome::Incomplete, None);
        }
    }
}

/// Wraps an upstream SSE byte stream, enforcing the tenant's token budget
/// and tool policy event by event. Raw upstream reads are first
/// reassembled into complete SSE events (see `SseFrameBuffer`), so
/// accounting and forwarding decisions are never made against a partial
/// frame. Each event's cost is recorded against the budget, and its tool
/// calls checked against policy, BEFORE it is yielded; an event that would
/// breach the budget or invoke a blocked tool is never forwarded — a
/// terminal SSE error event is yielded instead and the stream ends. On
/// completion (however it ends) exactly one `UsageEvent` is pushed to
/// `event_log`.
#[allow(clippy::too_many_arguments)]
pub fn enforce(
    tenant: String,
    provider: Provider,
    model: Option<String>,
    mut upstream: impl Stream<Item = reqwest::Result<Bytes>> + Unpin + Send + 'static,
    mut adapter: Box<dyn ProviderAdapter>,
    budget: Arc<BudgetRegistry>,
    blocked_tools: Vec<String>,
    event_log: Arc<EventLog>,
    now_ms: impl Fn() -> i64 + Send + 'static,
) -> impl Stream<Item = Result<Bytes, WeirError>> {
    async_stream::stream! {
        let mut tel = StreamTelemetry {
            event_log,
            tenant,
            provider,
            model,
            now_ms: Box::new(now_ms),
            tools_seen: Vec::new(),
            recorded_so_far: 0,
            emitted: false,
        };
        let mut frames = SseFrameBuffer::new();

        while let Some(chunk_res) = upstream.next().await {
            let raw = match chunk_res {
                Ok(raw) => raw,
                Err(e) => {
                    yield Err(WeirError::Upstream(e));
                    tel.emit(UsageOutcome::UpstreamError, None);
                    return;
                }
            };

            for event in frames.push(&raw) {
                let ts = (tel.now_ms)();
                let outcome = {
                    let mut acc = EventAccounting {
                        adapter: adapter.as_mut(),
                        budget: &budget,
                        tenant: &tel.tenant,
                        blocked_tools: &blocked_tools,
                        recorded_so_far: &mut tel.recorded_so_far,
                        tools_seen: &mut tel.tools_seen,
                    };
                    process_event(&mut acc, &event, ts)
                };
                match outcome {
                    Ok(EventOutcome::Forward(bytes)) => yield Ok(bytes),
                    Ok(EventOutcome::BudgetTrip) => {
                        yield Ok(Bytes::from_static(BUDGET_EXCEEDED_EVENT));
                        tel.emit(UsageOutcome::BudgetBlocked, None);
                        return;
                    }
                    Ok(EventOutcome::PolicyTrip(tool)) => {
                        yield Ok(policy_violation_event(&tool));
                        tel.emit(UsageOutcome::PolicyBlocked, Some(format!("blocked_tool:{tool}")));
                        return;
                    }
                    Err(e) => {
                        yield Err(e);
                        tel.emit(UsageOutcome::UpstreamError, None);
                        return;
                    }
                }
            }
        }

        if let Some(event) = frames.flush() {
            let ts = (tel.now_ms)();
            let outcome = {
                let mut acc = EventAccounting {
                    adapter: adapter.as_mut(),
                    budget: &budget,
                    tenant: &tel.tenant,
                    blocked_tools: &blocked_tools,
                    recorded_so_far: &mut tel.recorded_so_far,
                    tools_seen: &mut tel.tools_seen,
                };
                process_event(&mut acc, &event, ts)
            };
            match outcome {
                Ok(EventOutcome::Forward(bytes)) => yield Ok(bytes),
                Ok(EventOutcome::BudgetTrip) => {
                    yield Ok(Bytes::from_static(BUDGET_EXCEEDED_EVENT));
                    tel.emit(UsageOutcome::BudgetBlocked, None);
                    return;
                }
                Ok(EventOutcome::PolicyTrip(tool)) => {
                    yield Ok(policy_violation_event(&tool));
                    tel.emit(UsageOutcome::PolicyBlocked, Some(format!("blocked_tool:{tool}")));
                    return;
                }
                Err(e) => {
                    yield Err(e);
                    tel.emit(UsageOutcome::UpstreamError, None);
                    return;
                }
            }
        }

        tel.emit(UsageOutcome::Completed, None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BudgetLimit, ParsedConfig, TenantLimits};
    use crate::provider::{ChunkCost, OpenAiAdapter, ProviderAdapter};
    use crate::telemetry::{EventLog, UsageOutcome};
    use arc_swap::ArcSwap;
    use std::collections::HashMap;
    use std::time::Duration;

    struct FixedCostAdapter {
        cost_per_chunk: u64,
    }

    impl ProviderAdapter for FixedCostAdapter {
        fn chunk_cost(&mut self, _raw: &Bytes) -> ChunkCost {
            ChunkCost { estimated_tokens: self.cost_per_chunk, authoritative_total: None, tool_calls: Vec::new() }
        }

        fn non_streaming_cost(&self, _body: &Bytes) -> crate::provider::NonStreamingCost {
            crate::provider::NonStreamingCost { total_tokens: None, tool_calls: Vec::new() }
        }
    }

    struct AuthoritativeCostAdapter {
        totals: std::collections::VecDeque<u64>,
    }

    impl ProviderAdapter for AuthoritativeCostAdapter {
        fn chunk_cost(&mut self, _raw: &Bytes) -> ChunkCost {
            let total = self.totals.pop_front().unwrap_or(0);
            ChunkCost { estimated_tokens: 0, authoritative_total: Some(total), tool_calls: Vec::new() }
        }

        fn non_streaming_cost(&self, _body: &Bytes) -> crate::provider::NonStreamingCost {
            crate::provider::NonStreamingCost { total_tokens: None, tool_calls: Vec::new() }
        }
    }

    fn budget_with(tenant: &str, max_tokens: u64) -> Arc<BudgetRegistry> {
        let mut limits: TenantLimits = HashMap::new();
        limits.insert(
            tenant.to_string(),
            BudgetLimit { max_tokens, window: Duration::from_secs(60) },
        );
        let parsed = ParsedConfig { limits, policies: HashMap::new() };
        Arc::new(BudgetRegistry::new(Arc::new(ArcSwap::from_pointee(parsed))))
    }

    #[tokio::test]
    async fn forwards_chunks_within_budget() {
        let upstream = futures::stream::iter(vec![
            Ok(Bytes::from_static(b"chunk1\n\n")),
            Ok(Bytes::from_static(b"chunk2\n\n")),
        ]);
        let adapter: Box<dyn ProviderAdapter> = Box::new(FixedCostAdapter { cost_per_chunk: 10 });
        let budget = budget_with("acct_1", 1000);

        let out: Vec<_> = enforce(
            "acct_1".into(),
            Provider::OpenAi,
            None,
            upstream,
            adapter,
            budget,
            Vec::new(),
            Arc::new(EventLog::new(100)),
            || 0,
        )
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

        let out: Vec<_> = enforce(
            "acct_1".into(),
            Provider::OpenAi,
            None,
            upstream,
            adapter,
            budget,
            Vec::new(),
            Arc::new(EventLog::new(100)),
            || 0,
        )
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

        let out: Vec<_> = enforce(
            "acct_1".into(),
            Provider::OpenAi,
            None,
            upstream,
            adapter,
            budget,
            Vec::new(),
            Arc::new(EventLog::new(100)),
            || 0,
        )
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

        let out: Vec<_> = enforce(
            "acct_1".into(),
            Provider::OpenAi,
            None,
            upstream,
            adapter,
            budget,
            Vec::new(),
            Arc::new(EventLog::new(100)),
            || 0,
        )
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

        let out: Vec<_> = enforce(
            "acct_1".into(),
            Provider::OpenAi,
            None,
            upstream,
            adapter,
            budget,
            Vec::new(),
            Arc::new(EventLog::new(100)),
            || 0,
        )
        .collect()
        .await;

        assert_eq!(
            out.len(),
            2,
            "one raw chunk containing two SSE events must yield two forwarded events"
        );
    }

    #[tokio::test]
    async fn trips_on_blocked_tool_and_never_forwards_it() {
        struct ToolCallAdapter;
        impl ProviderAdapter for ToolCallAdapter {
            fn chunk_cost(&mut self, _raw: &Bytes) -> ChunkCost {
                ChunkCost {
                    estimated_tokens: 1,
                    authoritative_total: None,
                    tool_calls: vec!["send_email".to_string()],
                }
            }
            fn non_streaming_cost(&self, _body: &Bytes) -> crate::provider::NonStreamingCost {
                crate::provider::NonStreamingCost { total_tokens: None, tool_calls: Vec::new() }
            }
        }

        let upstream = futures::stream::iter(vec![Ok(Bytes::from_static(b"chunk1\n\n"))]);
        let adapter: Box<dyn ProviderAdapter> = Box::new(ToolCallAdapter);
        let budget = budget_with("acct_1", 1000); // plenty of budget — this must trip on policy, not budget
        let event_log = Arc::new(EventLog::new(100));

        let out: Vec<_> = enforce(
            "acct_1".into(),
            Provider::OpenAi,
            Some("gpt-4o-mini".to_string()),
            upstream,
            adapter,
            budget,
            vec!["send_email".to_string()],
            event_log.clone(),
            || 0,
        )
        .collect()
        .await;

        assert_eq!(out.len(), 1);
        let event = out[0].as_ref().unwrap();
        assert!(String::from_utf8_lossy(event).contains("policy_violation"));

        let events = event_log.since(0, 10);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].outcome, UsageOutcome::PolicyBlocked);
        assert_eq!(events[0].rule.as_deref(), Some("blocked_tool:send_email"));
        assert_eq!(events[0].tools_called, vec!["send_email".to_string()]);
    }

    #[tokio::test]
    async fn successful_stream_emits_one_unblocked_usage_event() {
        let upstream = futures::stream::iter(vec![
            Ok(Bytes::from_static(b"chunk1\n\n")),
            Ok(Bytes::from_static(b"chunk2\n\n")),
        ]);
        let adapter: Box<dyn ProviderAdapter> = Box::new(FixedCostAdapter { cost_per_chunk: 10 });
        let budget = budget_with("acct_1", 1000);
        let event_log = Arc::new(EventLog::new(100));

        let _: Vec<_> = enforce(
            "acct_1".into(),
            Provider::OpenAi,
            Some("gpt-4o-mini".to_string()),
            upstream,
            adapter,
            budget,
            Vec::new(),
            event_log.clone(),
            || 0,
        )
        .collect()
        .await;

        let events = event_log.since(0, 10);
        assert_eq!(events.len(), 1, "exactly one UsageEvent per completed stream, not one per chunk");
        assert_eq!(events[0].outcome, UsageOutcome::Completed);
        assert_eq!(events[0].tokens, 20);
        assert_eq!(events[0].model.as_deref(), Some("gpt-4o-mini"));
    }

    #[tokio::test]
    async fn dropping_stream_early_emits_incomplete_event() {
        use futures::StreamExt;
        // An upstream that would yield two forwardable events, but we drop the
        // stream after pulling only the first — simulating a client disconnect.
        let upstream = futures::stream::iter(vec![
            Ok(Bytes::from_static(b"chunk1\n\n")),
            Ok(Bytes::from_static(b"chunk2\n\n")),
        ]);
        let adapter: Box<dyn ProviderAdapter> = Box::new(FixedCostAdapter { cost_per_chunk: 1 });
        let budget = budget_with("acct_1", 1000);
        let event_log = Arc::new(EventLog::new(100));

        {
            let mut stream = Box::pin(enforce(
                "acct_1".into(),
                Provider::OpenAi,
                Some("gpt-4o-mini".to_string()),
                upstream,
                adapter,
                budget,
                Vec::new(),
                event_log.clone(),
                || 0,
            ));
            // Pull exactly one item (forwards chunk1), then drop the stream.
            let _first = stream.next().await;
            // stream dropped here at end of block, before completion
        }

        let events = event_log.since(0, 10);
        assert_eq!(events.len(), 1, "an early-dropped stream must still emit exactly one event");
        assert_eq!(events[0].outcome, UsageOutcome::Incomplete);
        assert_eq!(events[0].tokens, 1, "tokens already forwarded before the drop are recorded");
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
