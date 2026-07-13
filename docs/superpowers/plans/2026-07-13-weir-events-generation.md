# Weir `/events` generation ID Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a per-process `generation` identifier to Weir and wrap the `/events` response as `{ "generation": "<id>", "events": [ ... ] }`, so a downstream consumer (weir-agent) can detect a Weir restart and reset its cursor instead of silently stalling.

**Architecture:** Weir's `/events` cursor (`UsageEvent.id`) is monotonic per process — `EventLog.next_id` resets to 1 on every restart. A consumer persisting a cursor would miss all events after a restart. Fix: generate a `generation` string once at startup (stable for the process, changes on restart), carry it in `AppState`, and include it in the `/events` response object. This is a breaking change to the `/events` response shape, but weir-agent (its first real consumer) does not exist yet, so nothing deployed depends on the old shape.

**Tech Stack:** Existing Weir stack — Rust, Axum, Tokio, serde. No new dependency (generation is the process start time in nanoseconds since the epoch, as a string — dependency-free and changes on every restart).

## Global Constraints

- Privacy line unchanged: `/events` still exposes only metadata (no content/arguments). `generation` is an opaque per-process id, not sensitive.
- No new production dependency.
- The whole crate must compile and the FULL `cargo test` suite must pass at the end of the single task (this is one cohesive change; there is no intermediate non-compiling state to tolerate).

---

## File Structure

```
weir-proxy/
├── src/
│   ├── telemetry.rs   (add EventsResponse struct)
│   ├── gateway.rs     (AppState.generation; events_handler returns EventsResponse; test helpers + /events test)
│   └── main.rs        (generate generation at startup, wire into AppState)
├── tests/
│   └── proxy_flow_test.rs  (update the /events test: response shape + AppState construction)
└── README.md          (document the new /events response shape)
```

---

### Task 1: Add `generation` and wrap the `/events` response

**Files:**
- Modify: `src/telemetry.rs` (add `EventsResponse`)
- Modify: `src/gateway.rs` (`AppState.generation`, `events_handler`, test helpers, `/events` test)
- Modify: `src/main.rs` (generate + wire `generation`)
- Modify: `tests/proxy_flow_test.rs` (update `/events` test)
- Modify: `README.md`

**Interfaces:**
- Consumes: existing `EventLog::since`, `UsageEvent`, `AppState`.
- Produces: `EventsResponse { generation: String, events: Vec<UsageEvent> }` (serde Serialize + Deserialize); `AppState` gains `pub generation: String`; `GET /events` now returns `EventsResponse` JSON instead of a bare `Vec<UsageEvent>`.

- [ ] **Step 1: Add the `EventsResponse` struct to `src/telemetry.rs`**

Read `src/telemetry.rs` first. After the `UsageEvent` struct, add:

```rust
/// The `/events` HTTP response envelope. `generation` is a per-process
/// identifier that changes whenever Weir restarts, so a polling consumer
/// can detect a restart (its persisted cursor is only meaningful within a
/// single generation, since `UsageEvent.id` resets to 1 each process).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EventsResponse {
    pub generation: String,
    pub events: Vec<UsageEvent>,
}
```

- [ ] **Step 2: Update `src/gateway.rs` — `AppState` field, handler, test helpers, `/events` test**

Read `src/gateway.rs` first.

(a) Add the field to `AppState` (after `events`):
```rust
    pub events: Arc<EventLog>,
    /// Per-process identifier included in the `/events` response so a
    /// consumer can detect a Weir restart (see EventsResponse).
    pub generation: String,
```

(b) Change the import to bring in `EventsResponse` (match the file's existing telemetry `use`, e.g. `use crate::telemetry::{EventLog, EventsResponse, UsageEvent, UsageOutcome};`).

(c) Change `events_handler`'s return type and body:
```rust
async fn events_handler(
    State(state): State<AppState>,
    Query(query): Query<EventsQuery>,
) -> axum::Json<EventsResponse> {
    let since = query.since.unwrap_or(0);
    let limit = query.limit.unwrap_or(100).min(1000);
    axum::Json(EventsResponse {
        generation: state.generation.clone(),
        events: state.events.since(since, limit),
    })
}
```

(d) Update the test helper(s) that construct `AppState` (e.g. `state_with_tenant`) to set the new field, e.g. add `generation: "test-generation".to_string(),` to each `AppState { ... }` literal in the `#[cfg(test)]` module.

(e) Update the `/events` unit test (around the `let events: Vec<UsageEvent> = serde_json::from_slice(&body).unwrap();` line): deserialize the new envelope and assert against `.events`:
```rust
        let parsed: EventsResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.generation, "test-generation");
        let events = parsed.events;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tenant, "acct_1");
```
(Adjust to the test's existing assertions; the key change is parsing `EventsResponse` then using `.events`, and asserting the generation is the test value.)

- [ ] **Step 3: Wire `generation` in `src/main.rs`**

Read `src/main.rs` first. Compute the generation once at startup (process start time in nanoseconds, dependency-free) and add it to the `AppState` construction:

```rust
    let generation = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos().to_string())
        .unwrap_or_else(|_| "0".to_string());
```
Add `generation,` as a field in the `AppState { ... }` literal (alongside `events`). Place the `let generation = ...` before the `AppState` construction. Do not disturb the graceful-shutdown handling or anything else.

- [ ] **Step 4: Update `tests/proxy_flow_test.rs`**

Read `tests/proxy_flow_test.rs` first. Two changes:
- Every `AppState { ... }` construction (the `state_pointed_at` helper and any inline construction in the tests) needs `generation: "test-generation".to_string(),` added.
- The `events_endpoint_reflects_a_completed_request` test parses `Vec<weir::telemetry::UsageEvent>` from the `/events` body — change it to parse `weir::telemetry::EventsResponse` and assert against `.events` (and optionally `.generation == "test-generation"`), matching the gateway unit test pattern.

- [ ] **Step 5: Update the README `/events` section**

Read the `Telemetry` / `/events` section of `README.md`. Update it to show the response is now an object with a `generation` field wrapping the events array, e.g.:

> `GET /events?since=<event_id>&limit=<n>` returns a JSON object
> `{ "generation": "<id>", "events": [ ... ] }`. `generation` is a
> per-process identifier that changes whenever Weir restarts — a consumer
> that persists a cursor should treat the cursor as valid only within a
> single generation and reset it (to 0) when the generation changes, since
> event ids restart at 1 on each Weir process. Each event in `events`
> contains [existing field description].

Keep the rest of the section (fields, ring-buffer note, the unauthenticated-endpoint security note) intact; only the response-shape description changes.

- [ ] **Step 6: Build and run the full suite**

Run: `cargo build`
Expected: 0 errors.

Run: `cargo test`
Expected: the FULL suite passes (all lib + integration tests). The two `/events` tests now assert the enveloped shape.

- [ ] **Step 7: Commit**

```bash
git add src/telemetry.rs src/gateway.rs src/main.rs tests/proxy_flow_test.rs README.md
git commit -m "feat: wrap /events in a per-process generation envelope

Adds a startup-generated `generation` id and changes the /events response
from a bare array to { generation, events }, so a polling consumer can
detect a Weir restart (event ids reset per process) and reset its cursor
instead of silently missing all events from the restarted process."
```

---

## Self-Review Notes

- **Spec coverage:** implements the "Prerequisite: a small change to Weir" section of the weir-agent design spec — startup `generation`, `{generation, events}` response shape, tests + README updated.
- **Type consistency:** `EventsResponse` defined once in `telemetry.rs`, consumed by the gateway handler (Serialize) and both `/events` tests (Deserialize). `AppState.generation: String` set in `main.rs` (real) and the test helpers (`"test-generation"`).
- **No intermediate broken state:** unlike the prior policy+telemetry plan, this is a single cohesive task; the crate compiles and the full suite passes at the end of Task 1.
