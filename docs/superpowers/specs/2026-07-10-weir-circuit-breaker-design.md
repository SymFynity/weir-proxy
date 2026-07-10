# Weir — AI Proxy Circuit Breaker: Design Spec

**Status:** Approved
**Date:** 2026-07-10
**License:** Apache License 2.0

## Problem

As teams build more agentic and tool-calling AI systems, faulty state logic in recursive
agents or tool-calling loops can cause exponential, unmonitored API token spend — a
"Runaway Agentic Loop." Standard FinOps billing dashboards report hourly or daily,
long after the damage is done. Weir is a low-latency, streaming-native inline proxy that
sits between a client application and an LLM provider (OpenAI, Anthropic) and enforces
token budgets in real time, severing a stream the moment it breaches a configured ceiling.

Weir is a standalone open-source infrastructure project. It has no dependency on, and
makes no reference to, any hosted or commercial offering.

## Goals

- Inline stream interception of SSE responses without full-payload buffering.
- Per-tenant / per-API-key rolling-window token budgets, enforced continuously as a
  stream flows, not just at request admission.
- Sub-2ms added latency to time-to-first-token under peak load.
- Lock-free hot path: atomic counters, no per-request heap growth, no external
  state store.
- No prompt data cached or persisted anywhere — nothing leaves the process memory
  it's already in.

## Non-Goals

- Centralized multi-cluster management, alerting, or reporting. Weir is a single-node
  proxy; running many instances across an org is an operational/deployment concern
  outside this project's scope, not a feature it provides.
- Exact real-time token accounting. Interim costs are estimated; Weir reconciles
  against authoritative provider usage data as it becomes available in the stream.

## Architecture

A single Rust binary (Axum + Tokio + Hyper) deployed inline between a client
application and the upstream LLM provider. Client credentials pass through unmodified
— Weir never stores or reissues provider API keys. All state is in-process memory;
no Redis, no external dependencies.

```
Client app -> Weir (Axum) -> [budget check] -> Upstream (OpenAI / Anthropic)
                                  |
                      In-memory tenant budget table
                      (sliding-window atomic counters)
```

## Components

### Inbound Gateway
Accepts the client connection, extracts the tenant/API-key identity (from the
forwarded `Authorization` header or a configured header name) and the target
provider (by route or `Host`).

### Config Loader
Parses a TOML file mapping keys/tenants to budget ceilings and rolling-window
sizes. Watches the file (`notify` crate) and hot-swaps the active config via
`arc-swap` — no restart required. A malformed config at startup is fatal (fail
closed); a malformed config during a hot-reload is logged and the prior valid
config stays active (fail safe — a bad edit must not silently disable enforcement).

### Budget Registry
`DashMap<TenantId, BudgetState>`. Each `BudgetState` holds atomic counters
implementing a **sliding-window-counter approximation**: a weighted blend of the
current fixed window and the previous one, based on elapsed time into the current
window. Chosen over:

- **Fixed window counters** — simplest, but allows up to a 2x burst at window
  boundaries.
- **Exact sliding log** (timestamped deltas per key) — accurate, but unbounded
  per-key memory growth and per-request pruning cost.

The sliding-window-counter approximation gives O(1) memory per tenant, fully
lock-free atomic updates, and avoids the fixed-window boundary-burst problem —
satisfying the lock-free/no-hot-path-allocation goal directly.

### Provider Adapters
A `ProviderAdapter` trait with `OpenAiAdapter` and `AnthropicAdapter`
implementations, each responsible for parsing that provider's SSE framing and
locating usage data:

- **OpenAI** — incremental chunks carry no usage; an authoritative `usage` object
  typically arrives only in the final chunk (with `stream_options.include_usage`).
  Interim per-chunk cost is estimated with a real tokenizer (`tiktoken-rs`,
  `cl100k`/`o200k` encoding) rather than a character-count heuristic.
- **Anthropic** — no officially published open-source tokenizer; interim
  estimation reuses the OpenAI BPE tokenizer as a proxy. Anthropic's stream sends
  real incremental usage (input tokens at `message_start`, output tokens
  accumulating via `message_delta`), which is more accurate than the estimate and
  takes precedence over it the moment it's seen in a chunk.

Each adapter reconciles the running estimate against authoritative usage data the
instant it appears, correcting the tenant's ledger.

### Stream Enforcer
Wraps the upstream response stream. For each chunk: compute/estimate its cost, add
it to the tenant's sliding-window total, **then** decide whether to forward it.
Budget is checked before a chunk is yielded to the client, not after — an
over-budget chunk is never forwarded.

## Enforcement Semantics

Two trip points:

1. **Admission check** — if a tenant is already over its rolling-window budget when
   a new request arrives, reject before proxying upstream: a real `HTTP 429`, since
   no response headers have been committed yet.
2. **Mid-stream check** — for a request admitted while under budget, if forwarding
   the next chunk would breach the ceiling, Weir does not forward it. Instead it
   emits a terminal SSE error event (`event: error` /
   `data: {"error":"budget_exceeded"}`) and closes the connection. A true
   status-code 429 is not possible here — the response's `200` status and headers
   are already committed once streaming has begun.

## Error Handling

- Upstream connection errors (timeouts, resets) map to a distinct `502`-style
  structured error, so clients can differentiate "provider failed" from
  "you were cut off for budget."
- Every tripped stream ends by closing the TCP connection after the terminal error
  event, so the client's own retry/backoff logic resets its agent state naturally.
- Config errors: fatal at startup, logged-and-ignored on hot-reload (see Config
  Loader above).

## Testing

- **Unit tests** — sliding-window counter math (boundary behavior, burst limits),
  independent of networking.
- **Adapter tests** — golden SSE fixtures per provider, including usage split
  across multiple chunks and tool-call-only chunks with no text.
- **Integration tests** — a fake upstream SSE server (`wiremock` or a local Axum
  stub) driving full request → stream → trip scenarios, asserting a mid-stream
  trip never forwards an over-budget chunk and always closes cleanly.
- **Concurrency tests** — many simultaneous streams against a shared tenant
  budget, asserting cumulative spend never exceeds the ceiling by more than one
  in-flight chunk's worth. Some overshoot under concurrent races is an accepted
  trade-off of lock-free accounting, not a bug to eliminate.

## Deployment

Single architecture-native binary, packaged as a Docker image for Linux x86_64 and
ARM64.
