# Weir

Weir is an inline circuit breaker for AI API traffic. It sits between your
application (or an agentic tool like an IDE assistant) and OpenAI or
Anthropic, tracks a rolling token budget per tenant, and cuts off a request
the moment it would exceed that budget — before the over-budget content
ever reaches the client.

It exists to stop the **Runaway Agentic Loop**: a recursive agent or
tool-calling loop that goes wrong and silently runs up an unbounded API
bill before anyone notices. Traditional billing dashboards report hourly
or daily, long after the damage is done. Weir enforces in real time, on
every request and every streamed chunk.

Weir is the source-available core of [SymFynity](https://symfynity.com), an AI
governance platform. The proxy is standalone and stays that way: it has no
dependency on SymFynity, no account check, no phone-home, and no degraded
mode. Everything it exposes is generically useful on its own — self-host it,
ignore the rest, and it still does its job in full.

## How it works

- Configure a token budget per tenant (an API key, an application, a team
  — whatever `x-weir-tenant` identifies) in a TOML file.
- Point your client at Weir instead of the provider directly. Weir
  forwards the request upstream unmodified — your real API key still goes
  to the real provider; Weir never stores or reissues credentials.
- For streaming responses, Weir estimates each chunk's cost with a real
  tokenizer, reconciles against the provider's own usage data as it
  arrives, and checks the budget *before* forwarding each chunk. An
  over-budget chunk is never sent to the client — Weir emits a terminal
  error event and closes the connection instead.
- For non-streaming responses, Weir buffers the complete response, reads
  its authoritative usage, and only forwards it if that keeps the tenant
  within budget — otherwise the client gets a `429` instead of the
  response.
- A tenant already over budget is rejected at admission, before Weir even
  calls upstream.
- The config file hot-reloads on change — no restart needed to adjust a
  budget.

See [`docs/superpowers/specs/2026-07-10-weir-circuit-breaker-design.md`](docs/superpowers/specs/2026-07-10-weir-circuit-breaker-design.md)
for the full design.

## Quick start

### Build from source

Requires a recent stable Rust toolchain.

```bash
cargo build --release
cp weir.example.toml weir.toml
# edit weir.toml with your own tenant IDs and budgets
./target/release/weir
```

Weir listens on `0.0.0.0:8080` and reads `weir.toml` from the current
directory by default.

### Run with Docker

```bash
docker build -t weir:local .
docker run --rm -p 8080:8080 \
  -v "$(pwd)/weir.example.toml:/weir.toml:ro" \
  weir:local
```

The image was verified to build for both `linux/amd64` and `linux/arm64`
via `docker buildx build --platform linux/amd64,linux/arm64 -t weir:local .`

## Configuration

`weir.toml` maps tenant IDs to a token ceiling and a rolling window:

```toml
[tenants.acct_123]
max_tokens = 50000
window_seconds = 60

[tenants.acct_456]
max_tokens = 200000
window_seconds = 3600
```

A request from a tenant not listed here is rejected with `401`.

### Policy enforcement

Optionally, you can restrict which models or tools a tenant is allowed to
use. Add a `[tenants.<id>.policy]` block to define which models and tools
are **blocked** for a tenant. Anything not listed is allowed; omitting the
block entirely means no restrictions beyond the token budget:

```toml
[tenants.acct_123]
max_tokens = 50000
window_seconds = 60

[tenants.acct_123.policy]
blocked_models = ["gpt-3.5-turbo"]
blocked_tools = ["send_email", "execute_shell"]
```

- **blocked_models**: If a request's `model` field matches an entry in this
  list, the request is rejected with HTTP `403` **before any upstream call
  is made** — no tokens are spent. The client receives the specific model
  name that was blocked.

- **blocked_tools**: If a tool call in the upstream response invokes a
  blocked tool, Weir trips the request mid-stream (for streaming responses,
  a terminal error event is sent; for non-streaming, a `403` is returned) —
  the same way an over-budget response is handled. The client receives the
  specific tool name that was blocked, but never sees the tool arguments or
  response content.

Omitting the `policy` block entirely means no tool or model restrictions,
only the token budget applies. The config file hot-reloads on change, just
like budgets do.

Environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `WEIR_CONFIG` | `weir.toml` | Path to the config file |
| `WEIR_OPENAI_BASE` | `https://api.openai.com` | Upstream OpenAI base URL |
| `WEIR_ANTHROPIC_BASE` | `https://api.anthropic.com` | Upstream Anthropic base URL |
| `WEIR_EVENT_LOG_CAPACITY` | `10000` | Maximum number of recent events to hold in the `/events` ring buffer |

## Using it

Weir exposes two route prefixes that mirror the upstream provider's own
paths — `/openai/*` forwards to OpenAI, `/anthropic/*` forwards to
Anthropic. Every request must carry an `x-weir-tenant` header identifying
which configured tenant it belongs to; everything else (your real API
key, model, body) passes through unchanged.

### curl

```bash
curl http://localhost:8080/openai/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $OPENAI_API_KEY" \
  -H "x-weir-tenant: acct_123" \
  -d '{
    "model": "gpt-4o-mini",
    "stream": true,
    "messages": [{"role": "user", "content": "Say hi in five words."}]
  }'
```

```bash
curl http://localhost:8080/anthropic/v1/messages \
  -H "Content-Type: application/json" \
  -H "x-api-key: $ANTHROPIC_API_KEY" \
  -H "anthropic-version: 2023-06-01" \
  -H "x-weir-tenant: acct_123" \
  -d '{
    "model": "claude-sonnet-5",
    "max_tokens": 256,
    "stream": true,
    "messages": [{"role": "user", "content": "Say hi in five words."}]
  }'
```

### OpenAI SDK

Most OpenAI-compatible SDKs let you set both a base URL and default
headers on the client, which is all Weir needs:

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:8080/openai/v1",
    api_key="sk-...",  # your real OpenAI key — Weir passes it through
    default_headers={"x-weir-tenant": "acct_123"},
)
```

### Claude Code

Claude Code reads `ANTHROPIC_BASE_URL`, `ANTHROPIC_AUTH_TOKEN`, and
`ANTHROPIC_CUSTOM_HEADERS` from its settings file (`~/.claude/settings.json`,
or a project-level `.claude/settings.json`):

```json
{
  "env": {
    "ANTHROPIC_BASE_URL": "http://localhost:8080/anthropic",
    "ANTHROPIC_AUTH_TOKEN": "sk-ant-...",
    "ANTHROPIC_CUSTOM_HEADERS": "x-weir-tenant: acct_123"
  }
}
```

With this in place, every request Claude Code makes is routed through
Weir and counted against the `acct_123` budget in `weir.toml` — including
the tool-calling loops that are exactly what Weir is designed to catch.

### Telemetry

`GET /events?since=<event_id>&limit=<n>` returns a JSON object
`{ "generation": "<id>", "events": [ ... ] }`. `generation` is a
per-process identifier that changes whenever Weir restarts — a consumer
that persists a cursor should treat the cursor as valid only within a
single generation and reset it (to 0) when the generation changes, since
event ids restart at 1 on each Weir process. Each event in `events`
contains metadata only — tenant ID, provider, model, tool names (if any),
token count, a millisecond timestamp, and an `outcome` (`completed`,
`budget_blocked`, `policy_blocked`, `upstream_error`, or `incomplete`)
plus, for a policy block, the specific `rule` that fired (e.g.
`blocked_tool:send_email`). Exactly one event is recorded per request on
every terminal path — including `incomplete` when a streaming client
disconnects before the response finished. Event payloads and tool
arguments are never logged.

```bash
curl http://localhost:8080/events?since=0&limit=100
```

The event log is an in-memory ring buffer bounded by
`WEIR_EVENT_LOG_CAPACITY` (default 10,000 events). Once full, the oldest
events are evicted on each new event, regardless of whether any consumer
has read them — so a consumer that polls infrequently may miss events that
were evicted before it read them. This surface is designed for telemetry
collection — e.g., metrics export or SaaS agent auditing — not for durable
request tracking.

> **Note:** `/events` is unauthenticated and returns metadata for *all*
> tenants to anyone who can reach the port. This matches Weir's overall
> trust model (it runs inside your own network and trusts the
> `x-weir-tenant` header), but it means the endpoint must be kept on a
> trusted network — bind it to a private interface or front it with
> authentication before exposing it across a trust boundary (e.g. before a
> hosted agent polls it).

## Development

```bash
cargo test          # full test suite
cargo build --release
```

The implementation plan and design spec under `docs/superpowers/` describe
the full task-by-task build and the review history.

## License

Business Source License 1.1 — see [`LICENSE`](LICENSE).

Weir is source-available, not open source. The distinction is narrow and worth
stating plainly:

- **Running Weir in production, inside your own organisation, is free — and
  always will be.** The Additional Use Grant in the licence covers internal
  production use explicitly. Nothing is held back, crippled, or gated behind a
  licence key; there is no account check, no phone-home, and no degraded mode.
- **You can read, modify, fork, and self-host it.** If auditing what sits in
  your traffic path is why you're here, the licence doesn't get in your way.
- **The one prohibited use is competitive:** offering Weir to third parties as a
  hosted or embedded service that competes with SymFynity's paid product.

Four years after any given version is published, that version becomes available
under the Apache License 2.0 automatically. So the worst case — SymFynity
disappears, or turns hostile — is bounded: Weir is Apache-2.0 on a rolling
four-year lag regardless of what we do.

Weir 0.1.0 was published under Apache License 2.0 and remains available under
those terms. The Business Source License applies from 0.2.0 onward.
