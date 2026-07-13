use std::collections::VecDeque;
use std::sync::Mutex;

use crate::provider::Provider;

/// One completed request's outcome, kept for external telemetry only —
/// never prompt/response content, never tool call arguments, only names.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UsageEvent {
    pub id: u64,
    pub tenant: String,
    pub provider: Provider,
    pub model: Option<String>,
    pub tools_called: Vec<String>,
    pub tokens: u64,
    pub blocked: bool,
    pub block_reason: Option<String>,
    pub timestamp_ms: i64,
}

/// A bounded, mutex-guarded ring buffer of recent `UsageEvent`s. This is
/// NOT a hot-path structure in the sense `SlidingWindowCounter` is — it
/// receives one push per completed request, not per chunk, so a plain
/// `Mutex` is the correct, honest choice here.
pub struct EventLog {
    inner: Mutex<EventLogInner>,
    capacity: usize,
}

struct EventLogInner {
    events: VecDeque<UsageEvent>,
    next_id: u64,
}

impl EventLog {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(EventLogInner { events: VecDeque::new(), next_id: 1 }),
            capacity: capacity.max(1),
        }
    }

    /// Assigns the event a fresh monotonic id (overwriting whatever `id`
    /// the caller passed in) and appends it, evicting the oldest event(s)
    /// if the buffer is now over capacity.
    pub fn push(&self, mut event: UsageEvent) {
        let mut inner = self.inner.lock().unwrap();
        event.id = inner.next_id;
        inner.next_id += 1;
        inner.events.push_back(event);
        while inner.events.len() > self.capacity {
            inner.events.pop_front();
        }
    }

    /// Returns events with `id > since`, oldest first, capped at `limit`.
    pub fn since(&self, since: u64, limit: usize) -> Vec<UsageEvent> {
        let inner = self.inner.lock().unwrap();
        inner.events.iter().filter(|e| e.id > since).take(limit).cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_event(tenant: &str) -> UsageEvent {
        UsageEvent {
            id: 0, // overwritten by push()
            tenant: tenant.to_string(),
            provider: Provider::OpenAi,
            model: Some("gpt-4o-mini".to_string()),
            tools_called: vec![],
            tokens: 10,
            blocked: false,
            block_reason: None,
            timestamp_ms: 0,
        }
    }

    #[test]
    fn push_assigns_monotonic_ids() {
        let log = EventLog::new(10);
        log.push(sample_event("acct_1"));
        log.push(sample_event("acct_2"));

        let events = log.since(0, 10);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].id, 1);
        assert_eq!(events[1].id, 2);
    }

    #[test]
    fn since_filters_and_limits() {
        let log = EventLog::new(10);
        for i in 0..5 {
            log.push(sample_event(&format!("acct_{i}")));
        }
        let events = log.since(2, 10);
        assert_eq!(events.len(), 3); // ids 3, 4, 5
        assert_eq!(events[0].id, 3);

        let limited = log.since(0, 2);
        assert_eq!(limited.len(), 2);
        assert_eq!(limited[0].id, 1);
        assert_eq!(limited[1].id, 2);
    }

    #[test]
    fn evicts_oldest_when_over_capacity() {
        let log = EventLog::new(3);
        for i in 0..5 {
            log.push(sample_event(&format!("acct_{i}")));
        }
        // Only the last 3 pushed (ids 3, 4, 5) should remain.
        let events = log.since(0, 10);
        assert_eq!(events.len(), 3);
        assert_eq!(events.iter().map(|e| e.id).collect::<Vec<_>>(), vec![3, 4, 5]);
    }
}
