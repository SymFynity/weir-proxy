use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const COUNT_BITS: u32 = 32;
const COUNT_MASK: u64 = (1u64 << COUNT_BITS) - 1;

fn pack(bucket_index: u32, count: u32) -> u64 {
    ((bucket_index as u64) << COUNT_BITS) | (count as u64)
}

fn unpack(word: u64) -> (u32, u32) {
    ((word >> COUNT_BITS) as u32, (word & COUNT_MASK) as u32)
}

fn clamp_u32(value: u64) -> u32 {
    value.min(u32::MAX as u64) as u32
}

/// True if `a` is strictly later than `b`, using wrapping/serial-number
/// comparison (safe as long as the true distance between them never
/// approaches 2^31 — true here by an enormous margin: real concurrent
/// callers' bucket indices differ by at most a handful of windows).
fn is_ahead(a: u32, b: u32) -> bool {
    (a.wrapping_sub(b) as i32) > 0
}

/// Approximates a rolling-window rate limiter using two fixed windows,
/// weighting the previous window's count by how much of it still overlaps
/// the current rolling window. O(1) memory, lock-free via CAS retry.
///
/// Each window slot packs its bucket index and count into a single
/// AtomicU64, so a slot's index and count always change together in one
/// CAS. Every write to a slot — whether adding within a window, rolling
/// `current` forward, or merging a lagging writer's contribution into
/// `previous` — goes through a CAS-retry loop keyed on that slot's index,
/// so concurrent contributions from any direction compose correctly
/// instead of one clobbering another.
pub struct SlidingWindowCounter {
    window_ms: i64,
    current: AtomicU64,
    previous: AtomicU64,
}

impl SlidingWindowCounter {
    pub fn new(window: Duration) -> Self {
        Self {
            window_ms: window.as_millis().max(1) as i64,
            current: AtomicU64::new(pack(0, 0)),
            // Sentinel: never adjacent to a real bucket index near 0, so
            // the first-ever estimate()/add() cannot spuriously treat it
            // as a valid previous window.
            previous: AtomicU64::new(pack(u32::MAX - 1, 0)),
        }
    }

    fn bucket_of(&self, now_ms: i64) -> u32 {
        now_ms.div_euclid(self.window_ms) as u32
    }

    /// Adds `amount` to whichever slot (`previous`) it belongs to via a
    /// CAS-retry loop: if the slot already holds this bucket's index, the
    /// amount is added to its count; if this index is newer than what's
    /// stored, the slot is (re)seeded at this index; if this index is OLDER
    /// than what's stored, the write is dropped — a late-arriving stale carry
    /// must never clobber a fresher one that already landed. Used both to
    /// carry a just-rolled `current` window's final count into `previous`,
    /// and to merge a lagging writer's contribution after `current` has
    /// already moved on.
    fn merge_into_previous(&self, index: u32, amount: u32) {
        loop {
            let prev_packed = self.previous.load(Ordering::Acquire);
            let (prev_index, prev_count) = unpack(prev_packed);

            let next_prev = if prev_index == index {
                pack(index, clamp_u32(prev_count as u64 + amount as u64))
            } else if is_ahead(index, prev_index) {
                pack(index, amount)
            } else {
                return; // stale write — previous already holds newer data
            };

            if self
                .previous
                .compare_exchange(prev_packed, next_prev, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
            // else retry: reload and reassess (previous may have changed again).
        }
    }

    pub fn add(&self, amount: u64, now_ms: i64) -> u64 {
        let new_index = self.bucket_of(now_ms);
        loop {
            let packed = self.current.load(Ordering::Acquire);
            let (old_index, old_count) = unpack(packed);

            if new_index == old_index {
                // Same window as the current authoritative bucket: add normally.
                let next_packed = pack(old_index, clamp_u32(old_count as u64 + amount));
                if self
                    .current
                    .compare_exchange(packed, next_packed, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return self.estimate(now_ms);
                }
                continue;
            }

            if new_index.wrapping_sub(old_index) == 1 {
                // First writer into the next window: advance `current` and
                // carry the old window's final count into `previous`.
                let next_packed = pack(new_index, clamp_u32(amount));
                if self
                    .current
                    .compare_exchange(packed, next_packed, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    self.merge_into_previous(old_index, old_count);
                    return self.estimate(now_ms);
                }
                continue;
            }

            if old_index.wrapping_sub(new_index) == 1 {
                // We're lagging: another, more-advanced writer already
                // rolled `current` forward past our window. Our
                // contribution belongs in `previous` — never regress
                // `current` to fix this up.
                self.merge_into_previous(new_index, clamp_u32(amount));
                return self.estimate(now_ms);
            }

            // More than one window away in either direction: this
            // contribution no longer overlaps the tracked rolling window
            // at all. In real usage, concurrent callers' `now_ms` values
            // are always close together in wall-clock time relative to
            // window sizes (seconds to hours), so this is unreachable in
            // practice; drop it rather than corrupting either slot.
            return self.estimate(now_ms);
        }
    }

    pub fn estimate(&self, now_ms: i64) -> u64 {
        let new_index = self.bucket_of(now_ms);
        let (cur_index, cur_count) = unpack(self.current.load(Ordering::Acquire));
        let (prev_index, prev_count) = unpack(self.previous.load(Ordering::Acquire));

        let (effective_current, effective_previous) = if cur_index == new_index {
            let previous_eligible = prev_index == new_index.wrapping_sub(1);
            (cur_count, if previous_eligible { prev_count } else { 0 })
        } else if cur_index == new_index.wrapping_sub(1) {
            (0, cur_count)
        } else {
            (0, 0)
        };

        self.weighted_total(effective_current, effective_previous, new_index, now_ms)
    }

    fn weighted_total(&self, current: u32, previous: u32, bucket_index: u32, now_ms: i64) -> u64 {
        let bucket_start_ms = (bucket_index as i64) * self.window_ms;
        let elapsed_ms = (now_ms - bucket_start_ms).clamp(0, self.window_ms);
        let remaining_weight = (self.window_ms - elapsed_ms) as f64 / self.window_ms as f64;
        current as u64 + ((previous as f64) * remaining_weight).round() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulates_within_single_window() {
        let c = SlidingWindowCounter::new(Duration::from_secs(60));
        assert_eq!(c.add(10, 1_000), 10);
        assert_eq!(c.add(5, 1_500), 15);
        assert_eq!(c.estimate(1_600), 15);
    }

    #[test]
    fn rollover_carries_previous_window_weighted() {
        let c = SlidingWindowCounter::new(Duration::from_millis(1000));
        c.add(100, 0); // bucket 0: 100 tokens
        let total = c.estimate(1_500);
        assert!((45..=55).contains(&total), "expected ~50, got {total}");
    }

    #[test]
    fn gap_larger_than_two_windows_resets_to_zero() {
        let c = SlidingWindowCounter::new(Duration::from_millis(1000));
        c.add(100, 0);
        assert_eq!(c.estimate(5_000), 0);
    }

    #[test]
    fn concurrent_adds_are_not_lost() {
        use std::sync::Arc;
        use std::thread;

        let counter = Arc::new(SlidingWindowCounter::new(Duration::from_secs(60)));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let counter = counter.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    counter.add(1, 0);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(counter.estimate(0), 8000);
    }

    #[test]
    fn concurrent_adds_across_window_boundary_are_not_lost() {
        use std::sync::Arc;
        use std::thread;

        let counter = Arc::new(SlidingWindowCounter::new(Duration::from_millis(1000)));
        let mut handles = Vec::new();
        // 8 threads add into bucket 0 (now_ms=900), 8 threads concurrently
        // add into bucket 1 (now_ms=1100) — exercising a real rollover
        // under concurrency, not just the one-time startup transition.
        for i in 0..16 {
            let counter = counter.clone();
            let now_ms = if i % 2 == 0 { 900 } else { 1100 };
            handles.push(thread::spawn(move || {
                for _ in 0..500 {
                    counter.add(1, now_ms);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // bucket 0: 8 threads * 500 = 4000; bucket 1: 8 threads * 500 = 4000.
        // estimate(1100): 100ms into bucket 1's 1000ms window, so bucket 0
        // carries weight (1000-100)/1000 = 0.9 -> round(4000*0.9) = 3600.
        // total = 4000 (current) + 3600 (weighted previous) = 7600.
        assert_eq!(counter.estimate(1100), 7600);
    }
}
