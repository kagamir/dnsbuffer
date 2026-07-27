use std::collections::VecDeque;
use std::time::Duration;

const HOUR_MS: i64 = 3_600_000;
const DAY_MS: i64 = 86_400_000;

#[derive(Debug, Clone, serde::Serialize)]
pub struct StatsSnapshot {
    pub samples: usize,
    pub successes: usize,
    pub failure_rate: f64,
    pub avg_latency_ms: Option<f64>,
}

/// Call statistics for a single upstream.
/// A sliding window (the most recent N calls) drives weighted random selection;
/// long-term history bucketed by hour (kept for retention_days) drives the dashboard snapshot.
pub struct UpstreamStats {
    window: usize,
    samples: VecDeque<Sample>,
    /// None means keep forever (retention_days = 0).
    retention_ms: Option<i64>,
    history: VecDeque<HourBucket>,
}

#[derive(Clone, Copy)]
enum Sample {
    Success { latency_ms: f64 },
    Failure,
}

#[derive(Clone, Copy)]
struct HourBucket {
    hour_ms: i64,
    samples: usize,
    successes: usize,
    latency_sum_ms: f64,
}

impl UpstreamStats {
    pub fn new(window: usize) -> Self {
        Self::with_retention(window, 0)
    }

    /// `retention_days = 0` means history is kept forever.
    pub fn with_retention(window: usize, retention_days: u64) -> Self {
        Self {
            window: window.max(1),
            samples: VecDeque::new(),
            retention_ms: i64::try_from(retention_days)
                .ok()
                .filter(|days| *days > 0)
                .and_then(|days| days.checked_mul(DAY_MS)),
            history: VecDeque::new(),
        }
    }

    fn push(&mut self, s: Sample) {
        if self.samples.len() == self.window {
            self.samples.pop_front();
        }
        self.samples.push_back(s);
    }

    fn record_history(&mut self, now_ms: i64, success_latency_ms: Option<f64>) {
        let hour_ms = now_ms.div_euclid(HOUR_MS).saturating_mul(HOUR_MS);
        // On a clock rollback, merge into the latest bucket to keep the bucket order monotonic
        match self.history.back_mut() {
            Some(bucket) if bucket.hour_ms >= hour_ms => {
                bucket.samples += 1;
                if let Some(latency_ms) = success_latency_ms {
                    bucket.successes += 1;
                    bucket.latency_sum_ms += latency_ms;
                }
            }
            _ => self.history.push_back(HourBucket {
                hour_ms,
                samples: 1,
                successes: usize::from(success_latency_ms.is_some()),
                latency_sum_ms: success_latency_ms.unwrap_or(0.0),
            }),
        }
        if let Some(cutoff) = self.retention_cutoff(now_ms) {
            while self
                .history
                .front()
                .is_some_and(|bucket| bucket.hour_ms.saturating_add(HOUR_MS) <= cutoff)
            {
                self.history.pop_front();
            }
        }
    }

    fn retention_cutoff(&self, now_ms: i64) -> Option<i64> {
        self.retention_ms
            .map(|retention_ms| now_ms.saturating_sub(retention_ms))
    }

    pub fn record_success(&mut self, latency: Duration) {
        self.record_success_at(latency, unix_millis());
    }

    pub fn record_success_at(&mut self, latency: Duration, now_ms: i64) {
        let latency_ms = latency.as_secs_f64() * 1000.0;
        self.push(Sample::Success { latency_ms });
        self.record_history(now_ms, Some(latency_ms));
    }

    pub fn record_failure(&mut self) {
        self.record_failure_at(unix_millis());
    }

    pub fn record_failure_at(&mut self, now_ms: i64) {
        self.push(Sample::Failure);
        self.record_history(now_ms, None);
    }

    pub fn failure_rate(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let failures = self
            .samples
            .iter()
            .filter(|s| matches!(s, Sample::Failure))
            .count();
        failures as f64 / self.samples.len() as f64
    }

    pub fn avg_latency_ms(&self) -> f64 {
        let (sum, n) = self
            .samples
            .iter()
            .fold((0.0, 0u32), |(sum, n), s| match s {
                Sample::Success { latency_ms } => (sum + latency_ms, n + 1),
                Sample::Failure => (sum, n),
            });
        if n == 0 {
            100.0 // cold-start median
        } else {
            sum / n as f64
        }
    }

    /// Dashboard snapshot: based on the long-term history within retention_days, not the selector's sliding window.
    pub fn snapshot(&self) -> StatsSnapshot {
        self.snapshot_at(unix_millis())
    }

    pub fn snapshot_at(&self, now_ms: i64) -> StatsSnapshot {
        let cutoff = self.retention_cutoff(now_ms);
        let (samples, successes, latency_sum) = self
            .history
            .iter()
            .filter(|bucket| {
                cutoff.is_none_or(|cutoff| bucket.hour_ms.saturating_add(HOUR_MS) > cutoff)
            })
            .fold(
                (0usize, 0usize, 0.0),
                |(samples, successes, latency_sum), bucket| {
                    (
                        samples + bucket.samples,
                        successes + bucket.successes,
                        latency_sum + bucket.latency_sum_ms,
                    )
                },
            );
        StatsSnapshot {
            samples,
            successes,
            failure_rate: if samples == 0 {
                0.0
            } else {
                (samples - successes) as f64 / samples as f64
            },
            avg_latency_ms: (successes > 0).then(|| latency_sum / successes as f64),
        }
    }

    /// w = 1 / ((t_avg_ms + ε) × (1 + k·f)), ε=1.0; a negative k is treated as 0
    pub fn weight(&self, k: f64) -> f64 {
        let k = k.max(0.0);
        1.0 / ((self.avg_latency_ms() + 1.0) * (1.0 + k * self.failure_rate()))
    }
}

fn unix_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn cold_start_gives_neutral_weight() {
        let s = UpstreamStats::new(8);
        assert_eq!(s.failure_rate(), 0.0);
        assert_eq!(s.avg_latency_ms(), 100.0);
        let w = s.weight(5.0);
        assert!((w - 1.0 / 101.0).abs() < 1e-9);
    }

    #[test]
    fn snapshot_distinguishes_no_success_from_cold_start_value() {
        let mut stats = UpstreamStats::new(4);
        stats.record_failure();
        let snap = stats.snapshot();
        assert_eq!(snap.samples, 1);
        assert_eq!(snap.successes, 0);
        assert_eq!(snap.failure_rate, 1.0);
        assert_eq!(snap.avg_latency_ms, None);
        stats.record_success(Duration::from_millis(20));
        let snap = stats.snapshot();
        assert_eq!(snap.samples, 2);
        assert_eq!(snap.successes, 1);
        assert_eq!(snap.failure_rate, 0.5);
        assert_eq!(snap.avg_latency_ms, Some(20.0));
    }

    #[test]
    fn failures_lower_weight() {
        let mut good = UpstreamStats::new(8);
        let mut bad = UpstreamStats::new(8);
        for _ in 0..4 {
            good.record_success(Duration::from_millis(50));
            bad.record_success(Duration::from_millis(50));
        }
        for _ in 0..4 {
            bad.record_failure();
        }
        assert!(bad.failure_rate() > 0.4);
        assert!(good.weight(5.0) > bad.weight(5.0));
    }

    #[test]
    fn lower_latency_wins() {
        let mut fast = UpstreamStats::new(8);
        let mut slow = UpstreamStats::new(8);
        for _ in 0..8 {
            fast.record_success(Duration::from_millis(10));
            slow.record_success(Duration::from_millis(500));
        }
        assert!(fast.weight(5.0) > slow.weight(5.0));
    }

    #[test]
    fn window_evicts_oldest() {
        let mut s = UpstreamStats::new(4);
        for _ in 0..4 {
            s.record_failure();
        }
        assert_eq!(s.failure_rate(), 1.0);
        for _ in 0..4 {
            s.record_success(Duration::from_millis(10));
        }
        assert_eq!(s.failure_rate(), 0.0, "old failures evicted from window");
    }

    #[test]
    fn snapshot_counts_beyond_selector_window() {
        let mut s = UpstreamStats::new(4);
        for _ in 0..10 {
            s.record_success(Duration::from_millis(10));
        }
        // the selector window keeps only 4 entries, but the dashboard sample count covers the full history
        assert_eq!(s.snapshot().samples, 10);
        assert_eq!(s.snapshot().successes, 10);
    }

    #[test]
    fn snapshot_prunes_history_outside_retention() {
        const DAY_MS: i64 = 86_400_000;
        let mut s = UpstreamStats::with_retention(4, 7);
        let now = 100 * DAY_MS;
        s.record_failure_at(now - 8 * DAY_MS);
        s.record_success_at(Duration::from_millis(30), now - DAY_MS);
        s.record_success_at(Duration::from_millis(10), now);

        let snap = s.snapshot_at(now);
        assert_eq!(snap.samples, 2, "the sample from 8 days ago should be pruned");
        assert_eq!(snap.successes, 2);
        assert_eq!(snap.failure_rate, 0.0);
        assert_eq!(snap.avg_latency_ms, Some(20.0));

        let later = s.snapshot_at(now + 8 * DAY_MS);
        assert_eq!(later.samples, 0, "samples drop to zero once past the retention period");
        assert_eq!(later.failure_rate, 0.0);
        assert_eq!(later.avg_latency_ms, None);
    }

    #[test]
    fn zero_retention_keeps_full_history() {
        const DAY_MS: i64 = 86_400_000;
        let mut s = UpstreamStats::with_retention(2, 0);
        s.record_failure_at(0);
        s.record_success_at(Duration::from_millis(10), 400 * DAY_MS);
        let snap = s.snapshot_at(400 * DAY_MS);
        assert_eq!(snap.samples, 2);
        assert_eq!(snap.successes, 1);
        assert_eq!(snap.failure_rate, 0.5);
    }

    #[test]
    fn negative_k_treated_as_zero() {
        let mut s = UpstreamStats::new(4);
        for _ in 0..4 {
            s.record_failure();
        }
        let w = s.weight(-2.0);
        assert!(
            w.is_finite() && w > 0.0,
            "negative k must not produce inf/negative weight: {w}"
        );
        assert!((w - s.weight(0.0)).abs() < 1e-12);
    }
}
