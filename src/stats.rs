use std::collections::VecDeque;
use std::time::Duration;

#[derive(Debug, Clone, serde::Serialize)]
pub struct StatsSnapshot {
    pub samples: usize,
    pub successes: usize,
    pub failure_rate: f64,
    pub avg_latency_ms: Option<f64>,
}

/// 单个上游的滑动窗口统计：近 N 次调用的失败率与平均延迟，驱动加权随机选择。
pub struct UpstreamStats {
    window: usize,
    samples: VecDeque<Sample>,
}

#[derive(Clone, Copy)]
enum Sample {
    Success { latency_ms: f64 },
    Failure,
}

impl UpstreamStats {
    pub fn new(window: usize) -> Self {
        Self {
            window: window.max(1),
            samples: VecDeque::new(),
        }
    }

    fn push(&mut self, s: Sample) {
        if self.samples.len() == self.window {
            self.samples.pop_front();
        }
        self.samples.push_back(s);
    }

    pub fn record_success(&mut self, latency: Duration) {
        self.push(Sample::Success {
            latency_ms: latency.as_secs_f64() * 1000.0,
        });
    }

    pub fn record_failure(&mut self) {
        self.push(Sample::Failure);
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
            100.0 // 冷启动中值
        } else {
            sum / n as f64
        }
    }

    pub fn snapshot(&self) -> StatsSnapshot {
        let (latency_sum, successes) =
            self.samples
                .iter()
                .fold((0.0, 0usize), |(sum, count), sample| match sample {
                    Sample::Success { latency_ms } => (sum + latency_ms, count + 1),
                    Sample::Failure => (sum, count),
                });
        StatsSnapshot {
            samples: self.samples.len(),
            successes,
            failure_rate: self.failure_rate(),
            avg_latency_ms: (successes > 0).then(|| latency_sum / successes as f64),
        }
    }

    /// w = 1 / ((t_avg_ms + ε) × (1 + k·f))，ε=1.0；k 负值按 0 处理
    pub fn weight(&self, k: f64) -> f64 {
        let k = k.max(0.0);
        1.0 / ((self.avg_latency_ms() + 1.0) * (1.0 + k * self.failure_rate()))
    }
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
