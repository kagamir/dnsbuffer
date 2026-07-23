use std::collections::VecDeque;
use std::time::Duration;

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
        Self { window: window.max(1), samples: VecDeque::new() }
    }

    fn push(&mut self, s: Sample) {
        if self.samples.len() == self.window {
            self.samples.pop_front();
        }
        self.samples.push_back(s);
    }

    pub fn record_success(&mut self, latency: Duration) {
        self.push(Sample::Success { latency_ms: latency.as_secs_f64() * 1000.0 });
    }

    pub fn record_failure(&mut self) {
        self.push(Sample::Failure);
    }

    pub fn failure_rate(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let failures = self.samples.iter().filter(|s| matches!(s, Sample::Failure)).count();
        failures as f64 / self.samples.len() as f64
    }

    pub fn avg_latency_ms(&self) -> f64 {
        let (sum, n) = self.samples.iter().fold((0.0, 0u32), |(sum, n), s| match s {
            Sample::Success { latency_ms } => (sum + latency_ms, n + 1),
            Sample::Failure => (sum, n),
        });
        if n == 0 {
            100.0 // 冷启动中值
        } else {
            sum / n as f64
        }
    }

    /// w = 1 / ((t_avg_ms + ε) × (1 + k·f))，ε=1.0
    pub fn weight(&self, k: f64) -> f64 {
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
}
