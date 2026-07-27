use std::sync::{Arc, Mutex};

use crate::stats::UpstreamStats;

struct MetricMember {
    id: String,
    name: String,
    group: &'static str,
    stats: Arc<Mutex<UpstreamStats>>,
}

#[derive(Clone, Default)]
pub struct UpstreamMetrics(Arc<Vec<MetricMember>>);

#[derive(Debug, Clone, serde::Serialize)]
pub struct UpstreamSnapshot {
    pub id: String,
    pub name: String,
    pub group: &'static str,
    pub samples: usize,
    pub successes: usize,
    pub failure_rate: f64,
    pub avg_latency_ms: Option<f64>,
}

impl UpstreamMetrics {
    pub fn snapshot(&self) -> Vec<UpstreamSnapshot> {
        self.0
            .iter()
            .filter_map(|member| {
                let snapshot = member.stats.lock().ok()?.snapshot();
                Some(UpstreamSnapshot {
                    id: member.id.clone(),
                    name: member.name.clone(),
                    group: member.group,
                    samples: snapshot.samples,
                    successes: snapshot.successes,
                    failure_rate: snapshot.failure_rate,
                    avg_latency_ms: snapshot.avg_latency_ms,
                })
            })
            .collect()
    }
}

#[derive(Default)]
pub struct UpstreamMetricsBuilder(Vec<MetricMember>);

impl UpstreamMetricsBuilder {
    pub(crate) fn register(
        &mut self,
        name: String,
        group: &'static str,
        stats: Arc<Mutex<UpstreamStats>>,
    ) {
        let group_index = self.0.iter().filter(|member| member.group == group).count();
        self.0.push(MetricMember {
            id: format!("{group}-{group_index}"),
            name,
            group,
            stats,
        });
    }

    pub fn build(self) -> UpstreamMetrics {
        UpstreamMetrics(Arc::new(self.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_skips_poisoned_stats() {
        let stats = Arc::new(Mutex::new(UpstreamStats::new(4)));
        let poison = stats.clone();
        let _ = std::thread::spawn(move || {
            let _guard = poison.lock().unwrap();
            panic!("poison metrics lock");
        })
        .join();
        let mut builder = UpstreamMetricsBuilder::default();
        builder.register("poisoned".into(), "primary", stats);

        assert!(builder.build().snapshot().is_empty());
    }
}
