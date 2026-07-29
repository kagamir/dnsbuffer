use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::cache::Cache;

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub struct CacheSample {
    /// Number of cache entries at the moment of observation.
    pub size: u64,
    /// Cumulative hit rate since process start (0 when there are no queries).
    pub hit_rate: f64,
    /// Cumulative number of cache lookups at the moment of observation.
    #[serde(skip)]
    pub lookups: u64,
}

/// Hit rate vs. cache size observations: each time a cached request is handled,
/// record (current cache entry count, cumulative hit rate) as a point; connecting the points in ascending order of capacity yields the observation curve.
/// For example, plot (0, 0) at startup; after the 1st request misses and the cache holds 1 entry, plot (1, 0);
/// once 10 cumulative queries have hit once and the cache holds 10 entries, plot (10, 0.1).
/// Only the latest cumulative value is kept per capacity, so memory is bounded by max_entries;
/// the data lives in memory and shares the cache's lifetime.
pub struct CacheHitSampler {
    cache: Arc<Cache>,
    state: Mutex<SamplerState>,
}

#[derive(Default)]
struct SamplerState {
    latest: Option<CacheSample>,
    by_size: BTreeMap<u64, CacheSample>,
}

impl CacheHitSampler {
    pub fn new(cache: Arc<Cache>) -> Self {
        let sampler = Self {
            cache,
            state: Mutex::new(SamplerState::default()),
        };
        sampler.observe(); // Initial observation point, usually (0, 0)
        sampler
    }

    pub fn cache_len(&self) -> usize {
        self.cache.len()
    }

    /// (approximate bytes currently held by the cache, configured budget in bytes).
    pub fn memory_stats(&self) -> (u64, u64) {
        self.cache.memory_stats()
    }

    /// Record an observation point; skip if neither the cache entry count nor the cumulative lookup count has changed.
    pub fn observe(&self) {
        let (lookups, hits) = self.cache.hit_stats();
        let size = self.cache.len() as u64;
        let hit_rate = if lookups == 0 {
            0.0
        } else {
            hits.min(lookups) as f64 / lookups as f64
        };
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state
            .latest
            .is_some_and(|last| last.size == size && last.lookups == lookups)
        {
            return;
        }
        let sample = CacheSample {
            size,
            hit_rate,
            lookups,
        };
        state.latest = Some(sample);
        state.by_size.insert(size, sample);
    }

    /// The most recent observation point.
    pub fn latest(&self) -> Option<CacheSample> {
        self.state.lock().ok().and_then(|state| state.latest)
    }

    /// Output observation points in ascending order of cache capacity; for the same capacity, take the latest observation (under the cumulative measure the newer value overwrites the older one).
    pub fn points(&self) -> Vec<CacheSample> {
        self.state
            .lock()
            .map(|state| state.by_size.values().copied().collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::CacheKey;
    use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
    use hickory_proto::rr::rdata::A;
    use hickory_proto::rr::{Name, RData, Record, RecordType};
    use std::str::FromStr;

    fn key(name: &str) -> CacheKey {
        CacheKey {
            name: name.trim_end_matches('.').to_lowercase(),
            qtype: 1,
        }
    }

    fn response(name: &str) -> Message {
        let mut message = Message::new(1, MessageType::Response, OpCode::Query);
        message.metadata.response_code = ResponseCode::NoError;
        let parsed = Name::from_str(name).unwrap();
        let mut query = Query::new();
        query.set_name(parsed.clone());
        query.set_query_type(RecordType::A);
        message.add_query(query);
        message.add_answer(Record::from_rdata(
            parsed,
            300,
            RData::A(A::new(1, 2, 3, 4)),
        ));
        message
    }

    #[test]
    fn sampler_starts_at_origin_and_tracks_cumulative_hit_rate_per_request() {
        let cache = Arc::new(Cache::new(10));
        let sampler = CacheHitSampler::new(cache.clone());

        let initial = sampler.latest().unwrap();
        assert_eq!(
            (initial.size, initial.hit_rate),
            (0, 0.0),
            "initial point (0, 0)"
        );

        cache.get(&key("a.com."), 1); // Request 1: miss
        cache.put(key("a.com."), response("a.com."));
        sampler.observe();
        let first = sampler.latest().unwrap();
        assert_eq!(
            (first.size, first.hit_rate),
            (1, 0.0),
            "1 cached entry, cumulative hit rate 0"
        );

        cache.get(&key("a.com."), 2); // Request 2: hit
        sampler.observe();
        let second = sampler.latest().unwrap();
        assert_eq!(second.size, 1);
        assert_eq!(second.hit_rate, 0.5, "1 hit out of 2 cumulative queries");
        assert_eq!(second.lookups, 2);
    }

    #[test]
    fn observe_without_new_activity_does_not_duplicate_points() {
        let cache = Arc::new(Cache::new(10));
        let sampler = CacheHitSampler::new(cache.clone());

        sampler.observe();
        sampler.observe();
        assert_eq!(sampler.points().len(), 1, "no duplicate points when nothing changes");
    }

    #[test]
    fn points_sort_by_size_and_keep_latest_cumulative_rate_per_size() {
        let cache = Arc::new(Cache::new(10));
        let sampler = CacheHitSampler::new(cache.clone());

        cache.get(&key("a.com."), 1); // Request 1: miss, cumulative 0/1
        cache.put(key("a.com."), response("a.com."));
        sampler.observe(); // (1, 0.0)
        cache.get(&key("a.com."), 2); // Request 2: hit, cumulative 1/2
        sampler.observe(); // (1, 0.5) overwrites the older point at the same capacity
        cache.get(&key("b.com."), 3); // Request 3: miss, cumulative 1/3
        cache.put(key("b.com."), response("b.com."));
        cache.get(&key("b.com."), 4); // Request 4: hit, cumulative 2/4
        sampler.observe(); // (2, 0.5)

        let points = sampler.points();
        assert_eq!(points.len(), 3);
        assert_eq!((points[0].size, points[0].hit_rate), (0, 0.0));
        assert_eq!(
            (points[1].size, points[1].hit_rate),
            (1, 0.5),
            "take the latest cumulative value for the same capacity"
        );
        assert_eq!((points[2].size, points[2].hit_rate), (2, 0.5));
        assert!(
            points.windows(2).all(|pair| pair[0].size < pair[1].size),
            "observation points must be in ascending order of capacity"
        );
    }
}
