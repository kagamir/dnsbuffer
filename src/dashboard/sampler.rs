use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::cache::Cache;

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub struct CacheSample {
    /// 观测时刻的缓存条目数。
    pub size: u64,
    /// 自进程启动以来的累计命中率（无查询时为 0）。
    pub hit_rate: f64,
    /// 观测时刻的累计缓存查询次数。
    #[serde(skip)]
    pub lookups: u64,
}

/// 命中率-缓存大小观测：每处理一个经过缓存的请求就把
/// (当前缓存条数, 累计命中率) 记为一个点，点按容量升序连成线即为观测曲线。
/// 例如启动时描点 (0, 0)；第 1 条请求未命中后缓存 1 条，描点 (1, 0)；
/// 累计 10 次查询命中 1 次且缓存 10 条时，描点 (10, 0.1)。
/// 同一容量只保留最新累计值，因此内存以 max_entries 为上界；
/// 数据在内存中，与缓存同生命周期。
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
        sampler.observe(); // 初始观测点，通常为 (0, 0)
        sampler
    }

    pub fn cache_len(&self) -> usize {
        self.cache.len()
    }

    /// 记录一个观测点；缓存条数与累计查询数都没变化则不重复描点。
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

    /// 最近一次观测点。
    pub fn latest(&self) -> Option<CacheSample> {
        self.state.lock().ok().and_then(|state| state.latest)
    }

    /// 按缓存容量升序输出观测点；同一容量取最新观测（累计口径下新值覆盖旧值）。
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
            "初始描点 (0, 0)"
        );

        cache.get(&key("a.com."), 1); // 请求 1：未命中
        cache.put(key("a.com."), response("a.com."));
        sampler.observe();
        let first = sampler.latest().unwrap();
        assert_eq!(
            (first.size, first.hit_rate),
            (1, 0.0),
            "1 条缓存、累计命中率 0"
        );

        cache.get(&key("a.com."), 2); // 请求 2：命中
        sampler.observe();
        let second = sampler.latest().unwrap();
        assert_eq!(second.size, 1);
        assert_eq!(second.hit_rate, 0.5, "累计 2 次查询命中 1 次");
        assert_eq!(second.lookups, 2);
    }

    #[test]
    fn observe_without_new_activity_does_not_duplicate_points() {
        let cache = Arc::new(Cache::new(10));
        let sampler = CacheHitSampler::new(cache.clone());

        sampler.observe();
        sampler.observe();
        assert_eq!(sampler.points().len(), 1, "无变化时不重复描点");
    }

    #[test]
    fn points_sort_by_size_and_keep_latest_cumulative_rate_per_size() {
        let cache = Arc::new(Cache::new(10));
        let sampler = CacheHitSampler::new(cache.clone());

        cache.get(&key("a.com."), 1); // 请求 1：未命中，累计 0/1
        cache.put(key("a.com."), response("a.com."));
        sampler.observe(); // (1, 0.0)
        cache.get(&key("a.com."), 2); // 请求 2：命中，累计 1/2
        sampler.observe(); // (1, 0.5) 覆盖同容量旧点
        cache.get(&key("b.com."), 3); // 请求 3：未命中，累计 1/3
        cache.put(key("b.com."), response("b.com."));
        cache.get(&key("b.com."), 4); // 请求 4：命中，累计 2/4
        sampler.observe(); // (2, 0.5)

        let points = sampler.points();
        assert_eq!(points.len(), 3);
        assert_eq!((points[0].size, points[0].hit_rate), (0, 0.0));
        assert_eq!(
            (points[1].size, points[1].hit_rate),
            (1, 0.5),
            "同容量取最新累计值"
        );
        assert_eq!((points[2].size, points[2].hit_rate), (2, 0.5));
        assert!(
            points.windows(2).all(|pair| pair[0].size < pair[1].size),
            "观测点必须按容量升序"
        );
    }
}
