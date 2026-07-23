use hashlink::LinkedHashMap;
use hickory_proto::op::Message;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// 缓存键：规范化域名（小写、无尾点）+ qtype 数值。
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub name: String,
    pub qtype: u16,
}

impl CacheKey {
    pub fn from_query(query: &Message) -> Option<CacheKey> {
        let q = query.queries.first()?;
        Some(CacheKey {
            name: q.name().to_string().trim_end_matches('.').to_lowercase(),
            qtype: u16::from(q.query_type()),
        })
    }
}

struct CacheEntry {
    message: Message,
    expires_at: Instant,
}

/// 纯内存 FIFO 乐观缓存：命中即返回（过期也返回并标记）；
/// 读取不改变队列顺序；put 删旧插队尾；超限逐出最旧。
pub struct Cache {
    map: Mutex<LinkedHashMap<CacheKey, CacheEntry>>,
    max_entries: usize,
}

impl Cache {
    pub fn new(max_entries: usize) -> Self {
        Self { map: Mutex::new(LinkedHashMap::new()), max_entries: max_entries.max(1) }
    }

    /// 命中时克隆报文并把 id 改写为当前查询 id。bool 表示已过期（需后台刷新）。
    pub fn get(&self, key: &CacheKey, query_id: u16) -> Option<(Message, bool)> {
        let map = self.map.lock().ok()?;
        let entry = map.get(key)?;
        let mut msg = entry.message.clone();
        msg.metadata.id = query_id;
        let expired = Instant::now() >= entry.expires_at;
        Some((msg, expired))
    }

    /// TTL = answers 最小 TTL（无 answers 用 60s）。
    pub fn put(&self, key: CacheKey, message: Message) {
        let ttl = message
            .answers
            .iter()
            .map(|r| r.ttl)
            .min()
            .unwrap_or(60);
        let entry = CacheEntry {
            message,
            expires_at: Instant::now() + Duration::from_secs(u64::from(ttl)),
        };
        if let Ok(mut map) = self.map.lock() {
            map.remove(&key); // 旧条目移除，保证重新入队尾
            map.insert(key, entry);
            while map.len() > self.max_entries {
                map.pop_front();
            }
        }
    }

    pub fn len(&self) -> usize {
        self.map.lock().map(|m| m.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
    use hickory_proto::rr::rdata::A;
    use hickory_proto::rr::{Name, RData, Record, RecordType};
    use std::str::FromStr;

    fn response(id: u16, name: &str, ttl: u32) -> Message {
        let mut m = Message::new(id, MessageType::Response, OpCode::Query);
        m.metadata.response_code = ResponseCode::NoError;
        let n = Name::from_str(name).unwrap();
        let mut q = Query::new();
        q.set_name(n.clone());
        q.set_query_type(RecordType::A);
        m.add_query(q);
        m.add_answer(Record::from_rdata(n, ttl, RData::A(A::new(1, 2, 3, 4))));
        m
    }

    fn key(name: &str) -> CacheKey {
        CacheKey { name: name.trim_end_matches('.').to_lowercase(), qtype: 1 }
    }

    #[test]
    fn hit_rewrites_id_and_preserves_order() {
        let cache = Cache::new(10);
        cache.put(key("example.com."), response(0x1111, "example.com.", 300));
        let (msg, expired) = cache.get(&key("example.com."), 0x9999).expect("hit");
        assert_eq!(msg.metadata.id, 0x9999, "cached id must be rewritten");
        assert!(!expired);
    }

    #[test]
    fn expired_entry_still_returned_marked_expired() {
        let cache = Cache::new(10);
        cache.put(key("stale.com."), response(1, "stale.com.", 0)); // TTL 0 → 立即过期
        let (_, expired) = cache.get(&key("stale.com."), 2).expect("optimistic hit");
        assert!(expired, "ttl 0 entry must be flagged expired but still returned");
    }

    #[test]
    fn fifo_eviction_ignores_reads() {
        let cache = Cache::new(2);
        cache.put(key("a.com."), response(1, "a.com.", 300));
        cache.put(key("b.com."), response(2, "b.com.", 300));
        // 读 a 多次——FIFO 下不得改变淘汰顺序
        for _ in 0..5 {
            cache.get(&key("a.com."), 7);
        }
        cache.put(key("c.com."), response(3, "c.com.", 300));
        assert!(cache.get(&key("a.com."), 7).is_none(), "a 最先插入，必须最先被逐出");
        assert!(cache.get(&key("b.com."), 7).is_some());
        assert!(cache.get(&key("c.com."), 7).is_some());
    }

    #[test]
    fn put_replaces_and_moves_to_tail() {
        let cache = Cache::new(2);
        cache.put(key("a.com."), response(1, "a.com.", 300));
        cache.put(key("b.com."), response(2, "b.com.", 300));
        cache.put(key("a.com."), response(9, "a.com.", 300)); // 刷新 a → 移到队尾
        cache.put(key("c.com."), response(3, "c.com.", 300)); // 应逐出 b（现最旧）
        assert!(cache.get(&key("b.com."), 7).is_none());
        assert!(cache.get(&key("a.com."), 7).is_some());
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn missing_question_key_is_none() {
        let m = Message::new(5, MessageType::Query, OpCode::Query);
        assert!(CacheKey::from_query(&m).is_none());
    }
}
