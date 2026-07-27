use hashlink::LinkedHashMap;
use hickory_proto::op::Message;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Cache key: normalized domain name (lowercase, no trailing dot) + numeric qtype.
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

/// Pure in-memory LRU optimistic cache: return on hit (expired entries are returned and flagged too);
/// a read moves the entry to the tail (most recently used); put removes the old entry and inserts at the tail; over the limit, evict the least recently used.
pub struct Cache {
    map: Mutex<LinkedHashMap<CacheKey, CacheEntry>>,
    max_entries: usize,
    lookups: AtomicU64,
    hits: AtomicU64,
}

impl Cache {
    /// `max_entries` is clamped to ≥1 (0 would make the cache never able to hit).
    pub fn new(max_entries: usize) -> Self {
        Self {
            map: Mutex::new(LinkedHashMap::new()),
            max_entries: max_entries.max(1),
            lookups: AtomicU64::new(0),
            hits: AtomicU64::new(0),
        }
    }

    /// On a hit, clone the message and rewrite its id to the current query id, and refresh the entry as most recently used (LRU).
    /// The bool indicates it has expired (needs a background refresh).
    pub fn get(&self, key: &CacheKey, query_id: u16) -> Option<(Message, bool)> {
        self.lookups.fetch_add(1, Ordering::Relaxed);
        let mut map = self.map.lock().ok()?;
        let entry = map.to_back(key)?; // LRU: move to the tail on a hit
        self.hits.fetch_add(1, Ordering::Relaxed);
        let mut msg = entry.message.clone();
        msg.metadata.id = query_id;
        let expired = Instant::now() >= entry.expires_at;
        Some((msg, expired))
    }

    /// TTL = the minimum TTL among the answers (60s when there are no answers).
    pub fn put(&self, key: CacheKey, message: Message) {
        let ttl = message.answers.iter().map(|r| r.ttl).min().unwrap_or(60);
        let entry = CacheEntry {
            message,
            expires_at: Instant::now() + Duration::from_secs(u64::from(ttl)),
        };
        if let Ok(mut map) = self.map.lock() {
            map.remove(&key); // remove the old entry to guarantee re-insertion at the tail
            // Low-memory machines: when the OS refuses a memory request, treat the queue as full and evict by LRU to make room;
            // if the request still fails after evicting everything, give up on this write (dropping one cache entry is harmless).
            if !reserve_or_evict(&mut map, 1) {
                return;
            }
            map.insert(key, entry);
            while map.len() > self.max_entries {
                map.pop_front();
            }
        }
    }

    pub fn len(&self) -> usize {
        self.map.lock().map(|m| m.len()).unwrap_or(0)
    }

    /// Cumulative (lookups, hits) since process start; expired optimistic hits count as hits too.
    pub fn hit_stats(&self) -> (u64, u64) {
        (
            self.lookups.load(Ordering::Relaxed),
            self.hits.load(Ordering::Relaxed),
        )
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Reserve space for the `want` entries about to be inserted; on allocation failure, evict the least recently used by LRU and retry.
/// Returns false when memory still cannot be obtained after everything has been evicted.
fn reserve_or_evict(map: &mut LinkedHashMap<CacheKey, CacheEntry>, want: usize) -> bool {
    while map.try_reserve(want).is_err() {
        if map.pop_front().is_none() {
            return false;
        }
    }
    true
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
        CacheKey {
            name: name.trim_end_matches('.').to_lowercase(),
            qtype: 1,
        }
    }

    #[test]
    fn hit_rewrites_id() {
        let cache = Cache::new(10);
        cache.put(key("example.com."), response(0x1111, "example.com.", 300));
        let (msg, expired) = cache.get(&key("example.com."), 0x9999).expect("hit");
        assert_eq!(msg.metadata.id, 0x9999, "cached id must be rewritten");
        assert!(!expired);
    }

    #[test]
    fn expired_entry_still_returned_marked_expired() {
        let cache = Cache::new(10);
        cache.put(key("stale.com."), response(1, "stale.com.", 0)); // TTL 0 → expires immediately
        let (_, expired) = cache.get(&key("stale.com."), 2).expect("optimistic hit");
        assert!(
            expired,
            "ttl 0 entry must be flagged expired but still returned"
        );
    }

    #[test]
    fn lru_eviction_respects_reads() {
        let cache = Cache::new(2);
        cache.put(key("a.com."), response(1, "a.com.", 300));
        cache.put(key("b.com."), response(2, "b.com.", 300));
        // read a — under LRU, a becomes most recently used and b becomes least recently used
        cache.get(&key("a.com."), 7);
        cache.put(key("c.com."), response(3, "c.com.", 300));
        assert!(
            cache.get(&key("b.com."), 7).is_none(),
            "b is least recently used and must be evicted first"
        );
        assert!(cache.get(&key("a.com."), 7).is_some(), "a was read and should survive");
        assert!(cache.get(&key("c.com."), 7).is_some());
    }

    #[test]
    fn put_replaces_and_moves_to_tail() {
        let cache = Cache::new(2);
        cache.put(key("a.com."), response(1, "a.com.", 300));
        cache.put(key("b.com."), response(2, "b.com.", 300));
        cache.put(key("a.com."), response(9, "a.com.", 300)); // refresh a → move to the tail
        cache.put(key("c.com."), response(3, "c.com.", 300)); // should evict b (now the oldest)
        assert!(cache.get(&key("b.com."), 7).is_none());
        assert!(cache.get(&key("a.com."), 7).is_some());
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn reserve_failure_evicts_lru_until_empty() {
        let mut map = LinkedHashMap::new();
        let entry = |name: &str| CacheEntry {
            message: response(1, name, 300),
            expires_at: Instant::now(),
        };
        map.insert(key("a.com."), entry("a.com."));
        map.insert(key("b.com."), entry("b.com."));
        // Use an impossible-to-satisfy capacity to simulate the OS refusing a memory request: it should evict by LRU first, then give up if it still fails after emptying
        assert!(
            !reserve_or_evict(&mut map, isize::MAX as usize),
            "impossible reservation fails"
        );
        assert!(map.is_empty(), "entries evicted while trying to make room");
        map.insert(key("c.com."), entry("c.com."));
        assert!(reserve_or_evict(&mut map, 1), "normal reservation succeeds");
        assert_eq!(map.len(), 1, "success path must not evict");
    }

    #[test]
    fn missing_question_key_is_none() {
        let m = Message::new(5, MessageType::Query, OpCode::Query);
        assert!(CacheKey::from_query(&m).is_none());
    }

    #[test]
    fn hit_stats_count_lookups_including_expired_optimistic_hits() {
        let cache = Cache::new(10);
        cache.get(&key("miss.com."), 1);
        cache.put(key("hit.com."), response(1, "hit.com.", 300));
        cache.get(&key("hit.com."), 2);
        cache.put(key("stale.com."), response(1, "stale.com.", 0));
        cache.get(&key("stale.com."), 3);

        assert_eq!(cache.hit_stats(), (3, 2), "1 miss + 2 hits (including expired)");
    }
}
