use hickory_proto::op::{Message, MessageType, ResponseCode};
use hickory_proto::rr::rdata::{A, AAAA};
use hickory_proto::rr::{RData, Record, RecordType};
use std::collections::HashMap;
use std::net::IpAddr;

const HOSTS_TTL: u32 = 300;

/// Custom hosts: exact names + `*.` wildcard suffixes, building responses directly.
pub struct HostsMap {
    exact: HashMap<String, Vec<IpAddr>>,
    wildcard: HashMap<String, Vec<IpAddr>>, // key is the base domain with "*." stripped off
}

fn normalize(name: &str) -> String {
    name.trim_end_matches('.').to_lowercase()
}

impl HostsMap {
    pub fn from_entries(entries: &[crate::config::HostEntry]) -> Self {
        let mut exact = HashMap::new();
        let mut wildcard = HashMap::new();
        for e in entries {
            let name = normalize(&e.name);
            if let Some(base) = name.strip_prefix("*.") {
                wildcard.insert(base.to_string(), e.addrs.clone());
            } else {
                exact.insert(name, e.addrs.clone());
            }
        }
        Self { exact, wildcard }
    }

    pub fn is_empty(&self) -> bool {
        self.exact.is_empty() && self.wildcard.is_empty()
    }

    fn find(&self, name: &str) -> Option<&Vec<IpAddr>> {
        if let Some(v) = self.exact.get(name) {
            return Some(v);
        }
        // Wildcard: strip the leftmost label level by level, matching the remainder against the base domain (does not match the base domain itself)
        let mut rest = name;
        while let Some(pos) = rest.find('.') {
            rest = &rest[pos + 1..];
            if let Some(v) = self.wildcard.get(rest) {
                return Some(v);
            }
        }
        None
    }

    pub fn lookup(&self, query: &Message) -> Option<Message> {
        let q = query.queries.first()?;
        let name = normalize(&q.name().to_string());
        let addrs = self.find(&name)?;

        let mut resp = Message::new(
            query.metadata.id,
            MessageType::Response,
            query.metadata.op_code,
        );
        resp.metadata.response_code = ResponseCode::NoError;
        resp.metadata.recursion_desired = query.metadata.recursion_desired;
        resp.metadata.recursion_available = true;
        resp.add_query(q.clone());

        let owner = q.name().clone();
        match q.query_type() {
            RecordType::A => {
                for ip in addrs {
                    if let IpAddr::V4(v4) = ip {
                        resp.add_answer(Record::from_rdata(
                            owner.clone(),
                            HOSTS_TTL,
                            RData::A(A(*v4)),
                        ));
                    }
                }
            }
            RecordType::AAAA => {
                for ip in addrs {
                    if let IpAddr::V6(v6) = ip {
                        resp.add_answer(Record::from_rdata(
                            owner.clone(),
                            HOSTS_TTL,
                            RData::AAAA(AAAA(*v6)),
                        ));
                    }
                }
            }
            _ => {} // name matched but not an address-type query → NODATA
        }
        Some(resp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HostEntry;
    use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
    use hickory_proto::rr::{Name, RecordType};
    use std::str::FromStr;

    fn entries() -> Vec<HostEntry> {
        vec![
            HostEntry {
                name: "router.local".into(),
                addrs: vec!["192.168.1.1".parse().unwrap(), "fd00::1".parse().unwrap()],
            },
            HostEntry {
                name: "*.lab.example".into(),
                addrs: vec!["10.0.0.7".parse().unwrap()],
            },
        ]
    }

    fn query(name: &str, qtype: RecordType) -> Message {
        let mut m = Message::new(0x1234, MessageType::Query, OpCode::Query);
        let mut q = Query::new();
        q.set_name(Name::from_str(name).unwrap());
        q.set_query_type(qtype);
        m.add_query(q);
        m
    }

    #[test]
    fn exact_a_and_aaaa() {
        let h = HostsMap::from_entries(&entries());
        let resp = h
            .lookup(&query("Router.Local.", RecordType::A))
            .expect("hit");
        assert_eq!(resp.metadata.id, 0x1234);
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
        assert_eq!(resp.answers.len(), 1, "only the v4 addr for A query");
        let resp6 = h
            .lookup(&query("router.local.", RecordType::AAAA))
            .expect("hit");
        assert_eq!(resp6.answers.len(), 1, "only the v6 addr for AAAA query");
    }

    #[test]
    fn wildcard_matches_subdomains_only() {
        let h = HostsMap::from_entries(&entries());
        assert!(
            h.lookup(&query("box.lab.example.", RecordType::A))
                .is_some()
        );
        assert!(
            h.lookup(&query("a.b.lab.example.", RecordType::A))
                .is_some()
        );
        assert!(
            h.lookup(&query("lab.example.", RecordType::A)).is_none(),
            "wildcard does not match the base domain"
        );
    }

    #[test]
    fn hit_without_family_is_nodata() {
        let h = HostsMap::from_entries(&entries());
        let resp = h
            .lookup(&query("box.lab.example.", RecordType::AAAA))
            .expect("name hit");
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
        assert!(resp.answers.is_empty(), "no v6 for wildcard entry → NODATA");
    }

    #[test]
    fn miss_returns_none() {
        let h = HostsMap::from_entries(&entries());
        assert!(h.lookup(&query("nope.example.", RecordType::A)).is_none());
    }
}
