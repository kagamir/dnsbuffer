use anyhow::{bail, Context, Result};
use hickory_proto::op::{Message, MessageType, OpCode, Query};
use hickory_proto::rr::{Name, RData, RecordType};
use std::collections::BTreeSet;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;

use crate::config::UpstreamConfig;
use crate::resolver::Resolver;
use crate::upstream::{doh::DohResolver, dot::DotResolver, plain::PlainResolver};

/// Bootstrap 解析器组：为上游 DoH 域名解析 IP、为 ECH 拉取 HTTPS 记录。
/// 非 IP 形态的 bootstrap 服务器必须自带显式 ips（config.validate 已保证）。
pub struct Bootstrap {
    resolvers: Vec<Arc<dyn Resolver>>,
}

impl Bootstrap {
    pub fn from_config(servers: &[UpstreamConfig]) -> Result<Self> {
        let mut resolvers: Vec<Arc<dyn Resolver>> = Vec::new();
        for s in servers {
            let r: Arc<dyn Resolver> = match s {
                UpstreamConfig::Plain { addr } => Arc::new(PlainResolver::new(*addr)),
                UpstreamConfig::Dot { addr, domain, .. } => {
                    let tls = Arc::new(crate::tls::client_config(&[], &[], None)?);
                    Arc::new(DotResolver::new(*addr, domain, tls)?)
                }
                UpstreamConfig::Doh { url, ips, http3, .. } => {
                    // bootstrap 无 ECH（validate 已保证 ips 非空）；
                    // 默认 H2，配置显式 http3 = true 才启用 H3
                    Arc::new(DohResolver::new(url, ips.clone(), None, *http3)?)
                }
            };
            resolvers.push(r);
        }
        Ok(Self { resolvers })
    }

    pub fn is_empty(&self) -> bool {
        self.resolvers.is_empty()
    }

    fn make_query(domain: &str, rtype: RecordType) -> Result<Message> {
        let name = Name::from_str(&format!("{}.", domain.trim_end_matches('.')))
            .with_context(|| format!("invalid domain {domain}"))?;
        let mut m = Message::new(rand::random::<u16>(), MessageType::Query, OpCode::Query);
        m.metadata.recursion_desired = true;
        let mut q = Query::new();
        q.set_name(name);
        q.set_query_type(rtype);
        m.add_query(q);
        Ok(m)
    }

    async fn query(&self, domain: &str, rtype: RecordType) -> Result<Message> {
        let query = Self::make_query(domain, rtype)?;
        let mut last: Option<anyhow::Error> = None;
        for r in &self.resolvers {
            match r.resolve(&query).await {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    tracing::warn!("bootstrap resolver failed for {domain} {rtype}: {e:#}");
                    last = Some(e);
                }
            }
        }
        Err(last.unwrap_or_else(|| anyhow::anyhow!("no bootstrap resolvers configured")))
    }

    pub async fn resolve_ips(&self, domain: &str) -> Result<Vec<IpAddr>> {
        let mut ips = BTreeSet::new();
        for rtype in [RecordType::A, RecordType::AAAA] {
            match self.query(domain, rtype).await {
                Ok(resp) => {
                    for record in &resp.answers {
                        match &record.data {
                            RData::A(a) => {
                                ips.insert(IpAddr::V4(a.0));
                            }
                            RData::AAAA(aaaa) => {
                                ips.insert(IpAddr::V6(aaaa.0));
                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => tracing::warn!("bootstrap {rtype} lookup failed for {domain}: {e:#}"),
            }
        }
        if ips.is_empty() {
            bail!("bootstrap could not resolve any ip for {domain}");
        }
        Ok(ips.into_iter().collect())
    }

    pub async fn fetch_ech(&self, domain: &str) -> Result<Option<Vec<u8>>> {
        let resp = self.query(domain, RecordType::HTTPS).await?;
        for record in &resp.answers {
            if let RData::HTTPS(https) = &record.data {
                use hickory_proto::rr::rdata::svcb::{SvcParamKey, SvcParamValue};
                for (key, value) in &https.0.svc_params {
                    if matches!(key, SvcParamKey::EchConfigList) {
                        if let SvcParamValue::EchConfigList(list) = value {
                            return Ok(Some(list.0.clone()));
                        }
                    }
                }
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, ResponseCode};
    use hickory_proto::rr::rdata::svcb::{EchConfigList, SvcParamKey, SvcParamValue, SVCB};
    use hickory_proto::rr::rdata::{A, AAAA, HTTPS};
    use hickory_proto::rr::{Name, RData, Record, RecordType};
    use std::net::{IpAddr, SocketAddr};
    use std::str::FromStr;
    use tokio::net::UdpSocket;

    /// mock 上游：按 qtype 回 A/AAAA/HTTPS（带 ech 参数）记录。
    async fn spawn_mock_bootstrap_upstream(ech_bytes: Vec<u8>) -> SocketAddr {
        let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = sock.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65535];
            loop {
                let (n, peer) = sock.recv_from(&mut buf).await.unwrap();
                let query = Message::from_vec(&buf[..n]).unwrap();
                let q = query.queries[0].clone();
                let mut resp = Message::new(query.metadata.id, MessageType::Response, OpCode::Query);
                resp.metadata.response_code = ResponseCode::NoError;
                resp.add_query(q.clone());
                let name = q.name().clone();
                let rdata = match q.query_type() {
                    RecordType::A => Some(RData::A(A::new(93, 184, 216, 34))),
                    RecordType::AAAA => Some(RData::AAAA(AAAA::new(0x2606, 0x2800, 0, 0, 0, 0, 0, 1))),
                    RecordType::HTTPS => {
                        let svcb = SVCB::new(
                            1,
                            Name::root(),
                            vec![(
                                SvcParamKey::EchConfigList,
                                SvcParamValue::EchConfigList(EchConfigList(ech_bytes.clone())),
                            )],
                        );
                        Some(RData::HTTPS(HTTPS(svcb)))
                    }
                    _ => None,
                };
                if let Some(rdata) = rdata {
                    resp.add_answer(Record::from_rdata(name, 300, rdata));
                }
                sock.send_to(&resp.to_vec().unwrap(), peer).await.unwrap();
            }
        });
        addr
    }

    fn bootstrap_for(addr: SocketAddr) -> Bootstrap {
        Bootstrap::from_config(&[crate::config::UpstreamConfig::Plain { addr }]).unwrap()
    }

    #[tokio::test]
    async fn resolves_a_and_aaaa() {
        let addr = spawn_mock_bootstrap_upstream(vec![1, 2, 3]).await;
        let b = bootstrap_for(addr);
        let ips = b.resolve_ips("dns.example").await.expect("ips");
        assert!(ips.contains(&IpAddr::from_str("93.184.216.34").unwrap()));
        assert!(ips.iter().any(|ip| ip.is_ipv6()));
    }

    #[tokio::test]
    async fn fetches_ech_from_https_record() {
        let addr = spawn_mock_bootstrap_upstream(vec![0xAB, 0xCD]).await;
        let b = bootstrap_for(addr);
        let ech = b.fetch_ech("dns.example").await.expect("ech query");
        assert_eq!(ech, Some(vec![0xAB, 0xCD]));
    }

    #[test]
    fn is_empty_reflects_resolver_count() {
        let b = Bootstrap::from_config(&[]).unwrap();
        assert!(b.is_empty());
    }
}
