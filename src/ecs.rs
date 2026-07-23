use anyhow::{bail, Context, Result};
use hickory_proto::op::Message;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use crate::config::{EcsConfig, EcsMode};

/// ECS 子网：地址已按前缀掩码归零。
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct EcsSubnet {
    pub addr: IpAddr,
    pub prefix: u8,
}

/// 把 IP 掩码到指定前缀（v4 用 `v4_prefix`，v6 用 `v6_prefix`）。
pub fn mask_ip(ip: IpAddr, v4_prefix: u8, v6_prefix: u8) -> EcsSubnet {
    match ip {
        IpAddr::V4(v4) => {
            let bits = u32::from(v4);
            let mask = if v4_prefix == 0 { 0 } else { u32::MAX << (32 - v4_prefix) };
            EcsSubnet { addr: IpAddr::V4(Ipv4Addr::from(bits & mask)), prefix: v4_prefix }
        }
        IpAddr::V6(v6) => {
            let bits = u128::from(v6);
            let mask = if v6_prefix == 0 { 0 } else { u128::MAX << (128 - v6_prefix) };
            EcsSubnet { addr: IpAddr::V6(Ipv6Addr::from(bits & mask)), prefix: v6_prefix }
        }
    }
}

/// 解析 `"1.2.3.0/24"` 形式的子网；前缀超界或格式非法 bail。
pub fn parse_subnet(s: &str) -> Result<EcsSubnet> {
    let (addr, prefix) = s.split_once('/').with_context(|| format!("invalid subnet {s}"))?;
    let addr: IpAddr = addr.parse().with_context(|| format!("invalid subnet addr {s}"))?;
    let prefix: u8 = prefix.parse().with_context(|| format!("invalid prefix {s}"))?;
    let max = if addr.is_ipv4() { 32 } else { 128 };
    if prefix > max {
        bail!("prefix /{prefix} out of range for {s}");
    }
    Ok(match addr {
        IpAddr::V4(_) => mask_ip(addr, prefix, 0),
        IpAddr::V6(_) => mask_ip(addr, 0, prefix),
    })
}

/// 排除 loopback/私有(10/8、172.16/12、192.168/16)/链路本地/ULA(fc00::/7)/未指定地址。
pub fn is_global(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            !(v4.is_loopback() || v4.is_private() || v4.is_link_local() || v4.is_unspecified())
        }
        IpAddr::V6(v6) => {
            let seg0 = v6.segments()[0];
            !(v6.is_loopback()
                || v6.is_unspecified()
                || (seg0 & 0xffc0) == 0xfe80 // 链路本地 fe80::/10
                || (seg0 & 0xfe00) == 0xfc00) // ULA fc00::/7
        }
    }
}

/// UDP connect 不发包即可取出口地址；v4 失败则尝试 v6。
pub async fn detect_egress() -> Result<IpAddr> {
    for target in ["8.8.8.8:53", "[2001:4860:4860::8888]:53"] {
        let bind = if target.starts_with('[') { "[::]:0" } else { "0.0.0.0:0" };
        if let Ok(sock) = tokio::net::UdpSocket::bind(bind).await
            && sock.connect(target).await.is_ok()
            && let Ok(local) = sock.local_addr()
        {
            return Ok(local.ip());
        }
    }
    bail!("cannot detect egress ip")
}

/// 依据配置得出应注入的 ECS 子网；`Disabled` 直接 None，`Fixed`/`Auto` 失败时 warn 并回退 None。
pub async fn subnet_from_config(cfg: &EcsConfig) -> Option<EcsSubnet> {
    match cfg.mode {
        EcsMode::Disabled => None,
        EcsMode::Fixed => match parse_subnet(&cfg.fixed_subnet) {
            Ok(s) => Some(s),
            Err(e) => {
                tracing::warn!("invalid ecs.fixed_subnet, ECS disabled: {e:#}");
                None
            }
        },
        EcsMode::Auto => match detect_egress().await {
            Ok(ip) if is_global(&ip) => Some(mask_ip(ip, 24, 56)),
            Ok(ip) => {
                tracing::warn!("egress ip {ip} is not global, ECS disabled");
                None
            }
            Err(e) => {
                tracing::warn!("egress detection failed, ECS disabled: {e:#}");
                None
            }
        },
    }
}

/// 注入 ECS：已有 EDNS 则追加 option，否则创建。scope_prefix=0。
pub fn inject(query: &mut Message, subnet: &EcsSubnet) {
    use hickory_proto::op::Edns;
    use hickory_proto::rr::rdata::opt::{ClientSubnet, EdnsOption};

    let ecs = ClientSubnet::new(subnet.addr, subnet.prefix, 0);
    let edns = query.edns.get_or_insert_with(Edns::new);
    edns.set_max_payload(1232);
    edns.options_mut().insert(EdnsOption::Subnet(ecs));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::IpAddr;
    use std::str::FromStr;

    #[test]
    fn masks_v4_to_24_and_v6_to_56() {
        let v4 = mask_ip(IpAddr::from_str("203.0.113.77").unwrap(), 24, 56);
        assert_eq!(v4.addr, IpAddr::from_str("203.0.113.0").unwrap());
        assert_eq!(v4.prefix, 24);
        let v6 = mask_ip(IpAddr::from_str("2001:db8:aaaa:bbcc:1:2:3:4").unwrap(), 24, 56);
        assert_eq!(v6.addr, IpAddr::from_str("2001:db8:aaaa:bb00::").unwrap());
        assert_eq!(v6.prefix, 56);
    }

    #[test]
    fn parses_and_rejects_subnets() {
        let s = parse_subnet("198.51.100.0/24").unwrap();
        assert_eq!(s.prefix, 24);
        assert!(parse_subnet("198.51.100.0/33").is_err());
        assert!(parse_subnet("not-a-subnet").is_err());
        assert!(parse_subnet("2001:db8::/129").is_err());
    }

    #[test]
    fn global_detection() {
        assert!(is_global(&IpAddr::from_str("203.0.113.1").unwrap()));
        for private in ["10.0.0.1", "172.16.5.5", "192.168.1.1", "127.0.0.1", "fe80::1", "fd00::1"] {
            assert!(!is_global(&IpAddr::from_str(private).unwrap()), "{private} is not global");
        }
    }

    #[test]
    fn inject_adds_ecs_option() {
        use hickory_proto::op::{Message, MessageType, OpCode, Query};
        use hickory_proto::rr::{Name, RecordType};
        let mut m = Message::new(1, MessageType::Query, OpCode::Query);
        let mut q = Query::new();
        q.set_name(Name::from_str("example.com.").unwrap());
        q.set_query_type(RecordType::A);
        m.add_query(q);
        let subnet = parse_subnet("203.0.113.0/24").unwrap();
        inject(&mut m, &subnet);
        // 往返编解码后 ECS 选项仍在（证明 wire 层真实生效）
        let bytes = m.to_vec().unwrap();
        let decoded = hickory_proto::op::Message::from_vec(&bytes).unwrap();
        let edns = decoded.edns.as_ref().expect("edns present");
        assert!(
            edns.option(hickory_proto::rr::rdata::opt::EdnsCode::Subnet).is_some(),
            "ECS option must survive encode/decode"
        );
    }
}
