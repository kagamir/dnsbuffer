use anyhow::{Context, Result, bail};
use hickory_proto::op::Message;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use crate::config::EcsConfig;

/// ECS 子网：地址已按前缀掩码归零。
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct EcsSubnet {
    pub addr: IpAddr,
    pub prefix: u8,
}

/// 把 IP 掩码到指定前缀（调用方保证前缀不超过地址族位宽）。
pub fn mask_ip(ip: IpAddr, prefix: u8) -> EcsSubnet {
    match ip {
        IpAddr::V4(v4) => {
            let bits = u32::from(v4);
            let mask = if prefix == 0 {
                0
            } else {
                u32::MAX << (32 - prefix)
            };
            EcsSubnet {
                addr: IpAddr::V4(Ipv4Addr::from(bits & mask)),
                prefix,
            }
        }
        IpAddr::V6(v6) => {
            let bits = u128::from(v6);
            let mask = if prefix == 0 {
                0
            } else {
                u128::MAX << (128 - prefix)
            };
            EcsSubnet {
                addr: IpAddr::V6(Ipv6Addr::from(bits & mask)),
                prefix,
            }
        }
    }
}

/// 解析 `"1.2.3.0/24"` 形式的子网；前缀超界或格式非法 bail。
pub fn parse_subnet(s: &str) -> Result<EcsSubnet> {
    let (addr, prefix) = s
        .split_once('/')
        .with_context(|| format!("invalid subnet {s}"))?;
    let addr: IpAddr = addr
        .parse()
        .with_context(|| format!("invalid subnet addr {s}"))?;
    let prefix: u8 = prefix
        .parse()
        .with_context(|| format!("invalid prefix {s}"))?;
    let max = if addr.is_ipv4() { 32 } else { 128 };
    if prefix > max {
        bail!("prefix /{prefix} out of range for {s}");
    }
    Ok(mask_ip(addr, prefix))
}

/// 配置了 `fixed_subnet` 则解析使用，否则不注入 ECS；解析失败 warn 并禁用。
pub fn subnet_from_config(cfg: &EcsConfig) -> Option<EcsSubnet> {
    let s = cfg.fixed_subnet.as_deref()?;
    match parse_subnet(s) {
        Ok(sub) => Some(sub),
        Err(e) => {
            tracing::warn!("invalid ecs.fixed_subnet, ECS disabled: {e:#}");
            None
        }
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
        let v4 = mask_ip(IpAddr::from_str("203.0.113.77").unwrap(), 24);
        assert_eq!(v4.addr, IpAddr::from_str("203.0.113.0").unwrap());
        assert_eq!(v4.prefix, 24);
        let v6 = mask_ip(IpAddr::from_str("2001:db8:aaaa:bbcc:1:2:3:4").unwrap(), 56);
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
    fn subnet_from_config_uses_fixed_subnet_or_disables() {
        let none = EcsConfig { fixed_subnet: None };
        assert_eq!(
            subnet_from_config(&none),
            None,
            "no fixed_subnet means ECS off"
        );
        let some = EcsConfig {
            fixed_subnet: Some("203.0.113.0/24".into()),
        };
        let subnet = subnet_from_config(&some).expect("valid subnet enables ECS");
        assert_eq!(subnet.addr, IpAddr::from_str("203.0.113.0").unwrap());
        assert_eq!(subnet.prefix, 24);
        let bad = EcsConfig {
            fixed_subnet: Some("not-a-subnet".into()),
        };
        assert_eq!(
            subnet_from_config(&bad),
            None,
            "invalid subnet warns and disables"
        );
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
            edns.option(hickory_proto::rr::rdata::opt::EdnsCode::Subnet)
                .is_some(),
            "ECS option must survive encode/decode"
        );
    }
}
