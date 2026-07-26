pub mod doh;
pub mod doh3;
pub mod dot;
pub mod group;
pub mod hedged;
pub mod plain;
pub mod selector;

use std::net::IpAddr;

/// 连接尝试次序整理：偏好的地址族在前（稳定排序，同族保持原有顺序）。
/// `prefer_ipv6 = false`（默认）时 IPv4 优先。
pub fn sort_by_family(ips: &mut [IpAddr], prefer_ipv6: bool) {
    ips.sort_by_key(|ip| ip.is_ipv6() != prefer_ipv6);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn v6_moves_ahead_when_preferred() {
        let mut ips = vec![
            ip("1.1.1.1"),
            ip("2606:4700::1111"),
            ip("8.8.8.8"),
            ip("2001:4860:4860::8888"),
        ];
        sort_by_family(&mut ips, true);
        assert_eq!(
            ips,
            vec![
                ip("2606:4700::1111"),
                ip("2001:4860:4860::8888"),
                ip("1.1.1.1"),
                ip("8.8.8.8")
            ],
            "IPv6 first, original order preserved within each family"
        );
    }

    #[test]
    fn v4_moves_ahead_by_default() {
        let mut ips = vec![
            ip("2606:4700::1111"),
            ip("1.1.1.1"),
            ip("2001:4860:4860::8888"),
            ip("8.8.8.8"),
        ];
        sort_by_family(&mut ips, false);
        assert_eq!(
            ips,
            vec![
                ip("1.1.1.1"),
                ip("8.8.8.8"),
                ip("2606:4700::1111"),
                ip("2001:4860:4860::8888")
            ],
            "IPv4 first by default, original order preserved within each family"
        );
    }

    #[test]
    fn single_family_untouched() {
        let mut v4 = vec![ip("9.9.9.9"), ip("1.1.1.1")];
        sort_by_family(&mut v4, true);
        assert_eq!(v4, vec![ip("9.9.9.9"), ip("1.1.1.1")]);
    }
}
