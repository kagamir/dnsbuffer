pub mod doh;
pub mod doh3;
pub mod dot;
pub mod group;
pub mod plain;
pub mod selector;

use std::net::IpAddr;

/// 连接尝试次序整理：IPv6 在前、IPv4 在后（稳定排序，同族保持原有顺序）。
pub fn sort_v6_first(ips: &mut [IpAddr]) {
    ips.sort_by_key(|ip| ip.is_ipv4());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn v6_moves_ahead_of_v4() {
        let mut ips = vec![ip("1.1.1.1"), ip("2606:4700::1111"), ip("8.8.8.8"), ip("2001:4860:4860::8888")];
        sort_v6_first(&mut ips);
        assert_eq!(
            ips,
            vec![ip("2606:4700::1111"), ip("2001:4860:4860::8888"), ip("1.1.1.1"), ip("8.8.8.8")],
            "IPv6 first, original order preserved within each family"
        );
    }

    #[test]
    fn single_family_untouched() {
        let mut v4 = vec![ip("9.9.9.9"), ip("1.1.1.1")];
        sort_v6_first(&mut v4);
        assert_eq!(v4, vec![ip("9.9.9.9"), ip("1.1.1.1")]);
    }
}
