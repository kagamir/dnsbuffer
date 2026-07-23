use anyhow::{Context, Result};
use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::CertificateDer;

/// 构建上游 TLS 客户端配置：webpki 根证书 + 可选附加根（测试用自签）+
/// ALPN + 可选 ECH（TLS 1.3）。
///
/// provider 说明：本项目依赖树中 rustls 同时链接了 aws-lc-rs 与 ring 两个
/// crypto provider（quinn 引入 ring），因此不能依赖 process-level 默认
/// provider 的隐式解析——`ClientConfig::builder()` /
/// `CryptoProvider::get_default_or_install_from_crate_features()` 在存在
/// 多个候选 provider 且未安装进程级默认值时会 panic。这里对普通路径与 ECH
/// 路径统一使用 `ClientConfig::builder_with_provider(aws_lc_rs::default_provider())`
/// 显式指定 provider，避免歧义；ECH 所需的 HPKE 套件本来就只能来自
/// aws-lc-rs provider。
pub fn client_config(
    alpn: &[&[u8]],
    extra_roots: &[CertificateDer<'static>],
    ech_config_list: Option<&[u8]>,
) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    for cert in extra_roots {
        roots.add(cert.clone()).context("adding extra root cert")?;
    }

    let provider = std::sync::Arc::new(rustls::crypto::aws_lc_rs::default_provider());

    let mut config = match ech_config_list {
        Some(bytes) => {
            use rustls::client::{EchConfig, EchMode};
            use rustls_pki_types::EchConfigListBytes;

            let ech = EchConfig::new(
                EchConfigListBytes::from(bytes),
                rustls::crypto::aws_lc_rs::hpke::ALL_SUPPORTED_SUITES,
            )
            .context("parsing ECHConfigList")?;

            // with_ech() also pins TLS 1.3 as the only supported version.
            ClientConfig::builder_with_provider(provider)
                .with_ech(EchMode::Enable(ech))
                .context("enabling ECH")?
                .with_root_certificates(roots)
                .with_no_client_auth()
        }
        None => ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .context("selecting default protocol versions")?
            .with_root_certificates(roots)
            .with_no_client_auth(),
    };

    config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_plain_config_with_alpn() {
        let cfg = client_config(&[b"h2"], &[], None).expect("plain config");
        assert_eq!(cfg.alpn_protocols, vec![b"h2".to_vec()]);
    }

    #[test]
    fn garbage_ech_is_error_not_panic() {
        let r = client_config(&[b"h2"], &[], Some(b"not an ech config list"));
        assert!(r.is_err(), "garbage ECHConfigList must be a clean error");
    }
}
