use anyhow::{Context, Result};
use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::CertificateDer;

/// Builds the upstream TLS client configuration: webpki root certificates +
/// optional additional roots (self-signed for testing) + ALPN + optional ECH (TLS 1.3).
///
/// Provider note: in this project's dependency tree, rustls links both the
/// aws-lc-rs and ring crypto providers (quinn pulls in ring), so we cannot rely
/// on implicit resolution of the process-level default provider——`ClientConfig::builder()` /
/// `CryptoProvider::get_default_or_install_from_crate_features()` will panic when
/// multiple candidate providers exist and no process-level default has been
/// installed. Here both the regular path and the ECH path uniformly use
/// `ClientConfig::builder_with_provider(aws_lc_rs::default_provider())` to specify
/// the provider explicitly and avoid ambiguity; the HPKE suites required by ECH
/// can only come from the aws-lc-rs provider anyway.
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
