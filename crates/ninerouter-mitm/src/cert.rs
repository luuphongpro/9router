//! Quản lý chứng chỉ TLS và tự động ký chứng chỉ động cho từng tên miền qua SNI.

use std::collections::HashMap;
use std::fmt::Debug;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use anyhow::{anyhow, Result};
use parking_lot::RwLock;
use rcgen::{CertificateParams, KeyPair, SanType};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use tracing::{debug, error, info};

pub struct DynamicCertResolver {
    ca_cert: rcgen::Certificate,
    ca_key: KeyPair,
    cache: RwLock<HashMap<String, Arc<CertifiedKey>>>,
}

impl Debug for DynamicCertResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamicCertResolver").finish()
    }
}

impl DynamicCertResolver {
    /// Nạp Root CA key và crt từ thư mục chỉ định
    pub fn new(mitm_dir: &Path) -> Result<Self> {
        let key_path = mitm_dir.join("rootCA.key");
        let crt_path = mitm_dir.join("rootCA.crt");

        if !key_path.exists() || !crt_path.exists() {
            return Err(anyhow!("Root CA files not found in {:?}", mitm_dir));
        }

        let key_pem = fs::read_to_string(&key_path)
            .map_err(|e| anyhow!("Failed to read rootCA.key: {}", e))?;
        let crt_pem = fs::read_to_string(&crt_path)
            .map_err(|e| anyhow!("Failed to read rootCA.crt: {}", e))?;

        let ca_key = KeyPair::from_pem(&key_pem)
            .map_err(|e| anyhow!("Failed to parse Root CA private key: {}", e))?;
        let ca_params = CertificateParams::from_ca_cert_pem(&crt_pem)
            .map_err(|e| anyhow!("Failed to parse Root CA certificate PEM: {}", e))?;
        let ca_cert = ca_params.self_signed(&ca_key)
            .map_err(|e| anyhow!("Failed to initialize CA certificate instance: {}", e))?;

        info!("Root CA certificate loaded successfully from {:?}", mitm_dir);

        Ok(Self {
            ca_cert,
            ca_key,
            cache: RwLock::new(HashMap::new()),
        })
    }

    /// Sinh chứng chỉ lá (leaf cert) cho tên miền và đưa vào bộ nhớ đệm
    pub fn get_or_create_cert(&self, domain: &str) -> Result<Arc<CertifiedKey>> {
        {
            let cache = self.cache.read();
            if let Some(ck) = cache.get(domain) {
                return Ok(ck.clone());
            }
        }

        debug!("Generating dynamic TLS certificate for domain: {}", domain);

        let mut leaf_params = CertificateParams::new(vec![domain.to_string()])
            .map_err(|e| anyhow!("Failed to create cert params: {}", e))?;
        
        // Bổ sung wildcard SAN cho domain
        if let Ok(san) = format!("*.{}", domain).try_into() {
            leaf_params.subject_alt_names.push(SanType::DnsName(san));
        }

        let leaf_key = KeyPair::generate()
            .map_err(|e| anyhow!("Failed to generate leaf key pair: {}", e))?;
        let leaf_cert = leaf_params.signed_by(&leaf_key, &self.ca_cert, &self.ca_key)
            .map_err(|e| anyhow!("Failed to sign leaf cert with Root CA: {}", e))?;

        let cert_der = CertificateDer::from(leaf_cert.der().to_vec());
        let key_der = PrivateKeyDer::try_from(leaf_key.serialize_der())
            .map_err(|e| anyhow!("Failed to extract private key DER: {:?}", e))?;

        let signing_key = rustls::crypto::ring::sign::any_supported_type(&key_der)
            .map_err(|e| anyhow!("Failed to create signing key: {}", e))?;
        let certified_key = Arc::new(CertifiedKey::new(vec![cert_der], signing_key));

        let mut cache = self.cache.write();
        cache.insert(domain.to_string(), certified_key.clone());
        Ok(certified_key)
    }
}

impl ResolvesServerCert for DynamicCertResolver {
    fn resolve(&self, client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        let domain = client_hello.server_name().unwrap_or("localhost");
        match self.get_or_create_cert(domain) {
            Ok(key) => Some(key),
            Err(e) => {
                error!("Failed to resolve or generate certificate for {}: {}", domain, e);
                None
            }
        }
    }
}
