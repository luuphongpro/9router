//! Module phân giải DNS độc lập qua 8.8.8.8 để vượt qua ánh xạ /etc/hosts nội bộ.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::{anyhow, Result};
use parking_lot::RwLock;
use trust_dns_resolver::config::{NameServerConfig, Protocol, ResolverConfig, ResolverOpts};
use trust_dns_resolver::TokioAsyncResolver;

const CACHE_TTL: Duration = Duration::from_secs(300); // 5 phút

#[derive(Clone)]
struct CacheEntry {
    ip: IpAddr,
    expires_at: Instant,
}

#[derive(Clone)]
pub struct DnsResolver {
    resolver: TokioAsyncResolver,
    cache: Arc<RwLock<HashMap<String, CacheEntry>>>,
}

impl DnsResolver {
    /// Khởi tạo DNS resolver sử dụng máy chủ 8.8.8.8
    pub fn new() -> Result<Self> {
        let mut config = ResolverConfig::new();
        let google_dns: SocketAddr = "8.8.8.8:53".parse()?;
        config.add_name_server(NameServerConfig::new(google_dns, Protocol::Udp));

        let mut opts = ResolverOpts::default();
        opts.timeout = Duration::from_secs(5);
        opts.attempts = 2;

        let resolver = TokioAsyncResolver::tokio(config, opts);
        Ok(Self {
            resolver,
            cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Phân giải tên miền thành địa chỉ IP thật của máy chủ mục tiêu
    pub async fn resolve(&self, host: &str) -> Result<IpAddr> {
        // Kiểm tra cache trước
        {
            let cache = self.cache.read();
            if let Some(entry) = cache.get(host) {
                if Instant::now() < entry.expires_at {
                    return Ok(entry.ip);
                }
            }
        }

        // Truy vấn DNS thực tế
        let response = self.resolver.lookup_ip(host).await
            .map_err(|e| anyhow!("Failed to resolve host {}: {}", host, e))?;

        if let Some(ip) = response.iter().next() {
            let mut cache = self.cache.write();
            cache.insert(host.to_string(), CacheEntry {
                ip,
                expires_at: Instant::now() + CACHE_TTL,
            });
            Ok(ip)
        } else {
            Err(anyhow!("No IP address found for {}", host))
        }
    }
}
