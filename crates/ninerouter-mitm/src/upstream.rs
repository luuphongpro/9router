//! Chuyển tiếp passthrough an toàn và minh bạch tới máy chủ thực tế (Google, Copilot, Kiro...).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use futures_util::StreamExt;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, StreamBody};
use hyper::header::HeaderMap;
use hyper::{Method, Response, StatusCode, Uri};
use parking_lot::RwLock;
use reqwest::Client;
use tracing::{debug, info};
use crate::dns::DnsResolver;

pub struct UpstreamClient {
    dns: DnsResolver,
    client: Arc<RwLock<Client>>,
    known_ips: Arc<RwLock<HashMap<String, SocketAddr>>>,
}

impl UpstreamClient {
    pub async fn new(dns: DnsResolver) -> Result<Self> {
        let initial_hosts = [
            "daily-cloudcode-pa.googleapis.com",
            "cloudcode-pa.googleapis.com",
            "api.individual.githubcopilot.com",
            "q.us-east-1.amazonaws.com",
            "codewhisperer.us-east-1.amazonaws.com",
            "runtime.us-east-1.kiro.dev",
            "api2.cursor.sh",
        ];

        let mut known_ips = HashMap::new();
        let mut builder = Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .pool_idle_timeout(std::time::Duration::from_secs(60))
            .pool_max_idle_per_host(32);

        for host in initial_hosts {
            if let Ok(ip) = dns.resolve(host).await {
                let addr = SocketAddr::new(ip, 443);
                debug!("Resolved initial host {} -> {}", host, addr);
                builder = builder.resolve(host, addr);
                known_ips.insert(host.to_string(), addr);
            }
        }

        let client = builder.build()?;
        Ok(Self {
            dns,
            client: Arc::new(RwLock::new(client)),
            known_ips: Arc::new(RwLock::new(known_ips)),
        })
    }

    /// Đảm bảo Client đã ánh xạ địa chỉ IP thực tế của host này
    async fn ensure_host_resolved(&self, host: &str) -> Result<()> {
        let clean_host = host.split(':').next().unwrap_or(host);
        let exists = {
            let ips = self.known_ips.read();
            ips.contains_key(clean_host)
        };

        if !exists {
            let ip = self.dns.resolve(clean_host).await?;
            let addr = SocketAddr::new(ip, 443);
            info!("Adding new DNS mapping for {} -> {}", clean_host, addr);
            
            let mut ips = self.known_ips.write();
            ips.insert(clean_host.to_string(), addr);

            // Xây dựng lại reqwest client với ánh xạ mới
            let mut builder = Client::builder()
                .timeout(std::time::Duration::from_secs(300))
                .pool_idle_timeout(std::time::Duration::from_secs(60))
                .pool_max_idle_per_host(32);

            for (h, a) in ips.iter() {
                builder = builder.resolve(h, *a);
            }

            if let Ok(new_client) = builder.build() {
                let mut c = self.client.write();
                *c = new_client;
            }
        }
        Ok(())
    }

    /// Passthrough request nguyên bản tới máy chủ thật
    pub async fn passthrough(
        &self,
        target_host: &str,
        method: Method,
        uri: &Uri,
        headers: &HeaderMap,
        body: Bytes,
    ) -> Result<Response<BoxBody<Bytes, Box<dyn std::error::Error + Send + Sync>>>> {
        self.ensure_host_resolved(target_host).await?;

        let path_and_query = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
        let target_url = format!("https://{}{}", target_host, path_and_query);

        let client = {
            let c = self.client.read();
            c.clone()
        };

        let mut req_builder = client.request(method, &target_url);

        // Chuyển tiếp các header, ghi đè Host header bằng target_host
        for (name, value) in headers.iter() {
            let key = name.as_str().to_lowercase();
            if key == "host" {
                req_builder = req_builder.header("Host", target_host);
            } else if key != "content-length" {
                if let Ok(val_str) = value.to_str() {
                    req_builder = req_builder.header(name.as_str(), val_str);
                }
            }
        }

        if !body.is_empty() {
            req_builder = req_builder.body(body);
        }

        let upstream_res = req_builder
            .send()
            .await
            .map_err(|e| anyhow!("Passthrough request to {} failed: {}", target_url, e))?;

        let status = StatusCode::from_u16(upstream_res.status().as_u16())
            .unwrap_or(StatusCode::OK);

        let mut res_builder = Response::builder().status(status);

        for (k, v) in upstream_res.headers() {
            res_builder = res_builder.header(k, v);
        }

        let byte_stream = upstream_res.bytes_stream().map(|item| {
            item.map(hyper::body::Frame::data)
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        });

        let stream_body = StreamBody::new(byte_stream);
        Ok(res_builder.body(BodyExt::boxed(stream_body))?)
    }
}
