//! 9Router MITM Server — Viết bằng Rust với Tokio, Hyper và Rustls.
//! Giải quyết triệt để rò rỉ bộ nhớ native, footprint siêu nhẹ (~15-20MB RAM).

mod cert;
mod config;
mod dns;
mod proxy;
mod router;
mod upstream;

use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use anyhow::{anyhow, Result};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use rustls::ServerConfig;
use tokio::net::TcpListener;
use tokio::signal;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

use crate::cert::DynamicCertResolver;
use crate::config::AppConfig;
use crate::dns::DnsResolver;
use crate::proxy::ProxyHandler;
use crate::router::RouterForwarder;
use crate::upstream::UpstreamClient;

#[tokio::main]
async fn main() -> Result<()> {
    // Khởi tạo hệ thống logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,ninerouter_mitm=debug")),
        )
        .init();

    // Chọn CryptoProvider mặc định cho rustls
    let _ = rustls::crypto::ring::default_provider().install_default();

    info!("Starting 9Router MITM native Rust proxy server...");

    let config = AppConfig::load();
    let pid = std::process::id();
    info!("Process PID: {}, Target Port: {}", pid, config.port);

    // Ghi file PID để manager.js và CLI có thể theo dõi tiến trình
    let pid_file = config.mitm_dir.join(".mitm.pid");
    if let Err(e) = fs::write(&pid_file, pid.to_string()) {
        warn!("Failed to write PID file {:?}: {}", pid_file, e);
    } else {
        info!("PID file written to {:?}", pid_file);
    }

    // Khởi tạo bộ giải quyết chứng chỉ động với Root CA
    let cert_resolver = Arc::new(DynamicCertResolver::new(&config.mitm_dir)?);

    // Cấu hình Rustls server với ALPN HTTP/2 và HTTP/1.1
    let mut server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_cert_resolver(cert_resolver);

    server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let tls_acceptor = TlsAcceptor::from(Arc::new(server_config));

    // Khởi tạo DNS resolver độc lập qua 8.8.8.8
    let dns_resolver = DnsResolver::new()?;

    // Khởi tạo Upstream client cho các request passthrough
    let upstream_client = Arc::new(UpstreamClient::new(dns_resolver).await?);

    // Khởi tạo Router forwarder cho các request can thiệp
    let router_forwarder = Arc::new(RouterForwarder::new(config.clone()));

    // Khởi tạo Proxy handler
    let proxy_handler = Arc::new(ProxyHandler::new(
        config.clone(),
        upstream_client,
        router_forwarder,
    ));

    // Lắng nghe TCP trên cổng được chỉ định (mặc định 443)
    let bind_addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    let listener = TcpListener::bind(bind_addr).await
        .map_err(|e| anyhow!("Failed to bind port {}: {}", config.port, e))?;

    info!("🚀 9Router MITM Server listening on https://0.0.0.0:{}", config.port);
    info!("Router upstream base: {}", config.router_base);

    let pid_file_clone = pid_file.clone();
    let auto_builder = auto::Builder::new(TokioExecutor::new());

    tokio::select! {
        _ = async {
            loop {
                let (tcp_stream, remote_addr) = match listener.accept().await {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!("TCP accept error: {}", e);
                        continue;
                    }
                };

                let tls_acceptor = tls_acceptor.clone();
                let handler = proxy_handler.clone();
                let auto_builder = auto_builder.clone();

                tokio::spawn(async move {
                    let tls_stream = match tls_acceptor.accept(tcp_stream).await {
                        Ok(s) => s,
                        Err(e) => {
                            debug!("TLS handshake failed with {}: {}", remote_addr, e);
                            return;
                        }
                    };

                    let io = TokioIo::new(tls_stream);
                    let service = service_fn(move |req| {
                        let h = handler.clone();
                        async move { h.handle(req).await }
                    });

                    if let Err(e) = auto_builder.serve_connection_with_upgrades(io, service).await {
                        debug!("HTTP connection error with {}: {}", remote_addr, e);
                    }
                });
            }
        } => {}
        _ = shutdown_signal() => {
            info!("Shutdown signal received, terminating server...");
        }
    }

    // Xoá file PID khi dừng
    let _ = fs::remove_file(pid_file_clone);
    info!("Server shutdown cleanly.");
    Ok(())
}

/// Lắng nghe tín hiệu dừng chương trình (SIGINT, SIGTERM)
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
