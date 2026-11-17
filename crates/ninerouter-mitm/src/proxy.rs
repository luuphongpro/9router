//! Điều phối request: Health check, kiểm tra vòng lặp, phân luồng Intercept và Passthrough.

use std::sync::Arc;
use anyhow::Result;
use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::header::{HeaderMap, HeaderValue, CONTENT_TYPE, USER_AGENT};
use hyper::{Request, Response, StatusCode};
use serde_json::Value;
use tracing::info;

use crate::config::{AppConfig, Tool, ANTIGRAVITY_IDE_VERSION};
use crate::router::RouterForwarder;
use crate::upstream::UpstreamClient;

pub struct ProxyHandler {
    config: AppConfig,
    upstream: Arc<UpstreamClient>,
    router: Arc<RouterForwarder>,
}

impl ProxyHandler {
    pub fn new(config: AppConfig, upstream: Arc<UpstreamClient>, router: Arc<RouterForwarder>) -> Self {
        Self {
            config,
            upstream,
            router,
        }
    }

    /// Xử lý một HTTP request đến từ client IDE
    pub async fn handle(
        &self,
        req: Request<hyper::body::Incoming>,
    ) -> Result<Response<BoxBody<Bytes, Box<dyn std::error::Error + Send + Sync>>>> {
        let (parts, incoming_body) = req.into_parts();
        let path = parts.uri.path();

        // 1. Health check endpoint cho 9Router CLI / Manager
        if path == "/_mitm_health" {
            let pid = std::process::id();
            let body_str = format!("{{\"ok\":true,\"pid\":{}}}", pid);
            let res = Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/json")
                .body(Full::new(Bytes::from(body_str)).map_err(|never| match never {} as Box<dyn std::error::Error + Send + Sync>).boxed())?;
            return Ok(res);
        }

        // Lấy host từ header hoặc URI
        let host = parts.headers.get("host")
            .and_then(|h| h.to_str().ok())
            .unwrap_or_else(|| parts.uri.host().unwrap_or(""));

        // 2. Chống lặp request phát sinh từ chính 9Router
        if parts.headers.get("x-request-source").and_then(|v| v.to_str().ok()) == Some("local") {
            let body_bytes = incoming_body.collect().await?.to_bytes();
            return self.upstream.passthrough(host, parts.method, &parts.uri, &parts.headers, body_bytes).await;
        }

        // 3. Nhận diện công cụ
        let tool = match self.config.get_tool_for_host(host) {
            Some(t) => t,
            None => {
                let body_bytes = incoming_body.collect().await?.to_bytes();
                return self.upstream.passthrough(host, parts.method, &parts.uri, &parts.headers, body_bytes).await;
            }
        };

        let has_amz_target = parts.headers.contains_key("x-amz-target");

        // 4. Kiểm tra xem có phải chat request không
        if !self.config.is_chat_request(tool, path, has_amz_target) {
            let body_bytes = incoming_body.collect().await?.to_bytes();
            return self.upstream.passthrough(host, parts.method, &parts.uri, &parts.headers, body_bytes).await;
        }

        // Đọc toàn bộ body cho chat request
        let body_bytes = incoming_body.collect().await?.to_bytes();

        // 5. Antigravity: Viết lại host nếu cần và điều chỉnh phiên bản IDE
        let (target_host, processed_body, headers) = if tool == Tool::Antigravity {
            let rewritten_host = if host == "cloudcode-pa.googleapis.com" {
                "daily-cloudcode-pa.googleapis.com"
            } else {
                host
            };
            let (new_body, new_headers) = self.apply_antigravity_version_override(body_bytes.clone(), &parts.headers);
            (rewritten_host, new_body, new_headers)
        } else {
            (host, body_bytes.clone(), parts.headers.clone())
        };

        // 6. Trích xuất model từ request
        let extracted_model = self.extract_model(tool, path, &processed_body);

        // Kiểm tra model loại trừ (ví dụ inline tab completion)
        if let Some(ref model) = extracted_model {
            if self.config.is_model_excluded(tool, model) {
                return self.upstream.passthrough(target_host, parts.method, &parts.uri, &headers, processed_body).await;
            }
        }

        // 7. Kiểm tra ánh xạ model (Alias)
        let mapped_model = extracted_model.as_deref().and_then(|m| self.config.get_mapped_model(tool, m));

        match mapped_model {
            Some(target_model) => {
                info!("Intercepting {} chat request: model '{}' -> '{}'", tool.as_str(), extracted_model.unwrap_or_default(), target_model);
                let final_body = self.replace_model_in_body(tool, &processed_body, &target_model);
                let router_path = match tool {
                    Tool::Copilot => {
                        if path.contains("/chat/completions") { "/v1/chat/completions" }
                        else if path.contains("/v1/messages") { "/v1/messages" }
                        else if path.contains("/responses") { "/v1/responses" }
                        else { "/v1/chat/completions" }
                    },
                    _ => "/v1/chat/completions",
                };
                self.router.forward(router_path, &headers, final_body).await
            }
            None => {
                // Không có alias cấu hình -> passthrough trực tiếp tới upstream
                self.upstream.passthrough(target_host, parts.method, &parts.uri, &headers, processed_body).await
            }
        }
    }

    /// Trích xuất tên model từ path hoặc body JSON
    fn extract_model(&self, _tool: Tool, path: &str, body: &[u8]) -> Option<String> {
        // Trích xuất từ URL dạng .../models/{model}:...
        if let Some(idx) = path.find("/models/") {
            let rest = &path[idx + 8..];
            let end = rest.find(':').unwrap_or_else(|| rest.find('?').unwrap_or(rest.len()));
            let m = &rest[..end];
            if !m.is_empty() {
                return Some(m.to_string());
            }
        }

        // Trích xuất từ JSON body
        if let Ok(json) = serde_json::from_slice::<Value>(body) {
            if let Some(m) = json.get("model").and_then(|v| v.as_str()) {
                return Some(m.to_string());
            }
        }
        None
    }

    /// Thay thế model trong JSON body trước khi chuyển sang 9Router
    fn replace_model_in_body(&self, _tool: Tool, body: &[u8], target_model: &str) -> Bytes {
        if let Ok(mut json) = serde_json::from_slice::<Value>(body) {
            if json.is_object() {
                json["model"] = Value::String(target_model.to_string());
                if let Ok(serialized) = serde_json::to_vec(&json) {
                    return Bytes::from(serialized);
                }
            }
        }
        Bytes::copy_from_slice(body)
    }

    /// Ghi đè phiên bản Antigravity IDE trong header và metadata
    fn apply_antigravity_version_override(&self, body: Bytes, headers: &HeaderMap) -> (Bytes, HeaderMap) {
        let mut new_headers = headers.clone();

        // Cập nhật User-Agent
        if let Some(ua) = headers.get(USER_AGENT).and_then(|v| v.to_str().ok()) {
            if ua.contains("antigravity/") {
                let re = regex::Regex::new(r"antigravity/[^\s]+").unwrap();
                let new_ua = re.replace(ua, format!("antigravity/{}", ANTIGRAVITY_IDE_VERSION));
                if let Ok(hv) = HeaderValue::from_str(&new_ua) {
                    new_headers.insert(USER_AGENT, hv);
                }
            }
        }

        // Cập nhật metadata.ideVersion trong body JSON
        if let Ok(mut json) = serde_json::from_slice::<Value>(&body) {
            if let Some(metadata) = json.get_mut("metadata").and_then(|m| m.as_object_mut()) {
                let should_rewrite = metadata.get("ideName").and_then(|v| v.as_str()).map(|s| s.eq_ignore_ascii_case("antigravity")).unwrap_or(false)
                    || metadata.contains_key("ideVersion");
                if should_rewrite {
                    metadata.insert("ideVersion".to_string(), Value::String(ANTIGRAVITY_IDE_VERSION.to_string()));
                    if let Ok(serialized) = serde_json::to_vec(&json) {
                        return (Bytes::from(serialized), new_headers);
                    }
                }
            }
        }

        (body, new_headers)
    }
}
