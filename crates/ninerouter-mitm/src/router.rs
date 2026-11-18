//! Chuyển tiếp request đã can thiệp tới 9Router server và truyền stream phản hồi.

use std::collections::HashSet;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use futures_util::StreamExt;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, StreamBody};
use hyper::header::HeaderMap;
use hyper::{Response, StatusCode};
use reqwest::Client;
use crate::config::AppConfig;
use ninerouter_sse::format::Format;
use ninerouter_sse::pipeline::SseStreamPipeline;
use ninerouter_sse::translators::create_translator;

pub struct RouterForwarder {
    client: Client,
    config: AppConfig,
}

impl RouterForwarder {
    pub fn new(config: AppConfig) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .build()
            .unwrap_or_default();
        Self { client, config }
    }

    /// Gửi request đã chỉnh sửa tới 9Router và trả về streaming HTTP Response
    pub async fn forward(
        &self,
        path: &str,
        client_headers: &HeaderMap,
        body: Bytes,
    ) -> Result<Response<BoxBody<Bytes, Box<dyn std::error::Error + Send + Sync>>>> {
        let strip_headers: HashSet<&'static str> = [
            "host", "content-length", "connection", "transfer-encoding",
            "content-type", "authorization"
        ].into_iter().collect();

        let target_url = format!("{}{}", self.config.router_base, path);
        let mut req_builder = self.client.post(&target_url);

        // Chuyển tiếp header từ client (loại bỏ các header hop-by-hop)
        for (name, value) in client_headers.iter() {
            let key = name.as_str().to_lowercase();
            if !strip_headers.contains(key.as_str()) {
                if let Ok(val_str) = value.to_str() {
                    req_builder = req_builder.header(name.as_str(), val_str);
                }
            }
        }

        req_builder = req_builder.header("Content-Type", "application/json");
        if let Some(ref api_key) = self.config.router_api_key {
            req_builder = req_builder.header("Authorization", format!("Bearer {}", api_key));
        }

        let upstream_res = req_builder
            .body(body)
            .send()
            .await
            .map_err(|e| anyhow!("Failed to send request to 9Router at {}: {}", target_url, e))?;

        let status = StatusCode::from_u16(upstream_res.status().as_u16())
            .unwrap_or(StatusCode::OK);

        let mut res_builder = Response::builder().status(status);

        // Sao chép headers trả về từ 9Router
        for (k, v) in upstream_res.headers() {
            res_builder = res_builder.header(k, v);
        }

        let is_sse = upstream_res
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|ct| ct.contains("text/event-stream"))
            .unwrap_or(false);

        // Tạo body stream chuyển tiếp về client (sử dụng Rust SSE pipeline nếu bật và là stream)
        let boxed_body = if self.config.enable_rust_streamer && is_sse {
            tracing::info!("Using Rust zero-copy SSE pipeline for streaming response (path: {})", path);
            let raw_byte_stream = upstream_res.bytes_stream().map(|item| {
                item.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            });

            // Sử dụng translator OpenAI passthrough để chuẩn hoá chunk và tự động fallback nếu gặp lỗi format lạ
            let translator = create_translator(Format::Openai, Format::Openai);
            let sse_pipeline = SseStreamPipeline::new(raw_byte_stream, translator);

            let frame_stream = sse_pipeline.map(|item| {
                item.map(hyper::body::Frame::data)
            });

            BodyExt::boxed(StreamBody::new(frame_stream))
        } else {
            let byte_stream = upstream_res.bytes_stream().map(|item| {
                item.map(hyper::body::Frame::data)
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            });
            BodyExt::boxed(StreamBody::new(byte_stream))
        };

        Ok(res_builder.body(boxed_body)?)
    }
}
