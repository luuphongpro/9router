//! Bộ chuẩn hoá và passthrough cho OpenAI Chat Completion chunks.

use crate::parser::SseEvent;
use crate::schema::AccumulatedUsage;
use crate::translators::StreamTranslator;
use serde_json::{json, Value};
use std::time::{SystemTime, UNIX_EPOCH};

/// Bộ chuẩn hoá stream cho định dạng OpenAI nhằm đảm bảo tính tương thích với các AI SDK
pub struct OpenAiPassthroughTranslator {
    default_id: String,
    created_ts: u64,
    usage: AccumulatedUsage,
    stream_done_sent: bool,
}

impl Default for OpenAiPassthroughTranslator {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenAiPassthroughTranslator {
    pub fn new() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Self {
            default_id: format!("chatcmpl-{}", now),
            created_ts: now,
            usage: AccumulatedUsage::default(),
            stream_done_sent: false,
        }
    }
}

impl StreamTranslator for OpenAiPassthroughTranslator {
    fn translate_event(&mut self, event: &SseEvent) -> Vec<SseEvent> {
        if event.is_done {
            self.stream_done_sent = true;
            return vec![SseEvent::new_data("[DONE]")];
        }

        let mut parsed: Value = match serde_json::from_slice(&event.data) {
            Ok(v) => v,
            Err(e) => {
                // Tự động fallback chuyển tiếp nguyên bản nếu không parse được JSON và ghi log error
                tracing::error!(
                    "Failed to parse OpenAI SSE JSON payload (len: {}): {}. Falling back to raw passthrough for this event. Payload: '{}'",
                    event.data.len(),
                    e,
                    String::from_utf8_lossy(&event.data).chars().take(100).collect::<String>()
                );
                return vec![event.clone()];
            }
        };

        if let Some(obj) = parsed.as_object_mut() {
            // Đảm bảo có trường id hợp lệ
            if !obj.contains_key("id") || obj.get("id").and_then(|v| v.as_str()).map_or(true, |s| s.is_empty()) {
                obj.insert("id".to_string(), json!(self.default_id));
            }

            // Đảm bảo có trường object chuẩn
            if !obj.contains_key("object") {
                obj.insert("object".to_string(), json!("chat.completion.chunk"));
            }

            // Đảm bảo trường created timestamp
            if !obj.contains_key("created") {
                obj.insert("created".to_string(), json!(self.created_ts));
            }

            // Xử lý choices và delta
            if let Some(choices) = obj.get_mut("choices").and_then(|c| c.as_array_mut()) {
                for choice in choices {
                    if let Some(delta) = choice.get_mut("delta").and_then(|d| d.as_object_mut()) {
                        // Nâng các alias suy luận (reasoning / reasoning_details) thành reasoning_content
                        if !delta.contains_key("reasoning_content") {
                            if let Some(reasoning) = delta.get("reasoning").cloned() {
                                delta.insert("reasoning_content".to_string(), reasoning);
                            }
                        }

                        // Lọc bỏ mảng rỗng tool_calls: [] gây lỗi kết thúc suy luận sớm
                        if let Some(tc) = delta.get("tool_calls") {
                            if let Some(arr) = tc.as_array() {
                                if arr.is_empty() {
                                    delta.remove("tool_calls");
                                }
                            }
                        }
                    }
                }
            }

            // Cập nhật thống kê usage nếu có
            if let Some(u) = obj.get("usage") {
                if let Some(p) = u.get("prompt_tokens").and_then(|v| v.as_u64()) {
                    self.usage.input_tokens = p as u32;
                }
                if let Some(c) = u.get("completion_tokens").and_then(|v| v.as_u64()) {
                    self.usage.output_tokens = c as u32;
                }
            }
        }

        if let Ok(json_str) = serde_json::to_string(&parsed) {
            vec![SseEvent::new_data(json_str)]
        } else {
            vec![event.clone()]
        }
    }

    fn flush(&mut self) -> Vec<SseEvent> {
        if !self.stream_done_sent {
            self.stream_done_sent = true;
            vec![SseEvent::new_data("[DONE]")]
        } else {
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_passthrough_normalization() {
        let mut trans = OpenAiPassthroughTranslator::new();
        // Chunk thiếu object và id, có reasoning alias
        let input_ev = SseEvent::new_data(r#"{"choices":[{"index":0,"delta":{"reasoning":"Đang suy nghĩ..."}}]}"#);
        let out = trans.translate_event(&input_ev);
        assert_eq!(out.len(), 1);

        let parsed: Value = serde_json::from_slice(&out[0].data).unwrap();
        assert_eq!(parsed.get("object").unwrap().as_str().unwrap(), "chat.completion.chunk");
        assert!(parsed.get("id").is_some());
        assert_eq!(
            parsed["choices"][0]["delta"]["reasoning_content"].as_str().unwrap(),
            "Đang suy nghĩ..."
        );
    }
}
