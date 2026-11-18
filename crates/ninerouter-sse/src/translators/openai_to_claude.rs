//! Bộ chuyển đổi stream từ OpenAI Chat Completion Chunk sang Claude SSE events.

use crate::parser::SseEvent;
use crate::translators::StreamTranslator;
use serde_json::{json, Value};
use std::time::{SystemTime, UNIX_EPOCH};

/// Bộ chuyển đổi từ định dạng OpenAI Chunk sang Claude SSE
pub struct OpenAiToClaudeTranslator {
    message_id: String,
    model: Option<String>,
    started: bool,
    block_index: u32,
    finish_sent: bool,
    input_tokens: u32,
    output_tokens: u32,
}

impl Default for OpenAiToClaudeTranslator {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenAiToClaudeTranslator {
    pub fn new() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Self {
            message_id: format!("msg_{}", now),
            model: None,
            started: false,
            block_index: 0,
            finish_sent: false,
            input_tokens: 0,
            output_tokens: 0,
        }
    }

    /// Chuyển đổi finish_reason OpenAI sang stop_reason của Anthropic
    fn map_finish_reason(finish: &str) -> &'static str {
        match finish {
            "stop" => "end_turn",
            "tool_calls" => "tool_use",
            "length" => "max_tokens",
            _ => "end_turn",
        }
    }
}

impl StreamTranslator for OpenAiToClaudeTranslator {
    fn translate_event(&mut self, event: &SseEvent) -> Vec<SseEvent> {
        if event.is_done {
            return self.flush();
        }

        let parsed: Value = match serde_json::from_slice(&event.data) {
            Ok(v) => v,
            Err(_) => return Vec::new(),
        };

        let mut out_events = Vec::new();

        if let Some(id) = parsed.get("id").and_then(|v| v.as_str()) {
            if !self.started {
                self.message_id = format!("msg_{}", id.trim_start_matches("chatcmpl-"));
            }
        }
        if let Some(m) = parsed.get("model").and_then(|v| v.as_str()) {
            if self.model.is_none() {
                self.model = Some(m.to_string());
            }
        }

        // 1. Gửi message_start trước khi có nội dung đầu tiên
        if !self.started {
            self.started = true;
            let start_event = json!({
                "type": "message_start",
                "message": {
                    "id": self.message_id,
                    "type": "message",
                    "role": "assistant",
                    "model": self.model.as_deref().unwrap_or("claude-3-5-sonnet"),
                    "content": [],
                    "stop_reason": null,
                    "stop_sequence": null,
                    "usage": {
                        "input_tokens": self.input_tokens,
                        "output_tokens": 0
                    }
                }
            });
            let mut ev = SseEvent::new_data(serde_json::to_string(&start_event).unwrap_or_default());
            ev.event = Some("message_start".to_string());
            out_events.push(ev);

            // Mở text content block mặc định
            let block_start = json!({
                "type": "content_block_start",
                "index": self.block_index,
                "content_block": {
                    "type": "text",
                    "text": ""
                }
            });
            let mut ev = SseEvent::new_data(serde_json::to_string(&block_start).unwrap_or_default());
            ev.event = Some("content_block_start".to_string());
            out_events.push(ev);
        }

        // 2. Xử lý delta text & reasoning
        if let Some(choices) = parsed.get("choices").and_then(|c| c.as_array()) {
            for choice in choices {
                if let Some(delta) = choice.get("delta") {
                    // Xử lý text delta
                    if let Some(text) = delta.get("content").and_then(|c| c.as_str()) {
                        if !text.is_empty() {
                            self.output_tokens += 1;
                            let delta_ev = json!({
                                "type": "content_block_delta",
                                "index": self.block_index,
                                "delta": {
                                    "type": "text_delta",
                                    "text": text
                                }
                            });
                            let mut ev = SseEvent::new_data(serde_json::to_string(&delta_ev).unwrap_or_default());
                            ev.event = Some("content_block_delta".to_string());
                            out_events.push(ev);
                        }
                    }

                    // Xử lý reasoning delta
                    if let Some(thinking) = delta.get("reasoning_content").and_then(|c| c.as_str()) {
                        if !thinking.is_empty() {
                            let delta_ev = json!({
                                "type": "content_block_delta",
                                "index": self.block_index,
                                "delta": {
                                    "type": "thinking_delta",
                                    "thinking": thinking
                                }
                            });
                            let mut ev = SseEvent::new_data(serde_json::to_string(&delta_ev).unwrap_or_default());
                            ev.event = Some("content_block_delta".to_string());
                            out_events.push(ev);
                        }
                    }
                }

                // Xử lý kết thúc finish_reason
                if let Some(finish) = choice.get("finish_reason").and_then(|f| f.as_str()) {
                    let mapped_stop = Self::map_finish_reason(finish);

                    // Đóng text content block
                    let block_stop = json!({
                        "type": "content_block_stop",
                        "index": self.block_index
                    });
                    let mut ev = SseEvent::new_data(serde_json::to_string(&block_stop).unwrap_or_default());
                    ev.event = Some("content_block_stop".to_string());
                    out_events.push(ev);

                    // message_delta
                    let msg_delta = json!({
                        "type": "message_delta",
                        "delta": {
                            "stop_reason": mapped_stop,
                            "stop_sequence": null
                        },
                        "usage": {
                            "output_tokens": self.output_tokens
                        }
                    });
                    let mut ev = SseEvent::new_data(serde_json::to_string(&msg_delta).unwrap_or_default());
                    ev.event = Some("message_delta".to_string());
                    out_events.push(ev);

                    // message_stop
                    let msg_stop = json!({ "type": "message_stop" });
                    let mut ev = SseEvent::new_data(serde_json::to_string(&msg_stop).unwrap_or_default());
                    ev.event = Some("message_stop".to_string());
                    out_events.push(ev);

                    self.finish_sent = true;
                }
            }
        }

        // Cập nhật usage từ OpenAI chunk nếu có
        if let Some(u) = parsed.get("usage") {
            if let Some(p) = u.get("prompt_tokens").and_then(|v| v.as_u64()) {
                self.input_tokens = p as u32;
            }
            if let Some(c) = u.get("completion_tokens").and_then(|v| v.as_u64()) {
                self.output_tokens = c as u32;
            }
        }

        out_events
    }

    fn flush(&mut self) -> Vec<SseEvent> {
        let mut out = Vec::new();
        if !self.finish_sent {
            self.finish_sent = true;
            let block_stop = json!({
                "type": "content_block_stop",
                "index": self.block_index
            });
            let mut ev = SseEvent::new_data(serde_json::to_string(&block_stop).unwrap_or_default());
            ev.event = Some("content_block_stop".to_string());
            out.push(ev);

            let msg_delta = json!({
                "type": "message_delta",
                "delta": {
                    "stop_reason": "end_turn",
                    "stop_sequence": null
                },
                "usage": {
                    "output_tokens": self.output_tokens
                }
            });
            let mut ev = SseEvent::new_data(serde_json::to_string(&msg_delta).unwrap_or_default());
            ev.event = Some("message_delta".to_string());
            out.push(ev);

            let msg_stop = json!({ "type": "message_stop" });
            let mut ev = SseEvent::new_data(serde_json::to_string(&msg_stop).unwrap_or_default());
            ev.event = Some("message_stop".to_string());
            out.push(ev);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_translate_openai_to_claude() {
        let mut trans = OpenAiToClaudeTranslator::new();

        // 1. Chunk đầu tiên với content
        let ev1 = SseEvent::new_data(r#"{"id":"chatcmpl-456","model":"gpt-4o","choices":[{"index":0,"delta":{"content":"Chào bạn!"}}]}"#);
        let res1 = trans.translate_event(&ev1);
        // Mong đợi: message_start, content_block_start, content_block_delta
        assert_eq!(res1.len(), 3);
        assert_eq!(res1[0].event.as_deref(), Some("message_start"));
        assert_eq!(res1[1].event.as_deref(), Some("content_block_start"));
        assert_eq!(res1[2].event.as_deref(), Some("content_block_delta"));

        // 2. Chunk kết thúc
        let ev2 = SseEvent::new_data(r#"{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5}}"#);
        let res2 = trans.translate_event(&ev2);
        // Mong đợi: content_block_stop, message_delta, message_stop
        assert_eq!(res2.len(), 3);
        assert_eq!(res2[0].event.as_deref(), Some("content_block_stop"));
        assert_eq!(res2[1].event.as_deref(), Some("message_delta"));
        assert_eq!(res2[2].event.as_deref(), Some("message_stop"));
    }
}
