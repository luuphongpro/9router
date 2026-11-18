//! Bộ chuyển đổi stream từ Claude SSE sang OpenAI Chat Completion Chunk.

use crate::parser::SseEvent;
use crate::schema::{
    AccumulatedUsage, ChatCompletionChunk, ChunkChoice, ChunkDelta, FunctionCallDelta,
    ToolCallDelta,
};
use crate::translators::StreamTranslator;
use serde_json::Value;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Trạng thái theo dõi một tool call đang được stream trong Claude
#[allow(dead_code)]
struct ActiveToolCall {
    tool_call_index: u32,
    id: String,
    name: String,
}

/// Bộ chuyển đổi từ định dạng Claude SSE sang OpenAI Chat Completion Chunk
pub struct ClaudeToOpenAiTranslator {
    message_id: String,
    model: Option<String>,
    created_ts: u64,
    tool_call_counter: u32,
    active_tool_calls: HashMap<u32, ActiveToolCall>,
    server_tool_block_index: Option<u32>,
    usage: AccumulatedUsage,
    finish_reason: Option<String>,
    finish_reason_sent: bool,
}

impl Default for ClaudeToOpenAiTranslator {
    fn default() -> Self {
        Self::new()
    }
}

impl ClaudeToOpenAiTranslator {
    pub fn new() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Self {
            message_id: format!("chatcmpl-{}", now),
            model: None,
            created_ts: now,
            tool_call_counter: 0,
            active_tool_calls: HashMap::new(),
            server_tool_block_index: None,
            usage: AccumulatedUsage::default(),
            finish_reason: None,
            finish_reason_sent: false,
        }
    }

    /// Tạo một OpenAI chunk với thông tin id và model hiện tại
    fn build_chunk(&self, delta: ChunkDelta, finish_reason: Option<String>) -> ChatCompletionChunk {
        ChatCompletionChunk {
            id: self.message_id.clone(),
            object: "chat.completion.chunk".to_string(),
            created: self.created_ts,
            model: self.model.clone(),
            choices: vec![ChunkChoice {
                index: 0,
                delta,
                finish_reason,
            }],
            usage: None,
        }
    }

    /// Chuyển đổi mã stop_reason của Anthropic sang finish_reason của OpenAI
    fn convert_stop_reason(reason: &str) -> &'static str {
        match reason {
            "end_turn" | "stop_sequence" => "stop",
            "tool_use" => "tool_calls",
            "max_tokens" => "length",
            _ => "stop",
        }
    }
}

impl StreamTranslator for ClaudeToOpenAiTranslator {
    fn translate_event(&mut self, event: &SseEvent) -> Vec<SseEvent> {
        if event.is_done {
            return vec![SseEvent::new_data("[DONE]")];
        }

        let parsed: Value = match serde_json::from_slice(&event.data) {
            Ok(v) => v,
            Err(_) => return Vec::new(),
        };

        let event_type = parsed
            .get("type")
            .and_then(|t| t.as_str())
            .or_else(|| event.event.as_deref())
            .unwrap_or("");

        let mut out_events = Vec::new();

        match event_type {
            "message_start" => {
                if let Some(msg) = parsed.get("message") {
                    if let Some(id) = msg.get("id").and_then(|v| v.as_str()) {
                        self.message_id = format!("chatcmpl-{}", id);
                    }
                    if let Some(m) = msg.get("model").and_then(|v| v.as_str()) {
                        self.model = Some(m.to_string());
                    }
                    if let Some(u) = msg.get("usage") {
                        if let Some(inp) = u.get("input_tokens").and_then(|v| v.as_u64()) {
                            self.usage.input_tokens = inp as u32;
                        }
                        if let Some(cr) = u.get("cache_read_input_tokens").and_then(|v| v.as_u64()) {
                            self.usage.cache_read_input_tokens = Some(cr as u32);
                        }
                        if let Some(cc) = u.get("cache_creation_input_tokens").and_then(|v| v.as_u64()) {
                            self.usage.cache_creation_input_tokens = Some(cc as u32);
                        }
                    }
                }

                // Phát sinh chunk đầu tiên khai báo vai trò assistant
                let chunk = self.build_chunk(
                    ChunkDelta {
                        role: Some("assistant".to_string()),
                        ..Default::default()
                    },
                    None,
                );
                if let Ok(json_str) = serde_json::to_string(&chunk) {
                    out_events.push(SseEvent::new_data(json_str));
                }
            }

            "content_block_start" => {
                let block_index = parsed.get("index").and_then(|v| v.as_u64()).map(|v| v as u32);
                let content_block = parsed.get("content_block");

                if let Some(cb) = content_block {
                    let block_type = cb.get("type").and_then(|v| v.as_str()).unwrap_or("");
                    if block_type == "server_tool_use" {
                        self.server_tool_block_index = block_index;
                        return out_events;
                    }

                    if block_type == "tool_use" {
                        let id = cb.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let name = cb.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let current_tool_idx = self.tool_call_counter;
                        self.tool_call_counter += 1;

                        if let Some(idx) = block_index {
                            self.active_tool_calls.insert(
                                idx,
                                ActiveToolCall {
                                    tool_call_index: current_tool_idx,
                                    id: id.clone(),
                                    name: name.clone(),
                                },
                            );
                        }

                        let chunk = self.build_chunk(
                            ChunkDelta {
                                tool_calls: Some(vec![ToolCallDelta {
                                    index: current_tool_idx,
                                    id: Some(id),
                                    call_type: Some("function".to_string()),
                                    function: FunctionCallDelta {
                                        name: Some(name),
                                        arguments: Some(String::new()),
                                    },
                                }]),
                                ..Default::default()
                            },
                            None,
                        );
                        if let Ok(json_str) = serde_json::to_string(&chunk) {
                            out_events.push(SseEvent::new_data(json_str));
                        }
                    }
                }
            }

            "content_block_delta" => {
                let block_index = parsed.get("index").and_then(|v| v.as_u64()).map(|v| v as u32);
                if block_index == self.server_tool_block_index && self.server_tool_block_index.is_some() {
                    return out_events;
                }

                if let Some(delta) = parsed.get("delta") {
                    let delta_type = delta.get("type").and_then(|v| v.as_str()).unwrap_or("");
                    match delta_type {
                        "text_delta" => {
                            if let Some(text) = delta.get("text").and_then(|v| v.as_str()) {
                                if !text.is_empty() {
                                    let chunk = self.build_chunk(
                                        ChunkDelta {
                                            content: Some(text.to_string()),
                                            ..Default::default()
                                        },
                                        None,
                                    );
                                    if let Ok(json_str) = serde_json::to_string(&chunk) {
                                        out_events.push(SseEvent::new_data(json_str));
                                    }
                                }
                            }
                        }
                        "thinking_delta" => {
                            if let Some(thinking) = delta.get("thinking").and_then(|v| v.as_str()) {
                                if !thinking.is_empty() {
                                    let chunk = self.build_chunk(
                                        ChunkDelta {
                                            reasoning_content: Some(thinking.to_string()),
                                            ..Default::default()
                                        },
                                        None,
                                    );
                                    if let Ok(json_str) = serde_json::to_string(&chunk) {
                                        out_events.push(SseEvent::new_data(json_str));
                                    }
                                }
                            }
                        }
                        "input_json_delta" => {
                            if let Some(partial_json) = delta.get("partial_json").and_then(|v| v.as_str()) {
                                if let Some(idx) = block_index {
                                    if let Some(tool) = self.active_tool_calls.get(&idx) {
                                        let chunk = self.build_chunk(
                                            ChunkDelta {
                                                tool_calls: Some(vec![ToolCallDelta {
                                                    index: tool.tool_call_index,
                                                    id: Some(tool.id.clone()),
                                                    call_type: None,
                                                    function: FunctionCallDelta {
                                                        name: None,
                                                        arguments: Some(partial_json.to_string()),
                                                    },
                                                }]),
                                                ..Default::default()
                                            },
                                            None,
                                        );
                                        if let Ok(json_str) = serde_json::to_string(&chunk) {
                                            out_events.push(SseEvent::new_data(json_str));
                                        }
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }

            "content_block_stop" => {
                let block_index = parsed.get("index").and_then(|v| v.as_u64()).map(|v| v as u32);
                if block_index == self.server_tool_block_index {
                    self.server_tool_block_index = None;
                }
            }

            "message_delta" => {
                if let Some(u) = parsed.get("usage") {
                    if let Some(out) = u.get("output_tokens").and_then(|v| v.as_u64()) {
                        self.usage.output_tokens = out as u32;
                    }
                }

                if let Some(delta) = parsed.get("delta") {
                    if let Some(stop_reason) = delta.get("stop_reason").and_then(|v| v.as_str()) {
                        let mapped_finish = Self::convert_stop_reason(stop_reason).to_string();
                        self.finish_reason = Some(mapped_finish.clone());

                        let mut chunk = self.build_chunk(ChunkDelta::default(), Some(mapped_finish));
                        chunk.usage = Some(self.usage.to_chunk_usage());

                        if let Ok(json_str) = serde_json::to_string(&chunk) {
                            out_events.push(SseEvent::new_data(json_str));
                        }
                        self.finish_reason_sent = true;
                    }
                }
            }

            "message_stop" => {
                if !self.finish_reason_sent {
                    let finish = self.finish_reason.clone().unwrap_or_else(|| {
                        if !self.active_tool_calls.is_empty() {
                            "tool_calls".to_string()
                        } else {
                            "stop".to_string()
                        }
                    });

                    let mut chunk = self.build_chunk(ChunkDelta::default(), Some(finish));
                    chunk.usage = Some(self.usage.to_chunk_usage());

                    if let Ok(json_str) = serde_json::to_string(&chunk) {
                        out_events.push(SseEvent::new_data(json_str));
                    }
                    self.finish_reason_sent = true;
                }
                out_events.push(SseEvent::new_data("[DONE]"));
            }

            _ => {}
        }

        out_events
    }

    fn flush(&mut self) -> Vec<SseEvent> {
        let mut out = Vec::new();
        if !self.finish_reason_sent {
            let mut chunk = self.build_chunk(ChunkDelta::default(), Some("stop".to_string()));
            chunk.usage = Some(self.usage.to_chunk_usage());
            if let Ok(json_str) = serde_json::to_string(&chunk) {
                out.push(SseEvent::new_data(json_str));
            }
            self.finish_reason_sent = true;
        }
        out.push(SseEvent::new_data("[DONE]"));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_translate_claude_text_stream() {
        let mut trans = ClaudeToOpenAiTranslator::new();

        // 1. message_start
        let ev1 = SseEvent::new_data(
            r#"{"type":"message_start","message":{"id":"msg_123","model":"claude-3-5-sonnet","usage":{"input_tokens":15}}}"#,
        );
        let res1 = trans.translate_event(&ev1);
        assert_eq!(res1.len(), 1);
        let chunk1: ChatCompletionChunk = serde_json::from_slice(&res1[0].data).unwrap();
        assert_eq!(chunk1.choices[0].delta.role.as_deref(), Some("assistant"));

        // 2. content_block_delta
        let ev2 = SseEvent::new_data(
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Xin chào!"}}"#,
        );
        let res2 = trans.translate_event(&ev2);
        assert_eq!(res2.len(), 1);
        let chunk2: ChatCompletionChunk = serde_json::from_slice(&res2[0].data).unwrap();
        assert_eq!(chunk2.choices[0].delta.content.as_deref(), Some("Xin chào!"));

        // 3. message_delta
        let ev3 = SseEvent::new_data(
            r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":8}}"#,
        );
        let res3 = trans.translate_event(&ev3);
        assert_eq!(res3.len(), 1);
        let chunk3: ChatCompletionChunk = serde_json::from_slice(&res3[0].data).unwrap();
        assert_eq!(chunk3.choices[0].finish_reason.as_deref(), Some("stop"));
        let usage = chunk3.usage.unwrap();
        assert_eq!(usage.prompt_tokens, 15);
        assert_eq!(usage.completion_tokens, 8);
        assert_eq!(usage.total_tokens, 23);

        // 4. message_stop
        let ev4 = SseEvent::new_data(r#"{"type":"message_stop"}"#);
        let res4 = trans.translate_event(&ev4);
        assert_eq!(res4.len(), 1);
        assert!(res4[0].is_done);
    }
}
