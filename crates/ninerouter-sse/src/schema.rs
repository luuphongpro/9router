//! Cấu trúc dữ liệu chuẩn cho các event và chunk của OpenAI và Claude.

use serde::{Deserialize, Serialize};

/// Cấu trúc một chunk streaming phản hồi theo chuẩn OpenAI Chat Completion
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ChatCompletionChunk {
    pub id: String,
    pub object: String,
    pub created: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub choices: Vec<ChunkChoice>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<ChunkUsage>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ChunkChoice {
    pub index: u32,
    pub delta: ChunkDelta,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ChunkDelta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCallDelta>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallDelta {
    pub index: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub call_type: Option<String>,
    pub function: FunctionCallDelta,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FunctionCallDelta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arguments: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ChunkUsage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_read_input_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_creation_input_tokens: Option<u32>,
}

/// Thống kê lượng token sử dụng tích lũy trong toàn bộ stream
#[derive(Debug, Clone, Default)]
pub struct AccumulatedUsage {
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub cache_read_input_tokens: Option<u32>,
    pub cache_creation_input_tokens: Option<u32>,
}

impl AccumulatedUsage {
    pub fn to_chunk_usage(&self) -> ChunkUsage {
        let prompt_tokens = self.input_tokens
            + self.cache_read_input_tokens.unwrap_or(0)
            + self.cache_creation_input_tokens.unwrap_or(0);
        let completion_tokens = self.output_tokens;
        let total_tokens = prompt_tokens + completion_tokens;

        ChunkUsage {
            prompt_tokens,
            completion_tokens,
            total_tokens,
            cache_read_input_tokens: self.cache_read_input_tokens,
            cache_creation_input_tokens: self.cache_creation_input_tokens,
        }
    }
}
