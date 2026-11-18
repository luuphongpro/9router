//! Định danh các định dạng LLM model và protocol được hỗ trợ bởi 9Router.

use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Format {
    Openai,
    #[serde(rename = "openai-responses")]
    OpenaiResponses,
    Claude,
    Gemini,
    #[serde(rename = "gemini-cli")]
    GeminiCli,
    Antigravity,
    Kiro,
    Cursor,
    Ollama,
}

impl Format {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Openai => "openai",
            Self::OpenaiResponses => "openai-responses",
            Self::Claude => "claude",
            Self::Gemini => "gemini",
            Self::GeminiCli => "gemini-cli",
            Self::Antigravity => "antigravity",
            Self::Kiro => "kiro",
            Self::Cursor => "cursor",
            Self::Ollama => "ollama",
        }
    }
}

impl FromStr for Format {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "openai" => Ok(Self::Openai),
            "openai-responses" | "openai-response" => Ok(Self::OpenaiResponses),
            "claude" => Ok(Self::Claude),
            "gemini" => Ok(Self::Gemini),
            "gemini-cli" => Ok(Self::GeminiCli),
            "antigravity" => Ok(Self::Antigravity),
            "kiro" => Ok(Self::Kiro),
            "cursor" => Ok(Self::Cursor),
            "ollama" => Ok(Self::Ollama),
            _ => Err(()),
        }
    }
}
