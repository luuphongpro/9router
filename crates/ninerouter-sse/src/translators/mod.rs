//! Giao diện chung và bộ đăng ký cho các bộ chuyển đổi định dạng stream SSE.

pub mod claude_to_openai;
pub mod openai_passthrough;
pub mod openai_to_claude;

use crate::format::Format;
use crate::parser::SseEvent;

/// Trait định nghĩa hành vi chuyển đổi cho một luồng stream SSE
pub trait StreamTranslator: Send + Sync {
    /// Xử lý một event SSE đầu vào và sinh ra các event SSE đầu ra tương ứng
    fn translate_event(&mut self, event: &SseEvent) -> Vec<SseEvent>;

    /// Xử lý khi kết thúc luồng (flush các event cuối cùng hoặc tổng kết usage)
    fn flush(&mut self) -> Vec<SseEvent>;
}

/// Khởi tạo bộ chuyển đổi phù hợp dựa trên định dạng nguồn (upstream provider) và đích (client format)
pub fn create_translator(target_format: Format, source_format: Format) -> Box<dyn StreamTranslator> {
    match (target_format, source_format) {
        (Format::Claude, Format::Openai) => {
            Box::new(claude_to_openai::ClaudeToOpenAiTranslator::new())
        }
        (Format::Openai, Format::Claude) => {
            Box::new(openai_to_claude::OpenAiToClaudeTranslator::new())
        }
        (Format::Openai, Format::Openai) => {
            Box::new(openai_passthrough::OpenAiPassthroughTranslator::new())
        }
        // Trường hợp mặc định cho cùng định dạng hoặc chưa có bộ chuyển riêng
        _ => Box::new(openai_passthrough::OpenAiPassthroughTranslator::new()),
    }
}
