//! Thư viện xử lý và chuyển đổi luồng SSE (Server-Sent Events) hiệu năng cao cho 9Router.
//!
//! Cung cấp:
//! - `parser::SseParser`: Bóc tách event SSE zero-copy.
//! - `translators`: Dịch giữa các schema Claude <-> OpenAI với mức tiêu thụ RAM cực thấp.
//! - `pipeline::SseStreamPipeline`: Bọc async byte stream thành luồng đã chuyển đổi hoàn chỉnh.

pub mod format;
pub mod parser;
pub mod pipeline;
pub mod schema;
pub mod translators;

pub use format::Format;
pub use parser::{SseEvent, SseParser};
pub use pipeline::SseStreamPipeline;
pub use translators::{create_translator, StreamTranslator};
