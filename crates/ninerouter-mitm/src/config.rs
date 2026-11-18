//! Cấu hình hệ thống, ánh xạ tên miền, nhận diện công cụ và luật điều hướng.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use parking_lot::RwLock;
use regex::Regex;

pub const DEFAULT_LOCAL_ROUTER: &str = "http://localhost:20128";
pub const ANTIGRAVITY_IDE_VERSION: &str = "2.24.0";

/// Các công cụ AI được hỗ trợ can thiệp
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tool {
    Antigravity,
    Copilot,
    Kiro,
    Cursor,
}

impl Tool {
    pub fn as_str(&self) -> &'static str {
        match self {
            Tool::Antigravity => "antigravity",
            Tool::Copilot => "copilot",
            Tool::Kiro => "kiro",
            Tool::Cursor => "cursor",
        }
    }
}

/// Trình quản lý cấu hình và danh mục alias
#[derive(Clone)]
pub struct AppConfig {
    #[allow(dead_code)]
    pub data_dir: PathBuf,
    pub mitm_dir: PathBuf,
    pub router_base: String,
    pub router_api_key: Option<String>,
    pub port: u16,
    pub enable_rust_streamer: bool,
    tab_model_regex: Regex,
    aliases_cache: Arc<RwLock<(std::time::Instant, HashMap<String, HashMap<String, String>>)>>,
}

impl AppConfig {
    /// Khởi tạo cấu hình ứng dụng từ biến môi trường và thư mục mặc định
    pub fn load() -> Self {
        let data_dir = std::env::var("DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                let home = std::env::var("HOME").unwrap_or_else(|_| "/root".to_string());
                PathBuf::from(home).join(".9router")
            });

        let mitm_dir = data_dir.join("mitm");
        let _ = fs::create_dir_all(&mitm_dir);

        let router_base = std::env::var("MITM_ROUTER_BASE")
            .unwrap_or_else(|_| DEFAULT_LOCAL_ROUTER.to_string())
            .trim_end_matches('/')
            .to_string();

        let router_api_key = std::env::var("ROUTER_API_KEY")
            .ok()
            .and_then(|k| if k.trim().is_empty() { None } else { Some(k.trim().to_string()) });

        let port = std::env::var("MITM_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(443);

        let enable_rust_streamer = std::env::var("ENABLE_RUST_STREAMER")
            .map(|v| v != "0" && v.to_lowercase() != "false")
            .unwrap_or(true);

        let tab_model_regex = Regex::new(r"(?i)^tab[_-]").unwrap();

        Self {
            data_dir,
            mitm_dir,
            router_base,
            router_api_key,
            port,
            enable_rust_streamer,
            tab_model_regex,
            aliases_cache: Arc::new(RwLock::new((std::time::Instant::now() - std::time::Duration::from_secs(60), HashMap::new()))),
        }
    }

    /// Nhận diện công cụ từ host
    pub fn get_tool_for_host(&self, host: &str) -> Option<Tool> {
        let domain = host.split(':').next().unwrap_or(host);
        match domain {
            "daily-cloudcode-pa.googleapis.com" | "cloudcode-pa.googleapis.com" => Some(Tool::Antigravity),
            "api.individual.githubcopilot.com" => Some(Tool::Copilot),
            "q.us-east-1.amazonaws.com"
            | "codewhisperer.us-east-1.amazonaws.com"
            | "runtime.us-east-1.kiro.dev" => Some(Tool::Kiro),
            "api2.cursor.sh" => Some(Tool::Cursor),
            _ => None,
        }
    }

    /// Kiểm tra request có phải là chat request cần can thiệp không
    pub fn is_chat_request(&self, tool: Tool, path: &str, has_amz_target: bool) -> bool {
        match tool {
            Tool::Antigravity => path.contains(":generateContent") || path.contains(":streamGenerateContent"),
            Tool::Copilot => path.contains("/chat/completions") || path.contains("/v1/messages") || path.contains("/responses"),
            Tool::Kiro => has_amz_target || path.contains("/generateAssistantResponse"),
            Tool::Cursor => path.contains("/BidiAppend") || path.contains("/RunSSE") || path.contains("/RunPoll") || path.contains("/Run"),
        }
    }

    /// Kiểm tra model có thuộc danh sách không được can thiệp (ví dụ inline tab completion)
    pub fn is_model_excluded(&self, tool: Tool, model: &str) -> bool {
        match tool {
            Tool::Antigravity => self.tab_model_regex.is_match(model),
            _ => false,
        }
    }

    /// Lấy alias của model cho công cụ tương ứng từ aliases.json
    pub fn get_mapped_model(&self, tool: Tool, model: &str) -> Option<String> {
        self.reload_aliases_if_needed();
        let cache = self.aliases_cache.read();
        let tool_aliases = cache.1.get(tool.as_str())?;

        let normalized = model.strip_prefix("models/").unwrap_or(model);
        if let Some(target) = tool_aliases.get(normalized) {
            return Some(target.clone());
        }

        // Tìm kiếm theo prefix fallback
        for (k, v) in tool_aliases {
            if normalized.starts_with(k) || k.starts_with(normalized) {
                return Some(v.clone());
            }
        }
        None
    }

    /// Tự động nạp lại file aliases.json mỗi 5 giây nếu có thay đổi
    fn reload_aliases_if_needed(&self) {
        let now = std::time::Instant::now();
        {
            let cache = self.aliases_cache.read();
            if now.duration_since(cache.0) < std::time::Duration::from_secs(5) {
                return;
            }
        }

        let aliases_file = self.mitm_dir.join("aliases.json");
        let aliases = if let Ok(data) = fs::read_to_string(&aliases_file) {
            serde_json::from_str::<HashMap<String, HashMap<String, String>>>(&data).unwrap_or_default()
        } else {
            HashMap::new()
        };

        let mut cache = self.aliases_cache.write();
        *cache = (now, aliases);
    }
}
