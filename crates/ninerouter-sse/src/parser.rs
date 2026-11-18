//! Bộ phân tích cú pháp Server-Sent Events (SSE) hiệu năng cao, zero-copy trên luồng bytes.

use bytes::{Bytes, BytesMut};
use memchr::memchr;

/// Đại diện cho một event SSE đã được bóc tách từ stream
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    /// Tên event (từ trường `event: ...`), nếu không có mặc định là None (hoặc "message")
    pub event: Option<String>,
    /// Nội dung dữ liệu (từ một hoặc nhiều trường `data: ...`)
    pub data: Bytes,
    /// Định danh event (từ trường `id: ...`), nếu có
    pub id: Option<String>,
    /// Đánh dấu xem event này có phải là tín hiệu kết thúc [DONE] hay không
    pub is_done: bool,
}

impl SseEvent {
    /// Tạo nhanh một SSE event chứa chuỗi data
    pub fn new_data<B: Into<Bytes>>(data: B) -> Self {
        let bytes: Bytes = data.into();
        let is_done = bytes.as_ref() == b"[DONE]";
        Self {
            event: None,
            data: bytes,
            id: None,
            is_done,
        }
    }

    /// Định dạng event thành các bytes theo chuẩn SSE để truyền qua mạng (`data: ...\n\n`)
    pub fn to_wire_bytes(&self) -> Bytes {
        let mut buf = Vec::with_capacity(self.data.len() + 32);
        if let Some(ref evt) = self.event {
            buf.extend_from_slice(b"event: ");
            buf.extend_from_slice(evt.as_bytes());
            buf.push(b'\n');
        }
        if let Some(ref id) = self.id {
            buf.extend_from_slice(b"id: ");
            buf.extend_from_slice(id.as_bytes());
            buf.push(b'\n');
        }
        buf.extend_from_slice(b"data: ");
        buf.extend_from_slice(&self.data);
        buf.extend_from_slice(b"\n\n");
        Bytes::from(buf)
    }
}

/// Trạng thái phân tích cú pháp SSE dạng buffer tích lũy
#[derive(Default)]
pub struct SseParser {
    buffer: BytesMut,
    current_event: Option<String>,
    current_data: Vec<u8>,
    current_id: Option<String>,
}

impl SseParser {
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(8192),
            current_event: None,
            current_data: Vec::new(),
            current_id: None,
        }
    }

    /// Nạp thêm một chunk byte từ network vào buffer và trích xuất các SseEvent hoàn chỉnh
    pub fn feed(&mut self, chunk: &[u8]) -> Vec<SseEvent> {
        self.buffer.extend_from_slice(chunk);
        let mut events = Vec::new();

        while let Some(newline_pos) = memchr(b'\n', &self.buffer) {
            let line = self.buffer.split_to(newline_pos + 1);

            // Bỏ ký tự kết thúc dòng `\n` và `\r` nếu có
            let mut len = line.len();
            if len > 0 && line[len - 1] == b'\n' {
                len -= 1;
            }
            if len > 0 && line[len - 1] == b'\r' {
                len -= 1;
            }
            let line_slice = &line[..len];

            // Nếu dòng trống -> Kết thúc một event block
            if line_slice.is_empty() {
                if !self.current_data.is_empty() {
                    let data_bytes = Bytes::from(std::mem::take(&mut self.current_data));
                    let is_done = data_bytes.as_ref() == b"[DONE]";
                    events.push(SseEvent {
                        event: self.current_event.take(),
                        data: data_bytes,
                        id: self.current_id.take(),
                        is_done,
                    });
                } else if self.current_event.is_some() || self.current_id.is_some() {
                    self.current_event = None;
                    self.current_id = None;
                }
                continue;
            }

            // Dòng comment bắt đầu bằng dấu hai chấm `:` theo chuẩn SSE -> Bỏ qua
            if line_slice[0] == b':' {
                continue;
            }

            // Phân tách trường field name và value theo dấu `:`
            let (field, value) = if let Some(colon_pos) = memchr(b':', line_slice) {
                let f = &line_slice[..colon_pos];
                let mut v = &line_slice[colon_pos + 1..];
                // Chuẩn SSE: nếu ký tự đầu tiên sau dấu hai chấm là space thì bỏ qua
                if !v.is_empty() && v[0] == b' ' {
                    v = &v[1..];
                }
                (f, v)
            } else {
                (line_slice, &[][..])
            };

            match field {
                b"data" => {
                    if !self.current_data.is_empty() {
                        self.current_data.push(b'\n');
                    }
                    self.current_data.extend_from_slice(value);
                }
                b"event" => {
                    self.current_event = String::from_utf8(value.to_vec()).ok();
                }
                b"id" => {
                    self.current_id = String::from_utf8(value.to_vec()).ok();
                }
                _ => {
                    // Bỏ qua các trường không xác định
                }
            }
        }

        events
    }

    /// Flush dữ liệu còn sót lại khi stream đóng
    pub fn finish(&mut self) -> Option<SseEvent> {
        if !self.current_data.is_empty() {
            let data_bytes = Bytes::from(std::mem::take(&mut self.current_data));
            let is_done = data_bytes.as_ref() == b"[DONE]";
            Some(SseEvent {
                event: self.current_event.take(),
                data: data_bytes,
                id: self.current_id.take(),
                is_done,
            })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_event() {
        let mut parser = SseParser::new();
        let input = b"data: {\"test\":123}\n\n";
        let events = parser.feed(input);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, Bytes::from_static(b"{\"test\":123}"));
        assert_eq!(events[0].event, None);
        assert!(!events[0].is_done);
    }

    #[test]
    fn test_parse_multi_chunk() {
        let mut parser = SseParser::new();
        let chunk1 = b"event: message_delta\ndata: {\"index\":";
        let chunk2 = b"0}\n\n";
        
        let events1 = parser.feed(chunk1);
        assert_eq!(events1.len(), 0);

        let events2 = parser.feed(chunk2);
        assert_eq!(events2.len(), 1);
        assert_eq!(events2[0].event, Some("message_delta".to_string()));
        assert_eq!(events2[0].data, Bytes::from_static(b"{\"index\":0}"));
    }

    #[test]
    fn test_parse_done_event() {
        let mut parser = SseParser::new();
        let input = b"data: [DONE]\n\n";
        let events = parser.feed(input);
        assert_eq!(events.len(), 1);
        assert!(events[0].is_done);
    }
}
