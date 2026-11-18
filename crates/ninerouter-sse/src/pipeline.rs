//! Pipeline stream bất đồng bộ biến đổi SSE chunks với độ trễ thấp và không cấp phát thừa.

use crate::parser::{SseEvent, SseParser};
use crate::translators::StreamTranslator;
use bytes::Bytes;
use futures_util::stream::Stream;
use pin_project_lite::pin_project;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

pin_project! {
    /// Pipeline stream bất đồng bộ biến đổi trực tiếp luồng byte từ upstream thành luồng byte SSE chuẩn cho client
    pub struct SseStreamPipeline<S> {
        #[pin]
        inner_stream: S,
        parser: SseParser,
        translator: Box<dyn StreamTranslator>,
        pending_events: VecDeque<SseEvent>,
        stream_ended: bool,
    }
}

impl<S> SseStreamPipeline<S> {
    pub fn new(inner_stream: S, translator: Box<dyn StreamTranslator>) -> Self {
        Self {
            inner_stream,
            parser: SseParser::new(),
            translator,
            pending_events: VecDeque::new(),
            stream_ended: false,
        }
    }
}

impl<S, E> Stream for SseStreamPipeline<S>
where
    S: Stream<Item = Result<Bytes, E>>,
{
    type Item = Result<Bytes, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            // 1. Trả về các event đã được dịch và đang chờ trong hàng đợi
            if let Some(ev) = this.pending_events.pop_front() {
                return Poll::Ready(Some(Ok(ev.to_wire_bytes())));
            }

            // 2. Nếu luồng upstream đã kết thúc và không còn event chờ
            if *this.stream_ended {
                return Poll::Ready(None);
            }

            // 3. Đọc tiếp từ inner_stream
            match this.inner_stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(chunk))) => {
                    let events = this.parser.feed(&chunk);
                    for ev in events {
                        let translated = this.translator.translate_event(&ev);
                        this.pending_events.extend(translated);
                    }
                    // Nếu sau khi nạp chunk đã có event được sinh ra, tiếp tục vòng lặp để trả về
                    if !this.pending_events.is_empty() {
                        continue;
                    }
                }
                Poll::Ready(Some(Err(err))) => {
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(None) => {
                    *this.stream_ended = true;

                    // Flush nốt dữ liệu còn sót trong parser
                    if let Some(ev) = this.parser.finish() {
                        let translated = this.translator.translate_event(&ev);
                        this.pending_events.extend(translated);
                    }

                    // Flush các event kết thúc của bộ dịch
                    let flushed = this.translator.flush();
                    this.pending_events.extend(flushed);

                    if let Some(ev) = this.pending_events.pop_front() {
                        return Poll::Ready(Some(Ok(ev.to_wire_bytes())));
                    } else {
                        return Poll::Ready(None);
                    }
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::Format;
    use crate::translators::create_translator;
    use futures_util::stream;
    use futures_util::StreamExt;

    #[tokio::test]
    async fn test_pipeline_streaming() {
        let raw_chunks = vec![
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_abc\",\"model\":\"claude-3-5\",\"usage\":{\"input_tokens\":5}}}\n\n")),
            Ok(Bytes::from_static(b"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello Rust!\"}}\n\n")),
            Ok(Bytes::from_static(b"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n")),
            Ok(Bytes::from_static(b"data: {\"type\":\"message_stop\"}\n\n")),
        ];

        let byte_stream = stream::iter(raw_chunks);
        let translator = create_translator(Format::Claude, Format::Openai);
        let mut pipeline = SseStreamPipeline::new(byte_stream, translator);

        let mut collected = Vec::new();
        while let Some(res) = pipeline.next().await {
            let chunk_bytes = res.unwrap();
            collected.push(String::from_utf8(chunk_bytes.to_vec()).unwrap());
        }

        // Kiểm tra luồng dữ liệu trả về đều tuân thủ chuẩn SSE data: ...\n\n
        assert!(!collected.is_empty());
        assert!(collected.iter().all(|s| s.starts_with("data: ") && s.ends_with("\n\n")));
        assert!(collected.iter().any(|s| s.contains("Hello Rust!")));
        assert!(collected.iter().any(|s| s.contains("[DONE]")));
    }
}
