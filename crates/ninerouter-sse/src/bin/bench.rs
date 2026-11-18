//! Đo kiểm hiệu năng (Benchmark) thực tế của Rust SSE Streaming Pipeline.

use ninerouter_sse::parser::SseParser;
use ninerouter_sse::translators::claude_to_openai::ClaudeToOpenAiTranslator;
use ninerouter_sse::translators::StreamTranslator;
use std::time::Instant;

fn main() {
    println!("=== BẮT ĐẦU BENCHMARK RUST (ninerouter-sse) ===");
    
    // 1. Tạo tập dữ liệu thử nghiệm giả lập 10,000 SSE events từ Claude
    let mut raw_stream = Vec::new();
    
    // Event 1: message_start
    raw_stream.extend_from_slice(b"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_bench_01\",\"model\":\"claude-3-5-sonnet\",\"usage\":{\"input_tokens\":500}}}\n\n");
    
    // 9,997 events: content_block_delta
    for i in 0..9997 {
        let chunk = format!(
            "data: {{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{{\"type\":\"text_delta\",\"text\":\" token_{}\"}}}}\n\n",
            i
        );
        raw_stream.extend_from_slice(chunk.as_bytes());
    }
    
    // Event 9,999: message_delta
    raw_stream.extend_from_slice(b"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":9997}}\n\n");
    
    // Event 10,000: message_stop
    raw_stream.extend_from_slice(b"data: {\"type\":\"message_stop\"}\n\n");

    let total_bytes = raw_stream.len();
    println!("Tổng dung lượng đầu vào: {:.2} MB (10,000 events)", total_bytes as f64 / (1024.0 * 1024.0));

    // Đọc RSS trước khi chạy
    let rss_before = get_current_rss_kb();

    // 2. Chạy pipeline Rust
    let start_time = Instant::now();
    let mut parser = SseParser::new();
    let mut translator = ClaudeToOpenAiTranslator::new();

    // Giả lập nhận từng chunk TCP 1024 bytes
    let chunk_size = 1024;
    let mut out_count = 0;
    let mut out_bytes = 0;

    for slice in raw_stream.chunks(chunk_size) {
        let events = parser.feed(slice);
        for ev in events {
            let translated = translator.translate_event(&ev);
            for out_ev in translated {
                let wire = out_ev.to_wire_bytes();
                out_bytes += wire.len();
                out_count += 1;
            }
        }
    }

    let flushed = translator.flush();
    for out_ev in flushed {
        let wire = out_ev.to_wire_bytes();
        out_bytes += wire.len();
        out_count += 1;
    }

    let duration = start_time.elapsed();
    let rss_after = get_current_rss_kb();

    let duration_ms = duration.as_secs_f64() * 1000.0;
    let events_per_sec = 10000.0 / duration.as_secs_f64();
    let mb_per_sec = (total_bytes as f64 / (1024.0 * 1024.0)) / duration.as_secs_f64();

    println!("\n--- KẾT QUẢ RUST ---");
    println!("Thời gian xử lý: {:.2} ms", duration_ms);
    println!("Tốc độ xử lý: {:.0} events/giây ({:.2} MB/s)", events_per_sec, mb_per_sec);
    println!("Số chunks đầu ra: {}", out_count);
    println!("Dung lượng đầu ra: {:.2} MB", out_bytes as f64 / (1024.0 * 1024.0));
    println!("RAM (RSS) trước: {} KB | sau: {} KB (chênh lệch: +{} KB)", 
        rss_before, rss_after, rss_after.saturating_sub(rss_before));
    println!("================================================\n");
}

fn get_current_rss_kb() -> u64 {
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if line.starts_with("VmRSS:") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    return parts[1].parse().unwrap_or(0);
                }
            }
        }
    }
    0
}
