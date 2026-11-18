// Đo kiểm hiệu năng (Benchmark) thực tế của Node.js SSE Streaming Pipeline.

import fs from 'fs';

function getMemoryUsage() {
  const mem = process.memoryUsage();
  let rssKb = 0;
  try {
    const status = fs.readFileSync('/proc/self/status', 'utf8');
    for (const line of status.split('\n')) {
      if (line.startsWith('VmRSS:')) {
        const parts = line.split(/\s+/);
        rssKb = parseInt(parts[1], 10) || 0;
        break;
      }
    }
  } catch {}
  return {
    heapUsedMb: mem.heapUsed / (1024 * 1024),
    rssMb: (rssKb * 1024) / (1024 * 1024) || (mem.rss / (1024 * 1024))
  };
}

console.log("=== BẮT ĐẦU BENCHMARK NODE.JS (V8 Engine) ===");

// 1. Tạo tập dữ liệu giả lập 10,000 SSE events tương tự
const chunks = [];
chunks.push('data: {"type":"message_start","message":{"id":"msg_bench_01","model":"claude-3-5-sonnet","usage":{"input_tokens":500}}}\n\n');
for (let i = 0; i < 9997; i++) {
  chunks.push(`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":" token_${i}"}}\n\n`);
}
chunks.push('data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":9997}}\n\n');
chunks.push('data: {"type":"message_stop"}\n\n');

const rawStreamText = chunks.join('');
const rawStreamBuffer = Buffer.from(rawStreamText, 'utf8');
console.log(`Tổng dung lượng đầu vào: ${(rawStreamBuffer.length / (1024 * 1024)).toFixed(2)} MB (10,000 events)`);

// Đọc RAM trước khi chạy
const memBefore = getMemoryUsage();

// 2. Chạy luồng phân tích và chuyển đổi tương tự open-sse
const startTime = performance.now();

const encoder = new TextEncoder();
const decoder = new TextDecoder();
let buffer = '';
let outCount = 0;
let outBytes = 0;

// Trạng thái chuyển đổi Claude -> OpenAI
let messageId = 'chatcmpl-bench';
let createdTs = Math.floor(Date.now() / 1000);
let promptTokens = 0;
let outputTokens = 0;

// Giả lập nhận từng chunk TCP 1024 bytes
const chunkSize = 1024;
for (let offset = 0; offset < rawStreamBuffer.length; offset += chunkSize) {
  const slice = rawStreamBuffer.subarray(offset, Math.min(offset + chunkSize, rawStreamBuffer.length));
  const text = decoder.decode(slice, { stream: true });
  buffer += text;

  const lines = buffer.split('\n');
  buffer = lines.pop() || '';

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || !trimmed.startsWith('data:')) continue;

    const data = trimmed.slice(5).trim();
    if (data === '[DONE]') {
      const out = 'data: [DONE]\n\n';
      outBytes += out.length;
      outCount++;
      continue;
    }

    try {
      const parsed = JSON.parse(data);
      const eventType = parsed.type;

      if (eventType === 'message_start') {
        promptTokens = parsed.message?.usage?.input_tokens || 0;
        const chunkObj = {
          id: messageId,
          object: 'chat.completion.chunk',
          created: createdTs,
          choices: [{ index: 0, delta: { role: 'assistant' }, finish_reason: null }]
        };
        const out = `data: ${JSON.stringify(chunkObj)}\n\n`;
        const bytes = encoder.encode(out);
        outBytes += bytes.length;
        outCount++;
      } else if (eventType === 'content_block_delta') {
        const textDelta = parsed.delta?.text;
        if (textDelta) {
          const chunkObj = {
            id: messageId,
            object: 'chat.completion.chunk',
            created: createdTs,
            choices: [{ index: 0, delta: { content: textDelta }, finish_reason: null }]
          };
          const out = `data: ${JSON.stringify(chunkObj)}\n\n`;
          const bytes = encoder.encode(out);
          outBytes += bytes.length;
          outCount++;
        }
      } else if (eventType === 'message_delta') {
        outputTokens = parsed.usage?.output_tokens || 0;
        const chunkObj = {
          id: messageId,
          object: 'chat.completion.chunk',
          created: createdTs,
          choices: [{ index: 0, delta: {}, finish_reason: 'stop' }],
          usage: {
            prompt_tokens: promptTokens,
            completion_tokens: outputTokens,
            total_tokens: promptTokens + outputTokens
          }
        };
        const out = `data: ${JSON.stringify(chunkObj)}\n\n`;
        const bytes = encoder.encode(out);
        outBytes += bytes.length;
        outCount++;
      } else if (eventType === 'message_stop') {
        const out = 'data: [DONE]\n\n';
        const bytes = encoder.encode(out);
        outBytes += bytes.length;
        outCount++;
      }
    } catch (e) {
      // bỏ qua lỗi parse
    }
  }
}

const endTime = performance.now();
const memAfter = getMemoryUsage();

const durationMs = endTime - startTime;
const eventsPerSec = (10000 / (durationMs / 1000));
const mbPerSec = (rawStreamBuffer.length / (1024 * 1024)) / (durationMs / 1000);

console.log('\n--- KẾT QUẢ NODE.JS ---');
console.log(`Thời gian xử lý: ${durationMs.toFixed(2)} ms`);
console.log(`Tốc độ xử lý: ${eventsPerSec.toFixed(0)} events/giây (${mbPerSec.toFixed(2)} MB/s)`);
console.log(`Số chunks đầu ra: ${outCount}`);
console.log(`Dung lượng đầu ra: ${(outBytes / (1024 * 1024)).toFixed(2)} MB`);
console.log(`Heap Used trước: ${memBefore.heapUsedMb.toFixed(2)} MB | sau: ${memAfter.heapUsedMb.toFixed(2)} MB (chênh lệch: +${(memAfter.heapUsedMb - memBefore.heapUsedMb).toFixed(2)} MB)`);
console.log(`RAM (RSS) trước: ${memBefore.rssMb.toFixed(2)} MB | sau: ${memAfter.rssMb.toFixed(2)} MB (chênh lệch: +${(memAfter.rssMb - memBefore.rssMb).toFixed(2)} MB)`);
console.log('================================================\n');
