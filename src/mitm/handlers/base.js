const { log, err } = require("../logger");

const DEFAULT_LOCAL_ROUTER = "http://localhost:20128";
const ROUTER_BASE = String(process.env.MITM_ROUTER_BASE || DEFAULT_LOCAL_ROUTER)
  .trim()
  .replace(/\/+$/, "") || DEFAULT_LOCAL_ROUTER;
const API_KEY = process.env.ROUTER_API_KEY;

// Danh sách header không được chuyển tiếp sang 9Router
const STRIP_HEADERS = new Set([
  "host", "content-length", "connection", "transfer-encoding",
  "content-type", "authorization"
]);

// Hàm đợi socket drain hoặc close an toàn, tránh treo Promise vĩnh viễn khi client ngắt kết nối
function waitForDrain(res) {
  if (res.writableEnded || res.destroyed) return Promise.resolve();
  return new Promise((resolve) => {
    const onDrain = () => { cleanup(); resolve(); };
    const onClose = () => { cleanup(); resolve(); };
    const cleanup = () => {
      res.removeListener("drain", onDrain);
      res.removeListener("close", onClose);
    };
    res.once("drain", onDrain);
    res.once("close", onClose);
  });
}


/**
 * Gửi request body tới 9Router theo đường dẫn và trả về Fetch Response object.
 * Hỗ trợ chuyển tiếp clientHeaders (đã loại bỏ các header nhạy cảm/hop-by-hop).
 */
async function fetchRouter(openaiBody, path = "/v1/chat/completions", clientHeaders = {}, signal) {
  const forwarded = {};
  for (const [k, v] of Object.entries(clientHeaders)) {
    if (!STRIP_HEADERS.has(k.toLowerCase())) forwarded[k] = v;
  }

  const response = await fetch(`${ROUTER_BASE}${path}`, {
    method: "POST",
    headers: {
      ...forwarded,
      "Content-Type": "application/json",
      ...(API_KEY && { "Authorization": `Bearer ${API_KEY}` })
    },
    body: JSON.stringify(openaiBody),
    signal
  });

  return response;
}

/**
 * Chuyển tiếp stream SSE từ 9Router trực tiếp sang client response.
 * Xử lý hủy stream khi client ngắt kết nối và hỗ trợ backpressure.
 */
async function pipeSSE(routerRes, res, dumper) {
  const ct = routerRes.headers.get("content-type") || "application/json";
  const status = routerRes.status || 200;
  const resHeaders = { "Content-Type": ct, "Cache-Control": "no-cache", "Connection": "keep-alive" };
  if (ct.includes("text/event-stream")) resHeaders["X-Accel-Buffering"] = "no";
  res.writeHead(status, resHeaders);
  if (dumper) dumper.writeHeader(routerRes.status, Object.fromEntries(routerRes.headers));

  if (!routerRes.body) {
    const text = await routerRes.text().catch(() => "");
    if (dumper) { dumper.writeChunk(text); dumper.end(); }
    res.end(text);
    return;
  }

  const reader = routerRes.body.getReader();
  const decoder = new TextDecoder();
  let clientClosed = false;

  const onClientClose = () => {
    clientClosed = true;
    try { reader.cancel(); } catch {}
  };
  res.once("close", onClientClose);

  try {
    while (!clientClosed) {
      const { done, value } = await reader.read();
      if (done) {
        if (dumper) dumper.end();
        if (!res.writableEnded) res.end();
        break;
      }
      if (dumper) dumper.writeChunk(value);
      const canWrite = res.write(decoder.decode(value, { stream: true }));
      if (!canWrite && !clientClosed) {
        await waitForDrain(res);
      }
    }
  } catch (e) {
    if (!clientClosed) {
      err(`[mitm] pipeSSE read error: ${e.message}`);
    }
  } finally {
    res.removeListener("close", onClientClose);
    try { reader.cancel(); } catch {}
  }
}

/**
 * Chuyển tiếp stream SSE từ 9Router, chuyển đổi từng chunk qua hàm transformFn.
 * Đọc dữ liệu dòng SSE data:, parse JSON, gọi transformFn(parsed, state)
 * và ghi chuỗi SSE kết quả vào client response.
 */
async function pipeTransformedSSE(routerRes, res, transformFn, state) {
  const ct = routerRes.headers.get("content-type") || "application/json";
  const resHeaders = { "Content-Type": ct, "Cache-Control": "no-cache", "Connection": "keep-alive" };
  if (ct.includes("text/event-stream")) resHeaders["X-Accel-Buffering"] = "no";
  res.writeHead(200, resHeaders);

  if (!routerRes.body) {
    res.end(await routerRes.text().catch(() => ""));
    return;
  }

  const reader = routerRes.body.getReader();
  const decoder = new TextDecoder("utf-8", { fatal: false });
  let buffer = "";
  let clientClosed = false;

  const onClientClose = () => {
    clientClosed = true;
    try { reader.cancel(); } catch {}
  };
  res.once("close", onClientClose);

  try {
    while (!clientClosed) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop() || "";

      for (const line of lines) {
        if (clientClosed) break;
        const trimmed = line.trim();
        if (!trimmed || !trimmed.startsWith("data:")) continue;

        const data = trimmed.slice(5).trim();
        if (data === "[DONE]") continue;

        if (process.env.DEBUG_MITM) {
          log(`[SSE in] ${data.slice(0, 200)}`);
        }

        try {
          const parsed = JSON.parse(data);
          const result = transformFn(parsed, state);
          if (result != null) {
            const outputs = Array.isArray(result) ? result : [result];
            for (const output of outputs) {
              if (process.env.DEBUG_MITM) {
                const len = output.length || output.byteLength || 0;
                log(`[write binary frame] (${len}B) first 20B: ${Array.from(output.slice(0, 20)).join(",")}`);
              }
              const canWrite = res.write(Buffer.from(output));
              if (!canWrite && !clientClosed) {
                await waitForDrain(res);
              }
            }
          }
        } catch {
          // Bỏ qua các dòng không parse được JSON
        }
      }
    }

    // Flush trạng thái kết thúc stream nếu client chưa đóng
    if (!clientClosed) {
      try {
        const flushed = transformFn(null, state);
        if (flushed != null) {
          const outputs = Array.isArray(flushed) ? flushed : [flushed];
          for (const output of outputs) {
            res.write(output);
          }
        }
      } catch {}
      if (!res.writableEnded) res.end();
    }
  } catch (e) {
    if (!clientClosed) {
      err(`[mitm] pipeTransformedSSE error: ${e.message}`);
    }
  } finally {
    res.removeListener("close", onClientClose);
    try { reader.cancel(); } catch {}
  }
}

/**
 * Chuyển tiếp stream SSE từ 9Router, chuyển đổi từng chunk qua hàm transformFn,
 * và ghi binary EventStream frames về client (dành cho Kiro/AWS EventStream).
 */
async function pipeTransformedEventStream(routerRes, res, transformFn, state) {
  const resHeaders = {
    "Content-Type": "application/vnd.amazon.eventstream",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive"
  };
  res.writeHead(200, resHeaders);

  if (!routerRes.body) {
    res.end(await routerRes.text().catch(() => ""));
    return;
  }

  const reader = routerRes.body.getReader();
  const decoder = new TextDecoder("utf-8", { fatal: false });
  let buffer = "";
  let clientClosed = false;

  const onClientClose = () => {
    clientClosed = true;
    try { reader.cancel(); } catch {}
  };
  res.once("close", onClientClose);

  try {
    while (!clientClosed) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop() || "";

      for (const line of lines) {
        if (clientClosed) break;
        const trimmed = line.trim();
        if (!trimmed || !trimmed.startsWith("data:")) continue;

        const data = trimmed.slice(5).trim();
        if (data === "[DONE]") continue;

        if (process.env.DEBUG_MITM) {
          log(`[SSE in] ${data.slice(0, 200)}`);
        }

        try {
          const parsed = JSON.parse(data);
          const result = transformFn(parsed, state);
          if (result != null) {
            const outputs = Array.isArray(result) ? result : [result];
            for (const output of outputs) {
              if (process.env.DEBUG_MITM) {
                const len = output.length || output.byteLength || 0;
                log(`[write binary frame] (${len}B) first 20B: ${Array.from(output.slice(0, 20)).join(",")}`);
              }
              const canWrite = res.write(Buffer.from(output));
              if (!canWrite && !clientClosed) {
                await waitForDrain(res);
              }
            }
          }
        } catch {
          // Bỏ qua các dòng không parse được JSON
        }
      }
    }

    // Flush trạng thái kết thúc stream nếu client chưa đóng
    if (!clientClosed) {
      try {
        const flushed = transformFn(null, state);
        if (flushed != null) {
          const outputs = Array.isArray(flushed) ? flushed : [flushed];
          for (const output of outputs) {
            res.write(output);
          }
        }
      } catch {}
      if (!res.writableEnded) res.end();
    }
  } catch (e) {
    if (!clientClosed) {
      err(`[mitm] pipeTransformedEventStream error: ${e.message}`);
    }
  } finally {
    res.removeListener("close", onClientClose);
    try { reader.cancel(); } catch {}
  }
}

module.exports = { fetchRouter, pipeSSE, pipeTransformedSSE, pipeTransformedEventStream };
