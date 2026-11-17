const https = require("https");
const http2 = require("http2");
const tls = require("tls");
const fs = require("fs");
const path = require("path");
const dns = require("dns");
const { promisify } = require("util");
const { execSync } = require("child_process");
const { log, err, dumpRequest, createResponseDumper, clearDumpDir } = require("./logger");
const { IS_DEV, LSOF_BIN, TARGET_HOSTS, URL_PATTERNS, MODEL_SYNONYMS, MODEL_PATTERNS, MODEL_NO_MAP, getToolForHost, isChatRequest, extractModel } = require("./config");
const { DATA_DIR, MITM_DIR } = require("./paths");
const { generateCert, getCertForDomain } = require("./cert/generate");
const { getMitmAlias } = require("./dbReader");
const { applyAntigravityIdeVersionOverride } = require("./antigravityIdeVersion");
const LOCAL_PORT = 443;
const IS_WIN = process.platform === "win32";
const ENABLE_FILE_LOG = IS_DEV;

// Clear stale dump files on every MITM start (prevents unbounded disk usage)
clearDumpDir();
const INTERNAL_REQUEST_HEADER = { name: "x-request-source", value: "local" };

// Host rewrite for upstream forward: PROD cloudcode-pa is rate-limited (429),
// daily-cloudcode-pa (dev endpoint) accepts same body+token. Same trick as open-sse.
const HOST_REWRITE = {
  "cloudcode-pa.googleapis.com": "daily-cloudcode-pa.googleapis.com",
};

const handlers = {
  antigravity: require("./handlers/antigravity"),
  copilot: require("./handlers/copilot"),
  kiro: require("./handlers/kiro"),
  cursor: require("./handlers/cursor"),
};

// ── SSL / SNI ─────────────────────────────────────────────────

const certCache = new Map();
let rootCAPem;

function sniCallback(servername, cb) {
  try {
    if (certCache.has(servername)) return cb(null, certCache.get(servername));
    const certData = getCertForDomain(servername);
    if (!certData) return cb(new Error(`Failed to generate cert for ${servername}`));
    const ctx = require("tls").createSecureContext({
      key: certData.key,
      cert: `${certData.cert}\n${rootCAPem}`
    });
    certCache.set(servername, ctx);
    cb(null, ctx);
  } catch (e) {
    err(`SNI error for ${servername}: ${e.message}`);
    cb(e);
  }
}

let sslOptions;
try {
  if (!fs.existsSync(path.join(MITM_DIR, "rootCA.key")) || !fs.existsSync(path.join(MITM_DIR, "rootCA.crt"))) {
    log("Root CA missing, generating...");
    generateCert();
  }

  const rootKey = fs.readFileSync(path.join(MITM_DIR, "rootCA.key"));
  const rootCert = fs.readFileSync(path.join(MITM_DIR, "rootCA.crt"));
  rootCAPem = rootCert.toString("utf8");
  sslOptions = { key: rootKey, cert: rootCert, SNICallback: sniCallback };
} catch (e) {
  err(`Root CA not found: ${e.message}`);
  process.exit(1);
}

// ── Helpers ───────────────────────────────────────────────────

const cachedTargetIPs = {};
const CACHE_TTL_MS = 5 * 60 * 1000;

async function resolveTargetIP(hostname) {
  const cached = cachedTargetIPs[hostname];
  if (cached && Date.now() - cached.ts < CACHE_TTL_MS) return cached.ip;
  const resolver = new dns.Resolver();
  resolver.setServers(["8.8.8.8"]);
  const resolve4 = promisify(resolver.resolve4.bind(resolver));
  const addresses = await resolve4(hostname);
  cachedTargetIPs[hostname] = { ip: addresses[0], ts: Date.now() };
  return cachedTargetIPs[hostname].ip;
}

function collectBodyRaw(req) {
  return new Promise((resolve, reject) => {
    let chunks = [];
    let completed = false;

    const cleanup = () => {
      req.removeListener("data", onData);
      req.removeListener("end", onEnd);
      req.removeListener("error", onError);
      req.removeListener("close", onClose);
      chunks = null;
    };

    const onData = (chunk) => {
      if (chunks) chunks.push(chunk);
    };

    const onEnd = () => {
      if (completed) return;
      completed = true;
      const result = chunks ? Buffer.concat(chunks) : Buffer.alloc(0);
      cleanup();
      resolve(result);
    };

    const onError = (e) => {
      if (completed) return;
      completed = true;
      cleanup();
      reject(e);
    };

    const onClose = () => {
      if (completed) return;
      completed = true;
      cleanup();
      const err = new Error("Client closed connection prematurely");
      err.code = "ECONNRESET";
      reject(err);
    };

    req.on("data", onData);
    req.once("end", onEnd);
    req.once("error", onError);
    req.once("close", onClose);
  });
}

function getMappedModel(tool, model) {
  if (!model) return null;
  try {
    const aliases = getMitmAlias(tool);
    if (!aliases) return null;
    // Normalize via synonym map (e.g., public AG names -> backend model ids)
    const normalizedModel = String(model).replace(/^models\//, "");
    const lookup = MODEL_SYNONYMS?.[tool]?.[normalizedModel] || normalizedModel;
    if (aliases[lookup]) return aliases[lookup];
    // Prefix match fallback
    const prefixKey = Object.keys(aliases).find(k => k && aliases[k] && (lookup.startsWith(k) || k.startsWith(lookup)));
    if (prefixKey) return aliases[prefixKey];
    // Pattern fallback: catches AG renamed variants (e.g. deprecated pro IDs → gemini-pro-agent)
    const patterns = MODEL_PATTERNS?.[tool] || [];
    for (const { match, alias } of patterns) {
      if (match.test(lookup) && aliases[alias]) return aliases[alias];
    }
    return null;
  } catch { return null; }
}

/**
 * Forward request to real upstream.
 * Optional onResponse(rawBuffer) callback — if provided, tees the response
 * so it's both forwarded to client AND passed to the callback for inspection.
 * Also tees full stream into a dump file when ENABLE_FILE_LOG is on.
 */
async function passthrough(req, res, bodyBuffer, onResponse) {
  const originalHost = (req.headers.host || TARGET_HOSTS[0]).split(":")[0];
  // Only rewrite host for chat endpoints — daily-cloudcode-pa rejects auth/login requests
  const isChatEndpoint = req.url.includes(":generateContent") || req.url.includes(":streamGenerateContent");
  const targetHost = isChatEndpoint ? (HOST_REWRITE[originalHost] || originalHost) : originalHost;
  const dumper = ENABLE_FILE_LOG ? createResponseDumper(req, "passthrough") : null;

  const tool = getToolForHost(req.headers.host);
  const versionOverride = tool === "antigravity"
    ? applyAntigravityIdeVersionOverride(bodyBuffer, req.headers, req.url)
    : { bodyBuffer, headers: req.headers };
  const bodyForForwarding = versionOverride.bodyBuffer;
  const headersForForwarding = { ...versionOverride.headers, host: targetHost };
  if (bodyForForwarding !== bodyBuffer) {
    headersForForwarding["content-length"] = String(bodyForForwarding.length);
  }

  // ALPN negotiate: try HTTP/2 first (like browsers/mitmweb), fallback HTTP/1.1
  try {
    const proto = await negotiateAlpn(targetHost);
    if (proto === "h2") {
      return await passthroughHttp2(req, res, bodyForForwarding, headersForForwarding, targetHost, onResponse, dumper);
    }
  } catch (e) {
    err(`[mitm] ALPN negotiate failed: ${e.message}, fallback to HTTP/1.1`);
  }

  return passthroughHttps(req, res, bodyForForwarding, headersForForwarding, targetHost, onResponse, dumper);
}

// ── ALPN negotiation cache ────────────────────────────────────
const alpnCache = new Map(); // host → "h2" | "http/1.1"
async function negotiateAlpn(host) {
  if (alpnCache.has(host)) return alpnCache.get(host);
  const ip = await resolveTargetIP(host);
  return new Promise((resolve, reject) => {
    const socket = tls.connect({
      host: ip, port: 443, servername: host,
      ALPNProtocols: ["h2", "http/1.1"], rejectUnauthorized: false,
    }, () => {
      const proto = socket.alpnProtocol || "http/1.1";
      alpnCache.set(host, proto);
      log(`🔗 [mitm] ALPN ${host} → ${proto}`);
      socket.end();
      resolve(proto);
    });
    socket.once("error", reject);
    socket.setTimeout(5000, () => { socket.destroy(new Error("ALPN timeout")); });
  });
}

// Pool HTTP/2 session theo host — tái dùng thay vì tạo mới mỗi request
const h2SessionPool = new Map(); // host → http2.ClientHttp2Session

function getH2Session(targetHost, targetIP) {
  const existing = h2SessionPool.get(targetHost);
  if (existing && !existing.destroyed && !existing.closed) {
    return existing;
  }
  const session = http2.connect(`https://${targetHost}`, {
    createConnection: () => tls.connect({
      host: targetIP, port: 443, servername: targetHost,
      ALPNProtocols: ["h2"], rejectUnauthorized: false,
    }),
  });
  h2SessionPool.set(targetHost, session);
  session.once("error", () => {
    if (h2SessionPool.get(targetHost) === session) h2SessionPool.delete(targetHost);
  });
  session.once("close", () => {
    if (h2SessionPool.get(targetHost) === session) h2SessionPool.delete(targetHost);
  });
  session.once("goaway", () => {
    if (h2SessionPool.get(targetHost) === session) h2SessionPool.delete(targetHost);
    try { session.destroy(); } catch {}
  });
  return session;
}

// Chuyển tiếp HTTP/2 sử dụng module node:http2 native với pool session
async function passthroughHttp2(req, res, bodyBuffer, headers, targetHost, onResponse, dumper) {
  const targetIP = await resolveTargetIP(targetHost);
  // Loại bỏ các header dành riêng cho HTTP/1.1
  const h2Headers = {};
  for (const [k, v] of Object.entries(headers)) {
    const lk = k.toLowerCase();
    if (lk === "host" || lk === "connection" || lk === "keep-alive" ||
        lk === "transfer-encoding" || lk === "upgrade" || lk === "proxy-connection") continue;
    h2Headers[lk] = v;
  }
  h2Headers[":method"] = req.method;
  h2Headers[":path"] = req.url;
  h2Headers[":scheme"] = "https";
  h2Headers[":authority"] = targetHost;

  return new Promise((resolve) => {
    let stream = null;
    let settled = false;

    const cleanup = () => {
      if (settled) return;
      settled = true;
      res.removeListener("close", onClientAbort);
      req.removeListener("close", onClientAbort);
      if (stream && !stream.destroyed) {
        try { stream.destroy(); } catch {}
      }
      stream = null;
      resolve();
    };

    const onClientAbort = () => {
      cleanup();
    };

    req.once("close", onClientAbort);
    res.once("close", onClientAbort);

    let client;
    try {
      client = getH2Session(targetHost, targetIP);
    } catch (e) {
      err(`[mitm] http2 session error: ${e.message}`);
      if (!res.headersSent) res.writeHead(502);
      if (!res.writableEnded) res.end("Bad Gateway");
      cleanup();
      return;
    }

    try {
      stream = client.request(h2Headers, { endStream: bodyBuffer.length === 0 });
    } catch (e) {
      err(`[mitm] http2 request error: ${e.message}`);
      if (!res.headersSent) res.writeHead(502);
      if (!res.writableEnded) res.end("Bad Gateway");
      // Xoá session lỗi khỏi pool để request sau tạo session mới
      if (h2SessionPool.get(targetHost) === client) h2SessionPool.delete(targetHost);
      cleanup();
      return;
    }

    if (bodyBuffer.length > 0) stream.end(bodyBuffer);

    stream.once("response", (responseHeaders) => {
      if (settled) return;
      const status = responseHeaders[":status"];
      const outHeaders = {};
      for (const [k, v] of Object.entries(responseHeaders)) {
        if (k.startsWith(":")) continue;
        if (k === "connection" || k === "keep-alive" || k === "transfer-encoding") continue;
        outHeaders[k] = v;
      }
      res.writeHead(status, outHeaders);
      if (dumper) dumper.writeHeader(status, outHeaders);

      const chunks = [];
      stream.on("data", chunk => {
        if (settled) return;
        if (dumper) dumper.writeChunk(chunk);
        if (onResponse) chunks.push(chunk);
        const canWrite = res.write(chunk);
        if (!canWrite && stream && !stream.destroyed) {
          stream.pause();
          const resumeStream = () => {
            res.removeListener("drain", resumeStream);
            res.removeListener("close", resumeStream);
            if (stream && !stream.destroyed) stream.resume();
          };
          res.once("drain", resumeStream);
          res.once("close", resumeStream);
        }
      });

      stream.once("end", () => {
        if (settled) return;
        if (dumper) dumper.end();
        if (!res.writableEnded) res.end();
        if (onResponse) try { onResponse(Buffer.concat(chunks), outHeaders); } catch {}
        cleanup();
      });
    });

    stream.once("error", (e) => {
      err(`[mitm] http2 stream error: ${e.message}`);
      if (dumper) { dumper.writeChunk(`\n[ERROR h2-stream] ${e.message}\n`); dumper.end(); }
      if (!res.headersSent) res.writeHead(502);
      if (!res.writableEnded) res.end();
      cleanup();
    });
  });
}

// Chuyển tiếp HTTPS raw HTTP/1.1 với DNS độc lập và ngắt kết nối tự động khi client huỷ
async function passthroughHttps(req, res, bodyBuffer, headers, targetHost, onResponse, dumper) {
  const targetIP = await resolveTargetIP(targetHost);
  let forwardReq = null;
  let settled = false;

  const onClientClose = () => {
    if (settled) return;
    settled = true;
    if (forwardReq) {
      try { forwardReq.destroy(); } catch {}
    }
  };
  res.once("close", onClientClose);

  forwardReq = https.request({
    hostname: targetIP,
    port: 443,
    path: req.url,
    method: req.method,
    headers,
    servername: targetHost,
    rejectUnauthorized: false
  }, (forwardRes) => {
    if (settled) return;
    res.writeHead(forwardRes.statusCode, forwardRes.headers);
    if (dumper) dumper.writeHeader(forwardRes.statusCode, forwardRes.headers);

    if (!onResponse && !dumper) {
      forwardRes.pipe(res);
      forwardRes.on("end", () => {
        settled = true;
        res.removeListener("close", onClientClose);
      });
      return;
    }

    const chunks = [];
    forwardRes.on("data", chunk => {
      if (settled) return;
      if (dumper) dumper.writeChunk(chunk);
      if (onResponse) chunks.push(chunk);
      const canWrite = res.write(chunk);
      if (!canWrite && forwardRes && !forwardRes.destroyed) {
        forwardRes.pause();
        const resumeReq = () => {
          res.removeListener("drain", resumeReq);
          res.removeListener("close", resumeReq);
          if (forwardRes && !forwardRes.destroyed) forwardRes.resume();
        };
        res.once("drain", resumeReq);
        res.once("close", resumeReq);
      }
    });

    forwardRes.once("end", () => {
      if (settled) return;
      settled = true;
      res.removeListener("close", onClientClose);
      if (dumper) dumper.end();
      if (!res.writableEnded) res.end();
      if (onResponse) try { onResponse(Buffer.concat(chunks), forwardRes.headers); } catch {}
    });
  });

  forwardReq.once("error", (e) => {
    if (settled) return;
    settled = true;
    res.removeListener("close", onClientClose);
    err(`Passthrough error: ${e.message}`);
    if (dumper) { dumper.writeChunk(`\n[ERROR] ${e.message}\n`); dumper.end(); }
    if (!res.headersSent) res.writeHead(502);
    if (!res.writableEnded) res.end("Bad Gateway");
  });

  if (bodyBuffer.length > 0) forwardReq.write(bodyBuffer);
  forwardReq.end();
}

// ── Request handler ───────────────────────────────────────────

const server = https.createServer(sslOptions, async (req, res) => {
  try {
    if (req.url === "/_mitm_health") {
      const mem = process.memoryUsage();
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        ok: true,
        pid: process.pid,
        uptime: Math.round(process.uptime()),
        memory: {
          rssMB: +(mem.rss / 1024 / 1024).toFixed(1),
          heapUsedMB: +(mem.heapUsed / 1024 / 1024).toFixed(1),
          heapTotalMB: +(mem.heapTotal / 1024 / 1024).toFixed(1),
          externalMB: +(mem.external / 1024 / 1024).toFixed(1),
          arrayBuffersMB: +(mem.arrayBuffers / 1024 / 1024).toFixed(1),
        },
        h2Sessions: h2SessionPool.size
      }));
      return;
    }

    const bodyBuffer = await collectBodyRaw(req);
    if (ENABLE_FILE_LOG) dumpRequest(req, bodyBuffer, "raw");

    // Anti-loop: skip requests from 9Router
    if (req.headers[INTERNAL_REQUEST_HEADER.name] === INTERNAL_REQUEST_HEADER.value) {
      return passthrough(req, res, bodyBuffer);
    }

    const tool = getToolForHost(req.headers.host);
    if (!tool) return passthrough(req, res, bodyBuffer);

    // Kiro IDE posts chat to `/` with x-amz-target (not path /generateAssistantResponse)
    if (!isChatRequest(tool, req)) return passthrough(req, res, bodyBuffer);

    // Cursor uses binary proto — model extraction not possible at this layer.
    // Delegate directly to handler which decodes proto internally.
    if (tool === "cursor") {
      return handlers[tool].intercept(req, res, bodyBuffer, null, passthrough);
    }

    const model = extractModel(req.url, bodyBuffer);

    // Intentional passthrough: some models must never be re-routed (e.g. Antigravity
    // tab-autocomplete) so latency-critical inline completion stays native. Silent — this
    // is by design, not a leak, and fires per keystroke. See MODEL_NO_MAP in config.js.
    if (model && (MODEL_NO_MAP[tool] || []).some((re) => re.test(model))) {
      return passthrough(req, res, bodyBuffer);
    }

    const mappedModel = getMappedModel(tool, model);
    if (!mappedModel) {
      return passthrough(req, res, bodyBuffer);
    }

    return handlers[tool].intercept(req, res, bodyBuffer, mappedModel, passthrough);
  } catch (e) {
    if (e.code === "ECONNRESET" || req.destroyed || res.destroyed) {
      // Client dong ket noi som hoac bi huy (keystroke autocomplete cancel)
      return;
    }
    err(`Unhandled error: ${e.message}`);
    if (!res.headersSent) res.writeHead(500, { "Content-Type": "application/json" });
    if (!res.writableEnded) res.end(JSON.stringify({ error: { message: e.message, type: "mitm_error" } }));
  }
});

// Kill only processes LISTENING on LOCAL_PORT (not outbound connections)
function killPort(port) {
  try {
    let pidList = [];
    if (IS_WIN) {
      const psCmd = `powershell -NonInteractive -WindowStyle Hidden -Command ` +
        `"Get-NetTCPConnection -LocalPort ${port} -State Listen -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess"`;
      const out = execSync(psCmd, { encoding: "utf-8", windowsHide: true }).trim();
      if (!out) return;
      pidList = out.split(/\r?\n/).map(s => s.trim()).filter(p => p && Number(p) !== process.pid && Number(p) > 4);
    } else {
      const out = execSync(`${LSOF_BIN} -nP -iTCP:${port} -sTCP:LISTEN -t`, { encoding: "utf-8", windowsHide: true }).trim();
      if (!out) return;
      pidList = out.split("\n").filter(p => p && Number(p) !== process.pid);
    }
    if (pidList.length === 0) return;
    pidList.forEach(pid => {
      try {
        if (IS_WIN) execSync(`taskkill /F /PID ${pid}`, { windowsHide: true });
        else process.kill(Number(pid), "SIGKILL");
      } catch (e) {
        err(`Failed to kill PID ${pid}: ${e.message}`);
      }
    });
    log(`Killed ${pidList.length} process(es) on port ${port}`);
  } catch (e) {
    if (e.status !== 1) throw e;
  }
}

try {
  killPort(LOCAL_PORT);
} catch (e) {
  err(`Cannot kill process on port ${LOCAL_PORT}: ${e.message}`);
  process.exit(1);
}

server.listen(LOCAL_PORT, () => log(`🚀 Server ready on :${LOCAL_PORT}`));

server.on("error", (e) => {
  if (e.code === "EADDRINUSE") err(`Port ${LOCAL_PORT} already in use`);
  else if (e.code === "EACCES") err(`Permission denied for port ${LOCAL_PORT}`);
  else err(e.message);
  process.exit(1);
});

const { removeAllDNSEntriesSync } = require("./dns/dnsConfig");
let isShuttingDown = false;
const shutdown = () => {
  if (isShuttingDown) return;
  isShuttingDown = true;
  // Strip tool hosts from /etc/hosts so other apps aren't broken after exit
  removeAllDNSEntriesSync();
  const forceExit = setTimeout(() => process.exit(0), 1500);
  server.close(() => { clearTimeout(forceExit); process.exit(0); });
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
if (process.platform === "win32") process.on("SIGBREAK", shutdown);
