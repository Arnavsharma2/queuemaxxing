import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";

import { QueueError } from "./errors.js";

const PUBLIC_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../public");
const BODY_LIMIT = 300 * 1024;

function json(response, status, body, headers = {}) {
  const content = JSON.stringify(body);
  response.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(content),
    "cache-control": "no-store",
    ...headers,
  });
  response.end(content);
}

async function readJson(request) {
  const chunks = [];
  let length = 0;
  for await (const chunk of request) {
    length += chunk.length;
    if (length > BODY_LIMIT) throw new QueueError(413, "BODY_TOO_LARGE", "Request body is too large");
    chunks.push(chunk);
  }
  if (length === 0) return {};
  let body;
  try {
    body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw new QueueError(400, "INVALID_JSON", "Request body must be valid JSON");
  }
  if (body === null || Array.isArray(body) || typeof body !== "object") {
    throw new QueueError(400, "INVALID_BODY", "Request body must be a JSON object");
  }
  return body;
}

function decode(value) {
  try {
    return decodeURIComponent(value);
  } catch {
    throw new QueueError(400, "INVALID_PATH", "Path contains invalid URL encoding");
  }
}

function serveStatic(request, response, pathname) {
  const requested = pathname === "/" ? "index.html" : pathname.slice(1);
  if (!/^[a-zA-Z0-9._/-]+$/.test(requested) || requested.includes("..")) return false;
  const filePath = path.resolve(PUBLIC_DIR, requested);
  if (!filePath.startsWith(`${PUBLIC_DIR}${path.sep}`) && filePath !== path.join(PUBLIC_DIR, "index.html")) return false;
  if (!fs.existsSync(filePath) || !fs.statSync(filePath).isFile()) return false;
  const extension = path.extname(filePath);
  const contentTypes = {
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".svg": "image/svg+xml",
  };
  const data = fs.readFileSync(filePath);
  response.writeHead(200, {
    "content-type": contentTypes[extension] ?? "application/octet-stream",
    "content-length": data.length,
    "cache-control": "no-cache",
  });
  if (request.method === "HEAD") response.end();
  else response.end(data);
  return true;
}

export function createApp(store, { logger = console } = {}) {
  return http.createServer(async (request, response) => {
    const startedAt = Date.now();
    const requestId = request.headers["x-request-id"] || crypto.randomUUID();
    response.setHeader("x-request-id", requestId);

    try {
      const url = new URL(request.url, "http://localhost");
      const { pathname } = url;
      let match;

      if ((request.method === "GET" || request.method === "HEAD") && !pathname.startsWith("/v1/")) {
        if (serveStatic(request, response, pathname)) return;
      }

      if (request.method === "GET" && pathname === "/healthz") {
        return json(response, 200, { status: "ok" });
      }
      if (request.method === "GET" && pathname === "/v1/queues") {
        return json(response, 200, { queues: store.listQueues() });
      }
      if (request.method === "POST" && pathname === "/v1/queues") {
        const body = await readJson(request);
        const queue = store.createQueue(body.name, body);
        return json(response, 201, { queue }, { location: `/v1/queues/${encodeURIComponent(queue.name)}` });
      }
      if (request.method === "GET" && (match = pathname.match(/^\/v1\/queues\/([^/]+)$/))) {
        return json(response, 200, { queue: store.describeQueue(decode(match[1])) });
      }
      if (request.method === "POST" && (match = pathname.match(/^\/v1\/queues\/([^/]+)\/messages$/))) {
        const name = decode(match[1]);
        const body = await readJson(request);
        const result = store.enqueue(name, body);
        return json(response, result.duplicate ? 200 : 201, result);
      }
      if (request.method === "GET" && (match = pathname.match(/^\/v1\/queues\/([^/]+)\/messages$/))) {
        const name = decode(match[1]);
        const limit = url.searchParams.has("limit") ? Number(url.searchParams.get("limit")) : undefined;
        return json(response, 200, { messages: store.peek(name, { limit }) });
      }
      if (request.method === "POST" && (match = pathname.match(/^\/v1\/queues\/([^/]+)\/claims$/))) {
        const name = decode(match[1]);
        const body = await readJson(request);
        return json(response, 200, { messages: store.claim(name, body) });
      }
      if (request.method === "POST" && (match = pathname.match(/^\/v1\/queues\/([^/]+)\/messages\/([^/]+)\/(ack|nack|touch)$/))) {
        const [, rawName, rawId, action] = match;
        const name = decode(rawName);
        const id = decode(rawId);
        const body = await readJson(request);
        if (action === "ack") return json(response, 200, store.ack(name, id, body.receipt));
        if (action === "nack") return json(response, 200, { message: store.nack(name, id, body.receipt, body) });
        return json(response, 200, { message: store.touch(name, id, body.receipt, body) });
      }

      throw new QueueError(404, "ROUTE_NOT_FOUND", "No route matches this request");
    } catch (error) {
      const status = error instanceof QueueError ? error.status : 500;
      if (status === 500) logger.error?.(error);
      json(response, status, {
        error: {
          code: error instanceof QueueError ? error.code : "INTERNAL_ERROR",
          message: status === 500 ? "An internal error occurred" : error.message,
          ...(error instanceof QueueError && error.details !== undefined ? { details: error.details } : {}),
          requestId,
        },
      });
    } finally {
      logger.info?.(`${request.method} ${request.url} ${response.statusCode} ${Date.now() - startedAt}ms request_id=${requestId}`);
    }
  });
}
