import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { after, before, test } from "node:test";

import { createApp } from "../src/app.js";
import { QueueStore } from "../src/queue-store.js";

const directory = fs.mkdtempSync(path.join(os.tmpdir(), "queuemaxxing-http-"));
const store = new QueueStore({ dataDir: directory }).open();
const server = createApp(store, { logger: {} });
let baseUrl;

before(async () => {
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  baseUrl = `http://127.0.0.1:${server.address().port}`;
});

after(async () => {
  server.closeIdleConnections();
  server.closeAllConnections();
  await new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  store.close();
  fs.rmSync(directory, { recursive: true, force: true });
});

async function request(route, method = "GET", body) {
  const response = await fetch(`${baseUrl}${route}`, {
    method,
    headers: { "content-type": "application/json" },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  return { response, body: await response.json() };
}

test("HTTP lifecycle creates, publishes, claims, and acknowledges", async () => {
  let result = await request("/v1/queues", "POST", { name: "api", discipline: "fifo", priority: true });
  assert.equal(result.response.status, 201);
  result = await request("/v1/queues/api/messages", "POST", { payload: { hello: "world" }, priority: 4 });
  assert.equal(result.response.status, 201);
  result = await request("/v1/queues/api/claims", "POST", { limit: 1 });
  const message = result.body.messages.at(0);
  assert.deepEqual(message.payload, { hello: "world" });
  result = await request(`/v1/queues/api/messages/${message.id}/ack`, "POST", { receipt: message.receipt });
  assert.equal(result.body.acknowledged, true);
});

test("concurrent HTTP producers and consumers never duplicate a delivery", async () => {
  const messageCount = 60;
  let result = await request("/v1/queues", "POST", {
    name: "http-concurrency",
    discipline: "fifo",
    priority: true,
  });
  assert.equal(result.response.status, 201);

  const published = await Promise.all(Array.from({ length: messageCount }, (_, index) =>
    request("/v1/queues/http-concurrency/messages", "POST", {
      payload: { index },
      priority: index % 5,
    })));
  assert.ok(published.every(({ response }) => response.status === 201));

  const workerResults = await Promise.all(Array.from({ length: 12 }, () =>
    request("/v1/queues/http-concurrency/claims", "POST", { limit: 5 })));
  const claimed = workerResults.flatMap(({ body }) => body.messages);
  const ids = claimed.map((message) => message.id);
  assert.equal(claimed.length, messageCount);
  assert.equal(new Set(ids).size, messageCount);

  const acknowledgements = await Promise.all(claimed.map((message) =>
    request(`/v1/queues/http-concurrency/messages/${message.id}/ack`, "POST", {
      receipt: message.receipt,
    })));
  assert.ok(acknowledgements.every(({ response }) => response.status === 200));

  result = await request("/v1/queues/http-concurrency");
  assert.deepEqual(result.body.queue.stats, { total: 0, available: 0, delayed: 0, inFlight: 0 });
});

test("HTTP errors are structured and include request IDs", async () => {
  const result = await request("/v1/queues/missing/claims", "POST", {});
  assert.equal(result.response.status, 404);
  assert.equal(result.body.error.code, "QUEUE_NOT_FOUND");
  assert.equal(typeof result.body.error.requestId, "string");
});

test("HTTP endpoints reject non-object JSON bodies as bad requests", async () => {
  const response = await fetch(`${baseUrl}/v1/queues`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: "null",
  });
  const body = await response.json();
  assert.equal(response.status, 400);
  assert.equal(body.error.code, "INVALID_BODY");
});

test("operator console is served", async () => {
  const response = await fetch(`${baseUrl}/`);
  assert.equal(response.status, 200);
  assert.match(await response.text(), /<h1>Queuemaxxing<\/h1>/);
});
