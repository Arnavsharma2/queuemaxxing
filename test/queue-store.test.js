import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, test } from "node:test";

import { QueueStore } from "../src/queue-store.js";

const directories = [];

function storeAt(clock = () => Date.now()) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "queuemaxxing-"));
  directories.push(directory);
  return { directory, store: new QueueStore({ dataDir: directory, clock }).open() };
}

afterEach(() => {
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});

test("FIFO and LIFO queues resolve equal-priority messages in configured order", () => {
  for (const discipline of ["fifo", "lifo"]) {
    const { store } = storeAt(() => 1_000);
    store.createQueue(discipline, { discipline });
    store.enqueue(discipline, { payload: "first" });
    store.enqueue(discipline, { payload: "second" });
    const values = store.claim(discipline, { limit: 2 }).map((message) => message.payload);
    assert.deepEqual(values, discipline === "fifo" ? ["first", "second"] : ["second", "first"]);
    store.close();
  }
});

test("priority is primary and FIFO/LIFO is the tie breaker", () => {
  for (const discipline of ["fifo", "lifo"]) {
    const { store } = storeAt(() => 1_000);
    store.createQueue(`mixed-${discipline}`, { discipline, priority: true });
    store.enqueue(`mixed-${discipline}`, { payload: "low", priority: 1 });
    store.enqueue(`mixed-${discipline}`, { payload: "high-old", priority: 9 });
    store.enqueue(`mixed-${discipline}`, { payload: "high-new", priority: 9 });
    const expected = discipline === "fifo" ? ["high-old", "high-new", "low"] : ["high-new", "high-old", "low"];
    assert.deepEqual(store.claim(`mixed-${discipline}`, { limit: 3 }).map((message) => message.payload), expected);
    store.close();
  }
});

test("delay, priority, and LIFO compose without head-of-line blocking", () => {
  let now = 1_000;
  const { store } = storeAt(() => now);
  store.createQueue("frankenstein", { discipline: "lifo", priority: true });
  store.enqueue("frankenstein", { payload: "delayed-high", priority: 100, delayMs: 500 });
  store.enqueue("frankenstein", { payload: "ready-old", priority: 5 });
  store.enqueue("frankenstein", { payload: "ready-new", priority: 5 });
  assert.deepEqual(store.claim("frankenstein", { limit: 2 }).map((message) => message.payload), ["ready-new", "ready-old"]);
  now = 1_500;
  assert.equal(store.claim("frankenstein").at(0).payload, "delayed-high");
  store.close();
});

test("a queue-level delay applies by default and a message can override it", () => {
  let now = 2_000;
  const { store } = storeAt(() => now);
  store.createQueue("delay-queue", { discipline: "lifo", priority: true, defaultDelayMs: 500 });
  assert.equal(store.describeQueue("delay-queue").defaultDelayMs, 500);
  store.enqueue("delay-queue", { payload: "uses-default", priority: 10 });
  store.enqueue("delay-queue", { payload: "override", priority: 1, delayMs: 0 });
  assert.equal(store.claim("delay-queue").at(0).payload, "override");
  now += 500;
  assert.equal(store.claim("delay-queue").at(0).payload, "uses-default");
  store.close();
});

test("delayed messages remain unavailable until their timestamp", () => {
  let now = 5_000;
  const { store } = storeAt(() => now);
  store.createQueue("delay");
  store.enqueue("delay", { payload: "not-yet", delayMs: 2_000, priority: 100 });
  store.enqueue("delay", { payload: "now", priority: 1 });
  assert.equal(store.claim("delay").at(0).payload, "now");
  assert.deepEqual(store.claim("delay"), []);
  now = 7_000;
  assert.equal(store.claim("delay").at(0).payload, "not-yet");
  store.close();
});

test("unacknowledged messages replay with a new receipt after visibility timeout", () => {
  let now = 100;
  const { store } = storeAt(() => now);
  store.createQueue("replay", { defaultVisibilityTimeoutMs: 50 });
  store.enqueue("replay", { payload: { job: 1 } });
  const first = store.claim("replay").at(0);
  assert.equal(first.attempts, 1);
  now = 149;
  assert.deepEqual(store.claim("replay"), []);
  now = 150;
  const expiredView = store.peek("replay").at(0);
  assert.equal(expiredView.state, "queued");
  assert.equal(expiredView.receipt, undefined);
  const replay = store.claim("replay").at(0);
  assert.equal(replay.id, first.id);
  assert.notEqual(replay.receipt, first.receipt);
  assert.equal(replay.attempts, 2);
  assert.throws(() => store.ack("replay", first.id, first.receipt), /stale, invalid/);
  assert.equal(store.ack("replay", replay.id, replay.receipt).acknowledged, true);
  store.close();
});

test("durable log restores queues, delayed data, claims, and idempotency", () => {
  let now = 10_000;
  const { directory, store } = storeAt(() => now);
  store.createQueue("durable", { discipline: "lifo", priority: true, defaultVisibilityTimeoutMs: 100 });
  const published = store.enqueue("durable", { payload: "keep-me", priority: 3, delayMs: 50, idempotencyKey: "request-42" });
  store.close();

  const recovered = new QueueStore({ dataDir: directory, clock: () => now }).open();
  assert.equal(recovered.describeQueue("durable").stats.delayed, 1);
  assert.deepEqual(recovered.enqueue("durable", { payload: "different", idempotencyKey: "request-42" }), {
    message: published.message,
    duplicate: true,
  });
  now += 50;
  assert.equal(recovered.claim("durable").at(0).payload, "keep-me");
  recovered.close();
});

test("only one store process can own a data directory at a time", () => {
  const { directory, store } = storeAt();
  store.createQueue("exclusive");
  assert.throws(
    () => new QueueStore({ dataDir: directory }).open(),
    /already in use by process/,
  );
  store.close();

  const reopened = new QueueStore({ dataDir: directory }).open();
  assert.equal(reopened.describeQueue("exclusive").name, "exclusive");
  reopened.close();
});

test("a stale process lock is reclaimed during restart", () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "queuemaxxing-"));
  directories.push(directory);
  fs.writeFileSync(path.join(directory, ".queuemaxxing.lock"), JSON.stringify({
    pid: 2_147_483_647,
    token: "stale",
    createdAt: 0,
  }));

  const store = new QueueStore({ dataDir: directory }).open();
  store.createQueue("recovered");
  store.close();
  assert.equal(fs.existsSync(path.join(directory, ".queuemaxxing.lock")), false);
});

test("recovery discards a torn final write but rejects corruption", () => {
  const { directory, store } = storeAt();
  store.createQueue("safe");
  store.close();
  const logPath = path.join(directory, "events.ndjson");
  fs.appendFileSync(logPath, '{"body":"partial');
  const recovered = new QueueStore({ dataDir: directory }).open();
  assert.equal(recovered.listQueues().length, 1);
  recovered.close();
  fs.appendFileSync(logPath, '{"body":"{}","checksum":"wrong"}\n');
  assert.throws(() => new QueueStore({ dataDir: directory }).open(), /checksum mismatch/);
});

test("one message is never delivered twice across simultaneous claims", async () => {
  const { store } = storeAt();
  store.createQueue("concurrent");
  for (let index = 0; index < 200; index += 1) store.enqueue("concurrent", { payload: index });

  const batches = await Promise.all(Array.from({ length: 40 }, async () => {
    await new Promise((resolve) => setImmediate(resolve));
    return store.claim("concurrent", { limit: 5 });
  }));
  const ids = batches.flat().map((message) => message.id);
  assert.equal(ids.length, 200);
  assert.equal(new Set(ids).size, 200);
  assert.equal(store.describeQueue("concurrent").stats.inFlight, 200);
  store.close();
});

test("nack delays retry and touch extends a lease", () => {
  let now = 1_000;
  const { store } = storeAt(() => now);
  store.createQueue("leases", { defaultVisibilityTimeoutMs: 100 });
  store.enqueue("leases", { payload: "x" });
  let message = store.claim("leases").at(0);
  store.touch("leases", message.id, message.receipt, { visibilityTimeoutMs: 500 });
  now += 100;
  assert.deepEqual(store.claim("leases"), []);
  store.nack("leases", message.id, message.receipt, { delayMs: 100 });
  assert.deepEqual(store.claim("leases"), []);
  now += 100;
  message = store.claim("leases").at(0);
  assert.equal(message.attempts, 2);
  store.close();
});
