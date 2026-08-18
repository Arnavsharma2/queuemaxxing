#!/usr/bin/env node

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";

import { QueueStore } from "../src/queue-store.js";

const count = Number(process.argv[2] ?? 250);
if (!Number.isSafeInteger(count) || count < 1 || count > 10_000) {
  throw new Error("message count must be an integer between 1 and 10000");
}

const directory = fs.mkdtempSync(path.join(os.tmpdir(), "queuemaxxing-benchmark-"));
let store;

function rate(operations, milliseconds) {
  return Math.round(operations / (milliseconds / 1_000));
}

try {
  store = new QueueStore({ dataDir: directory }).open();
  store.createQueue("benchmark", { discipline: "fifo", priority: true });

  let startedAt = performance.now();
  for (let index = 0; index < count; index += 1) {
    store.enqueue("benchmark", { payload: { index }, priority: index % 10 });
  }
  const publishMs = performance.now() - startedAt;

  startedAt = performance.now();
  const claimed = [];
  while (claimed.length < count) {
    claimed.push(...store.claim("benchmark", { limit: Math.min(100, count - claimed.length) }));
  }
  const claimMs = performance.now() - startedAt;

  startedAt = performance.now();
  for (const message of claimed) store.ack("benchmark", message.id, message.receipt);
  const ackMs = performance.now() - startedAt;

  store.close();
  store = undefined;

  startedAt = performance.now();
  store = new QueueStore({ dataDir: directory }).open();
  const recoveryMs = performance.now() - startedAt;

  console.log(`Durable single-process benchmark (${count} messages)`);
  console.log(`publish  ${rate(count, publishMs).toLocaleString()} msg/s  (${publishMs.toFixed(1)} ms)`);
  console.log(`claim    ${rate(count, claimMs).toLocaleString()} msg/s  (${claimMs.toFixed(1)} ms)`);
  console.log(`ack      ${rate(count, ackMs).toLocaleString()} msg/s  (${ackMs.toFixed(1)} ms)`);
  console.log(`recovery ${recoveryMs.toFixed(1)} ms`);
  console.log("Each publish and acknowledgement includes an fsync; claims fsync once per batch.");
} finally {
  store?.close();
  fs.rmSync(directory, { recursive: true, force: true });
}
