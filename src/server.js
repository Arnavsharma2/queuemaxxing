import path from "node:path";

import { createApp } from "./app.js";
import { QueueStore } from "./queue-store.js";

const port = Number(process.env.PORT ?? 8080);
const host = process.env.HOST ?? "0.0.0.0";
const dataDir = path.resolve(process.env.DATA_DIR ?? "./data");

if (!Number.isSafeInteger(port) || port < 1 || port > 65535) {
  throw new Error("PORT must be an integer between 1 and 65535");
}

const store = new QueueStore({ dataDir }).open();
const server = createApp(store);

server.listen(port, host, () => {
  console.log(`Queuemaxxing listening on http://${host}:${port}`);
  console.log(`Durable event log: ${dataDir}`);
});

function shutdown(signal) {
  console.log(`${signal} received; draining HTTP connections`);
  server.close(() => {
    store.close();
    process.exit(0);
  });
  setTimeout(() => {
    store.close();
    process.exit(1);
  }, 10_000).unref();
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
