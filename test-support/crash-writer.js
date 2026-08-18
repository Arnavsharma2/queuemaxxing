import { QueueStore } from "../src/queue-store.js";

const store = new QueueStore({ dataDir: process.env.DATA_DIR }).open();
store.createQueue("crash-test");
store.enqueue("crash-test", { payload: { durable: true } });
process.send?.({ ready: true });

setInterval(() => {}, 60_000);
