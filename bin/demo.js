#!/usr/bin/env node

const baseUrl = (process.env.QUEUE_URL ?? "http://localhost:8080").replace(/\/$/, "");
const queueName = process.argv[2] ?? "orders";

async function call(path, options = {}) {
  const response = await fetch(`${baseUrl}${path}`, {
    ...options,
    headers: { "content-type": "application/json", ...options.headers },
  });
  const body = await response.json();
  if (!response.ok) throw new Error(`${response.status}: ${JSON.stringify(body)}`);
  return body;
}

try {
  console.log(`Creating priority FIFO queue '${queueName}'...`);
  await call("/v1/queues", {
    method: "POST",
    body: JSON.stringify({ name: queueName, discipline: "fifo", priority: true }),
  }).catch((error) => {
    if (!error.message.includes("QUEUE_EXISTS")) throw error;
  });

  console.log("Publishing normal, urgent, and delayed jobs...");
  await Promise.all([
    call(`/v1/queues/${queueName}/messages`, { method: "POST", body: JSON.stringify({ payload: { job: "normal" }, priority: 1 }) }),
    call(`/v1/queues/${queueName}/messages`, { method: "POST", body: JSON.stringify({ payload: { job: "urgent" }, priority: 10 }) }),
    call(`/v1/queues/${queueName}/messages`, { method: "POST", body: JSON.stringify({ payload: { job: "later" }, priority: 100, delayMs: 5_000 }) }),
  ]);

  const claimed = await call(`/v1/queues/${queueName}/claims`, {
    method: "POST",
    body: JSON.stringify({ limit: 2, visibilityTimeoutMs: 30_000 }),
  });
  console.log("Claim order:", claimed.messages.map((message) => message.payload.job).join(" → "));

  for (const message of claimed.messages) {
    await call(`/v1/queues/${queueName}/messages/${message.id}/ack`, {
      method: "POST",
      body: JSON.stringify({ receipt: message.receipt }),
    });
  }
  console.log("Acknowledged both jobs. The delayed job becomes claimable in five seconds.");
} catch (error) {
  console.error(error.message);
  process.exitCode = 1;
}
