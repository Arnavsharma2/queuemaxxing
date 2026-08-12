# Queuemaxxing 💪

A dependency-free HTTP queue that lets FIFO/LIFO ordering, priority, and per-message delay work together. Data is stored in its own checksummed, fsynced append-only log—no database and no delegated broker.

## Run it

Requires Node.js 22 or newer.

```bash
npm start
```

Open [http://localhost:8080](http://localhost:8080) for the included producer/consumer console. Queue data is written to `./data`; override it with `DATA_DIR`.

Or run in Docker with a persistent named volume:

```bash
docker compose up --build
```

## Quick tour

Create a delayed priority LIFO queue:

```bash
curl -sS http://localhost:8080/v1/queues \
  -H 'content-type: application/json' \
  -d '{"name":"jobs","discipline":"lifo","priority":true}'

curl -sS http://localhost:8080/v1/queues/jobs/messages \
  -H 'content-type: application/json' \
  -d '{"payload":{"task":"render"},"priority":50,"delayMs":2000,"idempotencyKey":"render-42"}'

curl -sS http://localhost:8080/v1/queues/jobs/claims \
  -H 'content-type: application/json' \
  -d '{"limit":1,"visibilityTimeoutMs":30000}'
```

Use the returned message ID and receipt to acknowledge it:

```bash
curl -sS http://localhost:8080/v1/queues/jobs/messages/MESSAGE_ID/ack \
  -H 'content-type: application/json' \
  -d '{"receipt":"RECEIPT"}'
```

The executable example creates a priority FIFO queue, publishes three jobs, claims in priority order, and acknowledges them:

```bash
npm run demo
```

## Semantics

- **Combinations:** FIFO, LIFO, priority FIFO, priority LIFO, and delay on any of them.
- **Priority:** larger signed integer first; FIFO/LIFO resolves ties.
- **Delay:** a message is invisible until `availableAt` and does not block ready work.
- **Concurrency:** many HTTP producers/consumers can operate concurrently; claim selection and durable mutation are one synchronous critical section.
- **Delivery:** at least once via receipt-based visibility leases. Unacked messages replay automatically.
- **Durability:** mutation responses follow append + `fsync`; startup verifies every complete record and truncates a torn tail.
- **Scope:** one server process owns one data directory. Do not mount a single directory into multiple instances.

See the [API reference](docs/API.md), the [OpenAPI 3.1 contract](openapi.yaml), and [architecture, replay, Pub/Sub, roadmap, and competitive rationale](docs/ARCHITECTURE.md).

## Verify it

```bash
npm test
npm run test:coverage
```

Tests cover FIFO/LIFO tie-breaking, priority, delay, expiry replay and stale receipts, nacks and lease extension, idempotency, restart recovery, torn writes, corruption detection, concurrent claim uniqueness, the HTTP lifecycle, error shape, and the web console.

## Repository map

```text
src/queue-store.js  durable log + queue state machine
src/app.js          HTTP routing and static console server
src/server.js       process lifecycle
public/             zero-build producer/consumer UI
bin/demo.js         API usage example
test/               state-machine and HTTP tests
docs/               API and design discussion
```

## Limitations

This submission makes durability and semantics explicit, but it is not a distributed broker. The current log needs segmentation/compaction for long-lived high-volume use, `fsync` per mutation caps throughput, and the single process is a single availability domain. The next production milestones are in `docs/ARCHITECTURE.md`.

## License

MIT
