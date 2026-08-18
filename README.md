# Queuemaxxing

This is a small HTTP queue built for the Queuemaxxing take-home prompt. A queue can be FIFO or LIFO, can optionally sort by priority, and can define a default delivery delay. A message may override that delay.

The server owns its storage. It does not use Redis, SQLite, another queue, or any external service. Changes are appended to a checksummed log and synced to disk before the API reports success.

## Run it

Node.js 22 or newer is required. There are no npm dependencies.

```bash
npm start
```

The API and the example web client will be available at [http://localhost:8080](http://localhost:8080). Data is stored in `./data` by default. Set `DATA_DIR` to use a different directory.

Docker works too:

```bash
docker compose up --build
```

The Compose file mounts a named volume at `/data`, so restarting the container does not remove queued messages.

## Queue behavior

Queue configuration is set when the queue is created:

| `discipline` | `priority` | Result |
| --- | --- | --- |
| `fifo` | `false` | FIFO |
| `lifo` | `false` | LIFO |
| `fifo` | `true` | Priority first, FIFO for ties |
| `lifo` | `true` | Priority first, LIFO for ties |

`defaultDelayMs` is set on the queue. An optional message-level `delayMs` overrides it. A delayed message is ignored until its availability time, so a delayed high-priority message does not block ready messages.

## API example

Create a priority LIFO queue:

```bash
curl -sS http://localhost:8080/v1/queues \
  -H 'content-type: application/json' \
  -d '{"name":"jobs","discipline":"lifo","priority":true,"defaultDelayMs":2000}'
```

Publish a high-priority message. It will use the queue's two-second delay:

```bash
curl -sS http://localhost:8080/v1/queues/jobs/messages \
  -H 'content-type: application/json' \
  -d '{"payload":{"task":"render"},"priority":50}'
```

Claim one available message:

```bash
curl -sS http://localhost:8080/v1/queues/jobs/claims \
  -H 'content-type: application/json' \
  -d '{"limit":1,"visibilityTimeoutMs":30000}'
```

Claims return a message ID and receipt. A successful worker acknowledges the message with both values:

```bash
curl -sS http://localhost:8080/v1/queues/jobs/messages/MESSAGE_ID/ack \
  -H 'content-type: application/json' \
  -d '{"receipt":"RECEIPT"}'
```

The full API is documented in [docs/API.md](docs/API.md) and [openapi.yaml](openapi.yaml).

## Requirement map

| Prompt requirement | Implementation | Executable evidence |
| --- | --- | --- |
| FIFO or LIFO | Monotonic enqueue sequence, sorted in the configured direction | Ordering matrix in `test/queue-store.test.js` |
| Priority | Priority is the primary key; FIFO/LIFO breaks ties | Mixed-priority ordering tests |
| Delay | Queue default with a per-message override; unavailable messages never block ready work | Composed delay + priority + LIFO test |
| Durable across restarts | Checksummed, sequenced append-only log; every accepted mutation is `fsync`ed | Clean-restart, torn-write, corruption, and `SIGKILL` recovery tests |
| No database or queue | Storage is implemented with Node's file APIs in `src/queue-store.js` | Zero runtime dependencies |
| Concurrency | A synchronous commit section atomically selects, persists, and applies claims | Concurrent HTTP producer/consumer test verifies 60 unique deliveries |
| Simple application | Browser console and CLI demo use the same public HTTP API | `npm start`, then `npm run demo` |

## Example client

The page served at `/` is a client of the public HTTP API. It can create queues, publish immediate or delayed messages, claim work, acknowledge it, and release it for retry. There is also a command-line example:

```bash
npm run demo
```

## Delivery and persistence

Claims use a visibility timeout. If a worker disappears without acknowledging its message, the message becomes eligible for another claim. This gives at-least-once delivery. The next claim gets a new receipt, so a slow worker cannot acknowledge a newer delivery by accident.

Each state change is written as a checksummed event. The server appends the whole record, calls `fsync`, and only then updates the in-memory view and returns success. Startup rebuilds the view by replaying the log. An incomplete final write is truncated; checksum failure in a complete record stops startup.

This design supports concurrent HTTP producers and consumers in one server process. The synchronous commit section serializes competing claims. It is not a clustered queue; an exclusive process lock prevents two server processes from accidentally sharing the same data directory, and stale locks are reclaimed after a process crash.

## Tests

```bash
npm test
npm run test:coverage
```

The tests cover all four ordering modes, delayed availability, visibility-timeout replay, stale receipts, explicit retry, lease extension, idempotent publishing, restart recovery, torn writes, corruption detection, concurrent claims, and the HTTP client flow.

Two tests target the highest-risk claims directly: one runs concurrent producers and consumers through the HTTP boundary, and another kills a writer with `SIGKILL` before reopening the same data directory.

For a reproducible local performance sample:

```bash
npm run benchmark -- 1000
```

The benchmark reports publish, claim, acknowledgement, and restart-replay timings. It intentionally keeps durability enabled, so each publish and acknowledgement includes an `fsync`.

## Design questions

The requested answers about message replay, a Pub/Sub version, additional features, and comparison with SQS, RabbitMQ, and Pulsar are in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Known limits

- One process and one disk are a single failure domain.
- The log is not compacted, so acknowledged messages remain in its history.
- Calling `fsync` for every mutation favors durability over throughput.
- Authentication, quotas, metrics, and dead-letter queues are not implemented.

Those are deliberate limits for this version rather than claims of production parity with an established broker.
