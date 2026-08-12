# Architecture and design notes

## Storage and durability

Queuemaxxing owns its storage. It does not call a database, broker, or managed queue. Every mutation becomes an immutable event in `events.ndjson`:

1. Serialize the complete event to JSON.
2. Wrap it with a SHA-256 checksum.
3. Append it to the open log file.
4. Call `fsync` before exposing the change in memory or returning success.
5. Apply the event to the in-memory materialized view.

On startup, events are checksummed and replayed in order. An incomplete final record—possible if power is lost during a write—is truncated. A corrupt complete record fails startup loudly rather than silently losing or inventing state.

This is intentionally a single-node design. The Node process is the serialization boundary: synchronous commit sections mean interleaved HTTP requests cannot claim the same message. Multiple consumers and producers may use one server concurrently, but two server processes must not share a data directory. Production evolution would add an OS file lock first, then segment logs and replicate them with Raft.

### Tradeoffs

- `fsync` per operation favors durability over peak throughput. Batch commit/group fsync would improve throughput.
- The event log retains acknowledged messages and idempotency keys, creating a complete audit trail but growing without bound. Snapshotting and compaction are the first operational follow-up.
- The in-memory index makes claims fast, while recovery time and memory scale with historical and live message counts respectively.
- JSON lines make the submission inspectable. A production format would use length-prefixed binary records with versioning.

## Ordering semantics

Only currently available messages participate in ordering. Delayed messages do not block ready messages.

For a queue with priority disabled, sequence number is sorted ascending for FIFO or descending for LIFO. With priority enabled, higher integer priority sorts first; sequence is the FIFO/LIFO tie-breaker. This precisely supports:

- FIFO
- LIFO
- priority FIFO
- priority LIFO
- delayed versions of all four

Strict global order is not promised once multiple consumers have claimed messages: processing and acknowledgement may complete in any order.

## Replay and delivery guarantee

Delivery is **at least once**:

- `claim` atomically leases a message and returns an opaque receipt.
- `ack` deletes the live message only when the current receipt matches.
- A worker may `touch` to extend long-running work, or `nack` to retry now/later.
- If a worker crashes, its visibility timeout expires. The next claim durably releases and re-leases the message with the same message ID, a new receipt, and an incremented attempt counter.
- A stale worker cannot acknowledge a newer delivery because its old receipt is rejected.
- Publisher retries can use an idempotency key. The mapping is durable, so the same queue/key never produces two messages, including after acknowledgement or restart.

Exactly-once side effects are impossible for a general HTTP queue to guarantee: a consumer can perform its side effect and crash before `ack`. Consumers should deduplicate on message ID or make their side effect idempotent. A transactionally coupled outbox/inbox is the robust option when the consumer controls a database.

## Refactoring into Pub/Sub

The storage log already resembles a topic. I would keep the append-only record as the source of truth and replace destructive queue acknowledgement with independent subscription cursors:

1. `POST /topics/:topic/messages` appends once to a partition.
2. Each subscription stores its filter, delivery policy, and committed offset per partition.
3. Claims lease `(subscription, partition, offset)` instead of globally leasing the message.
4. Acknowledgements advance a subscription's contiguous committed frontier; sparse acks remain in a small bitmap/set.
5. Retention becomes time/size based and a segment is collectible only after every durable subscription passes it (or its retention deadline does).
6. Consumer groups share one subscription and divide partitions; fan-out consumers use different subscriptions.

Priority complicates a replicated log because consuming out of offset order prevents simple compaction. I would either make priority a subscription policy backed by per-priority indexes, or expose priority lanes as separate topics and let consumers choose weighted fairness.

## More time

In order of likely value:

1. Dead-letter policies (`maxAttempts`, DLQ target) and redrive.
2. Log segments, snapshots, online compaction, disk quotas, and retention controls.
3. Group commit and an indexed timing wheel/heap for high-throughput delays.
4. Authentication, per-queue authorization, TLS, encryption at rest, and audit events.
5. Prometheus metrics, OpenTelemetry traces, structured logs, admin endpoints, and disk-pressure health checks.
6. Long polling, server-sent notifications, batch ack/nack, scheduled recurrence, and queue pause/purge.
7. Replication and leader election, followed by partitioning for horizontal scale.
8. Property-based/state-machine tests, crash-fault injection, benchmarks, and compatibility/version migration tests.

## Why choose it?

Choose Queuemaxxing when the useful product is a small, self-contained, embeddable durable queue—not an infrastructure program:

- One process, one mounted directory, zero runtime dependencies.
- One understandable HTTP/JSON API and a built-in operator console.
- FIFO/LIFO, priorities, and per-message delays compose rather than living in separate product modes.
- The log is human-inspectable and the durability mechanism is small enough to audit.
- Local/on-prem/edge deployments keep message data under the user's control.

Do **not** choose it today for multi-region availability, massive fan-out, protocol ecosystem, enterprise operations, or proven high throughput. SQS, RabbitMQ, and Pulsar are mature incumbents with replication, monitoring, security, SDKs, support, and years of failure testing. The honest wedge here is simplicity and composable scheduling on one node; the architecture section above is a roadmap, not a claim of parity.
