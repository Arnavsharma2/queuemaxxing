# Design notes

## Storage

I used an append-only event log because the prompt rules out handing storage to a database or another queue. The on-disk file is `events.ndjson` inside `DATA_DIR`.

For each mutation the server:

1. serializes the event;
2. adds a SHA-256 checksum;
3. writes the complete record to the open log;
4. calls `fsync`;
5. applies the event to the in-memory queue state.

The order matters. A client does not receive a success response for a message that only exists in memory.

Startup verifies the checksum and contiguous sequence number of every event before applying it. A crash can leave the last record incomplete, so recovery truncates an incomplete tail. A complete record with a bad checksum, invalid body, gap, duplicate, or reorder is treated differently: startup fails because silently accepting corruption would make the queue state untrustworthy.

Acknowledged messages remain in the history even though they are removed from the live in-memory view. This keeps recovery simple, but the log will grow. I would add segmented logs, snapshots, and compaction before using this for a long-running high-volume workload.

## Concurrency

One Node process owns the log. HTTP requests may arrive concurrently, but the queue mutation methods do not yield: selection, append, `fsync`, and in-memory application form one critical section. Two claims therefore cannot lease the same message.

This is concurrency within one server, not horizontal scaling. An exclusive PID/token lock prevents two live copies from opening the same directory, while a stale lock left by a crashed process is reclaimed during restart. A distributed version would need replication and leader election rather than shared-file access.

## Ordering

Every message receives a monotonically increasing sequence number.

- Without priority, FIFO sorts sequence ascending and LIFO sorts it descending.
- With priority, the larger priority value wins. Sequence order is only used to break equal-priority ties.
- A queue's `defaultDelayMs` applies unless the message supplies its own `delayMs`. Delayed messages do not enter the candidate set until `availableAt`.

Once several messages have been claimed by different workers, their completion order is outside the queue's control. The ordering guarantee applies to claims, not to completion of the work.

## How are replayed messages handled?

The queue provides at-least-once delivery.

A claim is a lease with an opaque receipt and a visibility deadline. The worker can acknowledge it, release it for retry, or extend it. If the deadline expires, a later claim can lease the same message again. The message ID stays the same, the attempt count increases, and the receipt changes. An old receipt cannot acknowledge the new lease.

Publisher retries are a separate problem. A producer can send an `idempotencyKey`; the queue stores that key with the original message and returns the existing message ID instead of enqueuing a second copy.

The queue cannot promise exactly-once side effects. A worker can finish its external work and crash before sending `ack`. Consumers should make their work idempotent or deduplicate using the message ID. If the consumer owns a database, an inbox/outbox pattern is the safer approach.

## How would this become Pub/Sub?

The append-only log can become a topic log, but acknowledgement can no longer remove a message globally. Each subscription needs its own position.

I would make these changes:

1. Append each published record once to a topic partition.
2. Store a committed offset per subscription and partition.
3. Lease deliveries using `(subscription, partition, offset)`.
4. Track out-of-order acknowledgements until the contiguous committed offset can advance.
5. Let consumer groups share a subscription; separate subscriptions provide fan-out.
6. Delete old log segments only after retention expires or all durable subscriptions have advanced past them.

Priority makes offsets less convenient because delivery no longer follows log order. I would probably use separate priority lanes and a weighted consumer policy instead of pretending one offset can represent an arbitrary priority order.

## What would I add with more time?

My first additions would be operational rather than more queue modes:

1. Dead-letter queues and a configurable maximum attempt count.
2. Segment rotation, snapshots, log compaction, disk limits, and retention settings.
3. Long polling and batch acknowledgement to reduce HTTP overhead.
4. Authentication, per-queue authorization, TLS, and encryption at rest.
5. Prometheus metrics, tracing, disk-pressure health checks, and better structured logs.
6. A timing heap or timing wheel instead of scanning live messages for delays.
7. Crash injection and property-based state-machine tests.
8. Replication and partitioning, if the expected workload justified the extra complexity.

## Why use this instead of SQS, RabbitMQ, or Pulsar?

The useful difference is its small deployment and the way the scheduling options compose. It is one process with one mounted directory, an HTTP API, and no service dependencies. It is easy to run locally or on an isolated machine, and the storage code is short enough to inspect.

That is a narrow reason, not a claim that this is a better general-purpose broker. SQS is a better fit when a managed AWS service and high availability matter. RabbitMQ has a much broader protocol, routing, and plugin ecosystem. Pulsar is designed for replicated, partitioned streaming and fan-out at a scale this project does not attempt.

I would choose this queue for a small self-hosted workload that needs priority plus FIFO/LIFO plus delay and values operational simplicity. I would choose an incumbent when I need clustering, multi-region durability, mature monitoring, support, or a proven throughput envelope.
