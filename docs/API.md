# HTTP API

All request and response bodies are JSON. Errors have the shape `{"error":{"code","message","requestId"}}`.

## Queues

### `POST /v1/queues`

```json
{
  "name": "orders",
  "discipline": "fifo",
  "priority": true,
  "defaultVisibilityTimeoutMs": 30000
}
```

`discipline` is `fifo` or `lifo`. `priority` defaults to `false`.

### `GET /v1/queues`

Lists queues, configuration, and live counts.

### `GET /v1/queues/:name`

Describes one queue.

## Messages

### `POST /v1/queues/:name/messages`

```json
{
  "payload": { "orderId": "o_123" },
  "priority": 10,
  "delayMs": 5000,
  "idempotencyKey": "checkout-o_123"
}
```

Payload may be any JSON value up to 256 KiB. Higher integer priority is processed first when the queue enables priority. Delay is between zero and 14 days. An idempotency key is scoped to a queue and retained permanently in this version.

### `GET /v1/queues/:name/messages?limit=20`

Administrative peek used by the console. It does not claim messages.

### `POST /v1/queues/:name/claims`

```json
{ "limit": 10, "visibilityTimeoutMs": 60000 }
```

Returns zero or more available messages. Each has a `receipt` required by acknowledgement operations. Limit is 1–100.

### `POST /v1/queues/:name/messages/:id/ack`

```json
{ "receipt": "opaque-claim-receipt" }
```

### `POST /v1/queues/:name/messages/:id/nack`

```json
{ "receipt": "opaque-claim-receipt", "delayMs": 1000 }
```

Releases a claim and optionally delays its next delivery.

### `POST /v1/queues/:name/messages/:id/touch`

```json
{ "receipt": "opaque-claim-receipt", "visibilityTimeoutMs": 60000 }
```

Extends a valid claim from the current time.

## Health

`GET /healthz` returns `{"status":"ok"}`.
