import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

import { badRequest, conflict, notFound } from "./errors.js";

const NAME_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$/;
const DEFAULT_VISIBILITY_TIMEOUT_MS = 30_000;
const MAX_VISIBILITY_TIMEOUT_MS = 12 * 60 * 60 * 1_000;
const MAX_DELAY_MS = 14 * 24 * 60 * 60 * 1_000;
const MAX_BATCH_SIZE = 100;
const LOCK_FILE = ".queuemaxxing.lock";

function checksum(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function encodeRecord(event) {
  const body = JSON.stringify(event);
  return `${JSON.stringify({ body, checksum: checksum(body) })}\n`;
}

function asInteger(value, field, { min = Number.MIN_SAFE_INTEGER, max = Number.MAX_SAFE_INTEGER } = {}) {
  if (!Number.isSafeInteger(value) || value < min || value > max) {
    throw badRequest("INVALID_ARGUMENT", `${field} must be an integer between ${min} and ${max}`);
  }
  return value;
}

function publicMessage(message) {
  return {
    id: message.id,
    payload: message.payload,
    priority: message.priority,
    enqueuedAt: message.enqueuedAt,
    availableAt: message.availableAt,
    attempts: message.attempts,
    ...(message.receipt ? {
      receipt: message.receipt,
      leaseExpiresAt: message.leaseExpiresAt,
    } : {}),
  };
}

export class QueueStore {
  constructor({ dataDir, clock = () => Date.now() }) {
    if (!dataDir) throw new Error("dataDir is required");
    this.dataDir = path.resolve(dataDir);
    this.logPath = path.join(this.dataDir, "events.ndjson");
    this.lockPath = path.join(this.dataDir, LOCK_FILE);
    this.clock = clock;
    this.queues = new Map();
    this.idempotency = new Map();
    this.sequence = 0;
    this.eventSequence = 0;
    this.fd = undefined;
    this.lockToken = undefined;
  }

  open() {
    fs.mkdirSync(this.dataDir, { recursive: true, mode: 0o700 });
    this.#acquireLock();
    try {
      this.#recover();
      this.fd = fs.openSync(this.logPath, "a", 0o600);
    } catch (error) {
      this.#releaseLock();
      throw error;
    }
    return this;
  }

  close() {
    let closeError;
    if (this.fd !== undefined) {
      const fd = this.fd;
      this.fd = undefined;
      try {
        fs.fsyncSync(fd);
      } catch (error) {
        closeError = error;
      }
      try {
        fs.closeSync(fd);
      } catch (error) {
        closeError ??= error;
      }
    }
    try {
      this.#releaseLock();
    } catch (error) {
      closeError ??= error;
    }
    if (closeError) throw closeError;
  }

  createQueue(name, options = {}) {
    this.#validateName(name);
    if (this.queues.has(name)) throw conflict("QUEUE_EXISTS", `Queue '${name}' already exists`);

    const discipline = options.discipline ?? "fifo";
    if (discipline !== "fifo" && discipline !== "lifo") {
      throw badRequest("INVALID_DISCIPLINE", "discipline must be 'fifo' or 'lifo'");
    }
    const priority = options.priority ?? false;
    if (typeof priority !== "boolean") throw badRequest("INVALID_PRIORITY", "priority must be a boolean");
    const defaultVisibilityTimeoutMs = asInteger(
      options.defaultVisibilityTimeoutMs ?? DEFAULT_VISIBILITY_TIMEOUT_MS,
      "defaultVisibilityTimeoutMs",
      { min: 1, max: MAX_VISIBILITY_TIMEOUT_MS },
    );
    const defaultDelayMs = asInteger(
      options.defaultDelayMs ?? 0,
      "defaultDelayMs",
      { min: 0, max: MAX_DELAY_MS },
    );

    const event = this.#event("queue.created", {
      name,
      discipline,
      priority,
      defaultDelayMs,
      defaultVisibilityTimeoutMs,
      createdAt: this.clock(),
    });
    this.#commit([event]);
    return this.describeQueue(name);
  }

  listQueues() {
    return [...this.queues.keys()].sort().map((name) => this.describeQueue(name));
  }

  describeQueue(name) {
    const queue = this.#queue(name);
    const now = this.clock();
    return {
      name: queue.name,
      discipline: queue.discipline,
      priority: queue.priority,
      defaultDelayMs: queue.defaultDelayMs,
      defaultVisibilityTimeoutMs: queue.defaultVisibilityTimeoutMs,
      createdAt: queue.createdAt,
      stats: this.#stats(queue, now),
    };
  }

  enqueue(name, { payload, priority = 0, delayMs, idempotencyKey } = {}) {
    const queue = this.#queue(name);
    if (payload === undefined) throw badRequest("PAYLOAD_REQUIRED", "payload is required");
    const payloadBytes = Buffer.byteLength(JSON.stringify(payload));
    if (payloadBytes > 256 * 1024) {
      throw badRequest("PAYLOAD_TOO_LARGE", "payload may not exceed 256 KiB");
    }
    priority = asInteger(priority, "priority", { min: -1_000_000, max: 1_000_000 });
    delayMs = asInteger(delayMs ?? queue.defaultDelayMs, "delayMs", { min: 0, max: MAX_DELAY_MS });
    if (idempotencyKey !== undefined && (typeof idempotencyKey !== "string" || idempotencyKey.length < 1 || idempotencyKey.length > 128)) {
      throw badRequest("INVALID_IDEMPOTENCY_KEY", "idempotencyKey must be a string between 1 and 128 characters");
    }

    const dedupeKey = idempotencyKey === undefined ? undefined : `${name}\0${idempotencyKey}`;
    const existingId = dedupeKey === undefined ? undefined : this.idempotency.get(dedupeKey);
    if (existingId) {
      const existing = queue.messages.get(existingId);
      return { message: existing ? publicMessage(existing) : { id: existingId }, duplicate: true };
    }

    const now = this.clock();
    const event = this.#event("message.enqueued", {
      queue: name,
      message: {
        id: crypto.randomUUID(),
        payload,
        priority,
        sequence: ++this.sequence,
        enqueuedAt: now,
        availableAt: now + delayMs,
        attempts: 0,
        state: "queued",
        receipt: null,
        leaseExpiresAt: null,
        idempotencyKey: idempotencyKey ?? null,
      },
    });
    this.#commit([event]);
    return { message: publicMessage(event.message), duplicate: false };
  }

  claim(name, { limit = 1, visibilityTimeoutMs } = {}) {
    const queue = this.#queue(name);
    limit = asInteger(limit, "limit", { min: 1, max: MAX_BATCH_SIZE });
    visibilityTimeoutMs = asInteger(
      visibilityTimeoutMs ?? queue.defaultVisibilityTimeoutMs,
      "visibilityTimeoutMs",
      { min: 1, max: MAX_VISIBILITY_TIMEOUT_MS },
    );
    const now = this.clock();
    const events = this.#releaseExpiredEvents(queue, now);
    const releasedIds = new Set(events.map((event) => event.id));
    const candidates = [...queue.messages.values()].filter((message) =>
      (message.state === "queued" || releasedIds.has(message.id)) && message.availableAt <= now,
    );

    candidates.sort((a, b) => {
      if (queue.priority && a.priority !== b.priority) return b.priority - a.priority;
      return queue.discipline === "fifo" ? a.sequence - b.sequence : b.sequence - a.sequence;
    });

    const selected = candidates.slice(0, limit);
    for (const message of selected) {
      events.push(this.#event("message.claimed", {
        queue: name,
        id: message.id,
        receipt: crypto.randomUUID(),
        leaseExpiresAt: now + visibilityTimeoutMs,
        claimedAt: now,
      }));
    }
    if (events.length > 0) this.#commit(events);
    return selected.map((message) => publicMessage(queue.messages.get(message.id)));
  }

  ack(name, id, receipt) {
    const queue = this.#queue(name);
    const message = this.#leasedMessage(queue, id, receipt);
    const event = this.#event("message.acked", { queue: name, id, receipt, ackedAt: this.clock() });
    this.#commit([event]);
    return { id: message.id, acknowledged: true };
  }

  nack(name, id, receipt, { delayMs = 0 } = {}) {
    const queue = this.#queue(name);
    this.#leasedMessage(queue, id, receipt);
    delayMs = asInteger(delayMs, "delayMs", { min: 0, max: MAX_DELAY_MS });
    const now = this.clock();
    const event = this.#event("message.released", {
      queue: name,
      id,
      receipt,
      availableAt: now + delayMs,
      reason: "nack",
      releasedAt: now,
    });
    this.#commit([event]);
    return publicMessage(queue.messages.get(id));
  }

  touch(name, id, receipt, { visibilityTimeoutMs } = {}) {
    const queue = this.#queue(name);
    this.#leasedMessage(queue, id, receipt);
    visibilityTimeoutMs = asInteger(
      visibilityTimeoutMs ?? queue.defaultVisibilityTimeoutMs,
      "visibilityTimeoutMs",
      { min: 1, max: MAX_VISIBILITY_TIMEOUT_MS },
    );
    const event = this.#event("message.touched", {
      queue: name,
      id,
      receipt,
      leaseExpiresAt: this.clock() + visibilityTimeoutMs,
    });
    this.#commit([event]);
    return publicMessage(queue.messages.get(id));
  }

  peek(name, { limit = 20 } = {}) {
    const queue = this.#queue(name);
    limit = asInteger(limit, "limit", { min: 1, max: MAX_BATCH_SIZE });
    const now = this.clock();
    return [...queue.messages.values()]
      .sort((a, b) => a.sequence - b.sequence)
      .slice(0, limit)
      .map((message) => {
        const view = publicMessage(message);
        const leaseExpired = message.state === "leased" && message.leaseExpiresAt <= now;
        if (leaseExpired) {
          delete view.receipt;
          delete view.leaseExpiresAt;
        }
        return {
          ...view,
          state: leaseExpired ? "queued" : message.state,
          delayed: message.availableAt > now,
        };
      });
  }

  #event(type, data) {
    return { eventSequence: ++this.eventSequence, type, ...data };
  }

  #commit(events) {
    if (this.fd === undefined) throw new Error("QueueStore is not open");
    const data = Buffer.from(events.map(encodeRecord).join(""), "utf8");
    let offset = 0;
    while (offset < data.length) {
      const written = fs.writeSync(this.fd, data, offset, data.length - offset);
      if (written === 0) throw new Error("Unable to append to the event log");
      offset += written;
    }
    fs.fsyncSync(this.fd);
    for (const event of events) this.#apply(event);
  }

  #apply(event) {
    this.eventSequence = Math.max(this.eventSequence, event.eventSequence ?? 0);
    switch (event.type) {
      case "queue.created": {
        if (!this.queues.has(event.name)) {
          this.queues.set(event.name, {
            name: event.name,
            discipline: event.discipline,
            priority: event.priority,
            defaultDelayMs: event.defaultDelayMs ?? 0,
            defaultVisibilityTimeoutMs: event.defaultVisibilityTimeoutMs,
            createdAt: event.createdAt,
            messages: new Map(),
          });
        }
        break;
      }
      case "message.enqueued": {
        const queue = this.queues.get(event.queue);
        if (!queue || queue.messages.has(event.message.id)) break;
        queue.messages.set(event.message.id, { ...event.message });
        this.sequence = Math.max(this.sequence, event.message.sequence);
        if (event.message.idempotencyKey) {
          this.idempotency.set(`${event.queue}\0${event.message.idempotencyKey}`, event.message.id);
        }
        break;
      }
      case "message.claimed": {
        const message = this.queues.get(event.queue)?.messages.get(event.id);
        if (!message) break;
        message.state = "leased";
        message.receipt = event.receipt;
        message.leaseExpiresAt = event.leaseExpiresAt;
        message.attempts += 1;
        break;
      }
      case "message.acked": {
        const queue = this.queues.get(event.queue);
        const message = queue?.messages.get(event.id);
        if (!message) break;
        queue.messages.delete(event.id);
        break;
      }
      case "message.released": {
        const message = this.queues.get(event.queue)?.messages.get(event.id);
        if (!message) break;
        message.state = "queued";
        message.receipt = null;
        message.leaseExpiresAt = null;
        message.availableAt = event.availableAt;
        break;
      }
      case "message.touched": {
        const message = this.queues.get(event.queue)?.messages.get(event.id);
        if (!message) break;
        message.leaseExpiresAt = event.leaseExpiresAt;
        break;
      }
      default:
        throw new Error(`Unknown event type '${event.type}'`);
    }
  }

  #releaseExpiredEvents(queue, now) {
    const events = [];
    for (const message of queue.messages.values()) {
      if (message.state === "leased" && message.leaseExpiresAt <= now) {
        events.push(this.#event("message.released", {
          queue: queue.name,
          id: message.id,
          receipt: message.receipt,
          availableAt: now,
          reason: "visibility_timeout",
          releasedAt: now,
        }));
      }
    }
    return events;
  }

  #leasedMessage(queue, id, receipt) {
    if (typeof receipt !== "string" || receipt.length === 0) {
      throw badRequest("RECEIPT_REQUIRED", "receipt is required");
    }
    const message = queue.messages.get(id);
    if (!message) throw notFound("MESSAGE_NOT_FOUND", `Message '${id}' does not exist`);
    if (message.state !== "leased" || message.receipt !== receipt || message.leaseExpiresAt <= this.clock()) {
      throw conflict("INVALID_RECEIPT", "The receipt is stale, invalid, or its visibility timeout has expired");
    }
    return message;
  }

  #queue(name) {
    this.#validateName(name);
    const queue = this.queues.get(name);
    if (!queue) throw notFound("QUEUE_NOT_FOUND", `Queue '${name}' does not exist`);
    return queue;
  }

  #validateName(name) {
    if (typeof name !== "string" || !NAME_PATTERN.test(name)) {
      throw badRequest("INVALID_QUEUE_NAME", "queue name must match /^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$/");
    }
  }

  #stats(queue, now) {
    const stats = { total: queue.messages.size, available: 0, delayed: 0, inFlight: 0 };
    for (const message of queue.messages.values()) {
      if (message.state === "leased" && message.leaseExpiresAt > now) stats.inFlight += 1;
      else if (message.availableAt > now) stats.delayed += 1;
      else stats.available += 1;
    }
    return stats;
  }

  #acquireLock() {
    const token = crypto.randomUUID();
    const owner = JSON.stringify({ pid: process.pid, token, createdAt: Date.now() });

    for (let attempt = 0; attempt < 3; attempt += 1) {
      let lockFd;
      try {
        lockFd = fs.openSync(this.lockPath, "wx", 0o600);
      } catch (error) {
        if (error.code !== "EEXIST") throw error;

        let existing;
        try {
          existing = JSON.parse(fs.readFileSync(this.lockPath, "utf8"));
        } catch {
          throw new Error(`Queue data directory '${this.dataDir}' has an unreadable lock file`);
        }
        if (Number.isSafeInteger(existing.pid) && this.#processExists(existing.pid)) {
          throw new Error(`Queue data directory '${this.dataDir}' is already in use by process ${existing.pid}`);
        }
        try {
          fs.unlinkSync(this.lockPath);
        } catch (unlinkError) {
          if (unlinkError.code !== "ENOENT") throw unlinkError;
        }
        continue;
      }

      try {
        fs.writeFileSync(lockFd, owner, "utf8");
        fs.fsyncSync(lockFd);
      } catch (error) {
        try {
          fs.unlinkSync(this.lockPath);
        } catch (unlinkError) {
          if (unlinkError.code !== "ENOENT") error.cause = unlinkError;
        }
        throw error;
      } finally {
        fs.closeSync(lockFd);
      }
      this.lockToken = token;
      return;
    }
    throw new Error(`Unable to acquire the lock for queue data directory '${this.dataDir}'`);
  }

  #releaseLock() {
    if (this.lockToken === undefined) return;
    try {
      const existing = JSON.parse(fs.readFileSync(this.lockPath, "utf8"));
      if (existing.token === this.lockToken) fs.unlinkSync(this.lockPath);
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    } finally {
      this.lockToken = undefined;
    }
  }

  #processExists(pid) {
    try {
      process.kill(pid, 0);
      return true;
    } catch (error) {
      return error.code !== "ESRCH";
    }
  }

  #recover() {
    if (!fs.existsSync(this.logPath)) return;
    const data = fs.readFileSync(this.logPath);
    let validBytes = 0;
    let cursor = 0;
    while (cursor < data.length) {
      const newline = data.indexOf(0x0a, cursor);
      if (newline === -1) break;
      const line = data.subarray(cursor, newline).toString("utf8");
      cursor = newline + 1;
      if (line.length === 0) {
        validBytes = cursor;
        continue;
      }
      let record;
      try {
        record = JSON.parse(line);
      } catch (error) {
        throw new Error(`Corrupt event log at byte ${validBytes}: ${error.message}`);
      }
      if (typeof record.body !== "string" || record.checksum !== checksum(record.body)) {
        throw new Error(`Event log checksum mismatch at byte ${validBytes}`);
      }
      this.#apply(JSON.parse(record.body));
      validBytes = cursor;
    }
    if (validBytes !== data.length) {
      fs.truncateSync(this.logPath, validBytes);
    }
  }
}
