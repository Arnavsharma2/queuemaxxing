export class QueueError extends Error {
  constructor(status, code, message, details) {
    super(message);
    this.name = "QueueError";
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

export function badRequest(code, message, details) {
  return new QueueError(400, code, message, details);
}

export function notFound(code, message) {
  return new QueueError(404, code, message);
}

export function conflict(code, message) {
  return new QueueError(409, code, message);
}
