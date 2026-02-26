export class CASConflictError extends Error {
  constructor(cause?: Error) {
    super("Offset was modified externally (CAS conflict)");
    this.name = "CASConflictError";
    this.cause = cause;
  }
}

export class AsyncCommitInFlightError extends Error {
  constructor() {
    super("Async commit already in flight");
    this.name = "AsyncCommitInFlightError";
  }
}

export class InvalidConsumerNameError extends Error {
  constructor(name: string) {
    super(
      `Invalid consumer name: "${name}" — must be non-empty and must not contain '.'`,
    );
    this.name = "InvalidConsumerNameError";
  }
}
