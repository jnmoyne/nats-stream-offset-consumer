/**
 * Thrown by createConsumer when the consumer name is empty, contains
 * characters other than letters, digits, '-', '_' and '=', or is too long
 * together with the stream name.
 */
export class InvalidConsumerNameError extends Error {
  constructor(name: string, reason = "use letters, digits, '-', '_' and '=' only") {
    super(`Invalid consumer name "${name}": ${reason}`);
    this.name = "InvalidConsumerNameError";
  }
}

/**
 * Thrown by createConsumer when the stream name cannot be used in the offset
 * entry's KV key or the ordered consumer names: it is empty or contains
 * characters other than letters, digits, '-', '_' and '='.
 */
export class InvalidStreamNameError extends Error {
  constructor(name: string, reason = "use letters, digits, '-', '_' and '=' only") {
    super(`Invalid stream name "${name}": ${reason}`);
    this.name = "InvalidStreamNameError";
  }
}

/**
 * Reported to the errorHandler when the stream was deleted and recreated
 * since the offset was stored (detected at startup or while consuming). The
 * stored offset refers to the old stream, so the consumer repositions itself
 * according to startPosition, like Kafka's auto.offset.reset. It is
 * informational: consumption continues.
 */
export class StreamRecreatedError extends Error {
  /** The stored (or last delivered) sequence, which referred to the old stream. */
  readonly storedSeq: number;

  constructor(storedSeq: number) {
    super(`offset consumer: stream was recreated, offset reset to the start position (stored sequence ${storedSeq})`);
    this.name = "StreamRecreatedError";
    this.storedSeq = storedSeq;
  }
}

/**
 * The offset entry for the stream and consumer name is owned by another live
 * instance (a consumer whose name starts with its owner prefix exists on the
 * stream). Thrown by createConsumer when that is visible at once, and by
 * consume() when its claim finds the owner alive (now or after the liveness
 * wait) or loses the CAS write to another owner. close() rejects with it when
 * another instance claimed the entry meanwhile: the release write is skipped
 * and the entry left untouched.
 */
export class AlreadyInUseError extends Error {
  /** Owner prefix recorded in the KV entry. */
  readonly owner: string;
  /** Hostname recorded by the owning instance. */
  readonly host: string;

  constructor(owner: string, host: string, context?: string) {
    const message = `Offset consumer already in use by another instance (owner "${owner}" on host "${host}")`;
    super(context ? `${context}: ${message}` : message);
    this.name = "AlreadyInUseError";
    this.owner = owner;
    this.host = host;
  }
}

/** Internal: an operation was attempted on a closed consumer. */
export class ConsumerClosedError extends Error {
  constructor() {
    super("offset consumer: consumer is closed");
    this.name = "ConsumerClosedError";
  }
}
