import type { JsMsg } from "@nats-io/jetstream";

export type StartPosition = "beginning" | "end";

export interface ConsumerConfig {
  stream: string;
  consumerName: string;
  autoCommit?: boolean;
  autoCommitIntervalMs?: number;
  filterSubjects?: string[];
  /**
   * Called when a terminal error occurs on the underlying ordered consumer,
   * or when an auto-commit fails.
   */
  errorHandler?: (consumer: OffsetConsumer, error: Error) => void;
  startPosition?: StartPosition;
  /**
   * Server-side inactivity threshold of the underlying ephemeral ordered
   * consumer, in milliseconds. After an instance crashes, its ordered consumer
   * (and with it the ownership of the offset entry) is released by the server
   * once this threshold elapses. It is also the grace window during which a
   * fresh ownership claim is considered alive even if its ordered consumer has
   * not been created yet. Default: 30000.
   */
  inactiveThresholdMs?: number;
}

export interface ConsumerMessages extends AsyncIterable<JsMsg> {
  stop(): void;
}

export interface OffsetConsumerInfo {
  /** Last committed stream sequence. */
  seq: number;
  /** RFC3339 time of the last commit. */
  committedAt: string;
  /**
   * Owner prefix of the instance currently holding the entry, or "" when no
   * instance owns it. Ordered consumers created by the owner have names
   * starting with this prefix.
   */
  owner: string;
  /** Hostname of the owning instance, or "". */
  host: string;
  /** RFC3339 time at which the owner claimed the entry, or "". */
  claimedAt: string;
}

export interface OffsetConsumer {
  /**
   * Starts delivering messages from the resolved start position. The returned
   * iterable keeps yielding across seek() restarts and ends on stop() or close().
   */
  consume(opts?: { signal?: AbortSignal }): Promise<ConsumerMessages>;
  /** Persists the last delivered sequence and waits for the KV write to be acknowledged. */
  commitSync(): Promise<void>;
  /**
   * Persists the last delivered sequence asynchronously. Returns immediately;
   * several commits may be pending at once and are applied in call order. The
   * returned promise settles when the KV write is acknowledged.
   */
  commitAsync(): Promise<void>;
  /** Fresh read of the offset entry, or null if it does not exist. */
  info(): Promise<OffsetConsumerInfo | null>;
  /**
   * Repositions the consumer to seq; the next message delivered is seq+1.
   * Consumption is stopped, its ordered consumer deleted, the new offset
   * written and consumption restarted if it was running.
   */
  seek(seq: number): Promise<void>;
  seekToBeginning(): Promise<void>;
  seekToEnd(): Promise<void>;
  /** Removes the offset entry. A later close() will not recreate it. */
  deleteConsumer(): Promise<void>;
  /**
   * Stops consumption, waits (bounded, 5s) for pending async commits, deletes
   * the ordered consumer, and releases ownership of the offset entry with one
   * final write. That write persists the highest sequence passed to a commit
   * call (commits still queued after the wait are abandoned, their promises
   * resolve, and the position is not lost) and, with autoCommit, the last
   * delivered sequence.
   */
  close(): Promise<void>;
}
