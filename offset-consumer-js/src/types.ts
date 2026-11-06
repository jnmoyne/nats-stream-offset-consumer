import type { JsMsg } from "@nats-io/jetstream";

export type StartPosition = "beginning" | "end";

export interface ConsumerConfig {
  /** Name of the JetStream stream to consume: letters, digits, '-', '_' and '=' only. */
  stream: string;
  /**
   * Logical name of this consumer: letters, digits, '-', '_' and '=' only.
   * The stream and consumer names together must not exceed 196 characters
   * (the server limits consumer names to 255, and the ordered consumer names
   * embed both).
   */
  consumerName: string;
  /**
   * Timer-based background commits of the last processed message (one the
   * application has moved past by asking the iterator for the next message).
   */
  autoCommit?: boolean;
  autoCommitIntervalMs?: number;
  filterSubjects?: string[];
  /**
   * Called when a terminal error occurs on the underlying ordered consumer,
   * when an auto-commit fails, or (informational, a StreamRecreatedError)
   * when the stream was found to have been recreated. It may call close().
   */
  errorHandler?: (consumer: OffsetConsumer, error: Error) => void;
  /**
   * Where a new consumer (no stored offset) begins. Default "beginning".
   * "end" is resolved to the stream's last sequence when the consumer is
   * created, and the claim at the first consume() stores that position, so
   * messages published in between, or while the consumer is down, are not
   * skipped. It also applies when the stream is found to have been recreated.
   */
  startPosition?: StartPosition;
  /**
   * Server-side inactivity threshold, in milliseconds, of the instance's
   * ephemeral ordered consumers. After an instance crashes, the server
   * deletes its ordered consumer once this threshold elapses, and another
   * instance can then take the offset entry over (after the liveness wait,
   * see consume()). Default: 30000.
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
  /**
   * Identifies the incarnation of the stream the offset refers to: the
   * stream's creation time in Unix nanoseconds, as a decimal string. "" for
   * entries written by older versions.
   */
  streamCreated: string;
}

export interface OffsetConsumer {
  /**
   * Claims ownership of the offset entry and starts delivering messages from
   * the resolved start position: a position recorded by seek() before, else
   * the stored offset, else startPosition. The claim runs every time
   * consumption starts from a stopped state. If the entry names another
   * instance, it is alive while a consumer whose name starts with its owner
   * prefix exists; if none exists, the claim waits 5s (measured locally)
   * and lists again, then writes with a CAS
   * against the revision read before the wait. The returned promise rejects
   * with AlreadyInUseError when the owner is alive or wrote the entry
   * during the wait; nothing is started then.
   * The returned iterable keeps yielding across seek() restarts and ends on
   * stop() or close(). A yielded message counts as processed (for autoCommit
   * and close()) once the application asks the iterator for the next one.
   */
  consume(opts?: { signal?: AbortSignal }): Promise<ConsumerMessages>;
  /**
   * Persists the last delivered sequence and waits for the KV write to be
   * acknowledged. Called while handling a message, it includes that message.
   * Before the first consume() it does nothing.
   */
  commitSync(): Promise<void>;
  /**
   * Persists the last delivered sequence asynchronously. Returns immediately;
   * several commits may be pending at once and are applied in call order. The
   * returned promise settles when the KV write is acknowledged. Before the
   * first consume() it does nothing.
   */
  commitAsync(): Promise<void>;
  /** Fresh read of the offset entry, or null if it does not exist. */
  info(): Promise<OffsetConsumerInfo | null>;
  /**
   * Repositions the consumer to seq; the next message delivered is seq+1.
   * While consuming, delivery is stopped, the new offset written, a new
   * ordered consumer started under the next generation prefix, and only then
   * the previous one deleted, so the instance stays visible on the server
   * throughout (if the write or the restart fails, the consumer is left
   * stopped). While not consuming, the position is only recorded: the claim
   * at the next consume() stores it.
   */
  seek(seq: number): Promise<void>;
  /** Repositions the consumer to the beginning of the stream, regardless of startPosition. */
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
   * processed sequence. The message currently yielded to the application is
   * not committed by close() and is redelivered on the next start.
   * The release write is conditional: if the entry no longer names this
   * instance as its owner (another instance claimed it after this one's
   * ordered consumer expired), it is left untouched and close() rejects
   * with AlreadyInUseError; if the entry no longer exists, nothing is
   * written. Before the first consume() close() writes nothing.
   */
  close(): Promise<void>;
}
