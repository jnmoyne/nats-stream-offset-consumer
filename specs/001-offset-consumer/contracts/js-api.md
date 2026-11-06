# JavaScript/TypeScript API Contract: Offset Consumer Library

**Package**: `@synadia-io/offset-consumer`
**Dependencies**: `@nats-io/jetstream` ^3.4.0, `@nats-io/kv` ^3.4.0
**Peer Dependencies**: `@nats-io/nats-core` ^3.4.0

---

## Construction

```typescript
/**
 * Ensures the offset-consumers KV bucket exists (an existing bucket is opened
 * as is, never overwritten; its replicas are only raised, best effort, to the
 * stream's), resolves startPosition ("beginning" -> 0, "end" -> the stream's
 * last sequence, used by the claim when there is no usable stored offset),
 * and reads the stored entry: if it names an owner and a consumer whose name
 * starts with that owner prefix exists on the stream (one listing, no wait),
 * throws AlreadyInUseError. Writes nothing and creates nothing: ownership is
 * claimed by consume().
 * Does NOT start consuming — call consume() for that.
 *
 * @throws {InvalidStreamNameError} if stream is empty or not [A-Za-z0-9_=-]+
 * @throws {InvalidConsumerNameError} if consumerName is not [A-Za-z0-9_=-]+, or
 *         stream.length + consumerName.length > 196
 * @throws {AlreadyInUseError} if another instance visibly owns the offset entry
 */
export async function createConsumer(nc: NatsConnection, config: ConsumerConfig): Promise<OffsetConsumer>;

/** All offset consumers of a stream (KV keys "{stream}.*"); empty Map if none. */
export async function listConsumers(nc: NatsConnection, stream: string): Promise<Map<string, OffsetConsumerInfo>>;
```

---

## OffsetConsumer Interface

```typescript
export interface OffsetConsumer {
  /**
   * Claims ownership of the offset entry (see "Claim" below), then starts
   * delivering messages. The claim runs inside consume() before its promise
   * resolves, every time consumption starts from a stopped state; claim
   * failures (AlreadyInUseError, KV/stream errors) reject the promise and
   * nothing is started. The returned iterable keeps yielding across seek()
   * restarts and ends on stop() or close(). stop() also deletes the ordered
   * consumer. A yielded message counts as processed once the application
   * asks the iterator for the next one.
   *
   * @throws {AlreadyInUseError} if another instance owns the offset entry
   */
  consume(opts?: { signal?: AbortSignal }): Promise<ConsumerMessages>;

  /**
   * Persists the last delivered sequence and waits for the acknowledgement.
   * Called while handling a message, it includes that message. Before the
   * first claim it does nothing (resolves, writes nothing).
   */
  commitSync(): Promise<void>;

  /**
   * Returns immediately; the write is chained behind pending writes so commits
   * are applied in call order. Settles when acknowledged. Before the first
   * claim it does nothing (resolves, writes nothing).
   */
  commitAsync(): Promise<void>;

  /** Fresh read of the offset entry, or null if it does not exist. */
  info(): Promise<OffsetConsumerInfo | null>;

  /**
   * After seek(N) the next message is N+1. While consuming (make before
   * break): stops delivery without deleting the server-side ordered consumer,
   * writes the new offset, starts the next generation, then deletes the old
   * server-side consumer in all cases (also when the write or the restart
   * failed; then the consumer is left stopped, and after a failed write its
   * position is unchanged). The instance therefore always has a consumer on
   * the server while consuming. While not consuming, the position is only
   * recorded (no write); the next claim stores it.
   */
  seek(seq: number): Promise<void>;
  /** Seeks to the beginning of the stream, regardless of startPosition. */
  seekToBeginning(): Promise<void>;
  seekToEnd(): Promise<void>;

  /** Removes the offset entry; a later close() will not recreate it. */
  deleteConsumer(): Promise<void>;

  /**
   * Stops the auto-commit timer (without waiting for it), waits (5s bound)
   * for pending commits (leftovers are abandoned, their promises resolve),
   * deletes the ordered consumer, and releases ownership with one final write
   * that persists the highest sequence passed to a commit call and, with
   * autoCommit, the last processed sequence. The message currently yielded to
   * the application is not committed (it is redelivered on the next start).
   * The release is conditional: the entry is read; missing -> nothing is
   * written; owner differs (another instance claimed it after this one's
   * ordered consumer expired) -> nothing is written and close() rejects with
   * AlreadyInUseError ("release skipped, the entry is no longer owned by this
   * instance", carrying the entry's owner/host); own -> CAS update
   * against the read revision, re-read and retried on a revision conflict
   * (at most 3 attempts in total, then rejects with "failed to release offset
   * entry"). The ordered consumer is deleted in every case. Before the first
   * claim close() writes nothing.
   */
  close(): Promise<void>;
}

export interface ConsumerMessages extends AsyncIterable<JsMsg> { stop(): void; }

export interface OffsetConsumerInfo {
  seq: number;
  committedAt: string;
  owner: string;      // "" when unowned
  host: string;
  streamCreated: string; // stream creation time, decimal Unix nanoseconds; "" for older entries
}
```

consume(), seek(), stop() and close() are serialized by one lifecycle lock (FIFO), so at most one ordered consumer is active even under concurrent seeks.

---

## ConsumerConfig

```typescript
export interface ConsumerConfig {
  stream: string;                       // [A-Za-z0-9_=-]+
  consumerName: string;                 // [A-Za-z0-9_=-]+; stream + consumer <= 196 chars
  autoCommit?: boolean;                 // default false; commits the last processed sequence
  autoCommitIntervalMs?: number;        // default 5000
  filterSubjects?: string[];
  errorHandler?: (consumer: OffsetConsumer, error: Error) => void; // may call close()
  startPosition?: "beginning" | "end";  // default "beginning"; resolved at creation, stored by the claim
  inactiveThresholdMs?: number;         // ordered consumer inactive threshold (crash-recovery delay); default 30000
}
```

The client recreates ordered consumers with a 5-minute inactive threshold; the library re-applies `inactiveThresholdMs` after every `ordered_consumer_recreated` event.

Auto-commit failures are reported as `Error("offset consumer: auto-commit failed: ...")` with the original error as `cause`; a commit rejected because the consumer is closed is not reported.

---

## Ownership and liveness

- **Liveness.** Another instance named as owner is alive while a consumer whose name starts with its owner prefix exists on the stream. No clocks are compared; the KV value has no `claimed_at` (older entries carrying it are read fine, the field is ignored).
- **Claim** (in consume(), within the lifecycle lock), up to 3 rounds: read the entry (revision r); if it names another owner: list consumers, a match -> `AlreadyInUseError`; else wait `internals.livenessWaitMs` (5000) measured locally (no jitter: the CAS decides between concurrent starters), list again, a match -> `AlreadyInUseError`. Stream info; recreated = the stored identity differs (or a legacy entry is past the stream's end). Position: a seek recorded while not consuming -> that position; entry missing or recreated -> the startPosition resolved at construction; else the stored seq/committed_at. CAS write (create if missing, update at r) with owner/host/stream_created, chained in the write pipeline with the commits. Success -> claimed, positions set to seq, `StreamRecreatedError` reported if recreated. CAS conflict -> re-read: another owner -> `AlreadyInUseError`; empty or own owner (a release, this instance's late commit) -> next round; after 3 rounds rejects with "failed to claim offset entry". Then the ordered consumer is started.
- **Close order.** Ordered consumer deleted, then the conditional release write (skipped before the first claim and when this instance deleted the entry; nothing is written when the entry is missing or owned by another instance).
- **Close order.** Ordered consumer deleted, then the conditional release write (skipped when this instance deleted the entry; nothing is written when the entry is missing or owned by another instance).

---

## Stream recreation

Every KV write carries `stream_created`, the stream's creation time as decimal Unix nanoseconds (converted exactly from the RFC3339 `created` of the stream info with BigInt; omitted when unknown). It is byte-identical to the other ports.

- At the claim, an entry is from an earlier incarnation if both identities are set and differ, or (legacy entry without identity) if its seq is past the stream's last sequence.
- While consuming, the `ordered_consumer_recreated`, `consumer_deleted`, `consumer_not_found`, `stream_not_found` and `heartbeats_missed` statuses start (at most one at a time) a watcher polling stream info every second until the stream exists again; a message whose `info.consumer` differs from the previous one's triggers the same check before it is yielded. On a new identity, deliveries are dropped, a `StreamRecreatedError` is reported and the consumer seeks to its start position on the new stream. The new identity is applied atomically with that seek write: commits queued before it carry the old identity, commits after it the new one.

---

## Error Types

```typescript
export class InvalidConsumerNameError extends Error {}
export class InvalidStreamNameError extends Error {}

/**
 * Another live instance owns the offset entry. Thrown by createConsumer and
 * consume(); close() rejects with it when another instance claimed the entry
 * meanwhile.
 */
export class AlreadyInUseError extends Error {
  readonly owner: string;
  readonly host: string;
}

/** Informational, reported to the errorHandler: the offset was reset to the start position. */
export class StreamRecreatedError extends Error {
  readonly storedSeq: number;   // the sequence that referred to the old stream
}
```

The owner prefix is `oc-{stream}-{consumer}-{nuid}`; ordered consumers created by an instance are named `oc-{stream}-{consumer}-{nuid}-g{generation}_{serial}`.
