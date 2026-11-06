# JavaScript/TypeScript API Contract: Offset Consumer Library

**Package**: `@synadia-io/offset-consumer`
**Dependencies**: `@nats-io/jetstream` ^3.4.0, `@nats-io/kv` ^3.4.0
**Peer Dependencies**: `@nats-io/nats-core` ^3.4.0

---

## Construction

```typescript
/**
 * Ensures the offset-consumers KV bucket exists, reads any stored offset,
 * resolves the start sequence and claims ownership of the offset entry.
 * Does NOT start consuming — call consume() for that.
 *
 * @throws {InvalidConsumerNameError} if consumerName is empty or contains '.'
 * @throws {AlreadyInUseError} if another live instance owns the offset entry
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
   * Starts delivering messages. The returned iterable keeps yielding across
   * seek() restarts and ends on stop() or close(). stop() also deletes the
   * ordered consumer.
   */
  consume(opts?: { signal?: AbortSignal }): Promise<ConsumerMessages>;

  /** Persists the last delivered sequence and waits for the acknowledgement. */
  commitSync(): Promise<void>;

  /**
   * Returns immediately; the write is chained behind pending writes so commits
   * are applied in call order. Settles when acknowledged.
   */
  commitAsync(): Promise<void>;

  /** Fresh read of the offset entry, or null if it does not exist. */
  info(): Promise<OffsetConsumerInfo | null>;

  /**
   * Stops consumption, deletes its ordered consumer, writes the new offset and
   * restarts consumption under a new generation prefix if it was running.
   * After seek(N) the next message is N+1. If the write fails the consumer is
   * left stopped with its position unchanged.
   */
  seek(seq: number): Promise<void>;
  seekToBeginning(): Promise<void>;
  seekToEnd(): Promise<void>;

  /** Removes the offset entry; a later close() will not recreate it. */
  deleteConsumer(): Promise<void>;

  /**
   * Waits (5s bound) for pending commits (leftovers are abandoned, their
   * promises resolve), deletes the ordered consumer, and releases ownership
   * with one final write that persists the highest sequence passed to a commit
   * call and, with autoCommit, the last delivered sequence.
   */
  close(): Promise<void>;
}

export interface ConsumerMessages extends AsyncIterable<JsMsg> { stop(): void; }

export interface OffsetConsumerInfo {
  seq: number;
  committedAt: string;
  owner: string;      // "" when unowned
  host: string;
  claimedAt: string;
}
```

---

## ConsumerConfig

```typescript
export interface ConsumerConfig {
  stream: string;
  consumerName: string;                 // non-empty, no '.'
  autoCommit?: boolean;                 // default false
  autoCommitIntervalMs?: number;        // default 5000
  filterSubjects?: string[];
  errorHandler?: (consumer: OffsetConsumer, error: Error) => void;
  startPosition?: "beginning" | "end";  // default "beginning"
  inactiveThresholdMs?: number;         // ordered consumer inactive threshold and claim grace window; default 30000
}
```

The client recreates ordered consumers with a 5-minute inactive threshold; the library re-applies `inactiveThresholdMs` after every `ordered_consumer_recreated` event.

---

## Error Types

```typescript
export class InvalidConsumerNameError extends Error {}

/** Another live instance owns the offset entry. Thrown by createConsumer. */
export class AlreadyInUseError extends Error {
  readonly owner: string;
  readonly host: string;
  readonly claimedAt: string;
}
```

Ordered consumers created by an instance are named `oc-{stream}-{consumer}-{nuid}-g{generation}_{serial}`.
