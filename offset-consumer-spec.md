# NATS JetStream Offset Consumer Library — Specification

**Version:** 1.1-draft  
**Target Languages:** Go, Java, JavaScript/TypeScript, Rust  
**Date:** 2026-11-06

---

## 1. Overview

The Offset Consumer library provides Kafka-style consumer semantics on top of NATS JetStream, implemented entirely on the client side. It gives application developers a familiar "durable consumer" experience — the ability to consume a stream from a persisted position and resume after restart — without relying on JetStream's server-side durable consumer mechanism.

### 1.1 Core Mechanics

Under the covers the library:

1. Uses a **JetStream ephemeral ordered consumer** as the message transport.
2. Persists the **last committed stream sequence number** in a **NATS KV bucket** (`offset-consumers`).
3. On startup, reads the persisted offset from the KV, **claims ownership** of the entry, and creates a new ephemeral ordered consumer starting at `stored_seq + 1`.
4. On first use (no KV key exists), starts from sequence 1 (the beginning of the stream) or, if configured, from the end of the stream.

This approach avoids server-side durable consumer lifecycle management while still providing durable, resumable consumption.

### 1.2 Quality of Service

**At-least-once delivery.** Any messages delivered after the last committed offset will be redelivered after a restart. The application callback is expected to be idempotent, consistent with Kafka consumer semantics.

---

## 2. KV Offset Store

### 2.1 Bucket

All offset consumers across all streams share a single KV bucket named **`offset-consumers`**.

The bucket is created automatically by the library on first use if it does not already exist. When creating the bucket the library:

1. Looks up the target stream's info to retrieve its `NumReplicas` value.
2. Creates the `offset-consumers` bucket with `NumReplicas` matching the stream (minimum 1), history 1, and a **delete-marker TTL of 10 minutes** (`LimitMarkerTTL` in nats.go, `limitMarker` in jnats, `markerTTL` in @nats-io/kv, `limit_markers` in async-nats). NATS KV deletes write a tombstone; the TTL lets the server purge the tombstones left by `DeleteConsumer()`. This requires nats-server 2.11 or later.

If the bucket already exists it is used as-is. Since the bucket is shared across all streams (which may have different replica counts), the first consumer to create the bucket sets its replica count. Operators who need a specific replica count should create the bucket manually before starting any consumers.

### 2.2 Key Schema

```
{stream-name}.{consumer-name}
```

The dot separator is consistent with NATS subject hierarchy conventions. The consumer name must not itself contain a dot (`.`) — the library validates this at construction time and returns an error if violated.

### 2.3 Value Schema

The value stored at each key is a JSON object:

```json
{
  "seq": 104872,
  "committed_at": "2026-02-25T10:45:00Z",
  "owner": "oc-orders-billing-service-4TzEx8Gq1Wq7Lx5kX9",
  "host": "billing-7",
  "claimed_at": "2026-02-25T10:40:00Z"
}
```

| Field | Type | Description |
|---|---|---|
| `seq` | uint64 | The stream sequence number of the last successfully committed message. On restart the ordered consumer is created at `seq + 1`. |
| `committed_at` | RFC3339 timestamp | Wall-clock time of the commit, for observability. |
| `owner` | string | Owner prefix of the live instance holding the entry (§2.5). Empty when no instance owns it. |
| `host` | string | Hostname of the owning instance, for diagnostics. |
| `claimed_at` | RFC3339 timestamp | When the owner claimed the entry. |

Readers tolerate missing fields (entries written by version 1.0 have no ownership fields and count as unowned).

### 2.4 Write Semantics

Commits and seeks are **plain KV puts**: last writer wins, and the library does not track KV revisions after startup. The only compare-and-set write is the ownership claim performed at construction (§2.5). Dropping per-commit CAS lets async commits be pipelined and removes the self-inflicted revision conflicts that concurrent commit paths (auto-commit timer plus manual commits) would otherwise cause.

### 2.5 Ownership Claim (single active consumer)

The library enforces that only one instance of a given `{stream}.{consumerName}` runs at a time, using the server's liveness tracking of ephemeral consumers:

1. Each instance generates an **owner prefix** `oc-{stream}-{consumerName}-{nuid}`. Every ordered consumer it creates is named `{owner}-g{generation}` followed by a client-specific suffix (`_{serial}` in Go and JS, `-{NUID}` in Java, `_{n}` after resets in Rust). The generation starts at 1 and increments on every restart of consumption (for example after a seek), so names never collide across restarts.
2. On construction the instance reads the entry. If `owner` is non-empty it refuses to start with an **AlreadyInUse** error (carrying `owner`, `host`, `claimed_at`) when either:
   - `claimed_at` is younger than the **inactive threshold** (grace window covering the gap between a claim and the creation of its first ordered consumer), or
   - the stream has a consumer whose name starts with `owner` (the owner's ordered consumer is still alive).
3. Otherwise the instance **claims** the entry with one CAS write: `create` when the key is absent (or deleted), `update` with the read revision otherwise. The claim keeps `seq` and `committed_at` and sets `owner`, `host` and `claimed_at = now`. A CAS failure means another instance claimed concurrently and is reported as AlreadyInUse.
4. Commits copy the ownership fields unchanged.
5. `Close()` deletes the ordered consumer and writes the entry back with empty ownership fields, so a restart is allowed immediately.
6. If the instance crashes, the server deletes its ephemeral ordered consumer after the inactive threshold and the next instance is admitted.

The inactive threshold is configurable in Go (`InactiveThreshold`, default 30s) and JS (`inactiveThresholdMs`, default 30000); it is the server default of 5s in Java (jnats cannot set it) and a fixed 30s in Rust (async-nats hard-codes it). JS re-applies the configured threshold after every client-side consumer recreation, because the client recreates with a 5-minute threshold.

**Out of scope** (documented limitations): a displaced owner (one whose consumer expired while it kept running) is not notified and keeps overwriting commits; there is no forced takeover; a starter that lands in the few-millisecond window of an ordered-consumer reset is not refused.

---

## 3. Configuration

Common fields in all ports: stream, consumer name, auto-commit flag and interval (default 5s), optional subject filters, optional error handler, start position for new consumers (beginning, default, or end).

### 3.1 Go

```go
type ConsumerConfig struct {
    Stream             string
    ConsumerName       string
    AutoCommit         bool
    AutoCommitInterval time.Duration
    FilterSubjects     []string
    ErrorHandler       func(OffsetConsumer, error)
    StartPosition      StartPosition
    InactiveThreshold  time.Duration // default 30s; crash-recovery delay and claim grace window
}
```

### 3.2 Java

```java
new ConsumerConfig.Builder(stream, consumerName)
    .autoCommit(boolean).autoCommitInterval(Duration)
    .filterSubjects(List<String>).errorHandler(ErrorHandler)
    .startPosition(StartPosition).build();
```

No inactive-threshold option: jnats does not expose it for ordered consumers.

### 3.3 JavaScript/TypeScript

```ts
interface ConsumerConfig {
  stream: string; consumerName: string;
  autoCommit?: boolean; autoCommitIntervalMs?: number;
  filterSubjects?: string[];
  errorHandler?: (consumer: OffsetConsumer, error: Error) => void;
  startPosition?: "beginning" | "end";
  inactiveThresholdMs?: number; // default 30000
}
```

### 3.4 Rust

```rust
pub struct ConsumerConfig {
    pub stream: String, pub consumer_name: String,
    pub auto_commit: bool, pub auto_commit_interval: Duration,
    pub filter_subjects: Vec<String>, pub start_position: StartPosition,
}
// error handler: third argument of OffsetConsumer::new
```

---

## 4. Consumer Lifecycle

### 4.1 Construction / Initialization

On construction the library:

1. Validates `ConsumerName` — must be non-empty and must not contain `.`.
2. Ensures the `offset-consumers` KV bucket exists (§2.1).
3. Reads the KV key `{stream}.{consumerName}` and performs the ownership check and claim (§2.5). An entry with no owner (released, or written by version 1.0) is claimed directly.
4. Resolves the start sequence from the stored `seq` (0 means "use the configured start position").
5. Does **not** start consuming yet — that is deferred to `Consume()`. Applications should call `Consume()` promptly: the claim grace window only lasts for the inactive threshold.

### 4.2 Starting Consumption

`Consume(handler)` creates the ephemeral ordered consumer under the next generation prefix (§2.5) with the resolved start sequence, any configured `FilterSubjects`, and the inactive threshold, and begins delivering messages. The handler receives the raw JetStream message; the library adds nothing to the message type. The Go, Java and Rust ports deliver through a callback; the JS port returns an async iterable that keeps yielding across seek restarts and ends on `stop()` or `close()`.

### 4.3 DeleteConsumer

Deletes the KV entry for this consumer. The next `NewConsumer` with the same config starts as a brand-new consumer. Does not stop a running consumer; a later `Close()` on the same instance does **not** recreate the entry.

### 4.4 Close

1. Marks the consumer closed and stops the auto-commit timer.
2. Waits (bounded, 5s) for pending async commits. Commits still queued after the wait are abandoned (their futures resolve) because step 4 persists their position anyway; writes already in flight are ordered before step 4 by the connection.
3. Stops delivery and **deletes the ordered consumer** from the server.
4. Writes the entry once more with `seq` = the highest sequence passed to a commit call (acknowledged or not), or the last delivered sequence when `AutoCommit` is enabled and messages are uncommitted. `owner`, `host` and `claimed_at` are cleared (ownership released).
5. Skips step 4 if `DeleteConsumer()` was called on this instance.

---

## 5. Commit API

### 5.1 What Gets Committed

The sequence number committed is the stream sequence of the **last message delivered to the application callback** at the time the commit is called. On the next restart the consumer resumes at `committed_seq + 1`.

### 5.2 CommitAsync

Returns immediately with a language-native future; the KV write is submitted asynchronously and the future completes when the server acknowledges it. Several async commits may be pending at once; they are applied in call order (pipelined publishes in Go, a single commit thread in Java, a promise chain in JS, a task chain in Rust). When there is nothing to commit, an already-completed future is returned.

| Port | Return type |
|---|---|
| Go | `jetstream.PubAckFuture` (select on `Ok()` / `Err()`) |
| Java | `CompletableFuture<Void>` (fails with `CommitException`) |
| JS/TS | `Promise<void>` |
| Rust | `JoinHandle<Result<()>>` |

The in-memory "last committed" position is only advanced once the acknowledgement arrives.

### 5.3 CommitSync

Submits the same write and waits for its acknowledgement. Returns an error on any KV failure.

### 5.4 Ordering guarantees

Commit submissions are serialized per instance, so the stored offset never regresses because two commit paths raced. A seek bumps an internal epoch so that acknowledgements of commits submitted before the seek do not move the in-memory position past the seek target.

---

## 6. Auto-Commit Mode

When `AutoCommit` is `true`:

- A background timer fires every `AutoCommitInterval` (default 5s).
- If the last delivered sequence has not advanced since the last successful commit, the timer does nothing — no KV write.
- Otherwise the timer performs a synchronous commit.
- Auto-commit errors are delivered via the configured `ErrorHandler`.
- On `Close()`, uncommitted messages are committed by the release write (§4.4).

---

## 7. Seek Operations

All seek operations are synchronous. They follow the same internal sequence:

1. Stop the current ordered consumer and delete it from the server.
2. Write the new offset to the KV (plain put, carrying the ownership fields).
3. If consumption was running, recreate the ordered consumer under the next generation prefix starting at the new position.

If the KV write fails, the seek fails and returns an error. The consumer is left stopped and its in-memory position unchanged — the application must handle the error and decide whether to retry.

- **Seek(seq)**: stores `seq`; delivery resumes at `seq + 1`.
- **SeekToBeginning()**: equivalent to `Seek(0)`.
- **SeekToEnd()**: reads the stream's current `LastSeq`, stores it; only messages published afterwards are delivered.

---

## 8. Error Handling

### 8.1 Ordered Consumer Errors

The NATS client automatically recreates ephemeral ordered consumers on transient problems and redelivers from the last known sequence. Terminal errors are delivered to the configured `ErrorHandler` together with a reference to the consumer. If no handler is configured, consumption stops silently.

### 8.2 Commit Errors

Surfaced via the async future or the sync return value as described in §5.

### 8.3 Already In Use

Construction fails with a dedicated error when another live instance owns the entry (§2.5): Go `ErrAlreadyInUse` (details via `*AlreadyInUseError`), Java `AlreadyInUseException`, JS `AlreadyInUseError`, Rust `OffsetConsumerError::AlreadyInUse { owner, host, claimed_at }`.

### 8.4 Consumer Name Validation

At construction time the library validates that `ConsumerName` is non-empty and contains no `.` character. An error is returned immediately if validation fails.

---

## 9. API Surface Summary

Every port exposes: construction, `Consume`, `CommitSync`, `CommitAsync`, `Info` (fresh read of the entry, returning seq, committed_at, owner, host, claimed_at, or nil/null/None when absent), `Seek`, `SeekToBeginning`, `SeekToEnd`, `DeleteConsumer`, `Close`, and a standalone `ListConsumers(stream)` returning a map of consumer name to the same info. The exact signatures are in `specs/001-offset-consumer/contracts/`.

---

## 10. Operational Notes

### 10.1 Single Active Consumer

The offset consumer pattern is designed for **single active consumer** use. There is no consumer group or partition coordination. A second instance of the same `{stream}.{consumerName}` is refused at construction while the first one is alive (§2.5); after a crash the second instance is admitted once the first one's ordered consumer has expired (inactive threshold). The ordered consumer names make ownership visible to operators: `nats consumer ls <stream>` shows `oc-<stream>-<consumer>-<nuid>-g<N>...` for every live instance.

### 10.2 Manual Bucket Management

Operators who need explicit control over the `offset-consumers` bucket (replica count, storage type, etc.) should create it manually before starting any consumers. The library detects the existing bucket and uses it as-is.

### 10.3 Key Naming Discipline

With a shared bucket, key uniqueness relies on application naming discipline. Consumer names should encode enough application context to be globally unique, e.g. `payment-service-prod` rather than `consumer1`.

### 10.4 Auto-Commit Timing and At-Least-Once Boundary

In auto-commit mode, the timer commits the sequence of the last message **delivered to the callback** at the time the timer fires — not the last message whose callback has returned. The reprocessing exposure window is up to one `AutoCommitInterval` of messages.

---

## 11. Open Items / Future Considerations

- **Displaced-owner detection** — a KV watch on the instance's own key would let a displaced owner stop itself. Not implemented.
- **Operator takeover** — a forced-claim option paired with displaced-owner detection.
- **Metrics / observability hooks** — lag metrics (committed seq vs. stream last seq).
- **.NET** — dropped until NATS.Net offers an ordered-consumer name prefix (see README).
