# NATS JetStream Offset Consumer Library — Specification

**Version:** 1.5-draft  
**Target Languages:** Go, Java, JavaScript/TypeScript, Rust  
**Date:** 2026-11-06

---

## 1. Overview

The Offset Consumer library provides Kafka-style consumer semantics on top of NATS JetStream, implemented entirely on the client side. It gives application developers a familiar "durable consumer" experience — the ability to consume a stream from a persisted position and resume after restart — without relying on JetStream's server-side durable consumer mechanism.

### 1.1 Core Mechanics

Under the covers the library:

1. Uses a **JetStream ephemeral ordered consumer** as the message transport.
2. Persists the **last committed stream sequence number** in a **NATS KV bucket** (`offset-consumers`).
3. When consumption starts, **claims ownership** of the entry in the KV and creates a new ephemeral ordered consumer starting at `stored_seq + 1`.
4. On first use (no KV key exists), starts from sequence 1 (the beginning of the stream) or, if configured, from the end of the stream. The end position is resolved and stored at construction.

This approach avoids server-side durable consumer lifecycle management while still providing durable, resumable consumption.

### 1.2 Quality of Service

**At-least-once delivery.** Any messages delivered after the last committed offset will be redelivered after a restart. The application callback is expected to be idempotent, consistent with Kafka consumer semantics. Implicit commits (auto-commit and the final write in `Close()`) only cover messages whose handler has returned, so they never turn a crash into a lost message (§6).

---

## 2. KV Offset Store

### 2.1 Bucket

All offset consumers across all streams share a single KV bucket named **`offset-consumers`**.

The bucket is created automatically by the library on first use if it does not already exist. The library first tries to open it and only creates it when it is missing. When creating the bucket the library:

1. Looks up the target stream's info to retrieve its `NumReplicas` value.
2. Creates the `offset-consumers` bucket with `NumReplicas` matching the stream (minimum 1), history 1, and a **delete-marker TTL of 10 minutes** (`LimitMarkerTTL` in nats.go, `limitMarker` in jnats, `markerTTL` in @nats-io/kv, `limit_markers` in async-nats). NATS KV deletes write a tombstone; the TTL lets the server purge the tombstones left by `DeleteConsumer()`. This requires nats-server 2.11 or later.

If the bucket already exists its configuration is never overwritten (an earlier version updated it on every start, which let an R1 stream scale the shared bucket down to R1). The only change the library makes to an existing bucket is to **raise** its replica count to the target stream's, best effort: if the update fails (permissions, too few servers) the bucket keeps working as it is. The replica count is never lowered, so the bucket converges to the highest replica count among the streams that use it. Operators who need a specific configuration should create the bucket manually before starting any consumers.

### 2.2 Key Schema

```
{stream-name}.{consumer-name}
```

The dot separator is consistent with NATS subject hierarchy conventions. Both names are validated at construction (§8.4).

### 2.3 Value Schema

The value stored at each key is a JSON object:

```json
{
  "seq": 104872,
  "committed_at": "2026-02-25T10:45:00Z",
  "owner": "oc-orders-billing-service-4TzEx8Gq1Wq7Lx5kX9",
  "host": "billing-7",
  "stream_created": "1772016000123456789"
}
```

| Field | Type | Description |
|---|---|---|
| `seq` | uint64 | The stream sequence number of the last successfully committed message. On restart the ordered consumer is created at `seq + 1`. |
| `committed_at` | RFC3339 timestamp | Wall-clock time of the commit, for observability. |
| `owner` | string | Owner prefix of the live instance holding the entry (§2.5). Empty when no instance owns it. |
| `host` | string | Hostname of the owning instance, for diagnostics. |
| `stream_created` | decimal string | Creation time of the stream incarnation `seq` refers to, in Unix nanoseconds (§2.6). Every write carries it. A string, so that JavaScript can carry it without precision loss. |

Readers tolerate missing fields (entries written by version 1.0 have no ownership fields and count as unowned; entries written by 1.1 have no `stream_created`) and ignore unknown ones (versions 1.1 to 1.4 also wrote a `claimed_at` timestamp, no longer used).

### 2.4 Write Semantics

Commits and seeks are **plain KV puts**: last writer wins, and the library does not track KV revisions after startup. The compare-and-set writes are the ownership claim when consumption starts and the release in `Close` (§2.5, §4.4). Dropping per-commit CAS lets async commits be pipelined and removes the self-inflicted revision conflicts that concurrent commit paths (auto-commit timer plus manual commits) would otherwise cause.

### 2.5 Ownership Claim (single active consumer)

The library enforces that only one instance of a given `{stream}.{consumerName}` runs at a time, using the server's liveness tracking of ephemeral consumers:

1. Each instance generates an **owner prefix** `oc-{stream}-{consumerName}-{nuid}`. Its ordered consumers are named `{owner}-g{generation}` followed by a client-specific suffix (`_{serial}` in Go and JS, `-{NUID}` in Java, `_{n}` after resets in Rust). The generation starts at 1 and increments on every restart of consumption (for example after a seek), so names never collide across restarts.
2. **The owner is alive while it has a consumer on the server**: a consumer whose name starts with `owner` exists on the stream. The library keeps that true for an owner's whole life:
   - it claims only when consumption starts, and creates the ordered consumer right after the claim;
   - a seek creates the new ordered consumer before deleting the old one (§7).
3. **Construction** reads the entry and does a quick check: if `owner` is non-empty and has a consumer, it refuses with **AlreadyInUse** (carrying `owner` and `host`). It writes nothing.
4. **The claim**, when consumption starts (`Consume`, or a restart after the instance was stopped), reads the entry at revision *r*.
   - If `owner` is another instance with a consumer, it refuses with AlreadyInUse.
   - If that owner has no consumer, it waits the **liveness wait** (5 seconds, measured with the local clock) and lists again; a consumer then means AlreadyInUse. The wait rides out an ordered-consumer reset, during which Go, JS and Rust delete the old consumer before creating the new one, and retry the creation after 1s and 2s if it fails.
   - It then writes the claim with a CAS against *r*: `create` when the key is absent (or deleted), `update` otherwise. Any write during the wait (a commit by the owner, another instance's claim, a release) makes the CAS fail. After a failure the entry is read again: another owner means AlreadyInUse; an empty owner or the instance's own late commit is retried, at most three rounds in all.
   - The claim stores the position consumption starts from (§4.2) and sets `owner` and `host`.
5. Commits copy the ownership fields unchanged. Nothing is written before the first claim.
6. `Close()` deletes the ordered consumer and writes the entry back with empty ownership fields, so a restart is allowed immediately. That write is conditional on the entry still naming the instance (§4.4).
7. If the instance crashes, the server deletes its ordered consumer after the inactive threshold, and the next instance is admitted one liveness wait later.

**No clock comparison.** No timestamp is compared across hosts. The only time measurement is the liveness wait, measured locally. The CAS against the revision read before the wait is what decides between concurrent starters, so no random jitter is needed: starters that wake from the wait at the same moment all write against the same revision, exactly one CAS succeeds, and the others re-read the entry, find another owner and get AlreadyInUse.

The inactive threshold is configurable in Go (`InactiveThreshold`, default 30s) and JS (`inactiveThresholdMs`, default 30000). It is 5s in Java, the server default that its ordered consumers get because jnats cannot set it there. It is a fixed 30s in Rust, which async-nats hard-codes for ordered consumers. JS re-applies the configured threshold after every client-side consumer recreation, because the client recreates with a 5-minute threshold.

**Out of scope** (documented limitations):
- **Displaced owner.** An owner whose consumer expired while it kept running is not notified and keeps overwriting commits, although its `Close` no longer clears the new owner's claim (§4.4).
- **No forced takeover.**
- **Owner without a consumer.** An owner that has no consumer looks dead after the liveness wait and can be taken over: after a failed seek, or once a Rust delivery loop has ended. Its next `Consume` runs the claim again and is refused.
- **Lost server, connected client.** An R1 in-memory ordered consumer lost with its server while the client stays connected is missing until the client notices the missed heartbeats, which can take longer than the liveness wait.

### 2.6 Stream recreation

Stream sequence numbers restart at 1 when a stream is deleted and recreated. A stored offset then refers to a stream that no longer exists. Measured with nats.go: the running ordered consumer reports "consumer deleted" once, then silently recreates itself on the new stream at the old position + 1, skipping the new stream's first messages, or waiting forever if the new stream never gets that far.

The library identifies a stream incarnation by its creation time (`stream_created`, §2.3).

- **At construction.** The stored offset belongs to an earlier incarnation when both identities are present and differ. For legacy entries without an identity, a stored `seq` beyond the stream's last sequence is the sign. The offset is then reset to the configured start position, like Kafka's `auto.offset.reset`. The claim stores the new identity, and the error handler receives an informational **StreamRecreated** error.
- **While consuming.** The library watches for client-side recreations of the ordered consumer. Go and JS use the client's reset, consumer-deleted, heartbeat and stream-not-found events. Java checks every second whether the ordered consumer's name changed. Rust polls the stream info every 5 seconds, because async-nats exposes no reset signal. All ports also check when a message arrives from a differently named consumer. After a signal, the library polls the stream info every second until the stream exists again. If the identity changed, it drops further deliveries from the old ordered consumer, reports StreamRecreated, and seeks to the configured start position on the new stream. The new identity is applied by the seek write itself: commits submitted before it carry the old identity and are therefore detected again at the next start, so a position from the old stream is never stored under the new stream's identity.

Detection latency depends on how quickly the client notices the deleted consumer. Go and JS get a "consumer deleted" event immediately. Java only recreates after three missed heartbeats, so the Java port pulls with a 5-second expiry, giving a 2.5-second heartbeat and detection within about 15 seconds, instead of 90 seconds with the jnats defaults. Rust detects within one 5-second poll.

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
    InactiveThreshold  time.Duration // default 30s; ordered consumer inactive threshold (crash-recovery delay, plus the 5s liveness wait)
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

1. Validates the stream and consumer names (§8.4).
2. Ensures the `offset-consumers` KV bucket exists (§2.1).
3. Resolves the configured start position: 0 for the beginning, the stream's current last sequence for the end. Resolving the end here, rather than when consumption starts, lets an end consumer resume from where it was created instead of skipping messages published in between. The claim stores it when there is no usable stored offset.
4. Reads the KV key `{stream}.{consumerName}` and refuses with AlreadyInUse if its owner visibly has a consumer (§2.5). It does not wait.
5. Writes nothing and creates nothing on the server. Consumption starts with `Consume()`.

### 4.2 Starting Consumption

`Consume(handler)` first **claims** the entry (§2.5). The claim stores the position consumption starts from:
- the position of a seek made while not consuming, if any;
- otherwise the stored offset;
- otherwise, when there is no usable stored offset (none, or the stream was recreated, §2.6), the start position resolved at construction.

It then creates the ephemeral ordered consumer under the next generation prefix at `startSeq + 1`, or from the first message when `startSeq` is 0, with any configured `FilterSubjects` and the inactive threshold, and begins delivering messages. The handler receives the raw JetStream message; the library adds nothing to the message type. The Go, Java and Rust ports deliver through a callback; the JS port returns an async iterable that keeps yielding across seek restarts and ends on `stop()` or `close()`.

Before the first claim, `CommitSync` and `CommitAsync` do nothing (an already-completed future) and `Close` writes nothing.

### 4.3 DeleteConsumer

Deletes the KV entry for this consumer. The next `NewConsumer` with the same config starts as a brand-new consumer. Does not stop a running consumer; a later `Close()` on the same instance does **not** recreate the entry.

`Consume`, `Seek` and `Close` are serialized by a per-instance lifecycle lock, so at most one ordered consumer is active per instance even when they are called concurrently.

### 4.4 Close

1. Marks the consumer closed and stops the auto-commit timer. Close does not wait for the timer's thread or task, so an error handler invoked by a failing auto-commit may call `Close()`.
2. Waits (bounded, 5s) for pending async commits. Commits still queued after the wait are abandoned (their futures resolve) because step 4 persists their position anyway; writes already in flight are ordered before step 4 by the connection.
3. Stops delivery and **deletes the ordered consumer** from the server.
4. Writes the entry once more with `seq` = the highest sequence passed to a commit call (acknowledged or not), or, when `AutoCommit` is enabled, the last **processed** sequence if it is higher. A message whose handler is still running is not committed, and is redelivered on the next start. `owner` and `host` are cleared (ownership released).
5. Step 4 is **conditional**. The entry is read first. If it no longer exists, nothing is written. If it names another owner (this instance was displaced without noticing, and another instance claimed the entry), nothing is written and `Close` fails with AlreadyInUse naming that owner. Otherwise the release is a CAS update against the revision just read; on a revision conflict, most likely one of the instance's own late async commits, the entry is read again, at most three attempts in total. A plain put here would clear the new owner's claim, and the next starter would be admitted without a liveness check while the new owner runs. Step 4 is also skipped if `DeleteConsumer()` was called on this instance, or if the instance never claimed the entry (it never consumed).

---

## 5. Commit API

### 5.1 What Gets Committed

An explicit commit (`CommitSync`, `CommitAsync`) commits the stream sequence of the **last message delivered to the application callback** at the time the commit is called, so a commit from inside the handler includes the current message. Implicit commits (auto-commit, the final write in `Close()`) commit the **last processed** sequence: the last message whose handler has returned. In JS, where messages are pulled from an async iterator, a message counts as processed when the application asks for the next one. On the next restart the consumer resumes at `committed_seq + 1`.

### 5.2 CommitAsync

Returns immediately with a language-native future; the KV write is submitted asynchronously and the future completes when the server acknowledges it, or fails when no acknowledgement arrives within 5 seconds. In Go this needs a JetStream context of its own: nats.go async publishes have no ack timeout by default, so a lost acknowledgement used to leave the future, and a `CommitSync` with a background context, pending forever. Several async commits may be pending at once; they are applied in call order (pipelined publishes in Go, a single commit thread in Java, a promise chain in JS, a task chain in Rust). When there is nothing to commit, an already-completed future is returned.

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
- If the last processed sequence has not advanced since the last successful commit, the timer does nothing — no KV write.
- Otherwise the timer synchronously commits the last processed sequence. It never commits a message whose handler is still running. Kafka gets the same guarantee by committing at the next `poll()`.
- Auto-commit errors are delivered via the configured `ErrorHandler`.
- On `Close()`, uncommitted messages are committed by the release write (§4.4).

---

## 7. Seek Operations

All seek operations are synchronous. They follow the same internal sequence:

While consuming:

1. Stop delivery from the current ordered consumer, keeping its server-side consumer.
2. Write the new offset to the KV (plain put, carrying the ownership fields).
3. Create the ordered consumer under the next generation prefix starting at the new position.
4. Delete the previous server-side consumer.

Steps 3 and 4 are make-before-break: the instance has a consumer on the server throughout, so a starter never sees it as gone.

If the KV write fails, the seek fails and returns an error. The previous consumer is deleted, and the instance is left stopped with its in-memory position unchanged, including the position that `Close()` would persist. The application must handle the error and decide whether to retry.

While not consuming (before the first `Consume`, or after a failed seek), a seek only records the position. The claim at the next `Consume` stores it, like a Kafka seek before `poll()`.

- **Seek(seq)**: stores `seq`; delivery resumes at `seq + 1`.
- **SeekToBeginning()**: equivalent to `Seek(0)`, and always the beginning of the stream, whatever the configured start position.
- **SeekToEnd()**: reads the stream's current `LastSeq`, stores it; only messages published afterwards are delivered.

---

## 8. Error Handling

### 8.1 Ordered Consumer Errors

The NATS client automatically recreates ephemeral ordered consumers on transient problems and redelivers from the last known sequence. Terminal errors are delivered to the configured `ErrorHandler` together with a reference to the consumer. If no handler is configured, consumption stops silently.

### 8.2 Commit Errors

Surfaced via the async future or the sync return value as described in §5.

### 8.3 Already In Use

Construction and `Consume` fail with a dedicated error when another live instance owns the entry (§2.5), and `Close` reports it when it skips the release because another instance claimed the entry: Go `ErrAlreadyInUse` (details via `*AlreadyInUseError{Owner, Host}`), Java `AlreadyInUseException`, JS `AlreadyInUseError`, Rust `OffsetConsumerError::AlreadyInUse { owner, host }`.

### 8.4 Name Validation

At construction time the library validates both names. Each must match `^[A-Za-z0-9_=-]+$`, which keeps it usable as a KV key token and inside consumer names: no `.`, wildcards, spaces or slashes. Together they must not exceed 196 characters. The server limits consumer names to 255 characters, and the ordered consumer names embed `oc-`, both names, two separators, a 22-character NUID, and up to 32 characters of generation and client suffix. An invalid consumer name gives the existing invalid-name error; an invalid stream name gives a dedicated invalid-stream-name error.

### 8.5 Stream Recreated

Reported to the error handler, not returned, when §2.6 detects a recreated stream: Go `ErrStreamRecreated`, Java `StreamRecreatedException`, JS `StreamRecreatedError`, Rust `OffsetConsumerError::StreamRecreated { stored_seq }`. Consumption continues from the start position.

---

## 9. API Surface Summary

Every port exposes: construction, `Consume`, `CommitSync`, `CommitAsync`, `Info` (fresh read of the entry, returning seq, committed_at, owner, host and stream_created, or nil/null/None when absent), `Seek`, `SeekToBeginning`, `SeekToEnd`, `DeleteConsumer`, `Close`, and a standalone `ListConsumers(stream)` returning a map of consumer name to the same info. The exact signatures are in `specs/001-offset-consumer/contracts/`.

---

## 10. Operational Notes

### 10.1 Single Active Consumer

The offset consumer pattern is designed for **single active consumer** use. There is no consumer group or partition coordination. A second instance of the same `{stream}.{consumerName}` is refused at construction or at `Consume` while the first one is alive (§2.5). After a crash, the second instance is admitted once the first one's ordered consumer has expired (inactive threshold), plus one liveness wait. The ordered consumer names make ownership visible to operators: `nats consumer ls <stream>` shows `oc-<stream>-<consumer>-<nuid>-g<N>...` for every live instance.

### 10.2 Manual Bucket Management

Operators who need explicit control over the `offset-consumers` bucket (replica count, storage type, etc.) should create it manually before starting any consumers. The library detects the existing bucket and uses it as-is.

### 10.3 Key Naming Discipline

With a shared bucket, key uniqueness relies on application naming discipline. Consumer names should encode enough application context to be globally unique, e.g. `payment-service-prod` rather than `consumer1`.

### 10.4 Auto-Commit Timing and At-Least-Once Boundary

In auto-commit mode, the timer commits the last message whose callback has **returned** at the time the timer fires. Version 1.1 committed the last message delivered to the callback, which could commit a message whose processing then failed. The reprocessing exposure window is up to one `AutoCommitInterval` of messages.

---

## 11. Open Items / Future Considerations

- **Displaced-owner detection** — not implemented. Comparing the entry's owner with the instance's own on every ordered-consumer reset is not reliable on its own: commits the client buffered during the disconnect are flushed on reconnect and can rewrite the old owner over a newcomer's claim, and some clients reset without telling the application.
- **Operator takeover** — a forced-claim option paired with displaced-owner detection.
- **Metrics / observability hooks** — lag metrics (committed seq vs. stream last seq).
- **.NET** — dropped until NATS.Net offers an ordered-consumer name prefix (see README).
