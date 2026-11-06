# Data Model: NATS JetStream Offset Consumer Library

**Feature Branch**: `001-offset-consumer` | **Date**: 2026-11-06 (revised: ownership claim, pipelined commits)

---

## Entities

### 1. OffsetConsumer

The primary user-facing entity. Represents a logical consumer bound to a specific stream and consumer name pair.

| Field (internal) | Type | Description |
|---|---|---|
| `js` | JetStream handle | JetStream context for creating ordered consumers and KV |
| `config` | ConsumerConfig | Immutable configuration provided at construction |
| `kv` | KeyValue | Handle to the `offset-consumers` KV bucket |
| `kvKey` | string | `{stream}.{consumerName}` — the KV key for this consumer |
| `owner`, `host`, `claimedAt` | string | Ownership identity recorded in the entry at construction; immutable |
| `startSeq` | uint64 | Resolved start sequence (from KV or config default) |
| `lastDeliveredSeq` | uint64 | Stream sequence of the last message delivered to handler |
| `lastCommittedSeq` / `lastCommittedAt` | uint64 / string | Last acknowledged commit |
| `lastRequestedSeq` / `lastRequestedAt` | uint64 / string | Highest sequence handed to a commit call; the release write on close persists at least this |
| `generation` | int | Number of consumption (re)starts; names the ordered consumer `{owner}-g{generation}` |
| `currentConsumer` | ordered consumer handle | The live ordered consumer (nil when stopped); deleted on seek/close |
| `commitEpoch` | uint64 | Bumped by seek; acks of older commits do not move the position |
| `pending writes` | pipeline | Go: `WaitGroup` of in-flight publishes; Java: single-thread executor; JS/Rust: promise/task chain |
| `autoCommitTimer` | Timer | Background timer for auto-commit (nil when disabled) |
| `mu` | Mutex/Lock | Protects all mutable state for thread safety |
| `closed`, `kvDeleted` | bool | Whether Close() / DeleteConsumer() has been called |

**Validation rules**:
- `config.ConsumerName` must be non-empty and must not contain `.`
- `config.Stream` must be non-empty
- `config.AutoCommitInterval` defaults to 5s; `InactiveThreshold` defaults to 30s (Go, JS)

**State transitions**:
```
Created → (claim) Consuming → Closed (consumer deleted, ownership released if claimed)
                       ↕
                    Seeking (stop delivery → write KV → start next generation → delete previous consumer)
```

- `Created`: After construction — KV read, start position resolved, nothing written. Construction fails with AlreadyInUse if the stored owner has a consumer. A seek here only records the position; commits do nothing.
- `Consuming`: After `Consume()` — entry claimed (CAS, after a 5s liveness wait if the stored owner had no consumer), ordered consumer `{owner}-g{N}` active, messages being delivered.
- `Seeking`: Transient — delivery stopped, KV updated, consumption restarted under `{owner}-g{N+1}`, then the `{owner}-g{N}` consumer deleted (make before break).
- `Closed`: After `Close()` — pending commits drained (bounded; leftovers abandoned), ordered consumer deleted, entry written back with empty ownership and the highest requested position (or the last processed one when auto-commit is on). The write is a CAS on an entry that still names the instance; if another instance owns it, nothing is written and `Close` fails with AlreadyInUse; if it was deleted, nothing is written.

---

### 2. ConsumerConfig

| Field | Type | Default | Validation |
|---|---|---|---|
| `Stream` | string | (required) | Non-empty |
| `ConsumerName` | string | (required) | Non-empty, no `.` character |
| `AutoCommit` | bool | `false` | — |
| `AutoCommitInterval` | duration | 5 seconds | Ignored when AutoCommit=false |
| `FilterSubjects` | string list | nil/empty | Optional |
| `ErrorHandler` | callback | nil | Optional (Rust: constructor argument) |
| `StartPosition` | enum | Beginning | `Beginning` or `End` — used when no stored offset exists or the stream was recreated; `End` is resolved to the stream's last sequence at construction and stored by the claim |
| `InactiveThreshold` | duration | 30s | Go and JS only; Java uses the server default (5s), Rust a fixed 30s |

---

### 3. OffsetEntry (KV Value)

| Field | Type | Description |
|---|---|---|
| `seq` | uint64 | Last committed stream sequence number |
| `committed_at` | RFC3339 timestamp | Wall-clock time of the commit |
| `owner` | string | Owner prefix `oc-{stream}-{consumer}-{nuid}` of the live instance; empty when unowned |
| `host` | string | Hostname of the owning instance |
| `stream_created` | decimal string | Stream creation time in Unix nanoseconds: the stream incarnation `seq` belongs to. Absent in 1.0/1.1 entries |

**JSON representation**:
```json
{
  "seq": 104872,
  "committed_at": "2026-02-25T10:45:00Z",
  "owner": "oc-orders-billing-service-4TzEx8Gq1Wq7Lx5kX9",
  "host": "billing-7",
  "stream_created": "1772016000123456789"
}
```

**KV key**: `{stream-name}.{consumer-name}` (e.g., `orders.billing-service`)

**Writes**: the claim at construction is a compare-and-set (create if absent, update by revision otherwise). Commits, seeks and the release on close are plain puts. Readers treat missing ownership fields (version 1.0 entries) as unowned. An entry is from an earlier stream incarnation when both `stream_created` values are present and differ, or, without a stored identity, when `seq` exceeds the stream's last sequence; its offset is then reset to the start position.

---

### 4. OffsetConsumerInfo

Returned by `Info()` and `ListConsumers()`: `seq`, `committedAt`, `owner`, `host`, `claimedAt`, `streamCreated` (the entry's fields, as read).

---

### 5. Commit Future

Language-native: Go `jetstream.PubAckFuture` (wrapped so `Ok()` fires only after the consumer recorded the ack; an already-completed future when there is nothing to commit), Java `CompletableFuture<Void>`, JS `Promise<void>`, Rust `JoinHandle<Result<()>>`.

---

### 6. Offset Store (KV Bucket)

| Property | Value |
|---|---|
| Bucket name | `offset-consumers` |
| Replicas | Matches target stream's `NumReplicas` (min 1) |
| History | 1 (only latest value per key) |
| Delete-marker TTL | 10 minutes (all ports; requires nats-server 2.11+) |
| TTL | None (offsets persist indefinitely) |

**Creation logic**: open; create only if missing; never overwrite an existing configuration. Raise (never lower) the replica count to the target stream's, best effort.

---

### 7. Ordered consumer names

Stream and consumer names: `^[A-Za-z0-9_=-]+$`, at most 196 characters together.


Ordered consumers: `{owner}-g{generation}` + client suffix: Go/JS `_{serial}` (serial increments on each client-side reset), Java `-{NUID}` (new NUID per reset), Rust none at first then `_{n}` per reset. The ownership check treats the owner as alive while a name matches `name.startsWith(owner)`, checked again after a 5s liveness wait before a takeover.

---

## Relationships

```
ConsumerConfig ──1:1──▶ OffsetConsumer ──1:1──▶ OffsetEntry (in KV, owned while the instance lives)
                                        │
                                        ├──1:1──▶ ordered consumer "{owner}-g{N}..." (on the stream)
                                        └──uses──▶ Offset Store (shared bucket)
```

---

## Error Types

| Error | Language | Description |
|---|---|---|
| `ErrAlreadyInUse` / `*AlreadyInUseError{Owner, Host}` | Go | Another live instance owns the entry (returned by `NewConsumer` and `Consume`; by `Close` when the release is skipped) |
| `ErrConsumerClosed` | Go | Operation attempted on a closed consumer |
| `ErrInvalidConsumerName` | Go | Consumer name empty or contains `.` |
| `ErrAlreadyConsuming` | Go | `Consume` called twice |
| `AlreadyInUseException` | Java | Checked; thrown by `OffsetConsumerFactory.create` |
| `CommitException` | Java | Commit/seek write failure (also the failure of `commitAsync` futures) |
| `AlreadyInUseError` | JS/TS | Thrown by `createConsumer` |
| `InvalidConsumerNameError` | JS/TS | Consumer name validation |
| `OffsetConsumerError::AlreadyInUse { owner, host }` | Rust | Returned by `OffsetConsumer::new`, `consume` and `close` |
| `OffsetConsumerError::{InvalidConsumerName, StreamRequired, ConsumerClosed, AlreadyConsuming, Nats, Other}` | Rust | Other errors |
