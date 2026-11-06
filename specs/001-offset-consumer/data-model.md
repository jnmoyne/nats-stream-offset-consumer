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
Created (claimed) → Consuming → Closed (consumer deleted, ownership released)
                       ↕
                    Seeking (stop + delete consumer → write KV → restart under next generation)
```

- `Created`: After construction — entry claimed, KV read, start sequence resolved, not yet consuming. Construction fails with AlreadyInUse if another live instance owns the entry.
- `Consuming`: After `Consume()` — ordered consumer `{owner}-g{N}` active, messages being delivered.
- `Seeking`: Transient — consumption stopped, ordered consumer deleted, KV updated, consumption restarted under `{owner}-g{N+1}`.
- `Closed`: After `Close()` — pending commits drained (bounded; leftovers abandoned), ordered consumer deleted, entry written back with empty ownership and the highest requested position (or the last delivered one when auto-commit is on).

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
| `StartPosition` | enum | Beginning | `Beginning` or `End` — only used when no stored offset exists |
| `InactiveThreshold` | duration | 30s | Go and JS only; Java uses the server default (5s), Rust a fixed 30s |

---

### 3. OffsetEntry (KV Value)

| Field | Type | Description |
|---|---|---|
| `seq` | uint64 | Last committed stream sequence number |
| `committed_at` | RFC3339 timestamp | Wall-clock time of the commit |
| `owner` | string | Owner prefix `oc-{stream}-{consumer}-{nuid}` of the live instance; empty when unowned |
| `host` | string | Hostname of the owning instance |
| `claimed_at` | RFC3339 timestamp | When the owner claimed the entry |

**JSON representation**:
```json
{
  "seq": 104872,
  "committed_at": "2026-02-25T10:45:00Z",
  "owner": "oc-orders-billing-service-4TzEx8Gq1Wq7Lx5kX9",
  "host": "billing-7",
  "claimed_at": "2026-02-25T10:40:00Z"
}
```

**KV key**: `{stream-name}.{consumer-name}` (e.g., `orders.billing-service`)

**Writes**: the claim at construction is a compare-and-set (create if absent, update by revision otherwise). Commits, seeks and the release on close are plain puts. Readers treat missing ownership fields (version 1.0 entries) as unowned.

---

### 4. OffsetConsumerInfo

Returned by `Info()` and `ListConsumers()`: `seq`, `committedAt`, `owner`, `host`, `claimedAt` (the entry's fields, as read).

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

**Creation logic**: Create if not exists; use as-is if exists.

---

### 7. Ordered consumer names

`{owner}-g{generation}` + client suffix: Go/JS `_{serial}` (serial increments on each client-side reset), Java `-{NUID}` (new NUID per reset), Rust none at first then `_{n}` per reset. The ownership check matches `name.startsWith(owner)`.

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
| `ErrAlreadyInUse` / `*AlreadyInUseError{Owner, Host, ClaimedAt}` | Go | Another live instance owns the entry (returned by `NewConsumer`) |
| `ErrConsumerClosed` | Go | Operation attempted on a closed consumer |
| `ErrInvalidConsumerName` | Go | Consumer name empty or contains `.` |
| `ErrAlreadyConsuming` | Go | `Consume` called twice |
| `AlreadyInUseException` | Java | Checked; thrown by `OffsetConsumerFactory.create` |
| `CommitException` | Java | Commit/seek write failure (also the failure of `commitAsync` futures) |
| `AlreadyInUseError` | JS/TS | Thrown by `createConsumer` |
| `InvalidConsumerNameError` | JS/TS | Consumer name validation |
| `OffsetConsumerError::AlreadyInUse { owner, host, claimed_at }` | Rust | Returned by `OffsetConsumer::new` |
| `OffsetConsumerError::{InvalidConsumerName, StreamRequired, ConsumerClosed, AlreadyConsuming, Nats, Other}` | Rust | Other errors |
