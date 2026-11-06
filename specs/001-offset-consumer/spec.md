# Feature Specification: NATS JetStream Offset Consumer Library

**Feature Branch**: `001-offset-consumer`
**Created**: 2026-02-26
**Status**: Draft (revised 2026-11-06: ownership claim replaces per-commit CAS; .NET dropped)
**Input**: User description: "NATS JetStream Offset Consumer Library"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Durable Resumable Consumption (Priority: P1)

A developer integrates the offset consumer library into their service to consume messages from a NATS JetStream stream. When the service restarts (planned or unplanned), it automatically resumes from where it left off — no messages are skipped and previously committed messages are not redelivered.

**Why this priority**: This is the core value proposition — durable, resumable consumption without server-side durable consumer management. Without this, the library has no reason to exist.

**Independent Test**: Can be fully tested by creating a consumer, processing messages, committing offsets, restarting the consumer, and verifying it resumes from the correct position.

**Acceptance Scenarios**:

1. **Given** a new consumer with no prior offset stored and default start position, **When** `Consume()` is called, **Then** messages are delivered starting from the beginning of the stream (sequence 1).
1b. **Given** a new consumer with no prior offset stored and start position set to "end", **When** `Consume()` is called, **Then** only messages published after the consumer starts are delivered.
2. **Given** a consumer that previously committed offset at sequence 500, **When** the consumer is reconstructed and `Consume()` is called, **Then** messages are delivered starting from sequence 501.
3. **Given** a consumer processing messages, **When** the application calls `CommitSync()`, **Then** the last delivered sequence number is persisted and can be read back on restart.
4. **Given** a consumer processing messages, **When** `CommitAsync()` is called, **Then** a future/promise is returned immediately that resolves once the offset write is acknowledged; several such commits may be pending at once and are applied in call order.

---

### User Story 2 - Manual and Automatic Commit Modes (Priority: P1)

A developer chooses between manual commit (explicit control over when offsets are persisted) and auto-commit (timer-based background persistence). In manual mode, the developer calls commit explicitly after processing logic completes. In auto-commit mode, offsets are committed on a configurable interval without explicit calls.

**Why this priority**: Commit control is essential for at-least-once delivery guarantees. Both modes are needed to serve different application patterns — manual for strict processing guarantees, auto for convenience.

**Independent Test**: Can be tested by configuring a consumer in each mode, processing messages, and verifying offsets are persisted at the expected times.

**Acceptance Scenarios**:

1. **Given** a consumer with `AutoCommit` disabled, **When** messages are delivered but no commit is called, **Then** no offset is persisted and a restart replays from the last committed position.
2. **Given** a consumer with `AutoCommit` enabled and interval set to 5 seconds, **When** messages are being processed, **Then** the offset is committed approximately every 5 seconds when new messages have been delivered.
3. **Given** a consumer with `AutoCommit` enabled, **When** no new messages have been delivered since the last commit, **Then** the auto-commit timer does not write to the offset store.
4. **Given** a consumer with `AutoCommit` enabled, **When** `Close()` is called with uncommitted messages, **Then** a final synchronous commit is performed before shutdown.

---

### User Story 3 - Seek to Arbitrary Position (Priority: P2)

A developer needs to reprocess historical messages or skip ahead in the stream. They use seek operations to reposition the consumer to a specific sequence number, the beginning of the stream, or the current end of the stream.

**Why this priority**: Seek is important for operational recovery and debugging but is not part of the primary consume-commit loop.

**Independent Test**: Can be tested by consuming to a known position, seeking to a different position, and verifying messages are delivered from the new position.

**Acceptance Scenarios**:

1. **Given** a running consumer at sequence 1000, **When** `Seek(500)` is called, **Then** the offset is updated to 500 and messages are delivered starting from sequence 501.
2. **Given** a running consumer, **When** `SeekToBeginning()` is called, **Then** the offset is reset and messages are delivered from the start of the stream.
3. **Given** a running consumer, **When** `SeekToEnd()` is called, **Then** the offset is set to the stream's current last sequence and only new messages published after the seek are delivered.
4. **Given** a seek operation, **When** the offset store write fails, **Then** an error is returned and the consumer is left stopped with its position unchanged.
5. **Given** a running consumer, **When** a seek succeeds, **Then** the previous message transport is deleted from the server and a new one is created under the next generation name.

---

### User Story 4 - Consumer Info Retrieval (Priority: P2)

A developer or operator wants to inspect a consumer's current committed offset without affecting its operation. They call `Info()` to perform a fresh read of the offset entry from the KV store and receive the last committed sequence number and timestamp. This is useful for monitoring, debugging, and operational tooling.

**Why this priority**: Observability into consumer state is essential for production operations and debugging.

**Independent Test**: Can be tested by creating a consumer, committing offsets, calling Info, and verifying the returned values match. Also test Info when no offset has been committed.

**Acceptance Scenarios**:

1. **Given** a consumer that has committed offset at sequence 500, **When** `Info()` is called, **Then** the returned info contains seq=500, a valid committed_at timestamp, and the ownership fields (owner, host, claimed_at) of the live instance.
2. **Given** a consumer name for which no entry exists in KV, **When** `Info()` is called, **Then** null/nil is returned. (An entry exists as soon as an instance has claimed it, with seq=0 until the first commit.)
3. **Given** a consumer whose offset was modified externally, **When** `Info()` is called, **Then** the returned info reflects the current KV state (fresh read, not cached).

---

### User Story 5 - List Consumers on a Stream (Priority: P2)

A developer or operator wants to discover all offset consumers that have committed offsets for a given stream. They call `ListConsumers()` (a static/standalone function) with a stream name to retrieve all consumer names and their offset info. This is useful for monitoring, operational tooling, and consumer lifecycle management.

**Why this priority**: Discoverability of consumers is essential for operational tooling and monitoring.

**Independent Test**: Can be tested by creating multiple consumers on a stream, committing offsets, calling ListConsumers, and verifying all consumers are returned with correct info.

**Acceptance Scenarios**:

1. **Given** three consumers ("a", "b", "c") have committed offsets on stream "orders", **When** `ListConsumers("orders")` is called, **Then** a map/list of all three is returned with their offset info.
2. **Given** no consumers have committed offsets on stream "empty", **When** `ListConsumers("empty")` is called, **Then** an empty map/list is returned.
3. **Given** consumers exist on multiple streams, **When** `ListConsumers("orders")` is called, **Then** only consumers for the "orders" stream are returned.

---

### User Story 6 - Consumer Decommissioning (Priority: P3)

An operator permanently retires a consumer and wants to clean up its stored offset. They call `DeleteConsumer()` to remove the offset entry so that if the same consumer name is ever reused, it starts fresh from the beginning.

**Why this priority**: Cleanup is a housekeeping operation, not part of normal consumption.

**Independent Test**: Can be tested by creating a consumer, committing an offset, calling delete, and verifying a new consumer with the same name starts from the beginning.

**Acceptance Scenarios**:

1. **Given** a consumer with a stored offset, **When** `DeleteConsumer()` is called, **Then** the offset entry is removed and a new consumer with the same name starts from the beginning.
2. **Given** a running consumer, **When** `DeleteConsumer()` is called without first calling `Close()`, **Then** the offset is deleted but the consumer continues running (the application should call `Close()` separately).

---

### User Story 7 - Error Visibility (Priority: P2)

A developer needs visibility into errors that occur during consumption or committing. Terminal errors from the underlying message transport and commit failures (including concurrency conflicts) are surfaced through clear, distinguishable error types and configurable callbacks.

**Why this priority**: Without error visibility, developers cannot build reliable applications. This is essential for production use.

**Independent Test**: Can be tested by injecting error conditions and verifying the correct errors are surfaced through the appropriate channels.

**Acceptance Scenarios**:

1. **Given** a consumer with an `ErrorHandler` configured, **When** the underlying message transport encounters a terminal error, **Then** the error handler is called with the error and a reference to the consumer.
2. **Given** a live instance owning a consumer's offset entry, **When** a second instance with the same stream and consumer name is constructed, **Then** construction fails with a distinguishable "already in use" error that names the owner, its host and claim time.
3. **Given** a consumer name containing a dot character, **When** the consumer is constructed, **Then** an error is returned immediately and no consumer is created.
4. **Given** an empty consumer name, **When** the consumer is constructed, **Then** an error is returned immediately.

---

### Edge Cases

- What happens when two consumers with the same name on the same stream start simultaneously? Both read an unowned entry; the ownership claim is a compare-and-set write, so exactly one wins and the other fails construction with "already in use".
- What happens when an instance crashes? Its ephemeral message transport expires on the server after the inactive threshold; until then a new instance is refused, afterwards it is admitted and resumes from the last committed offset.
- What happens when an instance is closed cleanly? Close deletes its message transport and releases ownership, so a new instance can start immediately.
- What happens when the offset store is unavailable during construction? An error is returned and the consumer is not created.
- What happens when the target stream does not exist? An error is returned during construction when attempting to look up stream info.
- What happens when a second `CommitAsync` is called while one is already in flight? It is queued behind the first; both futures resolve in order.
- What happens when `Close()` is called on a consumer that was never started? Close completes without error.
- What happens when seeking while messages are in flight? The seek stops the current message transport, writes the new offset, and recreates the transport from the new position.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST consume messages from a named stream starting from a persisted position, delivering each raw NATS JetStream message to a caller-supplied callback/handler function (e.g., `Consume(handler)`). The stream sequence number is accessible via standard message metadata. If no position exists, the starting position is configurable: beginning of stream (default) or end of stream (only new messages).
- **FR-002**: System MUST persist the last committed stream sequence number in a shared named key-value store, using a key derived from the stream name and consumer name.
- **FR-003**: System MUST support synchronous commit that blocks until the offset is persisted.
- **FR-004**: System MUST support asynchronous commit that returns immediately with a future/promise resolving when the offset write is acknowledged; the in-memory committed position advances only on acknowledgement.
- **FR-005**: System MUST allow several asynchronous commits to be pending at once and apply them in call order; commit submissions are serialized so the stored offset never regresses.
- **FR-006**: System MUST claim ownership of the offset entry at construction with a single compare-and-set write, recording an owner prefix, host and claim time in the entry; commits and seeks are plain writes carrying those fields unchanged.
- **FR-007**: System MUST refuse construction with a distinguishable "already in use" error when the entry is owned and either the claim is younger than the inactive threshold or a message transport named with the owner prefix still exists on the stream.
- **FR-008**: System MUST support auto-commit mode with a configurable timer interval (default: 5 seconds) that commits only when new messages have been delivered.
- **FR-009**: On close the system MUST wait (bounded) for pending asynchronous commits, delete its message transport from the server, and release ownership of the offset entry; when auto-commit is enabled and uncommitted messages exist the release write also commits them. A close after `DeleteConsumer()` MUST NOT recreate the entry.
- **FR-010**: System MUST NOT advance the committed offset on close when auto-commit is disabled.
- **FR-011**: System MUST support seeking to an arbitrary sequence number, the beginning, or the current end of the stream.
- **FR-012**: Seek operations MUST stop current consumption, delete its message transport, persist the new offset, and restart consumption from the new position under a new transport name when consumption was running.
- **FR-013**: System MUST validate that the consumer name is non-empty and does not contain a dot character, returning an error at construction time if invalid.
- **FR-014**: System MUST automatically create the shared offset store on first use if it does not already exist, matching the target stream's replication configuration, with history depth set to 1 (only the latest offset per key is retained).
- **FR-015**: System MUST use an existing offset store as-is if one already exists.
- **FR-016**: System MUST support retrieval of a consumer's current offset entry from the KV store via an `Info()` call that performs a fresh read and returns the committed sequence number, timestamp and ownership fields. Returns null/nil if no entry exists.
- **FR-017**: System MUST support listing all offset consumers for a given stream by using KV's filtered key listing with a `{streamName}.*` filter pattern (server-side filtering). For each matching key, the offset entry is read and the consumer name is extracted from the key suffix. Returns a map/list of consumer names to their offset info (seq, committed_at, owner, host, claimed_at). This is a standalone/static operation that does not require an existing consumer instance.
- **FR-018**: System MUST support deletion of a consumer's offset entry for permanent decommissioning.
- **FR-019**: System MUST deliver terminal transport errors to a configurable error handler with a reference to the affected consumer. If no error handler is configured and the error is unrecoverable, the consumer silently stops consumption.
- **FR-020**: System MUST provide at-least-once delivery semantics — messages delivered after the last committed offset are redelivered after restart.
- **FR-021**: System MUST support optional subject filtering for consumption. When filtering is active, the offset stores the stream sequence of the last delivered (matching) message; non-contiguous sequence gaps are expected and handled by the filter on replay.
- **FR-022**: System MUST be implemented as a client-side library for Go, Java, JavaScript/TypeScript, and Rust. (.NET is out of scope: NATS.Net has no ordered-consumer name prefix, which FR-006/FR-007 require.)
- **FR-023**: System MUST name every message transport it creates with the instance's owner prefix followed by a generation counter, so that ownership can be checked by listing the stream's consumers.
- **FR-024**: The inactive threshold of the message transport MUST be configurable where the client allows it (Go, JS/TS; default 30s) and documented where it does not (Java: server default 5s; Rust: fixed 30s).

**Note on FR-017 (ListConsumers) in Rust**: The `async-nats` Rust client does not currently expose a `keys_filtered` or equivalent server-side filtered key listing API. The Rust implementation falls back to fetching all keys via `keys()` and filtering client-side by the `{stream}.` prefix. All other languages use server-side filtered key listing.

### Key Entities

- **Offset Consumer**: A logical consumer identified by a stream name and consumer name pair. Manages the lifecycle of consuming messages from a stream and persisting offsets. All public methods are thread-safe and may be called concurrently from multiple goroutines/threads.
- **Offset Entry**: A persisted record containing the last committed stream sequence number, a timestamp, and the ownership fields (owner prefix, host, claim time; empty when unowned), keyed by `{stream-name}.{consumer-name}`.
- **Offset Store**: A shared key-value store (`offset-consumers` bucket) where all offset entries are persisted across all streams and consumers.
- **Commit Future**: A language-native handle (`jetstream.PubAckFuture`, `CompletableFuture<Void>`, `Promise<void>`, `JoinHandle<Result<()>>`) returned by asynchronous commit that allows the caller to observe success or failure without blocking.
- **Owner Prefix**: `oc-{stream}-{consumer}-{nuid}`, unique per instance; the prefix of every message transport name the instance creates.
- **Consumer Configuration**: The set of parameters controlling consumer behavior — stream name, consumer name, auto-commit settings, subject filters, error handler, start position, and (Go, JS/TS) inactive threshold.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A consumer that is stopped and restarted resumes delivering messages from exactly the next sequence after the last committed offset, with zero message loss.
- **SC-002**: A second instance of a live consumer is refused at construction with an error naming the owner; after a clean close a new instance starts immediately, and after a crash within one inactive threshold.
- **SC-003**: In auto-commit mode, no writes to the offset store occur during periods when no new messages are delivered.
- **SC-004**: Consumer construction with an invalid name (empty or containing a dot) fails immediately with a descriptive error, before any external resources are accessed.
- **SC-005**: The library is available and functionally equivalent across Go, Java, JavaScript/TypeScript and Rust, with language-idiomatic APIs.
- **SC-006**: Seek operations reposition the consumer accurately — after `Seek(N)`, the next message delivered has sequence `N+1`.
- **SC-007**: The library operates without any server-side durable consumer state, using only ephemeral ordered consumers and a shared key-value store.
- **SC-008**: At-least-once delivery is maintained — no committed messages are lost, and uncommitted messages are redelivered after restart.

## Clarifications

### Session 2026-02-26

- Q: How should the offset track position when subject filtering causes non-contiguous sequence numbers? → A: Offset stores the stream sequence of the last delivered (matching) message; gaps are expected and handled by the filter on replay.
- Q: What happens when no ErrorHandler is configured and a terminal unrecoverable error occurs? → A: Consumer silently stops consumption; no notification is provided.
- Q: What is the default auto-commit interval when none is specified? → A: 5 seconds.
- Q: Should the consumer object be thread-safe for concurrent use from multiple goroutines/threads? → A: Yes, all public methods are thread-safe.
- Q: Should new consumers (no stored offset) support a configurable initial start position? → A: Yes, configurable: beginning (default) or end of stream.
- Q: How should `Consume()` deliver messages to the application? → A: Via a caller-supplied callback/handler function (e.g., `Consume(handler)`).
- Q: How should transient NATS connection interruptions be handled? → A: Rely on underlying NATS client/ordered consumer reconnection; only surface terminal errors.
- Q: Should `Close()` accept a timeout or context for cancellation? → A: No, `Close()` blocks indefinitely; caller manages timeout externally.
- Q: What KV bucket history depth should the offset store use? → A: History = 1 (only latest offset retained per key).
- Q: What should the message callback receive? → A: The raw NATS JetStream message (sequence accessible via standard message metadata).

## Assumptions

- The NATS server supports JetStream with KV store capabilities.
- A single instance of a given `{stream}.{consumerName}` pair runs at a time; the library enforces this at startup but does not notify a displaced owner and offers no forced takeover. No consumer group coordination is provided.
- Application message handlers are idempotent, consistent with at-least-once delivery semantics.
- Transient NATS connection interruptions are handled by the underlying NATS client and ordered consumer reconnection mechanism. The offset consumer library does not implement its own reconnection logic; only terminal errors are surfaced.
- The offset store bucket is shared across all streams; the first consumer to create it determines its replication factor. Operators needing specific configuration should pre-create the bucket.
- Soft deletes with a 10-minute tombstone TTL are acceptable for offset entry cleanup.
- Go, Java, JavaScript/TypeScript and Rust are the implementation targets.
