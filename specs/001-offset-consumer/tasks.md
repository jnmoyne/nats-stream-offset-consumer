# Tasks: NATS JetStream Offset Consumer Library

**Input**: Design documents from `/specs/001-offset-consumer/`
**Prerequisites**: plan.md (required), spec.md (required), research.md, data-model.md, contracts/

**Tests**: Not explicitly requested in the feature specification. Test tasks are omitted.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing. Each language (Go, Java, .NET, JS/TS) is implemented in its own directory and tasks across languages are parallelizable.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Go**: `offset-consumer-go/`
- **Java**: `offset-consumer-java/src/main/java/io/synadia/offsetconsumer/`
- **.NET**: `offset-consumer-dotnet/src/Synadia.OffsetConsumer/`
- **JS/TS**: `offset-consumer-js/src/`

---

## Phase 1: Setup (Project Scaffolding)

**Purpose**: Initialize all four language project directories with build files and dependencies

- [x] T001 Create Go project with go.mod and dependency on `github.com/nats-io/nats.go` v1.49+ in `offset-consumer-go/go.mod`
- [x] T002 [P] Create Java Gradle project with `java-library` plugin, jnats v2.25.1+ dependency in `offset-consumer-java/build.gradle.kts` and `offset-consumer-java/settings.gradle.kts`
- [x] T003 [P] Create .NET project with `Synadia.OffsetConsumer.csproj` targeting net8.0, NATS.Net v2.7.2+ dependency in `offset-consumer-dotnet/OffsetConsumer.sln` and `offset-consumer-dotnet/src/Synadia.OffsetConsumer/Synadia.OffsetConsumer.csproj`
- [x] T004 [P] Create JS/TS project with package.json, tsconfig.json, tsup config, dependencies on `@nats-io/jetstream` ^3.3 and `@nats-io/kv` ^3.3, peer dependency on `@nats-io/nats-core` ^3.3 in `offset-consumer-js/package.json` and `offset-consumer-js/tsconfig.json`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Error types, configuration types, KV helpers, and consumer interfaces that ALL user stories depend on

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

### Go Foundational

- [x] T005 Implement sentinel errors (`ErrCASConflict`, `ErrAsyncCommitInFlight`, `ErrConsumerClosed`, `ErrInvalidConsumerName`, `ErrAlreadyConsuming`, `ErrNotConsuming`) per go-api.md contract in `offset-consumer-go/errors.go`
- [x] T006 [P] Implement `StartPosition` enum and `ConsumerConfig` struct with all fields (Stream, ConsumerName, AutoCommit, AutoCommitInterval, FilterSubjects, ErrorHandler, StartPosition) per go-api.md contract in `offset-consumer-go/config.go`
- [x] T007 [P] Implement `CommitFuture` interface with `Ok()` and `Err()` channels per go-api.md contract in `offset-consumer-go/commit.go`
- [x] T008 [P] Implement KV helper: bucket ensure (create-if-not-exists with replicas matching stream, history=1, SubjectDeleteMarkerTTL=10min), CAS update wrapper, offset entry JSON marshal/unmarshal (`seq` uint64 + `committed_at` RFC3339) per research.md R-008 in `offset-consumer-go/kv.go`
- [x] T009 Implement `OffsetConsumer` interface with all method signatures (Consume, CommitSync, CommitAsync, Seek, SeekToBeginning, SeekToEnd, DeleteConsumer, Close) and unexported `offsetConsumer` implementation struct with all internal fields per data-model.md in `offset-consumer-go/consumer.go`

### Java Foundational

- [x] T010 [P] Implement exception hierarchy: `CommitException`, `CASConflictException` (extends CommitException), `AsyncCommitInFlightException` (extends IllegalStateException) per java-api.md contract in `offset-consumer-java/src/main/java/io/synadia/offsetconsumer/CommitException.java`, `CASConflictException.java`, `AsyncCommitInFlightException.java`
- [x] T011 [P] Implement `StartPosition` enum (BEGINNING, END) per java-api.md contract in `offset-consumer-java/src/main/java/io/synadia/offsetconsumer/StartPosition.java`
- [x] T012 [P] Implement `ErrorHandler` functional interface per java-api.md contract in `offset-consumer-java/src/main/java/io/synadia/offsetconsumer/ErrorHandler.java`
- [x] T013 [P] Implement `ConsumerConfig` with builder pattern (stream, consumerName, autoCommit, autoCommitInterval, filterSubjects, errorHandler, startPosition) per java-api.md contract in `offset-consumer-java/src/main/java/io/synadia/offsetconsumer/ConsumerConfig.java`
- [x] T014 [P] Implement `OffsetConsumer` interface with all method signatures (consume, commitSync, commitAsync, seek, seekToBeginning, seekToEnd, deleteConsumer, close) per java-api.md contract in `offset-consumer-java/src/main/java/io/synadia/offsetconsumer/OffsetConsumer.java`

### .NET Foundational

- [x] T015 [P] Implement exception hierarchy: `CommitException`, `CASConflictException` (extends CommitException, wraps NatsKVWrongLastRevisionException) per dotnet-api.md contract in `offset-consumer-dotnet/src/Synadia.OffsetConsumer/CommitException.cs` and `CASConflictException.cs`
- [x] T016 [P] Implement `StartPosition` enum (Beginning, End) per dotnet-api.md contract in `offset-consumer-dotnet/src/Synadia.OffsetConsumer/StartPosition.cs`
- [x] T017 [P] Implement `ConsumerConfig` record with all properties (Stream, ConsumerName, AutoCommit, AutoCommitInterval, FilterSubjects, ErrorHandler, StartPosition) per dotnet-api.md contract in `offset-consumer-dotnet/src/Synadia.OffsetConsumer/ConsumerConfig.cs`
- [x] T018 [P] Implement `IOffsetConsumer` interface with all method signatures (ConsumeAsync, CommitSyncAsync, CommitAsync, SeekAsync, SeekToBeginningAsync, SeekToEndAsync, DeleteConsumerAsync) extending IAsyncDisposable per dotnet-api.md contract in `offset-consumer-dotnet/src/Synadia.OffsetConsumer/IOffsetConsumer.cs`

### JS/TS Foundational

- [x] T019 [P] Implement error types: `CASConflictError`, `AsyncCommitInFlightError`, `InvalidConsumerNameError` per js-api.md contract in `offset-consumer-js/src/errors.ts`
- [x] T020 [P] Implement `ConsumerConfig` interface, `StartPosition` type ("beginning" | "end"), `ConsumerMessages` interface, `OffsetConsumer` interface per js-api.md contract in `offset-consumer-js/src/types.ts`
- [x] T021 [P] Implement KV helper: bucket ensure (create-if-not-exists), CAS update wrapper, offset entry JSON serialization using `Kvm` from `@nats-io/kv` per research.md R-003b in `offset-consumer-js/src/kv.ts`
- [x] T022 [P] Create public API barrel export in `offset-consumer-js/src/index.ts` re-exporting all public types, errors, and `createConsumer` function

**Checkpoint**: Foundation ready — all types, errors, and interfaces defined. User story implementation can now begin.

---

## Phase 3: User Story 1 — Durable Resumable Consumption (Priority: P1) 🎯 MVP

**Goal**: A consumer resumes from the last committed offset after restart. Supports CommitSync and CommitAsync for explicit offset persistence.

**Independent Test**: Create consumer → process messages → commit offset → restart consumer → verify it resumes from correct position (last committed seq + 1).

### Go Implementation for US1

- [x] T023 [US1] Implement `NewConsumer()` constructor: validate consumer name (non-empty, no dot), look up stream info, ensure KV bucket via kv.go helper, read stored offset (or resolve start position from config), initialize all internal fields, return `OffsetConsumer` in `offset-consumer-go/consumer.go`
- [x] T024 [US1] Implement `Consume(handler)` method: create ordered consumer with `DeliverPolicy` and `OptStartSeq` based on resolved offset, set up `FilterSubjects` if configured, deliver messages via handler callback updating `lastDeliveredSeq`, handle `ErrAlreadyConsuming` and `ErrConsumerClosed` guards in `offset-consumer-go/consumer_impl.go`
- [x] T025 [US1] Implement `CommitSync(ctx)` method: marshal offset entry JSON, CAS update via kv.go helper using `lastRevision`, update `lastCommittedSeq` and `lastRevision` on success, wrap CAS failure as `ErrCASConflict` in `offset-consumer-go/consumer_impl.go`
- [x] T026 [US1] Implement `CommitAsync()` method: check `asyncCommitInFlight` flag (return `ErrAsyncCommitInFlight` if true), set flag, launch goroutine performing CAS update, return `commitFuture` struct implementing `CommitFuture` interface, clear flag on completion in `offset-consumer-go/consumer_impl.go`
- [x] T027 [US1] Implement `Close()` method (manual-commit path only): stop ordered consumer via `ConsumeContext.Stop()`, release resources, set `closed` flag. Auto-commit final commit is deferred to US2 in `offset-consumer-go/consumer_impl.go`

### Java Implementation for US1

- [x] T028 [P] [US1] Implement `OffsetConsumerFactory.create(Connection, ConsumerConfig)`: validate consumer name, get StreamContext, ensure KV bucket (create if not exists with replicas matching stream, history=1), read stored offset, resolve start sequence, return implementation in `offset-consumer-java/src/main/java/io/synadia/offsetconsumer/OffsetConsumerFactory.java`
- [x] T029 [US1] Implement `OffsetConsumerImpl`: internal fields (streamContext, kv, kvKey, lastRevision, startSeq, lastDeliveredSeq, lastCommittedSeq, asyncCommitInFlight, lock, closed), `consume(MessageHandler)` creating OrderedConsumerContext with DeliverPolicy/startSequence/filterSubjects, `commitSync()` with KV CAS update, `commitAsync()` returning CompletableFuture, `close()` (manual path) in `offset-consumer-java/src/main/java/io/synadia/offsetconsumer/OffsetConsumerImpl.java`

### .NET Implementation for US1

- [x] T030 [P] [US1] Implement `OffsetConsumer.CreateAsync(INatsJSContext, ConsumerConfig)`: validate consumer name, look up stream, ensure KV bucket (CreateStoreAsync or GetStoreAsync), read stored offset, resolve start sequence, return implementation in `offset-consumer-dotnet/src/Synadia.OffsetConsumer/OffsetConsumer.cs`
- [x] T031 [US1] Implement OffsetConsumer class: internal fields, `ConsumeAsync()` returning IAsyncEnumerable via ordered consumer with DeliverPolicy/OptStartSeq/FilterSubjects, `CommitSyncAsync()` with KV CAS UpdateAsync, `CommitAsync()` with in-flight guard, `DisposeAsync()` (manual path) in `offset-consumer-dotnet/src/Synadia.OffsetConsumer/OffsetConsumer.cs`

### JS/TS Implementation for US1

- [x] T032 [P] [US1] Implement `createConsumer(NatsConnection, ConsumerConfig)`: validate consumer name (throw InvalidConsumerNameError), create JetStream context via `jetstream(nc)`, look up stream, ensure KV bucket via kv.ts helper, read stored offset, resolve start sequence, return OffsetConsumer implementation in `offset-consumer-js/src/consumer.ts`
- [x] T033 [US1] Implement OffsetConsumer class: `consume()` creating ordered consumer with deliverPolicy/optStartSeq/filterSubjects returning ConsumerMessages (AsyncIterable wrapper with stop()), `commitSync()` with KV CAS update, `commitAsync()` with in-flight guard, `close()` (manual path), track lastDeliveredSeq/lastCommittedSeq/lastRevision in `offset-consumer-js/src/consumer.ts`
- [x] T034 [US1] Implement commit helper logic: offset entry JSON serialization (`{seq, committed_at}`), CAS update via kv.ts, wrap revision mismatch as CASConflictError in `offset-consumer-js/src/commit.ts`

**Checkpoint**: US1 complete — consumers can be created, consume messages, commit offsets (sync and async), and resume from stored position after restart.

---

## Phase 4: User Story 2 — Manual and Automatic Commit Modes (Priority: P1)

**Goal**: Auto-commit mode commits offsets on a configurable timer interval. Final commit on close when auto-commit is enabled.

**Independent Test**: Configure consumer with autoCommit=true and interval → verify offsets committed periodically → verify no commit when no new messages → verify final commit on Close().

### Go Implementation for US2

- [x] T035 [US2] Implement auto-commit timer: start background goroutine on `Consume()` when `AutoCommit=true`, commit every `AutoCommitInterval` only when `lastDeliveredSeq > lastCommittedSeq`, stop timer on Close() in `offset-consumer-go/consumer_impl.go`
- [x] T036 [US2] Update `Close()` to perform final synchronous commit when `AutoCommit=true` and `lastDeliveredSeq > lastCommittedSeq`, stop auto-commit timer before final commit in `offset-consumer-go/consumer_impl.go`

### Java Implementation for US2

- [x] T037 [P] [US2] Implement auto-commit in OffsetConsumerImpl: start ScheduledExecutorService timer on consume() when autoCommit=true, commit every autoCommitInterval only when lastDeliveredSeq > lastCommittedSeq, update close() to perform final commitSync and shutdown executor in `offset-consumer-java/src/main/java/io/synadia/offsetconsumer/OffsetConsumerImpl.java`

### .NET Implementation for US2

- [x] T038 [P] [US2] Implement auto-commit in OffsetConsumer: start Timer/PeriodicTimer on ConsumeAsync() when AutoCommit=true, commit every AutoCommitInterval only when lastDeliveredSeq > lastCommittedSeq, update DisposeAsync() to perform final CommitSyncAsync and stop timer in `offset-consumer-dotnet/src/Synadia.OffsetConsumer/OffsetConsumer.cs`

### JS/TS Implementation for US2

- [x] T039 [P] [US2] Implement auto-commit in consumer: start setInterval timer on consume() when autoCommit=true, commit every autoCommitIntervalMs only when lastDeliveredSeq > lastCommittedSeq, update close() to perform final commitSync and clearInterval in `offset-consumer-js/src/consumer.ts`

**Checkpoint**: US2 complete — auto-commit mode works with configurable interval, final commit on close, no unnecessary commits when idle.

---

## Phase 5: User Story 5 — Error Visibility (Priority: P2)

**Goal**: Terminal transport errors are surfaced through configurable error handler. CAS conflicts are distinguishable error types. Invalid consumer names are rejected at construction.

**Independent Test**: Configure error handler → inject terminal error → verify handler called. Attempt commit after external modification → verify CAS conflict error. Create consumer with invalid name → verify immediate error.

### Go Implementation for US5

- [x] T040 [US5] Wire error handler into ordered consumer consume options via `jetstream.ConsumeErrHandler`, filter for terminal errors (non-informational), invoke `config.ErrorHandler(consumer, err)` if set, otherwise stop consumption silently in `offset-consumer-go/consumer_impl.go`

### Java Implementation for US5

- [x] T041 [P] [US5] Wire error handler into consume flow: detect terminal errors from MessageConsumer/ordered consumer, invoke `config.errorHandler().onError(consumer, err)` if set, otherwise stop consumption silently in `offset-consumer-java/src/main/java/io/synadia/offsetconsumer/OffsetConsumerImpl.java`

### .NET Implementation for US5

- [x] T042 [P] [US5] Wire error handler into consume flow: monitor for terminal errors/NatsConnectionFailedException during ConsumeAsync enumeration, invoke `config.ErrorHandler(consumer, ex)` if set, otherwise stop consumption silently in `offset-consumer-dotnet/src/Synadia.OffsetConsumer/OffsetConsumer.cs`

### JS/TS Implementation for US5

- [x] T043 [P] [US5] Wire error handler into consume flow: monitor `messages.status()` AsyncIterable for terminal/heartbeat-missed events, invoke `config.errorHandler(consumer, err)` if set, otherwise stop consumption silently in `offset-consumer-js/src/consumer.ts`

**Checkpoint**: US5 complete — terminal errors surfaced via error handler, CAS conflicts distinguishable (already implemented in US1 commit methods), invalid names rejected at construction (already implemented in US1 constructors).

---

## Phase 6: User Story 3 — Seek to Arbitrary Position (Priority: P2)

**Goal**: Reposition consumer to a specific sequence, beginning, or end of stream. Seek stops consumption, persists new offset, restarts from new position.

**Independent Test**: Consume to known position → seek to different position → verify messages delivered from new position. Seek with KV failure → verify error returned and consumer left stopped.

### Go Implementation for US3

- [x] T044 [US3] Implement `Seek(ctx, seq)`: stop current ordered consumer, CAS update offset to seq via kv.go, update internal state (lastRevision, startSeq, lastDeliveredSeq, lastCommittedSeq), recreate ordered consumer from seq+1. On CAS failure return error and leave consumer stopped in `offset-consumer-go/seek.go`
- [x] T045 [US3] Implement `SeekToBeginning(ctx)` delegating to Seek(ctx, 0) and `SeekToEnd(ctx)` looking up stream info for last sequence then delegating to Seek in `offset-consumer-go/seek.go`

### Java Implementation for US3

- [x] T046 [P] [US3] Implement `seek(long seq)`, `seekToBeginning()`, `seekToEnd()`: stop current MessageConsumer, CAS update KV offset, update internal state, recreate ordered consumer. SeekToEnd looks up stream info for last seq. On CAS failure throw CASConflictException and leave consumer stopped in `offset-consumer-java/src/main/java/io/synadia/offsetconsumer/OffsetConsumerImpl.java`

### .NET Implementation for US3

- [x] T047 [P] [US3] Implement `SeekAsync(ulong seq)`, `SeekToBeginningAsync()`, `SeekToEndAsync()`: cancel current consumption, CAS update KV offset, update internal state, signal ConsumeAsync to restart from new position. On CAS failure throw CASConflictException and leave consumer stopped in `offset-consumer-dotnet/src/Synadia.OffsetConsumer/OffsetConsumer.cs`

### JS/TS Implementation for US3

- [x] T048 [P] [US3] Implement `seek(seq)`, `seekToBeginning()`, `seekToEnd()`: stop current ConsumerMessages, CAS update KV offset via kv.ts, update internal state, recreate ordered consumer. SeekToEnd looks up stream info for last seq. On CAS failure throw CASConflictError and leave consumer stopped in `offset-consumer-js/src/seek.ts`

**Checkpoint**: US3 complete — seek operations reposition consumer accurately, CAS failures leave consumer stopped.

---

## Phase 7: User Story 4 — Consumer Decommissioning (Priority: P3)

**Goal**: Permanently remove a consumer's offset entry so a new consumer with the same name starts fresh.

**Independent Test**: Create consumer → commit offset → call deleteConsumer → create new consumer with same name → verify it starts from beginning.

### Go Implementation for US4

- [x] T049 [US4] Implement `DeleteConsumer(ctx)`: delete KV key via `kv.Delete(ctx, kvKey)`, reset `lastRevision` to 0. Does not stop consumption — independent of Close() in `offset-consumer-go/consumer_impl.go`

### Java Implementation for US4

- [x] T050 [P] [US4] Implement `deleteConsumer()`: delete KV key via `kv.delete(kvKey)`, reset lastRevision. Does not stop consumption in `offset-consumer-java/src/main/java/io/synadia/offsetconsumer/OffsetConsumerImpl.java`

### .NET Implementation for US4

- [x] T051 [P] [US4] Implement `DeleteConsumerAsync()`: delete KV key via `store.DeleteAsync(kvKey)`, reset lastRevision. Does not stop consumption in `offset-consumer-dotnet/src/Synadia.OffsetConsumer/OffsetConsumer.cs`

### JS/TS Implementation for US4

- [x] T052 [P] [US4] Implement `deleteConsumer()`: delete KV key via kv.ts helper, reset lastRevision. Does not stop consumption in `offset-consumer-js/src/consumer.ts`

**Checkpoint**: US4 complete — offset entries can be deleted for decommissioning.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Finalize exports, validate examples, documentation

- [x] T053 [P] Verify Go public API surface matches go-api.md contract — ensure all exported types and methods are documented with godoc comments in `offset-consumer-go/*.go`
- [x] T054 [P] Verify Java public API surface matches java-api.md contract — ensure all public types have Javadoc in `offset-consumer-java/src/main/java/io/synadia/offsetconsumer/*.java`
- [x] T055 [P] Verify .NET public API surface matches dotnet-api.md contract — ensure all public types have XML doc comments in `offset-consumer-dotnet/src/Synadia.OffsetConsumer/*.cs`
- [x] T056 [P] Verify JS/TS public API surface matches js-api.md contract — ensure all exported types have JSDoc and barrel export is complete in `offset-consumer-js/src/index.ts`
- [x] T057 Run quickstart.md code examples against each implementation to validate API shape correctness

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion for respective language — BLOCKS all user stories
- **User Stories (Phase 3+)**: All depend on Foundational phase completion
  - US1 (P1): Must complete first — US2 depends on it
  - US2 (P1): Depends on US1 (extends Close and Consume with auto-commit)
  - US5 (P2): Depends on US1 (extends Consume with error handler wiring)
  - US3 (P2): Depends on US1 (requires working consume/KV infrastructure)
  - US4 (P3): Depends on US1 (requires working KV infrastructure)
  - US3, US4, US5 can proceed in parallel after US1
- **Polish (Phase 8)**: Depends on all user stories being complete

### User Story Dependencies

- **US1 (P1)**: Can start after Foundational (Phase 2) — No dependencies on other stories
- **US2 (P1)**: Depends on US1 (extends consume and close methods with auto-commit logic)
- **US5 (P2)**: Depends on US1 (extends consume method with error handler wiring) — can parallel with US2
- **US3 (P2)**: Depends on US1 (uses KV helper and ordered consumer recreation) — can parallel with US2, US5
- **US4 (P3)**: Depends on US1 (uses KV delete) — can parallel with US2, US3, US5

### Within Each User Story

- Go implementation first (reference implementation), then Java, .NET, JS/TS in parallel
- Constructor before consume method
- Consume before commit
- Core implementation before edge cases

### Parallel Opportunities

- All Setup tasks (T001-T004) can run in parallel across languages
- All Foundational tasks within different languages can run in parallel (Go: T005-T009, Java: T010-T014, .NET: T015-T018, JS/TS: T019-T022)
- After US1 completes: US2, US3, US4, US5 can start in parallel across languages
- Within any user story: different language implementations can run in parallel (all marked [P])

---

## Parallel Example: User Story 1

```bash
# After Go US1 tasks (T023-T027) establish the reference pattern:

# Launch all language implementations in parallel:
Task: T028 "Java OffsetConsumerFactory.create()"
Task: T030 ".NET OffsetConsumer.CreateAsync()"
Task: T032 "JS/TS createConsumer()"

# Then within each language, sequential: constructor → consume → commit → close
```

## Parallel Example: After US1 Complete

```bash
# All these user stories can start simultaneously:
Task: T035-T036 "Go auto-commit (US2)"
Task: T040 "Go error handler (US5)"
Task: T044-T045 "Go seek (US3)"
Task: T049 "Go delete consumer (US4)"

# And across languages for each story:
Task: T037 "Java auto-commit (US2)" + T038 ".NET auto-commit (US2)" + T039 "JS auto-commit (US2)"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (all 4 languages)
2. Complete Phase 2: Foundational (all 4 languages)
3. Complete Phase 3: User Story 1 — Durable Resumable Consumption
4. **STOP and VALIDATE**: Test consume → commit → restart → resume for each language
5. Deploy/demo if ready — this is the core value proposition

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add US1 → Test independently → **MVP!** (consume/commit/resume)
3. Add US2 → Test independently → Auto-commit mode available
4. Add US5 → Test independently → Error visibility for production use
5. Add US3 → Test independently → Seek operations for operational recovery
6. Add US4 → Test independently → Consumer decommissioning for cleanup
7. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. One developer builds US1 in Go (reference implementation)
3. Once Go US1 is done, other developers port to Java, .NET, JS/TS in parallel
4. After US1 is complete across all languages:
   - Developer A: US2 (auto-commit) across all languages
   - Developer B: US3 + US4 (seek + delete) across all languages
   - Developer C: US5 (error visibility) across all languages

---

## Notes

- [P] tasks = different files/languages, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story is independently completable and testable
- Go serves as reference implementation; other languages follow the same patterns
- All four languages share the same KV bucket format (`offset-consumers`) and key schema (`{stream}.{consumerName}`)
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
