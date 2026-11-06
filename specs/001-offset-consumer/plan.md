# Implementation Plan: NATS JetStream Offset Consumer Library

**Branch**: `001-offset-consumer` | **Date**: 2026-02-26 (revised 2026-11-06) | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/001-offset-consumer/spec.md`

## Summary

A client-side library (Go, Java, JS/TS, Rust) providing Kafka-style offset-based consumption on top of NATS JetStream. Uses ephemeral ordered consumers for message transport and a shared NATS KV bucket (`offset-consumers`) for durable offset persistence. Single-active-consumer semantics are enforced at startup by an ownership claim: each instance names its ordered consumers with a unique owner prefix recorded in the KV entry, and a new instance is refused while a consumer with the stored prefix is alive. Commits are plain, pipelined KV puts (the only compare-and-set write is the claim). Supports manual and auto-commit modes, seek operations, subject filtering, `Info`/`ListConsumers`, and per-consumer error handling. No server-side durable consumer state is required.

## Technical Context

**Language/Version**: Go 1.26+, Java 11+, TypeScript/JavaScript ES2022+, Rust 1.89+
**Primary Dependencies**:
- Go: `github.com/nats-io/nats.go` v1.55.0 (`jetstream` sub-package; `OrderedConsumerConfig.NamePrefix`/`InactiveThreshold`, `js.Options()`)
- Java: `io.nats:jnats` v2.26.4 (simplified API: `StreamContext`, `OrderedConsumerContext.getConsumerName()`, `OrderedConsumerConfiguration.consumerNamePrefix()`)
- JS/TS: `@nats-io/jetstream` v3.4.0, `@nats-io/kv` v3.4.0 (peer: `@nats-io/nats-core` v3.4.0; `OrderedConsumerOptions.name_prefix`/`inactive_threshold`)
- Rust: `async-nats` v0.51.0 (`pull::OrderedConfig.name`, `Stream::consumer_names`), `nuid`, `hostname`

**Storage**: NATS KV bucket `offset-consumers` — JSON values with `seq` (uint64), `committed_at` (RFC3339), `owner` (prefix string), `host`, `claimed_at` (RFC3339) fields
**Testing**:
- Go: `go test` with embedded nats-server (`github.com/nats-io/nats-server/v2/test`, v2.15.2)
- Java: JUnit 5 with `io.nats:jnats-server-runner` 4.0.2 (spawns the `nats-server` binary from PATH)
- JS/TS: Vitest with nats-server binary
- Rust: `cargo test` with nats-server binary

**Target Platform**: Cross-platform library (any platform supported by NATS clients)
**Project Type**: Library (four separate language-specific packages: Go, Java, JS/TS, Rust)
**Performance Goals**: Minimal overhead over raw ordered consumer; commit latency dominated by KV round-trip
**Constraints**: Thread-safe public API; single active consumer per `{stream}.{consumerName}` pair, enforced by the startup ownership claim (no displaced-owner detection, no forced takeover); at-least-once delivery
**Scale/Scope**: Single library per language, ~500-1000 LOC each

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### Pre-Phase 0 Check

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Code Quality | PASS | Single responsibility per type; language-idiomatic conventions; explicit error handling; minimal dependencies (only NATS client) |
| II. Testing Standards | PASS | Integration tests with embedded NATS server; test naming convention defined; deterministic tests with in-process server |
| III. UX Consistency | PASS | API mirrors Kafka consumer semantics; error messages are actionable; config uses consistent naming across languages |
| IV. Performance | PASS | No hot-path allocations beyond KV serialization; benchmarks planned for commit path |
| Quality Gates | PASS | Lint, test, coverage (80%), benchmark, docs gates all applicable |
| Development Workflow | PASS | Feature branch; conventional commits; test-first; incremental delivery by language |

### Post-Phase 1 Re-check

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Code Quality | PASS | API contracts defined per language with clear single-responsibility types. Error types wrapped with library-specific sentinels. Doc comments on all public symbols. |
| II. Testing Standards | PASS | Test strategy defined (R-006). Embedded NATS server for deterministic tests. Test helper patterns documented. |
| III. UX Consistency | PASS | Consistent API shape across all four languages. Builder pattern (Java), interface (TS), struct (Go, Rust). Error messages describe what/why/how; the already-in-use error names the owner, host and claim time. |
| IV. Performance | PASS | No unnecessary allocations. KV operations are the only I/O. Commits are pipelined plain puts; the single CAS write happens at startup. |

**Gate result**: PASS — no violations.

## Project Structure

### Documentation (this feature)

```text
specs/001-offset-consumer/
├── plan.md              # This file
├── research.md          # Phase 0 output — NATS API research across all languages
├── data-model.md        # Phase 1 output — entity definitions and state machine
├── quickstart.md        # Phase 1 output — usage examples per language
├── contracts/
│   ├── go-api.md        # Go API contract
│   ├── java-api.md      # Java API contract
│   ├── js-api.md        # JS/TS API contract
│   └── rust-api.md      # Rust API contract
└── tasks.md             # Phase 2 output (generated by /speckit.tasks)
```

### Source Code (repository root)

Each language implementation lives in its own directory at the repository root, following that language's conventions:

```text
offset-consumer-go/
├── go.mod
├── go.sum
├── consumer.go            # OffsetConsumer interface, OffsetConsumerInfo, implementation struct
├── consumer_impl.go       # NewConsumer() (ownership claim), Consume, commits, Info, Close
├── config.go              # ConsumerConfig, option defaults, StartPosition
├── errors.go              # Sentinel errors, AlreadyInUseError
├── seek.go                # Seek operations, DeleteConsumer
├── kv.go                  # Offset entry JSON, bucket ensure, claim (CAS), liveness check, put subject, ListConsumers
├── helper_test.go         # Embedded nats-server helpers
└── consumer_test.go

offset-consumer-java/
├── settings.gradle.kts
├── build.gradle.kts
└── src/
    ├── main/java/io/synadia/offsetconsumer/
    │   ├── OffsetConsumer.java          # Interface
    │   ├── OffsetConsumerFactory.java   # create() with ownership claim, listConsumers()
    │   ├── OffsetConsumerImpl.java      # Implementation (single commit thread)
    │   ├── OffsetEntry.java             # KV value JSON in/out
    │   ├── OffsetConsumerInfo.java
    │   ├── ConsumerConfig.java
    │   ├── CommitException.java
    │   ├── AlreadyInUseException.java
    │   ├── ErrorHandler.java
    │   └── StartPosition.java
    └── test/java/io/synadia/offsetconsumer/

offset-consumer-js/
├── package.json
├── tsconfig.json
├── tsup.config.ts
├── src/
│   ├── index.ts
│   ├── consumer.ts          # createConsumer (claim), listConsumers, OffsetConsumerImpl
│   ├── kv.ts                # Offset entry JSON, bucket ensure, claim (CAS)
│   ├── errors.ts
│   └── types.ts
└── test/

offset-consumer-rust/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── config.rs
│   ├── consumer.rs          # OffsetConsumer::new (claim), consume, commits, seek, close, list_consumers
│   ├── kv.rs                # Offset entry JSON, bucket ensure, claim (CAS)
│   └── errors.rs
└── tests/
```

**Structure Decision**: Multi-directory layout at repo root, one directory per language. Each directory is an independent package/module following its language's standard project layout. Shared spec and design artifacts live in `specs/001-offset-consumer/`.

## Complexity Tracking

No constitution violations — table not needed.
