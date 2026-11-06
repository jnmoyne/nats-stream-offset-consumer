# offset-consumer Development Guidelines

Auto-generated from all feature plans. Last updated: 2026-11-06

## Active Technologies
- Go 1.26+ (primary), Java 11+ (primary), TypeScript/JavaScript ES2022+ (primary), Rust 1.89+ (primary) + `github.com/nats-io/nats.go` v1.55.0 (Go), `io.nats:jnats` v2.26.4 (Java), `@nats-io/jetstream` v3.4.0 & `@nats-io/kv` v3.4.0 (JS/TS), `async-nats` v0.51.0 (Rust) (001-offset-consumer)
- NATS KV bucket `offset-consumers` — JSON values with `seq` (uint64), `committed_at` (RFC3339), `owner` (owner prefix `oc-{stream}-{consumer}-{nuid}`, empty when unowned), `host`, `stream_created` (stream creation time, decimal Unix nanoseconds) fields (001-offset-consumer)
- Single active consumer: the entry is claimed (one CAS write) when consumption starts; the stored owner is alive while a consumer named with its owner prefix exists, re-checked after a local 5s wait before a takeover (no timestamps compared); seeks are make-before-break; ordered consumers are named `{owner}-g{generation}...`; Close's release write is a CAS on an entry still naming the instance; commits are plain pipelined puts (001-offset-consumer)
- .NET port dropped 2026-11-06 (NATS.Net has no ordered-consumer name prefix); see README.md

## Project Structure

```text
offset-consumer-go/      # Go module (go test ./...)
offset-consumer-java/    # Gradle project (./gradlew test; needs nats-server on PATH)
offset-consumer-js/      # npm package (npm test; needs nats-server on PATH)
offset-consumer-rust/    # Cargo crate (cargo test; needs nats-server on PATH)
specs/001-offset-consumer/   # spec, plan, data model, research, quickstart, contracts, tasks
offset-consumer-spec.md      # design specification (1.5)
README.md
```

## Commands

```sh
cd offset-consumer-go   && go vet ./... && go test ./... -count=1
cd offset-consumer-java && ./gradlew test
cd offset-consumer-js   && npm install && npx tsc --noEmit -p . && npm run build && npm test
cd offset-consumer-rust && cargo test
```

## Code Style

Go, Java, TypeScript, Rust: follow each language's standard conventions; keep the four ports semantically identical (Go is the reference)

## Recent Changes
- 2026-11-06 (spec 1.5): claim moved from construction to Consume (constructor only pre-checks, writes nothing); seek is make-before-break; takeover of an owner without consumers waits 5s (no jitter: the CAS decides between concurrent starters) and CASes against the pre-wait revision; `claimed_at` removed from the value, Info and AlreadyInUse
- 2026-11-06 (spec 1.4): presence consumer and second liveness listing removed, claim grace window restored; Close's release write is a CAS on an entry that still names the instance (AlreadyInUse if another instance claimed it, nothing written if deleted); no displaced-owner detection
- 2026-11-06 (spec 1.3): clock-free ownership liveness: presence consumer created before the claim, second liveness listing after 1s, `claimed_at` no longer compared
- 2026-11-06 (spec 1.2): auto-commit and Close commit the last *processed* sequence; StartPosition End resolved and stored at construction; existing bucket never overwritten (replicas only raised); Consume/Seek/Close serialized; ErrorHandler may Close; Go commit ack timeout; stream recreation detected via `stream_created`; stricter name validation; Rust delivery loop no longer stops on the first error
- 2026-11-06: ownership claim replaces per-commit CAS; pipelined async commits; Info/ListConsumers carry ownership fields; client libraries upgraded (nats.go 1.55.0, jnats 2.26.4, @nats-io 3.4.0, async-nats 0.51.0); Rust port added; .NET port removed
- 001-offset-consumer: Added Go, Java, JS/TS implementations of the offset consumer

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
