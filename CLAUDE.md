# offset-consumer Development Guidelines

Auto-generated from all feature plans. Last updated: 2026-11-06

## Active Technologies
- Go 1.26+ (primary), Java 11+ (primary), TypeScript/JavaScript ES2022+ (primary), Rust 1.89+ (primary) + `github.com/nats-io/nats.go` v1.55.0 (Go), `io.nats:jnats` v2.26.4 (Java), `@nats-io/jetstream` v3.4.0 & `@nats-io/kv` v3.4.0 (JS/TS), `async-nats` v0.51.0 (Rust) (001-offset-consumer)
- NATS KV bucket `offset-consumers` — JSON values with `seq` (uint64), `committed_at` (RFC3339), `owner` (owner prefix `oc-{stream}-{consumer}-{nuid}`, empty when unowned), `host`, `claimed_at` (RFC3339) fields (001-offset-consumer)
- Single active consumer enforced at startup by an ownership claim (one CAS write); commits are plain pipelined puts; ordered consumers are named `{owner}-g{generation}...` (001-offset-consumer)
- .NET port dropped 2026-11-06 (NATS.Net has no ordered-consumer name prefix); see README.md

## Project Structure

```text
offset-consumer-go/      # Go module (go test ./...)
offset-consumer-java/    # Gradle project (./gradlew test; needs nats-server on PATH)
offset-consumer-js/      # npm package (npm test; needs nats-server on PATH)
offset-consumer-rust/    # Cargo crate (cargo test; needs nats-server on PATH)
specs/001-offset-consumer/   # spec, plan, data model, research, quickstart, contracts, tasks
offset-consumer-spec.md      # design specification (1.1)
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
- 2026-11-06: ownership claim replaces per-commit CAS; pipelined async commits; Info/ListConsumers carry ownership fields; client libraries upgraded (nats.go 1.55.0, jnats 2.26.4, @nats-io 3.4.0, async-nats 0.51.0); Rust port added; .NET port removed
- 001-offset-consumer: Added Go, Java, JS/TS implementations of the offset consumer

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
