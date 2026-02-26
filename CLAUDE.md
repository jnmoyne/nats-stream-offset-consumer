# offset-consumer Development Guidelines

Auto-generated from all feature plans. Last updated: 2026-02-26

## Active Technologies
- Go 1.21+ (primary), Java 11+ (primary), .NET 8+ (primary), TypeScript/JavaScript ES2022+ (primary) + `github.com/nats-io/nats.go` v1.49+ (Go), `io.nats:jnats` v2.25.1+ (Java), `NATS.Net` v2.7.2+ (.NET), `@nats-io/jetstream` v3.3+ & `@nats-io/kv` v3.3+ (JS/TS) (001-offset-consumer)
- NATS KV bucket `offset-consumers` — JSON values with `seq` and `committed_at` fields (001-offset-consumer)
- Go 1.21+, Java 11+, .NET 8+, TypeScript/JavaScript ES2022+ (001-offset-consumer)
- NATS KV bucket `offset-consumers` — JSON values with `seq` (uint64) and `committed_at` (RFC3339) fields (001-offset-consumer)

- Go 1.21+ (primary), Java 11+ (primary), .NET (stretch goal) + `github.com/nats-io/nats.go` v1.49+ (Go), `io.nats:jnats` v2.25.1+ (Java) (001-offset-consumer)

## Project Structure

```text
src/
tests/
```

## Commands

# Add commands for Go 1.21+ (primary), Java 11+ (primary), .NET (stretch goal)

## Code Style

Go 1.21+ (primary), Java 11+ (primary), .NET (stretch goal): Follow standard conventions

## Recent Changes
- 001-offset-consumer: Added Go 1.21+, Java 11+, .NET 8+, TypeScript/JavaScript ES2022+
- 001-offset-consumer: Added Go 1.21+ (primary), Java 11+ (primary), .NET 8+ (primary), TypeScript/JavaScript ES2022+ (primary) + `github.com/nats-io/nats.go` v1.49+ (Go), `io.nats:jnats` v2.25.1+ (Java), `NATS.Net` v2.7.2+ (.NET), `@nats-io/jetstream` v3.3+ & `@nats-io/kv` v3.3+ (JS/TS)

- 001-offset-consumer: Added Go 1.21+ (primary), Java 11+ (primary), .NET (stretch goal) + `github.com/nats-io/nats.go` v1.49+ (Go), `io.nats:jnats` v2.25.1+ (Java)

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
