# Research: NATS JetStream Offset Consumer Library

**Feature Branch**: `001-offset-consumer` | **Date**: 2026-02-26, revised 2026-11-06 (ownership claim; client upgrades; .NET dropped)

---

## R-001: Go NATS Client — Ordered Consumer & KV APIs

**Decision**: Use `github.com/nats-io/nats.go` v1.55.0 with the `jetstream` sub-package (requires Go 1.26).

### Key API Surfaces (Go)

- **Ordered consumer**: `js.OrderedConsumer(ctx, stream, OrderedConsumerConfig{})`. The first server-side consumer is created by this call (not by `Consume`). Names are `{NamePrefix}_{serial}`; the serial increments on every client-side reset, which deletes the previous consumer (best effort) and creates the next one with `CreateOrUpdateConsumer`. `InactiveThreshold` is configurable (client default 5m). Stopping a `ConsumeContext` does **not** delete the server-side consumer.
- **Current consumer name**: `cons.CachedInfo().Name` (refreshed on reset).
- **Liveness check**: `stream.ConsumerNames(ctx)` lister (`Name()` channel, `Err()` after the loop).
- **KV**: `Get` returns `ErrKeyNotFound` for missing and deleted keys; `Create` fails with `ErrKeyExists` (10071); `Update(key, value, revision)` fails with `ErrKeyRevisionMismatch`; `Put` is a plain write returning the revision.
- **Async put**: `js.PublishAsync(subject, data)` returns a `PubAckFuture`. The KV put subject is `[apiPrefix if non-default]$KV.{bucket}.{key}`, where the API prefix comes from `js.Options()` (`Domain` → `$JS.{domain}.API.`, or `APIPrefix`) and a mirrored bucket uses its external API prefix (`KeyValueBucketStatus.StreamInfo().Config.Mirror`). The library replicates nats.go's `mapStreamToKVS` logic.
- **Error handler**: `jetstream.ConsumeErrHandler`; `ErrOrderedConsumerReset` is informational and filtered.

---

## R-002: Java NATS Client — Ordered Consumer & KV APIs

**Decision**: Use `io.nats:jnats` v2.26.4 with the simplified API; tests use `io.nats:jnats-server-runner` 4.0.2 (`io.nats.NatsServerRunner`, `getNatsLocalhostUri()`).

### Key API Surfaces (Java)

- **Ordered consumer**: `streamContext.createOrderedConsumer(OrderedConsumerConfiguration)` → `OrderedConsumerContext`. `consumerNamePrefix(prefix)` yields names `{prefix}-{NUID}`, with a new NUID on every reset; the old consumer is **not** deleted on reset (it expires). `getConsumerName()` returns the current name (null before the first consume). No inactive-threshold option exists for ordered consumers, so the server default for ephemeral consumers (5s) applies.
- **Liveness check / delete**: `streamContext.getConsumerNames()`, `streamContext.deleteConsumer(name)`; `MessageConsumer.close()` unsubscribes but does not delete the consumer.
- **KV**: `get` returns null for missing/deleted keys; `create`, `update(key, value, revision)`, `put` return the revision; a CAS mismatch is `JetStreamApiException` with code 10071 (or 10164 on replicated buckets). `nc.keyValue(bucket)` does not verify the bucket exists; `kv.keys()` throws 10059 when it is missing.
- **JSON**: `io.nats.client.support.JsonParser` / `JsonValueUtils` for parsing, `Encoding.jsonEncode` for writing.
- **Async commits**: a dedicated single-thread executor keeps writes ordered and off the caller's thread; `commitSync` is `commitAsync().get()`.

---

## R-003: JavaScript/TypeScript NATS Client — Ordered Consumer & KV APIs

**Decision**: Use `@nats-io/jetstream` v3.4.0 and `@nats-io/kv` v3.4.0 (peer `@nats-io/nats-core` v3.4.0; `nuid` and `nanos` come from nats-core).

### Key API Surfaces (JS/TS)

- **Ordered consumer**: `js.consumers.get(stream, { name_prefix, inactive_threshold, deliver_policy, ... })`; names are `{name_prefix}_{serial}`. The client deletes the old consumer on reset but recreates with a hard-coded 5-minute `inactive_threshold` (still true in 3.4.0, `lib/consumer.js getConsumerOpts`); the library re-applies the configured threshold with `jsm.consumers.update(stream, name, { inactive_threshold })` on the `ordered_consumer_recreated` status event. `consumer.delete()` deletes the current consumer; `messages.stop()` does not.
- **Liveness check**: `jsm.consumers.list(stream)` (no names-only lister).
- **KV**: `kv.get` may return entries with `operation` `DEL`/`PURGE`; the library treats anything but `PUT` as absent. `create`/`update` conflicts are `JetStreamApiError` with code 10071 or 10164 (`JetStreamApiCodes`).
- **Consumption**: `consume()` returns an async iterable; the library wraps it so that it continues across seek restarts (`nextSource`) and discards messages still buffered by a stopped source.

---

## R-003b: Rust NATS Client — Ordered Consumer & KV APIs

**Decision**: Use `async-nats` v0.51.0 (MSRV 1.89), plus `nuid` 0.5 for owner prefixes and `hostname` 0.4.

### Key API Surfaces (Rust)

- **Ordered consumer**: `stream.create_consumer(pull::OrderedConfig { name: Some(..), deliver_policy, filter_subjects, ..Default::default() })` then `consumer.messages()`. With a name, resets rename to `{name}_{n}` and delete the previous consumer (0.51). `inactive_threshold` is hard-coded to 30s; `max_reset_attempts` does not exist (resets retry forever). Dropping the stream does not delete the consumer. The current name is available from `consumer.cached_info().name` before `messages()` and from `msg.info()?.consumer` afterwards.
- **Known quirk**: a reset before any message was delivered recreates with `DeliverPolicy::All` instead of the requested start sequence.
- **Liveness check / delete**: `stream.consumer_names()` (a `futures::Stream` of names), `stream.delete_consumer(name)`.
- **KV**: `entry(key)` returns `Option<Entry>` with `operation` (`Put`/`Delete`/`Purge`); `create` → `CreateErrorKind::AlreadyExists`; `update(key, value, revision)` → `UpdateErrorKind::WrongLastRevision`; no filtered `keys()` (client-side prefix filter).
- **Breaking changes 0.38 → 0.51 handled**: `OrderedConfig` gained fields (use `..Default::default()`), new error kinds, consumer renaming on reset.

---

## R-004: Go Project Layout

Single package at the module root (`github.com/synadia-io/offset-consumer-go`): `consumer.go`, `consumer_impl.go`, `config.go`, `errors.go`, `seek.go`, `kv.go`, tests alongside.

---

## R-005: Java Project Layout

Gradle `java-library`, package `io.synadia.offsetconsumer`: `OffsetConsumer`, `OffsetConsumerFactory`, `OffsetConsumerImpl`, `OffsetEntry`, `OffsetConsumerInfo`, `ConsumerConfig`, `ErrorHandler`, `StartPosition`, `CommitException`, `AlreadyInUseException`.

---

## R-005b: JavaScript/TypeScript Project Layout

`tsup` dual CJS/ESM build; `src/index.ts`, `consumer.ts`, `kv.ts`, `errors.ts`, `types.ts`; tests under `test/` with Vitest.

---

## R-005c: Rust Project Layout

Single crate `offset-consumer`: `src/lib.rs`, `config.rs`, `consumer.rs`, `kv.rs`, `errors.rs`; integration tests under `tests/`.

---

## R-006: Testing Strategy

Embedded NATS server for Go (`nats-server/v2/test`); `jnats-server-runner` for Java; a spawned `nats-server -js` process for JS and Rust. Every port has the same scenario set: basic consume, commit sync/async (plus chained and nothing-to-commit), auto-commit, seek (before consuming and while consuming, verifying the generation-2 consumer name), delete, invalid name, start position end, already consuming, info/list ownership fields, second instance refused, release on close, crash recovery (connection dropped without close; recovery after the inactive threshold: ~3s Go/JS with a 2s threshold, ~6s Java, ~31s Rust).

---

## R-007: Ownership Claim Design

**Decision**: Detect a second live instance at construction instead of through per-commit CAS conflicts.

**Rationale**: Ordered consumers are ephemeral and the server already tracks their liveness. Naming them with a per-instance prefix recorded in the KV entry lets a starter check `consumer_names(stream)` for the owner's prefix. Per-commit CAS forced commits to be serialized (each needs the previous revision), produced self-inflicted conflicts when the auto-commit timer raced a manual commit, and only surfaced a collision after both instances had consumed.

**Mechanics**: owner prefix `oc-{stream}-{consumer}-{nuid}`; generation suffix `-g{N}` per (re)start so restarts never collide with a lingering consumer (Go's `CreateOrUpdateConsumer` would otherwise attach to an identical leftover); claim grace window equal to the inactive threshold; one CAS write for the claim (create/update); release and consumer deletion on close.

**Rejected alternatives**: a KV lock key with a TTL refreshed by a heartbeat (more moving parts, a second writer); storing the exact consumer name (stale after every client-side reset).

**Not implemented**: displaced-owner detection (would need a KV watch on the entry) and operator takeover.

---

## R-008: KV Bucket Creation — Delete-Marker TTL

All ports create the bucket with a 10-minute delete-marker TTL so tombstones left by `DeleteConsumer()` are purged: nats.go `KeyValueConfig.LimitMarkerTTL`, jnats `KeyValueConfiguration.Builder.limitMarker(Duration)`, @nats-io/kv `KvOptions.markerTTL` (milliseconds), async-nats `kv::Config.limit_markers` (`server_2_11` feature, on by default). Each sets `allow_msg_ttl` and `subject_delete_marker_ttl` on the backing stream, which needs nats-server 2.11+. An existing bucket is used as-is, so a bucket created by an older version keeps its configuration.
