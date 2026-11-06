# NATS JetStream Offset Consumer

Kafka-style, offset-based consumption on top of NATS JetStream, implemented
entirely on the client side. Each consumer reads a stream through an
**ephemeral ordered consumer** and persists the last committed stream sequence
in a shared **NATS KV bucket** (`offset-consumers`). On restart it resumes at
`committed + 1`. No server-side durable consumer state is needed.

Ports: **Go**, **Java**, **JavaScript/TypeScript**, **Rust**. The .NET port was
dropped; see [Why there is no .NET port](#why-there-is-no-net-port).

Design document: [`offset-consumer-spec.md`](offset-consumer-spec.md).
Per-language API contracts: [`specs/001-offset-consumer/contracts/`](specs/001-offset-consumer/contracts/).

## How it works

- **Transport**: a JetStream ephemeral ordered consumer created at
  `stored_seq + 1` (or at the beginning/end of the stream for a new consumer).
- **Offset store**: one KV bucket shared by all streams and consumers, keyed
  `{stream}.{consumer}`, created on first use with the stream's replica count,
  history 1 and a 10-minute delete-marker TTL. The value is JSON:

  ```json
  {"seq": 104872, "committed_at": "2026-02-25T10:45:00Z",
   "owner": "oc-orders-billing-<nuid>", "host": "node-7", "claimed_at": "2026-02-25T10:40:00Z"}
  ```

- **Commits** (`CommitSync` / `CommitAsync`) write the sequence of the last
  message delivered to the handler. Async commits never block on the server;
  several may be pending and they are applied in call order. Writes are plain
  KV puts (last writer wins); there is no per-commit compare-and-set.
- **Auto-commit** runs a timer that commits only when new messages were
  delivered; failures go to the configured error handler.
- **Seek** stops delivery, deletes the ordered consumer, writes the new
  position and restarts delivery under a new ordered consumer. In JS the
  iterable returned by `consume()` keeps yielding across the restart.
- **Close** waits up to 5s for pending async commits, deletes the ordered
  consumer, and writes the entry once more with ownership cleared. That write
  persists the highest sequence passed to a commit call, so commits still
  queued after the wait are abandoned without losing the position.
- **Delivery is at-least-once**: handlers must be idempotent.

## Single active consumer: ownership claim

Only one instance of a given `{stream}.{consumer}` may run at a time. The
library enforces this at startup, using the server's own liveness tracking
instead of per-commit CAS:

1. Every instance generates an **owner prefix** `oc-{stream}-{consumer}-{nuid}`
   and creates its ordered consumers with names starting with that prefix
   (`{owner}-g{generation}` plus a client-specific suffix; the generation
   increments on every restart, for example after a seek).
2. On startup the instance reads the KV entry. If it carries an owner, the
   instance refuses to start (`AlreadyInUse` error, naming the owner, its host
   and claim time) when either the claim is younger than the inactive
   threshold, or the stream still has a consumer whose name starts with that
   owner prefix.
3. Otherwise it **claims** the entry with a single compare-and-set write
   (create when absent, update-by-revision otherwise), keeping the stored
   offset and setting `owner`, `host` and `claimed_at`. Losing that race is
   reported as `AlreadyInUse` too.
4. Every commit carries the ownership fields unchanged. `Close` deletes the
   ordered consumer and writes the entry back with an empty owner (and the
   final commit, when auto-commit is on), so a restart is allowed immediately.
5. If the instance crashes, the server deletes its ephemeral ordered consumer
   after the **inactive threshold**, and the next instance is allowed in.

What is deliberately **not** covered:

- A running instance does not detect that it was displaced (for example after
  being partitioned long enough for its consumer to expire). Its later commits
  overwrite the newcomer's. Detecting this needs a KV watch; it is not implemented.
- There is no forced takeover. An operator who must evict a live owner has to
  stop it; deleting its ordered consumer is not enough, the client recreates it.
- During an ordered-consumer reset there is a window of a few milliseconds in
  which the old consumer is gone and the new one not yet created; a starter
  landing exactly then is not refused.

### Inactive threshold per language

| Port | Threshold | Source |
|---|---|---|
| Go | `ConsumerConfig.InactiveThreshold`, default 30s | `OrderedConsumerConfig.InactiveThreshold` |
| JS/TS | `inactiveThresholdMs`, default 30000 | `OrderedConsumerOptions.inactive_threshold`; the client resets consumers with a 5-minute threshold, so the library re-applies the configured value after every `ordered_consumer_recreated` event |
| Java | 5s, not configurable | jnats exposes no inactive threshold for ordered consumers; the server default for ephemeral consumers applies |
| Rust | 30s, not configurable | async-nats hard-codes it in `OrderedConfig` |

The same value is the grace window during which a fresh claim counts as alive
even if its ordered consumer cannot be found yet. Call `Consume` promptly after
constructing a consumer.

## Ports

| | Client | Async commit returns | Error on collision |
|---|---|---|---|
| Go (`offset-consumer-go/`) | nats.go 1.55.0, Go 1.26+ | `jetstream.PubAckFuture` (pipelined publishes to the KV subject) | `ErrAlreadyInUse` / `*AlreadyInUseError` |
| Java (`offset-consumer-java/`) | jnats 2.26.4, Java 11+ | `CompletableFuture<Void>` (single commit thread) | `AlreadyInUseException` |
| JS/TS (`offset-consumer-js/`) | `@nats-io/jetstream` 3.4.0, `@nats-io/kv` 3.4.0 | `Promise<void>` (chained writes) | `AlreadyInUseError` |
| Rust (`offset-consumer-rust/`) | async-nats 0.51.0, Rust 1.89+ | `JoinHandle<Result<()>>` (chained writes) | `OffsetConsumerError::AlreadyInUse` |

Every port also offers `Info()` (fresh read of the entry, including the
ownership fields) and a standalone `ListConsumers(stream)`.

### Per-language caveats

- **Java**: the ordered consumer expires 5s after the connection drops, so
  crash recovery is fast, but a long GC pause or network blip can make the
  client recreate the consumer under a new name (old one never deleted, it
  expires on its own). Harmless for ownership checks, which match on prefix.
- **JS/TS**: see the threshold note above. `ConsumerMessages.stop()` also
  deletes the ordered consumer.
- **Rust**: if the ordered consumer resets before the first message was
  delivered, async-nats 0.51 restarts it with `DeliverPolicy::All` instead of
  the requested start sequence. Under at-least-once semantics this only causes
  redelivery, but it can be a lot of it. The crash-recovery test takes ~35s
  because of the fixed 30s threshold.

## Running the tests

All suites need a `nats-server` binary (Java, JS and Rust spawn it from `PATH`;
Go embeds the server package).

```sh
cd offset-consumer-go   && go test ./... -count=1
cd offset-consumer-java && ./gradlew test
cd offset-consumer-js   && npm install && npm test
cd offset-consumer-rust && cargo test
```

## Why there is no .NET port

The ownership check needs a stable prefix shared by all ordered consumers an
instance creates, including the ones the client recreates on resets. The .NET
client cannot provide one:

- `NatsJSOrderedConsumerOpts` has no name or name-prefix option (checked in
  NATS.Net 2.7.2 through 3.3.2 and on `main`).
- Consumer names come from `Nuid.NewNuid()` inside an internal method and
  change on every recreate; the current one is only visible through
  `INatsJSConsumer.Info.Name` after the fact.

Storing the current name instead of a prefix would leave a stale-name window
on every reset, during which a second instance would be admitted. The other
building blocks exist (`InactiveThreshold`, `ListConsumerNamesAsync`,
`DeleteConsumerAsync`), so the port can be revived once NATS.Net gains an
ordered-consumer name prefix.

## Benchmark results

Measured 2026-11-06 on one machine (Apple M5 Ultra, macOS 27) over loopback
against nats-server v2.16.0-dev with a file store: an R1 stream preloaded with
10M messages of 128 B by `nats bench`, each port consuming the first 2M with
a no-op handler, best of two runs. Scenarios: `none` (no commits), `auto-1s`
(auto-commit every second), `async-N` / `sync-N` (`CommitAsync` / `CommitSync`
every N messages). Full method, raw data and reproduction steps are in
[`BENCHMARK.md`](BENCHMARK.md); harnesses live under `bench/` and each port.

#### Delivery throughput (messages/s, best of runs)

| Scenario | Go | Java | JS/TS | Rust |
|---|---:|---:|---:|---:|
| none | 1,262,364 | 1,066,305 | 553,964 | 1,439,520 |
| auto-1s | 1,248,727 | 1,005,415 | 542,140 | 1,436,708 |
| async-1000 | 1,215,863 | 1,041,500 | 534,730 | 1,386,698 |
| async-100 | 1,074,983 | 936,553 | 499,525 | 1,250,269 |
| async-10 | 652,953 | 965,446 | 447,479 | 993,171 |
| sync-1000 | 1,211,777 | 1,020,983 | 520,908 | 1,367,922 |
| sync-100 | 838,361 | 741,501 | 426,821 | 835,405 |

#### Throughput relative to the no-commit baseline of the same port

| Scenario | Go | Java | JS/TS | Rust |
|---|---:|---:|---:|---:|
| none | 100% | 100% | 100% | 100% |
| auto-1s | 99% | 94% | 98% | 100% |
| async-1000 | 96% | 98% | 97% | 96% |
| async-100 | 85% | 88% | 90% | 87% |
| async-10 | 52% | 91% | 81% | 69% |
| sync-1000 | 96% | 96% | 94% | 95% |
| sync-100 | 66% | 70% | 77% | 58% |

#### Commit cost (microseconds of added wall time per commit, versus the no-commit baseline)

| Scenario | Go | Java | JS/TS | Rust |
|---|---:|---:|---:|---:|
| async-1000 | 30 | 22 | 65 | 26 |
| async-100 | 14 | 13 | 20 | 11 |
| async-10 | 7 | 1 | 4 | 3 |
| sync-1000 | 33 | 42 | 114 | 36 |
| sync-100 | 40 | 41 | 54 | 50 |

Takeaways:

- Delivery with no commits is client-bound: Rust 1.44M, Go 1.26M, Java 1.07M,
  JS 0.55M messages/s (184 / 162 / 137 / 71 MiB/s of payload).
- Committing every 1000 messages, sync or async, costs at most 6%; auto-commit
  is free. One KV round trip on loopback is 30 to 50 µs, about 40 messages'
  worth of delivery.
- Sync every 100 messages costs 23 to 42% because each commit stalls delivery
  for a round trip; async every 100 costs 10 to 15%.
- Async every 10 messages shows the design difference: Go pipelines the KV
  writes and has all 200k commits acknowledged by the time delivery ends; Java,
  JS and Rust queue them and drain at 15k to 25k commits/s, so those runs
  finished 8 to 14 s after the last message. Keep sustained async commit rates
  below roughly 10k/s outside Go, or commit on a timer.
