# Offset consumer benchmark

Measured 2026-11-06 on a single machine over loopback. Numbers are indicative of
relative cost, not of what a production cluster will do.

## Setup

| | |
|---|---|
| Machine | Apple M5 Ultra, 30 cores, 96 GB, macOS 27.0.1 |
| Server | nats-server v2.16.0-dev, `nats-server -js` (file store), same host as the clients |
| Stream | `BENCH`, R1, file storage, 10,000,000 messages of 128 B (1.6 GiB) |
| Preload | `nats bench js pub async bench.data --stream BENCH --msgs 10000000 --size 128B --batch 1000 --clients 4` (301k msgs/s) |
| Go | nats.go 1.55.0, Go 1.27.3 |
| Java | jnats 2.26.4, OpenJDK 27 (run through Gradle `JavaExec`) |
| JS/TS | @nats-io/jetstream and kv 3.4.0, Node 26.13 |
| Rust | async-nats 0.51.0, Rust 1.99.1, release build |

## Method

Each run creates an offset consumer under a fresh name, consumes the first
2,000,000 messages of `BENCH` with a handler that only counts, commits
according to the scenario, waits for the last async commit, performs one
final `CommitSync`, and calls `Close`. Each cell was run twice and the run
with the shorter delivery time is reported (spread between runs was under 5%
except Rust `sync-100`, 24%).

Scenarios:

| Scenario | What the handler does |
|---|---|
| `none` | no commits until the final one |
| `auto-1s` | `AutoCommit` with a 1 s interval |
| `async-N` | `CommitAsync` every N messages (N = 1000, 100, 10) |
| `sync-N` | `CommitSync` every N messages (N = 1000, 100) |

Metrics:

- **Delivery throughput** = 2,000,000 / time until the 2,000,000th message was delivered.
- **Wall time** = time until `Close` returned, so it includes draining pending async commits.
- **Commit cost** = (delivery time − delivery time of `none`) / number of commits, i.e. the added wall time per commit while messages are flowing.

Harnesses: `offset-consumer-go/bench`, `offset-consumer-java/src/test/java/.../Bench.java` (`./gradlew bench`), `offset-consumer-js/bench/bench.mjs`, `offset-consumer-rust/examples/bench.rs`. Driver: `bench/run-benchmarks.sh`; tables: `bench/report.py bench/results.jsonl`; raw data: `bench/results.jsonl`.

## Results

### Delivery throughput (messages/s, best of runs)

| Scenario | Go | Java | JS/TS | Rust |
|---|---:|---:|---:|---:|
| none | 1,262,364 | 1,066,305 | 553,964 | 1,439,520 |
| auto-1s | 1,248,727 | 1,005,415 | 542,140 | 1,436,708 |
| async-1000 | 1,215,863 | 1,041,500 | 534,730 | 1,386,698 |
| async-100 | 1,074,983 | 936,553 | 499,525 | 1,250,269 |
| async-10 | 652,953 | 965,446 | 447,479 | 993,171 |
| sync-1000 | 1,211,777 | 1,020,983 | 520,908 | 1,367,922 |
| sync-100 | 838,361 | 741,501 | 426,821 | 835,405 |

### Throughput relative to the no-commit baseline of the same port

| Scenario | Go | Java | JS/TS | Rust |
|---|---:|---:|---:|---:|
| none | 100% | 100% | 100% | 100% |
| auto-1s | 99% | 94% | 98% | 100% |
| async-1000 | 96% | 98% | 97% | 96% |
| async-100 | 85% | 88% | 90% | 87% |
| async-10 | 52% | 91% | 81% | 69% |
| sync-1000 | 96% | 96% | 94% | 95% |
| sync-100 | 66% | 70% | 77% | 58% |

### Wall time for 2M messages including final commit and close (seconds), commits performed

| Scenario | Go | Java | JS/TS | Rust |
|---|---:|---:|---:|---:|
| none | 1.59s / 1 | 1.88s / 1 | 3.61s / 1 | 1.39s / 1 |
| auto-1s | 1.60s / timer | 1.99s / timer | 3.69s / timer | 1.39s / timer |
| async-1000 | 1.65s / 2,001 | 1.92s / 2,001 | 3.74s / 2,001 | 1.44s / 2,001 |
| async-100 | 1.86s / 20,001 | 3.09s / 20,001 | 4.00s / 20,001 | 1.74s / 20,001 |
| async-10 | 3.06s / 200,001 | 15.92s / 200,001 | 12.37s / 200,001 | 11.55s / 200,001 |
| sync-1000 | 1.65s / 2,001 | 1.96s / 2,001 | 3.84s / 2,001 | 1.46s / 2,001 |
| sync-100 | 2.39s / 20,001 | 2.70s / 20,001 | 4.69s / 20,001 | 2.39s / 20,001 |

### Commit cost (microseconds of added wall time per commit, versus the no-commit baseline)

| Scenario | Go | Java | JS/TS | Rust |
|---|---:|---:|---:|---:|
| async-1000 | 30 | 22 | 65 | 26 |
| async-100 | 14 | 13 | 20 | 11 |
| async-10 | 7 | 1 | 4 | 3 |
| sync-1000 | 33 | 42 | 114 | 36 |
| sync-100 | 40 | 41 | 54 | 50 |

## Observations

1. **Baseline delivery** (no commits) is client-bound: Rust 1.44M, Go 1.26M,
   Java 1.07M and JS 0.55M messages/s, which is 184, 162, 137 and 71 MiB/s of
   payload. The server was not the limit; the preload wrote 300k msgs/s with
   fsync and delivery reads from a hot page cache.
2. **Committing every 1000 messages costs at most 6%**, whether sync or async,
   in every port. One KV round trip on loopback is 30 to 50 µs, about the time
   it takes to deliver 40 messages, so amortised over 1000 it disappears.
3. **Every 100 messages** costs 10 to 15% for async and 23 to 42% for sync. A
   sync commit stalls delivery for the whole round trip; an async one only
   costs the publish.
4. **Every 10 messages (async)** separates the designs. Go pipelines the KV
   writes on the connection (`PublishAsync`), so delivery drops to 52% but the
   200,001 commits are all acknowledged by the time the last message arrives.
   Java, JS and Rust keep delivery at 81 to 91% because commits only queue, but
   the queue drains one KV round trip at a time, roughly 15k (Java), 21k (Rust)
   and 25k (JS) commits/s, so the run only finishes 8 to 14 s later. In Java and
   Rust delivery kept running during that drain and reached the end of the 10M
   stream, which also slowed the drain.
5. **Auto-commit is free** (within 6% of the baseline): one write per second
   regardless of message rate.
6. **Guidance**: commit on a timer or every few hundred to a thousand messages.
   Sustained async commit rates above about 10k/s are only sensible with the Go
   port; in the other ports they build a backlog that `Close` has to drain.

## Finding (fixed): Close with a large async backlog

The first run of this benchmark showed that `Close` waited for pending async
commits for at most 5 s (Go, Java, Rust; JS waited for the whole chain) and
then, in Java and Rust, wrote the release entry while queued commits were
still being written behind it, so a late commit could put the owner back and
the next instance would be refused until the inactive threshold expired. All
four ports now record the highest sequence passed to a commit call, abandon
whatever is still queued once the bounded wait expires (those futures still
resolve), and persist that sequence in the release write, which is ordered
after any write already in flight. Covered by a "close with commit backlog"
test in every suite. The numbers above were measured before the fix; it adds
one comparison per commit and does not change the write path.

## Caveats

- Everything ran on one host over loopback; network latency would raise the
  per-commit cost of the sync scenarios proportionally.
- Java and JS warm up their JIT inside the measured run; Go and Rust do not.
- The handler does no work, so these are upper bounds on delivery rate.
- Payloads are 128 B; larger messages shift the balance towards bytes/s.
- Two runs per cell on a desktop machine; treat differences under about 5% as noise.

## Reproducing

```sh
nats stream add BENCH --subjects 'bench.>' --storage file --replicas 1 --defaults
nats bench js pub async bench.data --stream BENCH --msgs 10000000 --size 128B --batch 1000 --clients 4
MSGS=2000000 RUNS=2 ./bench/run-benchmarks.sh      # writes bench/results.jsonl
python3 bench/report.py bench/results.jsonl         # markdown tables
```
