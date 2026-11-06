#!/usr/bin/env bash
# Runs the offset-consumer throughput benchmark for every port and scenario.
# Prerequisites: a nats-server with JetStream on $SERVER and a preloaded stream
# (see BENCHMARK.md). Results are appended as JSON lines to $OUT.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SERVER="${SERVER:-nats://127.0.0.1:4222}"
STREAM="${STREAM:-BENCH}"
MSGS="${MSGS:-2000000}"
RUNS="${RUNS:-2}"
LANGS="${LANGS:-go java js rust}"
SCENARIOS="${SCENARIOS:-none auto-1s async-1000 async-100 async-10 sync-1000 sync-100}"
OUT="${OUT:-$ROOT/bench/results.jsonl}"
export PATH="$HOME/.cargo/bin:$PATH"

echo "building harnesses..."
(cd "$ROOT/offset-consumer-go" && go build -o "$ROOT/bench/.bench-go" ./bench)
(cd "$ROOT/offset-consumer-java" && ./gradlew -q compileTestJava)
(cd "$ROOT/offset-consumer-js" && npm run build --silent)
(cd "$ROOT/offset-consumer-rust" && cargo build --release --example bench --quiet)

run_one() {
  local lang=$1 scenario=$2
  case "$lang" in
    go)   "$ROOT/bench/.bench-go" -server "$SERVER" -stream "$STREAM" -msgs "$MSGS" -scenario "$scenario" ;;
    java) (cd "$ROOT/offset-consumer-java" && ./gradlew -q bench -PbenchArgs="--server $SERVER --stream $STREAM --msgs $MSGS --scenario $scenario") ;;
    js)   (cd "$ROOT/offset-consumer-js" && node bench/bench.mjs --server "$SERVER" --stream "$STREAM" --msgs "$MSGS" --scenario "$scenario") ;;
    rust) "$ROOT/offset-consumer-rust/target/release/examples/bench" --server "$SERVER" --stream "$STREAM" --msgs "$MSGS" --scenario "$scenario" ;;
  esac
}

for lang in $LANGS; do
  for scenario in $SCENARIOS; do
    for ((r = 1; r <= RUNS; r++)); do
      line=$(run_one "$lang" "$scenario" | grep '^{')
      echo "$line" | tee -a "$OUT"
    done
  done
done
echo "done: $OUT"
