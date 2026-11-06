// Throughput benchmark under different commit scenarios. Consumes --msgs
// messages from --stream with a fresh consumer name and prints one JSON line.
// Build the package first (npm run build), then:
//   node bench/bench.mjs --scenario sync-100 --msgs 2000000
import { connect } from "@nats-io/transport-node";
import { createConsumer } from "../dist/index.js";

const opts = { server: "nats://127.0.0.1:4222", stream: "BENCH", msgs: 2_000_000, scenario: "none" };
const argv = process.argv.slice(2);
for (let i = 0; i + 1 < argv.length; i += 2) {
  const key = argv[i].replace(/^--/, "");
  if (!(key in opts)) throw new Error(`unknown flag ${argv[i]}`);
  opts[key] = key === "msgs" ? Number(argv[i + 1]) : argv[i + 1];
}

let mode = "none";
let every = 0;
if (opts.scenario.startsWith("auto")) mode = "auto";
else if (opts.scenario.startsWith("async-")) { mode = "async"; every = Number(opts.scenario.slice(6)); }
else if (opts.scenario.startsWith("sync-")) { mode = "sync"; every = Number(opts.scenario.slice(5)); }
else if (opts.scenario !== "none") throw new Error(`unknown scenario ${opts.scenario}`);

const nc = await connect({ servers: opts.server });
const config = { stream: opts.stream, consumerName: `bench-js-${opts.scenario}-${Date.now()}` };
if (mode === "auto") { config.autoCommit = true; config.autoCommitIntervalMs = 1000; }
const consumer = await createConsumer(nc, config);

let count = 0;
let bytes = 0;
let commits = 0;
let last = null;
let recvMs = 0;

const start = process.hrtime.bigint();
const messages = await consumer.consume();
for await (const msg of messages) {
  count++;
  bytes += msg.data.length;
  if (mode === "sync" && count % every === 0) {
    await consumer.commitSync();
    commits++;
  } else if (mode === "async" && count % every === 0) {
    last = consumer.commitAsync();
    commits++;
  }
  if (count >= opts.msgs) {
    recvMs = Number(process.hrtime.bigint() - start) / 1e6;
    break;
  }
}
if (last) await last;
if (mode !== "auto") {
  await consumer.commitSync();
  commits++;
}
await consumer.close();
const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;

const info = await consumer.info();
console.log(JSON.stringify({ lang: "js", scenario: opts.scenario, msgs: opts.msgs, bytes, recv_ms: Number(recvMs.toFixed(3)), elapsed_ms: Number(elapsedMs.toFixed(3)), commits, final_seq: info.seq }));
await consumer.deleteConsumer();
await nc.close();
if (info.seq < opts.msgs) console.error(`warning: final seq ${info.seq} < msgs ${opts.msgs}`);
