import { describe, it, expect, beforeAll, afterAll, afterEach, beforeEach, onTestFinished } from "vitest";
import { connect } from "@nats-io/transport-node";
import { nanos, type NatsConnection } from "@nats-io/nats-core";
import { AckPolicy, jetstream, jetstreamManager, type JsMsg } from "@nats-io/jetstream";
import { Kvm } from "@nats-io/kv";
import { hostname } from "node:os";
import { createConsumer, internals, listConsumers } from "../src/consumer.js";
import {
  AlreadyInUseError,
  InvalidConsumerNameError,
  InvalidStreamNameError,
  StreamRecreatedError,
} from "../src/errors.js";
import { BUCKET_NAME, BUCKET_STREAM_NAME, streamCreatedId } from "../src/kv.js";
import type { OffsetConsumer, ConsumerMessages } from "../src/types.js";
import { startNatsServer, type NatsServer } from "./nats-server-helper.js";

// Keep the takeover wait short in tests; tests of the wait set it explicitly.
internals.livenessWaitMs = 300;

const TEST_STREAM = "test-stream";
const NUM_MESSAGES = 100;

function uniqueName(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.floor(Math.random() * 100000)}`;
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

/** Polls cond until it returns true or timeoutMs elapses. */
async function waitUntil(timeoutMs: number, cond: () => boolean | Promise<boolean>): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await cond()) {
      return true;
    }
    await sleep(50);
  }
  return cond();
}

async function storedSeq(c: OffsetConsumer): Promise<number> {
  return (await c.info())?.seq ?? 0;
}

/** Reads up to n messages (or until timeoutMs) and returns their sequences. */
async function take(messages: ConsumerMessages, n: number, timeoutMs = 5000): Promise<number[]> {
  const received: number[] = [];
  const timeout = setTimeout(() => messages.stop(), timeoutMs);
  try {
    for await (const msg of messages) {
      received.push(msg.seq);
      if (received.length >= n) {
        break;
      }
    }
  } finally {
    clearTimeout(timeout);
  }
  return received;
}

/** The instance's owner prefix (internal). */
function ownerOf(c: OffsetConsumer): string {
  return (c as unknown as { owner: string }).owner;
}

async function consumerNamesWithPrefix(nc: NatsConnection, prefix: string): Promise<string[]> {
  const jsm = await jetstreamManager(nc);
  const names: string[] = [];
  for await (const ci of jsm.consumers.list(TEST_STREAM)) {
    if (ci.name.startsWith(prefix)) {
      names.push(ci.name);
    }
  }
  return names;
}
describe("streamCreatedId", () => {
  it("converts RFC3339 timestamps exactly to decimal Unix nanoseconds", () => {
    const base = BigInt(Date.UTC(2026, 10, 6, 10, 11, 12)) * 1_000_000n;
    expect(streamCreatedId("2026-11-06T10:11:12.123456789Z")).toBe((base + 123456789n).toString());
    expect(streamCreatedId("2026-11-06T10:11:12.5Z")).toBe((base + 500000000n).toString());
    expect(streamCreatedId("2026-11-06T10:11:12Z")).toBe(base.toString());
    expect(streamCreatedId("2026-11-06T11:11:12.000000001+01:00")).toBe((base + 1n).toString());
    expect(streamCreatedId("2026-11-06T08:41:12.000000001-01:30")).toBe((base + 1n).toString());
    expect(streamCreatedId("1969-12-31T23:59:59.9Z")).toBe("-100000000");
    expect(streamCreatedId("0001-01-01T00:00:00Z")).toBe("");
    expect(streamCreatedId("")).toBe("");
    expect(streamCreatedId(undefined)).toBe("");
    expect(streamCreatedId("not a time")).toBe("");
  });
});

describe("OffsetConsumer integration tests", () => {
  let server: NatsServer;

  // Track consumers for cleanup
  const consumers: OffsetConsumer[] = [];

  beforeAll(async () => {
    server = await startNatsServer();
  });

  afterAll(async () => {
    if (server) {
      await server.stop();
    }
  });

  afterEach(async () => {
    for (const c of consumers) {
      try {
        await c.deleteConsumer();
      } catch {
        // ignore
      }
      try {
        await c.close();
      } catch {
        // ignore
      }
    }
    consumers.length = 0;
  });

  async function setupStream(serverUrl: string) {
    const nc = await connect({ servers: serverUrl });
    const jsm = await jetstreamManager(nc);

    // Create stream (ignore if already exists)
    try {
      await jsm.streams.add({
        name: TEST_STREAM,
        subjects: ["test.>"],
      });
    } catch {
      // stream may already exist
    }

    // Publish messages
    const js = jetstream(nc);
    for (let i = 1; i <= NUM_MESSAGES; i++) {
      await js.publish(`test.${i}`, new TextEncoder().encode(`msg-${i}`));
    }

    return nc;
  }

  it("basic consume - receives messages from stream", async () => {
    const nc = await setupStream(server.url);
    try {
      const consumer = await createConsumer(nc, {
        stream: TEST_STREAM,
        consumerName: uniqueName("test-basic"),
      });
      consumers.push(consumer);

      const messages = await consumer.consume();
      const received = await take(messages, NUM_MESSAGES);
      messages.stop();

      expect(received.length).toBeGreaterThan(0);
      console.log(`basic consume: received ${received.length} messages`);
    } finally {
      await nc.close();
    }
  }, 15000);

  it("commitSync and resume - resumes from committed position", async () => {
    const nc = await setupStream(server.url);
    try {
      const name = uniqueName("test-resume");

      // Phase 1: consume messages and commit
      const c1 = await createConsumer(nc, {
        stream: TEST_STREAM,
        consumerName: name,
      });
      consumers.push(c1);

      const messages1 = await c1.consume();
      const received1 = await take(messages1, 10);
      messages1.stop();

      expect(received1.length).toBeGreaterThan(0);
      const lastSeq = received1[received1.length - 1];

      await c1.commitSync();
      await c1.close();
      console.log(`Phase 1: committed at seq ${lastSeq}, received ${received1.length} messages`);

      // Publish more messages so the resumed consumer has something to receive
      const js = jetstream(nc);
      for (let i = NUM_MESSAGES + 1; i <= NUM_MESSAGES + 10; i++) {
        await js.publish(`test.${i}`, new TextEncoder().encode(`msg-${i}`));
      }

      // Phase 2: reopen with same name, verify resume position
      const c2 = await createConsumer(nc, {
        stream: TEST_STREAM,
        consumerName: name,
      });
      consumers.push(c2);

      const messages2 = await c2.consume();
      const [firstResumeSeq] = await take(messages2, 1);
      messages2.stop();

      console.log(`Phase 2: first message at seq ${firstResumeSeq} (expected ${lastSeq + 1})`);
      expect(firstResumeSeq).toBe(lastSeq + 1);
    } finally {
      await nc.close();
    }
  }, 20000);

  it("commitAsync - resolves and the stored offset reflects the ack", async () => {
    const nc = await setupStream(server.url);
    try {
      const consumer = await createConsumer(nc, {
        stream: TEST_STREAM,
        consumerName: uniqueName("test-async"),
      });
      consumers.push(consumer);

      const messages = await consumer.consume();
      const received = await take(messages, NUM_MESSAGES);
      const delivered = received[received.length - 1];

      await consumer.commitAsync();
      const info = await consumer.info();
      expect(info?.seq).toBe(delivered);
      console.log(`commitAsync succeeded, stored seq ${info?.seq}`);
    } finally {
      await nc.close();
    }
  }, 15000);

  it("commitAsync chained - several pending commits all complete", async () => {
    const nc = await setupStream(server.url);
    try {
      const consumer = await createConsumer(nc, {
        stream: TEST_STREAM,
        consumerName: uniqueName("test-chained"),
      });
      consumers.push(consumer);

      const messages = await consumer.consume();
      await take(messages, 1);

      const p1 = consumer.commitAsync();
      const p2 = consumer.commitAsync();
      const p3 = consumer.commitAsync();
      await Promise.all([p1, p2, p3]);
      console.log("three pipelined async commits acknowledged");
    } finally {
      await nc.close();
    }
  }, 15000);

  it("commitAsync with nothing to commit resolves immediately", async () => {
    const nc = await setupStream(server.url);
    try {
      const consumer = await createConsumer(nc, {
        stream: TEST_STREAM,
        consumerName: uniqueName("test-nothing"),
      });
      consumers.push(consumer);
      await consumer.commitAsync();
      await consumer.commitSync();
    } finally {
      await nc.close();
    }
  }, 10000);

  it("autoCommit - stores offset automatically", async () => {
    const nc = await setupStream(server.url);
    try {
      const name = uniqueName("test-autocommit");

      const c1 = await createConsumer(nc, {
        stream: TEST_STREAM,
        consumerName: name,
        autoCommit: true,
        autoCommitIntervalMs: 1000,
      });
      consumers.push(c1);

      const messages = await c1.consume();
      const received = await take(messages, NUM_MESSAGES);
      // Wait for auto-commit to fire
      await sleep(1500);
      messages.stop();

      console.log(`Received ${received.length} messages before close`);

      await c1.close();

      // Reopen and verify stored offset
      const c2 = await createConsumer(nc, {
        stream: TEST_STREAM,
        consumerName: name,
      });
      consumers.push(c2);

      const info = await c2.info();
      console.log(`Stored offset after auto-commit: ${info?.seq}`);
      expect(info?.seq).toBeGreaterThan(0);
    } finally {
      await nc.close();
    }
  }, 20000);

  it("seek - repositions consumer to specific sequence", async () => {
    const nc = await setupStream(server.url);
    try {
      const name = uniqueName("test-seek");

      // Phase 1: create, consume, commit, close
      const c1 = await createConsumer(nc, {
        stream: TEST_STREAM,
        consumerName: name,
      });
      consumers.push(c1);

      const messages1 = await c1.consume();
      await take(messages1, 1);
      messages1.stop();

      await c1.commitSync();
      await c1.close();

      // Phase 2: reopen, seek to 50, consume and check first message
      const c2 = await createConsumer(nc, {
        stream: TEST_STREAM,
        consumerName: name,
      });
      consumers.push(c2);

      await c2.seek(50);

      const messages2 = await c2.consume();
      const [firstSeq] = await take(messages2, 1);
      messages2.stop();

      console.log(`First message after Seek(50): seq ${firstSeq} (expected 51)`);
      expect(firstSeq).toBe(51);
    } finally {
      await nc.close();
    }
  }, 20000);

  it("seek while consuming - restarts under a new generation and keeps the iterable going", async () => {
    const nc = await setupStream(server.url);
    try {
      const consumer = await createConsumer(nc, {
        stream: TEST_STREAM,
        consumerName: uniqueName("test-seekgen"),
      });
      consumers.push(consumer);
      const owner = ownerOf(consumer);

      const messages = await consumer.consume();
      const iter = messages[Symbol.asyncIterator]();
      // Drain the initial backlog.
      let last = 0;
      while (last < NUM_MESSAGES) {
        const r = await iter.next();
        if (r.done) throw new Error("iterator ended early");
        last = r.value.seq;
      }

      const before = await consumerNamesWithPrefix(nc, `${owner}-g`);
      expect(before.length).toBe(1);
      expect(before[0]).toContain("-g1_");

      await consumer.seek(50);

      // The same iterable continues from the new position.
      const next = await iter.next();
      expect(next.done).toBe(false);
      expect(next.value.seq).toBe(51);

      const after = await consumerNamesWithPrefix(nc, `${owner}-g`);
      console.log(`consumers after seek: ${after}`);
      expect(after.length).toBe(1);
      expect(after[0]).toContain("-g2_");

      // stop() deletes the ordered consumer.
      messages.stop();
      await sleep(200);
      expect((await consumerNamesWithPrefix(nc, `${owner}-g`)).length).toBe(0);
    } finally {
      await nc.close();
    }
  }, 20000);

  it("deleteConsumer - removes stored offset", async () => {
    const nc = await setupStream(server.url);
    try {
      const name = uniqueName("test-delete");

      // Phase 1: create and commit an offset
      const c1 = await createConsumer(nc, {
        stream: TEST_STREAM,
        consumerName: name,
      });

      const messages1 = await c1.consume();
      await take(messages1, 1);
      messages1.stop();

      await c1.commitSync();
      await c1.close();

      // Phase 2: reopen, verify offset exists, then delete
      const c2 = await createConsumer(nc, {
        stream: TEST_STREAM,
        consumerName: name,
      });

      const before = await c2.info();
      expect(before?.seq).toBeGreaterThan(0);
      console.log(`Before delete: seq=${before?.seq}`);

      await c2.deleteConsumer();
      await c2.close();
      expect(await c2.info()).toBeNull();

      // Phase 3: new consumer with same name should start from beginning
      const c3 = await createConsumer(nc, {
        stream: TEST_STREAM,
        consumerName: name,
      });
      consumers.push(c3);
      expect(await c3.info()).toBeNull();

      await c3.consume();
      const after = await c3.info();
      console.log(`After delete: seq=${after?.seq} (expected 0)`);
      expect(after?.seq).toBe(0);
    } finally {
      await nc.close();
    }
  }, 20000);

  it("invalid consumer name - throws InvalidConsumerNameError", async () => {
    const nc = await connect({ servers: server.url });
    try {
      await expect(
        createConsumer(nc, {
          stream: TEST_STREAM,
          consumerName: "",
        })
      ).rejects.toThrow(InvalidConsumerNameError);

      await expect(
        createConsumer(nc, {
          stream: TEST_STREAM,
          consumerName: "has.dot",
        })
      ).rejects.toThrow(InvalidConsumerNameError);

      for (const name of ["has*star", "has>gt", "has space", "has/slash", "x".repeat(200)]) {
        await expect(createConsumer(nc, { stream: TEST_STREAM, consumerName: name })).rejects.toThrow(
          InvalidConsumerNameError,
        );
      }
      await expect(createConsumer(nc, { stream: "bad stream", consumerName: "ok" })).rejects.toThrow(
        InvalidStreamNameError,
      );
      await expect(createConsumer(nc, { stream: "", consumerName: "ok" })).rejects.toThrow(InvalidStreamNameError);

      const jsm = await jetstreamManager(nc);
      await jsm.streams.add({ name: TEST_STREAM, subjects: ["test.>"] }).catch(() => {});
      const valid = await createConsumer(nc, { stream: TEST_STREAM, consumerName: `Ok_name-1=x${Date.now()}` });
      consumers.push(valid);

      console.log("Invalid consumer name validation works correctly");
    } finally {
      await nc.close();
    }
  }, 10000);

  it("startPosition end - receives no messages when no new publishes", async () => {
    const nc = await setupStream(server.url);
    try {
      const consumer = await createConsumer(nc, {
        stream: TEST_STREAM,
        consumerName: uniqueName("test-startend"),
        startPosition: "end",
      });
      consumers.push(consumer);

      const messages = await consumer.consume();
      const received = await take(messages, 1, 2000);

      console.log(`Messages received with startPosition "end" (no new publishes): ${received.length}`);
      expect(received.length).toBe(0);
    } finally {
      await nc.close();
    }
  }, 10000);

  it("info and listConsumers expose the ownership fields", async () => {
    const nc = await setupStream(server.url);
    try {
      const name = uniqueName("test-owner");
      const consumer = await createConsumer(nc, { stream: TEST_STREAM, consumerName: name });
      consumers.push(consumer);
      expect(await consumer.info()).toBeNull(); // nothing is written before consume()

      const messages = await consumer.consume();
      const info = await consumer.info();
      expect(info).not.toBeNull();
      expect(info!.owner.startsWith(`oc-${TEST_STREAM}-${name}-`)).toBe(true);
      expect(info!.host).toBe(hostname());
      expect(info!.seq).toBe(0);
      await take(messages, 1);
      await consumer.commitSync();

      const list = await listConsumers(nc, TEST_STREAM);
      const listed = list.get(name);
      expect(listed).toBeDefined();
      expect(listed!.owner).toBe(info!.owner);
      expect(listed!.seq).toBeGreaterThan(0);

      const after = await consumer.info();
      expect(after!.owner).toBe(info!.owner);
    } finally {
      await nc.close();
    }
  }, 15000);

  it("second instance is refused while the first is alive", async () => {
    const nc = await setupStream(server.url);
    try {
      const name = uniqueName("test-refused");
      const c1 = await createConsumer(nc, { stream: TEST_STREAM, consumerName: name });
      consumers.push(c1);
      const messages = await c1.consume();
      await take(messages, 1);
      const info = (await c1.info())!;

      let err: unknown;
      try {
        await createConsumer(nc, { stream: TEST_STREAM, consumerName: name });
      } catch (e) {
        err = e;
      }
      expect(err).toBeInstanceOf(AlreadyInUseError);
      const inUse = err as AlreadyInUseError;
      expect(inUse.owner).toBe(info.owner);
      expect(inUse.host).toBe(info.host);
      console.log(`refused: ${inUse.message}`);

      expect((await c1.info())!.owner).toBe(info.owner);
    } finally {
      await nc.close();
    }
  }, 15000);

  it("close deletes the ordered consumer and releases ownership", async () => {
    const nc = await setupStream(server.url);
    try {
      const name = uniqueName("test-release");
      const c1 = await createConsumer(nc, { stream: TEST_STREAM, consumerName: name });
      const messages = await c1.consume();
      await take(messages, 1);
      await c1.commitSync();
      const owner1 = (await c1.info())!.owner;
      const committed = (await c1.info())!.seq;

      expect((await consumerNamesWithPrefix(nc, `${owner1}-g`)).length).toBe(1);
      await c1.close();
      expect((await consumerNamesWithPrefix(nc, owner1)).length).toBe(0);

      const released = (await c1.info())!;
      expect(released.owner).toBe("");
      expect(released.host).toBe("");
      expect(released.seq).toBe(committed);

      const c2 = await createConsumer(nc, { stream: TEST_STREAM, consumerName: name });
      consumers.push(c2);
      await c2.consume();
      const info2 = (await c2.info())!;
      expect(info2.owner).not.toBe("");
      expect(info2.owner).not.toBe(owner1);
      expect(info2.seq).toBe(committed);
    } finally {
      await nc.close();
    }
  }, 15000);

  it("close with a backlog of async commits persists the highest requested position", async () => {
    const nc = await setupStream(server.url);
    try {
      const name = uniqueName("test-backlog");
      const consumer = await createConsumer(nc, { stream: TEST_STREAM, consumerName: name });
      const messages = await consumer.consume();
      const received = await take(messages, NUM_MESSAGES);
      const delivered = received[received.length - 1];
      const owner = (await consumer.info())!.owner;

      let last: Promise<void> = Promise.resolve();
      for (let i = 0; i < 20000; i++) {
        last = consumer.commitAsync();
      }

      const previous = internals.closeTimeoutMs;
      internals.closeTimeoutMs = 100;
      const start = Date.now();
      try {
        await consumer.close();
      } finally {
        internals.closeTimeoutMs = previous;
      }
      expect(Date.now() - start).toBeLessThan(3000);
      await last;

      const info = (await consumer.info())!;
      expect(info.seq).toBe(delivered);
      expect(info.owner).toBe("");
      expect((await consumerNamesWithPrefix(nc, owner)).length).toBe(0);
      await consumer.deleteConsumer();
    } finally {
      await nc.close();
    }
  }, 15000);

  it("bucket is created with a 10 minute delete-marker TTL", async () => {
    const nc = await setupStream(server.url);
    try {
      const consumer = await createConsumer(nc, { stream: TEST_STREAM, consumerName: uniqueName("test-ttl") });
      consumers.push(consumer);
      const jsm = await jetstreamManager(nc);
      const cfg = (await jsm.streams.info("KV_offset-consumers")).config;
      expect(cfg.subject_delete_marker_ttl).toBe(10 * 60 * 1000 * 1_000_000);
      expect(cfg.allow_msg_ttl).toBe(true);
    } finally {
      await nc.close();
    }
  }, 10000);

  it("crash recovery - refused until its ordered consumer expired, then admitted at consume", async () => {
    const nc = await setupStream(server.url);
    try {
      const name = uniqueName("test-crash");
      const crashed = await connect({ servers: server.url });
      const c1 = await createConsumer(crashed, {
        stream: TEST_STREAM,
        consumerName: name,
        inactiveThresholdMs: 2000,
      });
      const messages = await c1.consume();
      await take(messages, 1);
      const owner1 = (await c1.info())!.owner;

      // Simulate a crash: drop the connection without closing the consumer.
      await crashed.close();

      const config = { stream: TEST_STREAM, consumerName: name, inactiveThresholdMs: 2000 };
      await expect(createConsumer(nc, config)).rejects.toThrow(AlreadyInUseError);

      const start = Date.now();
      let recovered: OffsetConsumer | null = null;
      while (!recovered) {
        try {
          recovered = await createConsumer(nc, config);
        } catch (e) {
          expect(e).toBeInstanceOf(AlreadyInUseError);
          expect(Date.now() - start).toBeLessThan(15000);
          await sleep(200);
        }
      }
      consumers.push(recovered);
      await recovered.consume(); // the claim takes the entry over after the liveness wait
      console.log(`recovered after ${Date.now() - start} ms`);
      expect((await consumerNamesWithPrefix(nc, owner1)).length).toBe(0);
      expect((await recovered.info())!.owner).not.toBe(owner1);
    } finally {
      await nc.close();
    }
  }, 30000);

  it("autoCommit skips the message the application is handling", async () => {
    const nc = await setupStream(server.url);
    try {
      const c = await createConsumer(nc, {
        stream: TEST_STREAM,
        consumerName: uniqueName("test-ac-skip"),
        autoCommit: true,
        autoCommitIntervalMs: 100,
      });
      consumers.push(c);
      const last = (await (await jetstreamManager(nc)).streams.info(TEST_STREAM)).state.last_seq;

      let release!: () => void;
      const gate = new Promise<void>((r) => (release = r));
      const messages = await c.consume();
      const loop = (async () => {
        for await (const msg of messages) {
          if (msg.seq === 3) {
            await gate;
          }
        }
      })();

      expect(await waitUntil(5000, async () => (await storedSeq(c)) === 2)).toBe(true);
      await sleep(500);
      expect(await storedSeq(c)).toBe(2);
      release();
      expect(await waitUntil(5000, async () => (await storedSeq(c)) === last)).toBe(true);
      messages.stop();
      await loop;
    } finally {
      await nc.close();
    }
  }, 20000);

  it("close with autoCommit skips the message the application is handling", async () => {
    const nc = await setupStream(server.url);
    try {
      const name = uniqueName("test-close-skip");
      const c = await createConsumer(nc, {
        stream: TEST_STREAM,
        consumerName: name,
        autoCommit: true,
        autoCommitIntervalMs: 3_600_000,
      });
      let entered!: () => void;
      const inHandler = new Promise<void>((r) => (entered = r));
      let release!: () => void;
      const gate = new Promise<void>((r) => (release = r));
      const messages = await c.consume();
      const loop = (async () => {
        for await (const msg of messages) {
          if (msg.seq === 5) {
            entered();
            await gate;
          }
        }
      })();
      await inHandler;
      await c.close();
      release();
      await loop;

      const c2 = await createConsumer(nc, { stream: TEST_STREAM, consumerName: name });
      consumers.push(c2);
      expect(await storedSeq(c2)).toBe(4);
    } finally {
      await nc.close();
    }
  }, 15000);

  it("startPosition end is persisted at creation", async () => {
    const nc = await setupStream(server.url);
    try {
      const name = uniqueName("test-end-persisted");
      const jsm = await jetstreamManager(nc);
      const last = (await jsm.streams.info(TEST_STREAM)).state.last_seq;

      const c1 = await createConsumer(nc, { stream: TEST_STREAM, consumerName: name, startPosition: "end" });
      const js = jetstream(nc);
      for (let i = 0; i < 2; i++) {
        // published after creation, before consume(): delivered
        await js.publish("test.early", new TextEncoder().encode("early"));
      }
      const m1 = await c1.consume();
      expect(await storedSeq(c1)).toBe(last); // stored by the claim
      expect(await take(m1, 2)).toEqual([last + 1, last + 2]);
      await c1.commitSync();
      await c1.close();

      for (let i = 0; i < 3; i++) {
        await js.publish("test.new", new TextEncoder().encode("new"));
      }

      const c2 = await createConsumer(nc, { stream: TEST_STREAM, consumerName: name, startPosition: "end" });
      consumers.push(c2);
      const messages = await c2.consume();
      const received = await take(messages, 3);
      messages.stop();
      expect(received).toEqual([last + 3, last + 4, last + 5]);
    } finally {
      await nc.close();
    }
  }, 15000);

  it("seekToBeginning goes to the beginning even with startPosition end", async () => {
    const nc = await setupStream(server.url);
    try {
      const c = await createConsumer(nc, {
        stream: TEST_STREAM,
        consumerName: uniqueName("test-end-begin"),
        startPosition: "end",
      });
      consumers.push(c);
      const messages = await c.consume();
      await c.seekToBeginning();
      const [first] = await take(messages, 1);
      messages.stop();
      expect(first).toBe(1);
    } finally {
      await nc.close();
    }
  }, 15000);

  it("concurrent seeks leave exactly one ordered consumer", async () => {
    const nc = await setupStream(server.url);
    try {
      const c = await createConsumer(nc, { stream: TEST_STREAM, consumerName: uniqueName("test-conc-seek") });
      consumers.push(c);
      const owner = ownerOf(c);
      const messages = await c.consume();
      const iter = messages[Symbol.asyncIterator]();
      expect((await iter.next()).done).toBe(false);

      await Promise.all(Array.from({ length: 10 }, (_, i) => c.seek(i * 5)));
      await sleep(200);
      const names = await consumerNamesWithPrefix(nc, `${owner}-g`);
      expect(names.length).toBe(1);

      // Seeks are applied in call order: the last one (45) wins.
      const next = await iter.next();
      expect(next.done).toBe(false);
      expect(next.value.seq).toBe(46);
      messages.stop();
    } finally {
      await nc.close();
    }
  }, 15000);
});

/**
 * Tests that delete or pre-create the bucket or the stream, each against its
 * own server. Cleanup closes the consumers, then the connection, then the
 * server, so that close() does not wait on timeouts.
 */
describe("OffsetConsumer tests with a dedicated server", () => {
  let server: NatsServer;
  let nc: NatsConnection;
  const owned: OffsetConsumer[] = [];
  const extraConns: NatsConnection[] = [];

  async function newConnection(): Promise<NatsConnection> {
    const c = await connect({ servers: server.url });
    extraConns.push(c);
    return c;
  }

  async function createTestStream(n: number): Promise<void> {
    const jsm = await jetstreamManager(nc);
    await jsm.streams.add({ name: TEST_STREAM, subjects: ["test.>"] });
    const js = jetstream(nc);
    for (let i = 1; i <= n; i++) {
      await js.publish(`test.${i}`, new TextEncoder().encode(`msg-${i}`));
    }
  }

  /** Deletes the test stream, recreates it empty, and publishes n messages. */
  async function recreateStream(n: number): Promise<void> {
    const jsm = await jetstreamManager(nc);
    await jsm.streams.delete(TEST_STREAM);
    await createTestStream(n);
  }

  async function newConsumer(config: Partial<Parameters<typeof createConsumer>[1]> = {}): Promise<OffsetConsumer> {
    const c = await createConsumer(nc, { stream: TEST_STREAM, consumerName: uniqueName("c"), ...config });
    owned.push(c);
    return c;
  }

  /** Iterates messages in the background, recording their sequences. */
  function drain(messages: ConsumerMessages, onMessage: (msg: JsMsg) => void = () => {}): Promise<void> {
    return (async () => {
      for await (const msg of messages) {
        onMessage(msg);
      }
    })();
  }

  beforeEach(async () => {
    server = await startNatsServer();
    nc = await connect({ servers: server.url });
    await createTestStream(NUM_MESSAGES);
  });

  afterEach(async () => {
    for (const c of owned) {
      await c.close().catch(() => {});
    }
    owned.length = 0;
    for (const c of extraConns) {
      await c.close().catch(() => {});
    }
    extraConns.length = 0;
    await nc.close();
    await server.stop();
  });

  it("an existing bucket's configuration is not overwritten", async () => {
    const kvm = new Kvm(nc);
    await kvm.create(BUCKET_NAME, { description: "operator managed", history: 1 });
    await newConsumer();
    const jsm = await jetstreamManager(nc);
    const cfg = (await jsm.streams.info(BUCKET_STREAM_NAME)).config;
    expect(cfg.description).toBe("operator managed");
    expect(cfg.allow_msg_ttl ?? false).toBe(false);
  }, 15000);

  it("the errorHandler can close the consumer when an auto-commit fails", async () => {
    let closing: Promise<void> | null = null;
    const c = await newConsumer({
      autoCommit: true,
      autoCommitIntervalMs: 200,
      errorHandler: (oc, err) => {
        if (err.message.includes("auto-commit failed") && !closing) {
          closing = oc.close();
        }
      },
    });
    const messages = await c.consume();
    // Make commits fail: without the bucket, KV writes get no responders.
    const jsm = await jetstreamManager(nc);
    await jsm.streams.delete(BUCKET_STREAM_NAME);
    const loop = drain(messages);

    expect(await waitUntil(10000, () => closing !== null)).toBe(true);
    let settled = false;
    void closing!.then(
      () => (settled = true),
      () => (settled = true),
    );
    expect(await waitUntil(10000, () => settled)).toBe(true);
    await loop; // close() ended the iterable
  }, 25000);

  it("a failed seek leaves the position unchanged", async () => {
    const name = uniqueName("c");
    const c = await newConsumer({ consumerName: name });
    const messages = await c.consume();
    const iter = messages[Symbol.asyncIterator]();
    let last = 0;
    while (last < 20) {
      last = (await iter.next()).value.seq;
    }
    await c.commitSync();
    const owner = (await c.info())!.owner;

    const jsm = await jetstreamManager(nc);
    await jsm.streams.delete(BUCKET_STREAM_NAME);
    await expect(c.seek(5)).rejects.toThrow();

    // With the bucket (and an entry still naming this instance, so that the
    // conditional release goes ahead) back, the release write persists the
    // committed position, not the failed seek target.
    const kv = await new Kvm(nc).create(BUCKET_NAME, { history: 1 });
    await kv.put(
      `${TEST_STREAM}.${name}`,
      JSON.stringify({ seq: 0, committed_at: "", owner, host: "" }),
    );
    await c.close();
    expect(await storedSeq(c)).toBe(20);
  }, 15000);

  it("a stored offset from an earlier incarnation of the stream is reset at startup", async () => {
    const name = uniqueName("c");
    const c1 = await createConsumer(nc, { stream: TEST_STREAM, consumerName: name });
    const messages = await c1.consume();
    const received = await take(messages, NUM_MESSAGES);
    expect(received[received.length - 1]).toBe(NUM_MESSAGES);
    await c1.commitSync();
    const oldCreated = (await c1.info())!.streamCreated;
    expect(oldCreated).toMatch(/^\d+$/);
    await c1.close();

    await recreateStream(NUM_MESSAGES + 20); // passes the old position: only the stream identity tells
    const reported: StreamRecreatedError[] = [];
    const c2 = await newConsumer({
      consumerName: name,
      errorHandler: (_, err) => {
        if (err instanceof StreamRecreatedError) {
          reported.push(err);
        }
      },
    });
    expect(reported.length).toBe(0);
    const m2 = await c2.consume(); // the claim detects it
    expect(reported.length).toBe(1);
    expect(reported[0].storedSeq).toBe(NUM_MESSAGES);

    const info = (await c2.info())!;
    expect(info.seq).toBe(0);
    const jsm = await jetstreamManager(nc);
    expect(info.streamCreated).toBe(streamCreatedId((await jsm.streams.info(TEST_STREAM)).created));
    expect(info.streamCreated).not.toBe(oldCreated);

    const [first] = await take(m2, 1);
    m2.stop();
    expect(first).toBe(1);
  }, 20000);

  it("a stream recreated between construction and consume is detected by the claim", async () => {
    const name = uniqueName("c");
    const c1 = await createConsumer(nc, { stream: TEST_STREAM, consumerName: name });
    const received = await take(await c1.consume(), NUM_MESSAGES);
    expect(received[received.length - 1]).toBe(NUM_MESSAGES);
    await c1.commitSync();
    await c1.close();

    const reported: StreamRecreatedError[] = [];
    const c2 = await newConsumer({
      consumerName: name,
      errorHandler: (_, err) => {
        if (err instanceof StreamRecreatedError) {
          reported.push(err);
        }
      },
    });
    await recreateStream(NUM_MESSAGES + 20); // passes the old position: only the identity tells
    const messages = await c2.consume();
    expect(reported.length).toBe(1);
    expect(await storedSeq(c2)).toBe(0);
    const [first] = await take(messages, 1);
    messages.stop();
    expect(first).toBe(1);
  }, 20000);

  it("a legacy entry without stream identity past the stream's end is reset", async () => {
    const name = uniqueName("c");
    const kv = await new Kvm(nc).create(BUCKET_NAME, { history: 1 });
    await kv.put(`${TEST_STREAM}.${name}`, '{"seq":5000,"committed_at":"2026-01-01T00:00:00Z"}');
    const reported: StreamRecreatedError[] = [];
    const c = await newConsumer({
      consumerName: name,
      errorHandler: (_, err) => {
        if (err instanceof StreamRecreatedError) {
          reported.push(err);
        }
      },
    });
    await c.consume();
    expect(await storedSeq(c)).toBe(0);
    expect(reported.map((e) => e.storedSeq)).toEqual([5000]);
  }, 15000);

  it("a stream recreated while consuming is detected and the new stream's messages are delivered", async () => {
    let reported = false;
    const afterReset: number[] = [];
    let last = 0;
    const c = await newConsumer({
      errorHandler: (_, err) => {
        if (err instanceof StreamRecreatedError) {
          reported = true;
        }
      },
    });
    const loop = drain(await c.consume(), (msg) => {
      last = msg.seq;
      if (reported) {
        afterReset.push(msg.seq);
      }
    });
    expect(await waitUntil(5000, () => last === NUM_MESSAGES)).toBe(true);

    await recreateStream(10);
    expect(await waitUntil(20000, () => reported)).toBe(true);
    expect(await waitUntil(10000, () => afterReset.length >= 10)).toBe(true);
    expect(afterReset).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    // The seek write switched the stored offset to the new stream's identity.
    const jsm = await jetstreamManager(nc);
    const created = streamCreatedId((await jsm.streams.info(TEST_STREAM)).created);
    await c.commitSync();
    const info = (await c.info())!;
    expect(info.streamCreated).toBe(created);
    expect(info.seq).toBe(10);
    await c.close();
    await loop;
  }, 45000);
  it("of concurrent starters exactly one wins and only its ordered consumer exists", async () => {
    const name = uniqueName("c");
    const conns = await Promise.all(Array.from({ length: 4 }, () => newConnection()));
    const results = await Promise.allSettled(
      conns.map(async (conn) => {
        const c = await createConsumer(conn, { stream: TEST_STREAM, consumerName: name });
        try {
          await c.consume();
        } catch (err) {
          await c.close();
          throw err;
        }
        return c;
      }),
    );
    const winners: OffsetConsumer[] = [];
    for (const r of results) {
      if (r.status === "fulfilled") {
        winners.push(r.value);
        owned.push(r.value);
      } else {
        expect(r.reason).toBeInstanceOf(AlreadyInUseError);
      }
    }
    expect(winners.length).toBe(1);
    const names = await consumerNamesWithPrefix(nc, `oc-${TEST_STREAM}-${name}-`);
    expect(names.length).toBe(1);
    expect(names[0].startsWith(ownerOf(winners[0]))).toBe(true);
  }, 15000);

  it("close does not release an entry another instance claimed meanwhile", async () => {
    const name = uniqueName("c");
    const c = await newConsumer({ consumerName: name });
    const owner = ownerOf(c);
    let last = 0;
    const loop = drain(await c.consume(), (msg) => (last = msg.seq));
    expect(await waitUntil(5000, () => last === NUM_MESSAGES)).toBe(true);
    const kv = await new Kvm(nc).open(BUCKET_NAME);
    await kv.put(
      `${TEST_STREAM}.${name}`,
      // claimed_at, written by older versions, is ignored.
      '{"seq":3,"committed_at":"","owner":"oc-someone-else","host":"h","claimed_at":""}',
    );

    const err = await c.close().then(
      () => null,
      (e: unknown) => e,
    );
    expect(err).toBeInstanceOf(AlreadyInUseError);
    expect((err as AlreadyInUseError).owner).toBe("oc-someone-else");
    expect((err as AlreadyInUseError).message).toContain("release skipped");
    await loop; // close() ended the iterable
    const info = await c.info();
    expect(info?.owner).toBe("oc-someone-else");
    expect(info?.seq).toBe(3);
    expect(await consumerNamesWithPrefix(nc, owner)).toEqual([]);
  }, 15000);

  it("close does not recreate an entry deleted by someone else", async () => {
    const name = uniqueName("c");
    const c = await newConsumer({ consumerName: name });
    await c.consume();
    expect(await c.info()).not.toBeNull();
    const kv = await new Kvm(nc).open(BUCKET_NAME);
    await kv.delete(`${TEST_STREAM}.${name}`);
    await c.close();
    expect(await c.info()).toBeNull();
  }, 15000);

  it("the conditional release succeeds right after a burst of async commits", async () => {
    const c = await newConsumer();
    const commits: Promise<void>[] = [];
    let last = 0;
    const loop = drain(await c.consume(), (msg) => {
      last = msg.seq;
      commits.push(c.commitAsync());
    });
    expect(await waitUntil(5000, () => last === NUM_MESSAGES)).toBe(true);
    await c.close();
    await loop;
    await Promise.all(commits);
    const info = await c.info();
    expect(info?.owner).toBe("");
    expect(info?.seq).toBe(NUM_MESSAGES);
  }, 15000);

  /** Sets the takeover wait for the current test. */
  function withLivenessWait(ms: number): void {
    const { livenessWaitMs } = internals;
    internals.livenessWaitMs = ms;
    onTestFinished(() => {
      internals.livenessWaitMs = livenessWaitMs;
    });
  }

  /** Stores an entry owned by owner, an instance that has no consumer on the server. */
  async function putOwnedEntry(name: string, owner: string, seq: number): Promise<void> {
    const kv = await new Kvm(nc).create(BUCKET_NAME, { history: 1 });
    await kv.put(`${TEST_STREAM}.${name}`, JSON.stringify({ seq, committed_at: "", owner, host: "h" }));
  }

  it("nothing is written before the first consume", async () => {
    const c = await newConsumer();
    await c.commitSync();
    await c.commitAsync();
    await c.close();
    expect(await c.info()).toBeNull();
    expect(await consumerNamesWithPrefix(nc, "oc-")).toEqual([]);
  }, 15000);

  it("the second instance to consume is refused", async () => {
    const name = uniqueName("c");
    const a = await newConsumer({ consumerName: name });
    const b = await createConsumer(await newConnection(), { stream: TEST_STREAM, consumerName: name });
    owned.push(b);
    await a.consume();
    const err = await b.consume().then(
      () => null,
      (e: unknown) => e,
    );
    expect(err).toBeInstanceOf(AlreadyInUseError);
    expect((err as AlreadyInUseError).owner).toBe(ownerOf(a));
    await b.close();
    expect((await a.info())?.owner).toBe(ownerOf(a));
  }, 15000);

  it("of concurrent starters taking over a dead owner exactly one wins (no jitter needed)", async () => {
    // All starters wake from the wait at the same moment; the CAS against
    // the revision read before the wait lets exactly one of them win.
    withLivenessWait(500);
    const name = uniqueName("c");
    await putOwnedEntry(name, "oc-dead-owner", 7);
    const consumers = await Promise.all(
      Array.from({ length: 4 }, async () => {
        const c = await createConsumer(await newConnection(), { stream: TEST_STREAM, consumerName: name });
        owned.push(c);
        return c;
      }),
    );
    const results = await Promise.allSettled(consumers.map((c) => c.consume()));
    let wins = 0;
    for (const r of results) {
      if (r.status === "fulfilled") {
        wins++;
      } else {
        expect(r.reason).toBeInstanceOf(AlreadyInUseError);
      }
    }
    expect(wins).toBe(1);
    expect((await consumerNamesWithPrefix(nc, `oc-${TEST_STREAM}-${name}-`)).length).toBe(1);
  }, 15000);

  it("an owner without a consumer is taken over only after the liveness wait", async () => {
    withLivenessWait(1000);
    const name = uniqueName("c");
    await putOwnedEntry(name, "oc-gone-owner", 7);
    const c = await newConsumer({ consumerName: name });
    const start = Date.now();
    const messages = await c.consume();
    expect(Date.now() - start).toBeGreaterThanOrEqual(1000);
    expect(await storedSeq(c)).toBe(7);
    expect((await c.info())?.owner).toBe(ownerOf(c));
    const [first] = await take(messages, 1);
    messages.stop();
    expect(first).toBe(8);
  }, 15000);

  it("a consumer of the owner appearing during the wait makes the takeover fail", async () => {
    withLivenessWait(1000);
    const name = uniqueName("c");
    const owner = "oc-resetting-owner";
    await putOwnedEntry(name, owner, 0);
    const c = await newConsumer({ consumerName: name });
    const jsm = await jetstreamManager(nc);
    const late = sleep(300).then(() =>
      jsm.consumers.add(TEST_STREAM, {
        name: `${owner}-g1_2`,
        ack_policy: AckPolicy.Explicit,
        inactive_threshold: nanos(60_000),
      }),
    );
    await expect(c.consume()).rejects.toThrow(AlreadyInUseError);
    await late;
  }, 15000);

  it("a write to the entry during the wait makes the claim fail", async () => {
    withLivenessWait(1000);
    const name = uniqueName("c");
    const owner = "oc-committing-owner";
    await putOwnedEntry(name, owner, 3);
    const c = await newConsumer({ consumerName: name });
    const write = sleep(300).then(() => putOwnedEntry(name, owner, 4));
    await expect(c.consume()).rejects.toThrow(AlreadyInUseError);
    await write;
    const info = await c.info();
    expect(info?.owner).toBe(owner);
    expect(info?.seq).toBe(4);
  }, 15000);

  it("a seek creates the new ordered consumer before deleting the old one", async () => {
    const c = await newConsumer();
    const owner = ownerOf(c);
    let last = 0;
    const loop = drain(await c.consume(), (msg) => (last = msg.seq));
    expect(await waitUntil(5000, () => last === NUM_MESSAGES)).toBe(true);

    let stop = false;
    let gaps = 0;
    let polls = 0;
    const jsm = await jetstreamManager(nc);
    const poller = (async () => {
      while (!stop) {
        let alive = false;
        try {
          for await (const ci of jsm.consumers.list(TEST_STREAM)) {
            if (ci.name.startsWith(owner)) {
              alive = true;
            }
          }
        } catch {
          continue;
        }
        polls++;
        if (!alive) {
          gaps++;
        }
      }
    })();
    for (let i = 0; i < 20; i++) {
      await c.seek(i);
    }
    stop = true;
    await poller;
    expect(polls).toBeGreaterThan(0);
    expect(gaps).toBe(0);
    const names = await consumerNamesWithPrefix(nc, owner);
    expect(names.length).toBe(1);
    expect(names[0].startsWith(`${owner}-g21_`)).toBe(true);
    await c.close();
    await loop;
  }, 20000);

  it("a seek before the first consume is only recorded, and stored by the claim", async () => {
    const c = await newConsumer();
    await c.seek(42);
    expect(await c.info()).toBeNull();
    const messages = await c.consume();
    expect(await storedSeq(c)).toBe(42);
    const [first] = await take(messages, 1);
    messages.stop();
    expect(first).toBe(43);
  }, 15000);
});
