import { describe, it, expect, beforeAll, afterAll, afterEach } from "vitest";
import { connect } from "@nats-io/transport-node";
import type { NatsConnection } from "@nats-io/nats-core";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { hostname } from "node:os";
import { createConsumer, internals, listConsumers } from "../src/consumer.js";
import { AlreadyInUseError, InvalidConsumerNameError } from "../src/errors.js";
import type { OffsetConsumer, ConsumerMessages } from "../src/types.js";
import { startNatsServer, type NatsServer } from "./nats-server-helper.js";

const TEST_STREAM = "test-stream";
const NUM_MESSAGES = 100;

function uniqueName(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.floor(Math.random() * 100000)}`;
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

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
      const owner = (await consumer.info())!.owner;

      const messages = await consumer.consume();
      const iter = messages[Symbol.asyncIterator]();
      // Drain the initial backlog.
      let last = 0;
      while (last < NUM_MESSAGES) {
        const r = await iter.next();
        if (r.done) throw new Error("iterator ended early");
        last = r.value.seq;
      }

      const before = await consumerNamesWithPrefix(nc, owner);
      expect(before.length).toBe(1);
      expect(before[0]).toContain("-g1_");

      await consumer.seek(50);

      // The same iterable continues from the new position.
      const next = await iter.next();
      expect(next.done).toBe(false);
      expect(next.value.seq).toBe(51);

      const after = await consumerNamesWithPrefix(nc, owner);
      console.log(`consumers after seek: ${after}`);
      expect(after.length).toBe(1);
      expect(after[0]).toContain("-g2_");

      // stop() deletes the ordered consumer as well.
      messages.stop();
      await sleep(200);
      expect((await consumerNamesWithPrefix(nc, owner)).length).toBe(0);
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

      const info = await consumer.info();
      expect(info).not.toBeNull();
      expect(info!.owner.startsWith(`oc-${TEST_STREAM}-${name}-`)).toBe(true);
      expect(info!.host).toBe(hostname());
      expect(Number.isNaN(Date.parse(info!.claimedAt))).toBe(false);
      expect(info!.seq).toBe(0);

      const messages = await consumer.consume();
      await take(messages, 1);
      await consumer.commitSync();

      const list = await listConsumers(nc, TEST_STREAM);
      const listed = list.get(name);
      expect(listed).toBeDefined();
      expect(listed!.owner).toBe(info!.owner);
      expect(listed!.seq).toBeGreaterThan(0);

      const after = await consumer.info();
      expect(after!.owner).toBe(info!.owner);
      expect(after!.claimedAt).toBe(info!.claimedAt);
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
      expect(inUse.claimedAt).toBe(info.claimedAt);
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

      expect((await consumerNamesWithPrefix(nc, owner1)).length).toBe(1);
      await c1.close();
      expect((await consumerNamesWithPrefix(nc, owner1)).length).toBe(0);

      const released = (await c1.info())!;
      expect(released.owner).toBe("");
      expect(released.host).toBe("");
      expect(released.claimedAt).toBe("");
      expect(released.seq).toBe(committed);

      const c2 = await createConsumer(nc, { stream: TEST_STREAM, consumerName: name });
      consumers.push(c2);
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

  it("crash recovery - refused until the ordered consumer expires, then allowed", async () => {
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
      console.log(`recovered after ${Date.now() - start} ms`);
      expect((await consumerNamesWithPrefix(nc, owner1)).length).toBe(0);
      expect((await recovered.info())!.owner).not.toBe(owner1);
    } finally {
      await nc.close();
    }
  }, 30000);
});
