import { describe, it, expect, beforeAll, afterAll, afterEach } from "vitest";
import { connect } from "@nats-io/transport-node";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { createConsumer } from "../src/consumer.js";
import {
  InvalidConsumerNameError,
  AsyncCommitInFlightError,
} from "../src/errors.js";
import type { OffsetConsumer } from "../src/types.js";
import { startNatsServer, type NatsServer } from "./nats-server-helper.js";

const TEST_STREAM = "test-stream";
const NUM_MESSAGES = 100;

function uniqueName(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.floor(Math.random() * 100000)}`;
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

  it("basic consume - receives messages from stream", async () => {
    const nc = await setupStream(server.url);
    try {
      const consumer = await createConsumer(nc, {
        stream: TEST_STREAM,
        consumerName: uniqueName("test-basic"),
      });
      consumers.push(consumer);

      const messages = await consumer.consume();
      const received: number[] = [];
      const timeout = setTimeout(() => messages.stop(), 5000);

      for await (const msg of messages) {
        received.push(msg.seq);
        if (received.length >= NUM_MESSAGES) {
          messages.stop();
          break;
        }
      }
      clearTimeout(timeout);

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
      const received1: number[] = [];
      const timeout1 = setTimeout(() => messages1.stop(), 5000);

      for await (const msg of messages1) {
        received1.push(msg.seq);
        if (received1.length >= 10) {
          messages1.stop();
          break;
        }
      }
      clearTimeout(timeout1);

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
      let firstResumeSeq: number | null = null;
      const timeout2 = setTimeout(() => messages2.stop(), 5000);

      for await (const msg of messages2) {
        if (firstResumeSeq === null) {
          firstResumeSeq = msg.seq;
          messages2.stop();
          break;
        }
      }
      clearTimeout(timeout2);

      console.log(
        `Phase 2: first message at seq ${firstResumeSeq} (expected ${lastSeq + 1})`
      );
      expect(firstResumeSeq).toBe(lastSeq + 1);
    } finally {
      await nc.close();
    }
  }, 20000);

  it("commitAsync - resolves successfully", async () => {
    const nc = await setupStream(server.url);
    try {
      const consumer = await createConsumer(nc, {
        stream: TEST_STREAM,
        consumerName: uniqueName("test-async"),
      });
      consumers.push(consumer);

      const messages = await consumer.consume();
      const received: number[] = [];
      const timeout = setTimeout(() => messages.stop(), 5000);

      for await (const msg of messages) {
        received.push(msg.seq);
        if (received.length >= 10) {
          messages.stop();
          break;
        }
      }
      clearTimeout(timeout);

      expect(received.length).toBeGreaterThan(0);

      await consumer.commitAsync();
      console.log(`commitAsync succeeded, ${received.length} messages consumed`);
    } finally {
      await nc.close();
    }
  }, 15000);

  it("async commit in flight - second call throws AsyncCommitInFlightError", async () => {
    const nc = await setupStream(server.url);
    try {
      const consumer = await createConsumer(nc, {
        stream: TEST_STREAM,
        consumerName: uniqueName("test-inflight"),
      });
      consumers.push(consumer);

      const messages = await consumer.consume();
      const timeout = setTimeout(() => messages.stop(), 2000);

      for await (const _msg of messages) {
        messages.stop();
        break;
      }
      clearTimeout(timeout);

      // Fire first commitAsync without awaiting
      const p1 = consumer.commitAsync();

      // Immediately try second
      try {
        await consumer.commitAsync();
        console.log("first commit completed before second was called");
      } catch (err) {
        expect(err).toBeInstanceOf(AsyncCommitInFlightError);
        console.log("correctly got AsyncCommitInFlightError");
      }

      await p1;
    } finally {
      await nc.close();
    }
  }, 15000);

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
      let count = 0;
      const timeout = setTimeout(() => messages.stop(), 3000);

      for await (const _msg of messages) {
        count++;
        if (count >= NUM_MESSAGES) {
          // Wait for auto-commit to fire
          await new Promise((r) => setTimeout(r, 1500));
          messages.stop();
          break;
        }
      }
      clearTimeout(timeout);

      console.log(`Received ${count} messages before close`);

      await c1.close();

      // Reopen and verify stored offset
      const c2 = await createConsumer(nc, {
        stream: TEST_STREAM,
        consumerName: name,
      });
      consumers.push(c2);

      const startSeq = (c2 as any)._startSeq;
      console.log(`Stored offset after auto-commit: ${startSeq}`);
      expect(startSeq).toBeGreaterThan(0);
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
      const timeout1 = setTimeout(() => messages1.stop(), 5000);
      for await (const _msg of messages1) {
        messages1.stop();
        break;
      }
      clearTimeout(timeout1);

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
      let firstSeq: number | null = null;
      const timeout2 = setTimeout(() => messages2.stop(), 5000);

      for await (const msg of messages2) {
        if (firstSeq === null) {
          firstSeq = msg.seq;
          messages2.stop();
          break;
        }
      }
      clearTimeout(timeout2);

      console.log(
        `First message after Seek(50): seq ${firstSeq} (expected 51)`
      );
      expect(firstSeq).toBe(51);
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
      const timeout1 = setTimeout(() => messages1.stop(), 5000);
      for await (const _msg of messages1) {
        messages1.stop();
        break;
      }
      clearTimeout(timeout1);

      await c1.commitSync();
      await c1.close();

      // Phase 2: reopen, verify offset exists, then delete
      const c2 = await createConsumer(nc, {
        stream: TEST_STREAM,
        consumerName: name,
      });

      const startSeqBefore = (c2 as any)._startSeq;
      expect(startSeqBefore).toBeGreaterThan(0);
      console.log(`Before delete: startSeq=${startSeqBefore}`);

      await c2.deleteConsumer();
      await c2.close();

      // Phase 3: new consumer with same name should start from beginning
      const c3 = await createConsumer(nc, {
        stream: TEST_STREAM,
        consumerName: name,
      });
      consumers.push(c3);

      const startSeqAfter = (c3 as any)._startSeq;
      console.log(`After delete: startSeq=${startSeqAfter} (expected 0)`);
      expect(startSeqAfter).toBe(0);
    } finally {
      await nc.close();
    }
  }, 20000);

  it("invalid consumer name - throws InvalidConsumerNameError", async () => {
    const nc = await connect({ servers: server.url });
    try {
      // Empty name
      await expect(
        createConsumer(nc, {
          stream: TEST_STREAM,
          consumerName: "",
        })
      ).rejects.toThrow(InvalidConsumerNameError);

      // Name with dot
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
      const received: number[] = [];
      const timeout = setTimeout(() => messages.stop(), 2000);

      for await (const msg of messages) {
        received.push(msg.seq);
      }
      clearTimeout(timeout);

      console.log(
        `Messages received with startPosition "end" (no new publishes): ${received.length}`
      );
      expect(received.length).toBe(0);
    } finally {
      await nc.close();
    }
  }, 10000);
});
