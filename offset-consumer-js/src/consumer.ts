import type { NatsConnection } from "@nats-io/nats-core";
import {
  jetstream,
  jetstreamManager,
  DeliverPolicy,
  type JsMsg,
  type ConsumerMessages as NatsConsumerMessages,
  type JetStreamClient,
} from "@nats-io/jetstream";
import type { KV } from "@nats-io/kv";
import type {
  ConsumerConfig,
  ConsumerMessages,
  OffsetConsumer,
} from "./types.js";
import {
  InvalidConsumerNameError,
  AsyncCommitInFlightError,
} from "./errors.js";
import { ensureBucket, encodeOffset, decodeOffset, casUpdate } from "./kv.js";

const DEFAULT_AUTO_COMMIT_INTERVAL_MS = 5000;

/**
 * Creates a new offset consumer for the given stream and consumer name.
 * Ensures the offset-consumers KV bucket exists, reads any stored offset,
 * and resolves the start sequence. Does NOT start consuming — call consume() for that.
 */
export async function createConsumer(
  nc: NatsConnection,
  config: ConsumerConfig,
): Promise<OffsetConsumer> {
  // Validate consumer name
  if (!config.consumerName || config.consumerName.includes(".")) {
    throw new InvalidConsumerNameError(config.consumerName);
  }

  if (!config.stream) {
    throw new Error("offset consumer: stream name is required");
  }

  // Create JetStream context
  const js = jetstream(nc);

  // Look up stream info to get replicas and lastSeq
  const jsm = await jetstreamManager(nc);
  const streamInfo = await jsm.streams.info(config.stream);
  const replicas = streamInfo.config.num_replicas;

  // Ensure KV bucket exists with matching replica count
  const kv = await ensureBucket(nc, replicas);

  const kvKey = `${config.stream}.${config.consumerName}`;

  let lastRevision = 0;
  let startSeq = 0;
  let lastCommittedSeq = 0;
  let lastDeliveredSeq = 0;

  // Read stored offset
  try {
    const entry = await kv.get(kvKey);
    if (entry && entry.value && entry.value.length > 0) {
      const offset = decodeOffset(entry.value);
      lastRevision = entry.revision;
      startSeq = offset.seq;
      lastCommittedSeq = offset.seq;
      lastDeliveredSeq = offset.seq;
    }
  } catch (err: unknown) {
    // If the key doesn't exist, that's fine — use config.startPosition
    // The NATS KV client throws an error when the key is not found.
    // We check for the common "not found" pattern.
    const message =
      err instanceof Error ? err.message : String(err);
    if (!message.includes("not found")) {
      throw err;
    }
  }

  return new OffsetConsumerImpl(
    nc,
    js,
    kv,
    kvKey,
    config,
    lastRevision,
    startSeq,
    lastDeliveredSeq,
    lastCommittedSeq,
    streamInfo.state.last_seq,
  );
}

/**
 * Wraps a NATS ConsumerMessages to track last delivered sequence
 * and expose the ConsumerMessages interface.
 */
function wrapMessages(
  inner: NatsConsumerMessages,
  impl: OffsetConsumerImpl,
): ConsumerMessages {
  const wrapper: ConsumerMessages = {
    stop() {
      inner.stop();
    },
    [Symbol.asyncIterator](): AsyncIterator<JsMsg> {
      const innerIter = inner[Symbol.asyncIterator]();
      return {
        async next(): Promise<IteratorResult<JsMsg>> {
          const result = await innerIter.next();
          if (!result.done) {
            const msg = result.value;
            impl.trackDelivered(msg.seq);
          }
          return result;
        },
        async return(value?: unknown): Promise<IteratorResult<JsMsg>> {
          if (innerIter.return) {
            return innerIter.return(value);
          }
          return { done: true, value: undefined as unknown as JsMsg };
        },
      };
    },
  };
  return wrapper;
}

class OffsetConsumerImpl implements OffsetConsumer {
  private nc: NatsConnection;
  private js: JetStreamClient;
  private kv: KV;
  private kvKey: string;
  private config: ConsumerConfig;
  private _lastRevision: number;
  private _startSeq: number;
  private _lastDeliveredSeq: number;
  private _lastCommittedSeq: number;
  private asyncCommitInFlight = false;
  private _closed = false;
  private currentMessages: NatsConsumerMessages | null = null;
  private streamLastSeq: number;
  private autoCommitTimer: ReturnType<typeof setInterval> | null = null;

  constructor(
    nc: NatsConnection,
    js: JetStreamClient,
    kv: KV,
    kvKey: string,
    config: ConsumerConfig,
    lastRevision: number,
    startSeq: number,
    lastDeliveredSeq: number,
    lastCommittedSeq: number,
    streamLastSeq: number,
  ) {
    this.nc = nc;
    this.js = js;
    this.kv = kv;
    this.kvKey = kvKey;
    this.config = config;
    this._lastRevision = lastRevision;
    this._startSeq = startSeq;
    this._lastDeliveredSeq = lastDeliveredSeq;
    this._lastCommittedSeq = lastCommittedSeq;
    this.streamLastSeq = streamLastSeq;
  }

  /** Called by the wrapper to track the last delivered sequence. */
  trackDelivered(seq: number): void {
    this._lastDeliveredSeq = seq;
  }

  async consume(opts?: { signal?: AbortSignal }): Promise<ConsumerMessages> {
    if (this._closed) {
      throw new Error("offset consumer: consumer is closed");
    }
    if (this.currentMessages) {
      throw new Error("offset consumer: already consuming");
    }

    // Build ordered consumer options
    const orderOpts: Record<string, unknown> = {};

    if (this._startSeq > 0) {
      orderOpts.deliver_policy = DeliverPolicy.StartSequence;
      orderOpts.opt_start_seq = this._startSeq + 1;
    } else if (this.config.startPosition === "end") {
      orderOpts.deliver_policy = DeliverPolicy.New;
    } else {
      orderOpts.deliver_policy = DeliverPolicy.All;
    }

    if (this.config.filterSubjects && this.config.filterSubjects.length > 0) {
      orderOpts.filter_subjects = this.config.filterSubjects;
    }

    // Create ordered consumer (omit consumer name for ordered)
    const consumer = await this.js.consumers.get(
      this.config.stream,
      orderOpts,
    );

    // Start consuming
    const natsMessages = await consumer.consume();
    this.currentMessages = natsMessages;

    // If abort signal provided, stop on abort
    if (opts?.signal) {
      const onAbort = () => {
        natsMessages.stop();
      };
      if (opts.signal.aborted) {
        natsMessages.stop();
      } else {
        opts.signal.addEventListener("abort", onAbort, { once: true });
      }
    }

    // Monitor status for terminal errors if errorHandler is configured
    if (this.config.errorHandler) {
      this.monitorStatus(natsMessages);
    }

    // Start auto-commit timer if configured
    if (this.config.autoCommit) {
      this.startAutoCommit();
    }

    return wrapMessages(natsMessages, this);
  }

  private monitorStatus(messages: NatsConsumerMessages): void {
    const errorHandler = this.config.errorHandler!;
    (async () => {
      for await (const status of messages.status()) {
        const terminalTypes = [
          "heartbeats_missed",
          "consumer_not_found",
          "stream_not_found",
          "consumer_deleted",
        ];
        if (terminalTypes.includes(status.type)) {
          errorHandler(this, new Error(`Consumer status: ${status.type}`));
        }
      }
    })().catch(() => {
      // Status iterator ended — normal during shutdown
    });
  }

  private startAutoCommit(): void {
    const intervalMs =
      this.config.autoCommitIntervalMs ?? DEFAULT_AUTO_COMMIT_INTERVAL_MS;
    this.autoCommitTimer = setInterval(() => {
      if (this._lastDeliveredSeq > this._lastCommittedSeq) {
        this.commitSync().catch(() => {
          // Auto-commit failures are silently ignored
          // (consistent with Kafka consumer behavior)
        });
      }
    }, intervalMs);
  }

  async commitSync(): Promise<void> {
    if (this._closed) {
      throw new Error("offset consumer: consumer is closed");
    }

    const seq = this._lastDeliveredSeq;
    if (seq === 0) {
      return; // nothing to commit
    }

    const data = encodeOffset(seq, new Date().toISOString());
    const newRev = await casUpdate(
      this.kv,
      this.kvKey,
      data,
      this._lastRevision,
    );

    this._lastRevision = newRev;
    this._lastCommittedSeq = seq;
  }

  async commitAsync(): Promise<void> {
    if (this._closed) {
      throw new Error("offset consumer: consumer is closed");
    }
    if (this.asyncCommitInFlight) {
      throw new AsyncCommitInFlightError();
    }

    this.asyncCommitInFlight = true;
    try {
      await this.commitSync();
    } finally {
      this.asyncCommitInFlight = false;
    }
  }

  async seek(seq: number): Promise<void> {
    if (this._closed) {
      throw new Error("offset consumer: consumer is closed");
    }

    // Stop current consumption if active
    if (this.currentMessages) {
      this.currentMessages.stop();
      this.currentMessages = null;
    }

    // Stop auto-commit timer
    this.stopAutoCommit();

    // Persist new offset via CAS
    const data = encodeOffset(seq, new Date().toISOString());
    const newRev = await casUpdate(
      this.kv,
      this.kvKey,
      data,
      this._lastRevision,
    );

    // Update internal state
    this._lastRevision = newRev;
    this._startSeq = seq;
    this._lastDeliveredSeq = seq;
    this._lastCommittedSeq = seq;
  }

  async seekToBeginning(): Promise<void> {
    return this.seek(0);
  }

  async seekToEnd(): Promise<void> {
    // Get current stream info for the latest sequence
    const jsm = await jetstreamManager(this.nc);
    const streamInfo = await jsm.streams.info(this.config.stream);
    return this.seek(streamInfo.state.last_seq);
  }

  async deleteConsumer(): Promise<void> {
    await this.kv.delete(this.kvKey);
    this._lastRevision = 0;
  }

  async close(): Promise<void> {
    if (this._closed) {
      return;
    }
    this._closed = true;

    // Stop auto-commit timer
    this.stopAutoCommit();

    // Stop current consumption
    if (this.currentMessages) {
      this.currentMessages.stop();
      this.currentMessages = null;
    }

    // Final commit if autoCommit is enabled and there are uncommitted messages
    if (
      this.config.autoCommit &&
      this._lastDeliveredSeq > this._lastCommittedSeq
    ) {
      // Temporarily un-close for the final commit
      this._closed = false;
      try {
        await this.commitSync();
      } finally {
        this._closed = true;
      }
    }
  }

  private stopAutoCommit(): void {
    if (this.autoCommitTimer) {
      clearInterval(this.autoCommitTimer);
      this.autoCommitTimer = null;
    }
  }
}
