import type { NatsConnection } from "@nats-io/nats-core";
import { nanos, nuid } from "@nats-io/nats-core";
import {
  jetstream,
  jetstreamManager,
  DeliverPolicy,
  type Consumer,
  type ConsumerMessages as NatsConsumerMessages,
  type JetStreamClient,
  type JetStreamManager,
  type JsMsg,
  type OrderedConsumerOptions,
} from "@nats-io/jetstream";
import { Kvm, type KV } from "@nats-io/kv";
import type {
  ConsumerConfig,
  ConsumerMessages,
  OffsetConsumer,
  OffsetConsumerInfo,
} from "./types.js";
import { AlreadyInUseError, InvalidConsumerNameError } from "./errors.js";
import {
  BUCKET_NAME,
  ClaimConflictError,
  claimEntry,
  emptyEntry,
  encodeEntry,
  ensureBucket,
  entryToInfo,
  isStreamNotFound,
  readEntry,
  type OffsetEntry,
} from "./kv.js";

const DEFAULT_AUTO_COMMIT_INTERVAL_MS = 5000;
const DEFAULT_INACTIVE_THRESHOLD_MS = 30_000;

/** Internal knobs (not part of the public API); tests shorten the close wait. */
export const internals = {
  /** Bound on how long close() waits for pending async commits. */
  closeTimeoutMs: 5000,
};

/**
 * Creates a new offset consumer for the given stream and consumer name.
 * Ensures the offset-consumers KV bucket exists, reads any stored offset,
 * resolves the start sequence and claims ownership of the offset entry.
 * Throws AlreadyInUseError if another live instance owns it.
 * Does NOT start consuming — call consume() for that.
 */
export async function createConsumer(
  nc: NatsConnection,
  config: ConsumerConfig,
): Promise<OffsetConsumer> {
  if (!config.consumerName || config.consumerName.includes(".")) {
    throw new InvalidConsumerNameError(config.consumerName);
  }
  if (!config.stream) {
    throw new Error("offset consumer: stream name is required");
  }

  const js = jetstream(nc);
  const jsm = await jetstreamManager(nc);
  const streamInfo = await jsm.streams.info(config.stream);
  const kv = await ensureBucket(nc, streamInfo.config.num_replicas);

  const kvKey = `${config.stream}.${config.consumerName}`;
  const owner = `oc-${config.stream}-${config.consumerName}-${nuid.next()}`;
  const host = await getHostname();
  const graceMs = config.inactiveThresholdMs ?? DEFAULT_INACTIVE_THRESHOLD_MS;

  // Read the stored entry and check that nobody else owns it.
  const stored = await readEntry(kv, kvKey);
  const entry = stored?.entry ?? emptyEntry();
  const revision = stored?.revision ?? 0;
  if (entry.owner) {
    await checkOwnership(jsm, config.stream, entry, graceMs);
  }

  // Claim ownership with a single CAS write that keeps the stored offset.
  const claim: OffsetEntry = {
    seq: entry.seq,
    committed_at: entry.committed_at,
    owner,
    host,
    claimed_at: new Date().toISOString(),
  };
  try {
    await claimEntry(kv, kvKey, claim, revision);
  } catch (err) {
    if (err instanceof ClaimConflictError) {
      const current = (await readEntry(kv, kvKey))?.entry ?? entry;
      throw new AlreadyInUseError(current.owner, current.host, current.claimed_at);
    }
    throw err;
  }

  return new OffsetConsumerImpl(nc, js, jsm, kv, kvKey, config, claim);
}

/**
 * Throws AlreadyInUseError if the entry's owner is still alive: either its
 * claim is younger than the grace window, or a consumer with its prefix still
 * exists on the stream.
 */
async function checkOwnership(
  jsm: JetStreamManager,
  stream: string,
  entry: OffsetEntry,
  graceMs: number,
): Promise<void> {
  const inUse = new AlreadyInUseError(entry.owner, entry.host, entry.claimed_at);
  const claimed = Date.parse(entry.claimed_at);
  if (!Number.isNaN(claimed) && Date.now() - claimed < graceMs) {
    throw inUse;
  }
  for await (const ci of jsm.consumers.list(stream)) {
    if (ci.name.startsWith(entry.owner)) {
      throw inUse;
    }
  }
}

async function getHostname(): Promise<string> {
  try {
    // Dynamic, untyped import so the library stays usable outside Node.
    const moduleName = "node:os";
    const os = (await import(moduleName)) as { hostname: () => string };
    return os.hostname();
  } catch {
    return "";
  }
}

/**
 * Lists all offset consumers for the given stream by scanning KV keys with the stream name prefix.
 * Returns a Map of consumer name to OffsetConsumerInfo, empty if none exist.
 */
export async function listConsumers(
  nc: NatsConnection,
  stream: string,
): Promise<Map<string, OffsetConsumerInfo>> {
  const result = new Map<string, OffsetConsumerInfo>();
  const kvm = new Kvm(nc);

  let kv: KV;
  try {
    kv = await kvm.open(BUCKET_NAME);
  } catch (err) {
    if (isStreamNotFound(err)) {
      return result; // bucket does not exist: no consumers
    }
    throw err;
  }

  const prefix = `${stream}.`;
  const keys = await kv.keys(`${stream}.*`);
  for await (const key of keys) {
    const stored = await readEntry(kv, key);
    if (stored) {
      result.set(key.slice(prefix.length), entryToInfo(stored.entry));
    }
  }
  return result;
}

/** One (re)start of consumption: the underlying ordered consumer and its messages. */
interface Source {
  gen: number;
  consumer: Consumer;
  messages: NatsConsumerMessages;
}

/**
 * Wraps the consumer's message sources so that the iterable handed to the
 * application keeps yielding across seek() restarts and ends on stop()/close().
 */
function wrapMessages(impl: OffsetConsumerImpl): ConsumerMessages {
  return {
    stop() {
      void impl.stopConsumption();
    },
    async *[Symbol.asyncIterator](): AsyncIterator<JsMsg> {
      let gen = 0;
      for (;;) {
        const source = await impl.nextSource(gen);
        if (!source) {
          return;
        }
        gen = source.gen;
        for await (const msg of source.messages) {
          if (!impl.isCurrentSource(gen)) {
            // The source was stopped (seek/stop/close): drop what it still
            // had buffered and move on to the next source, if any.
            break;
          }
          impl.trackDelivered(msg.seq);
          yield msg;
        }
      }
    },
  };
}

class OffsetConsumerImpl implements OffsetConsumer {
  private readonly nc: NatsConnection;
  private readonly js: JetStreamClient;
  private readonly jsm: JetStreamManager;
  private readonly kv: KV;
  private readonly kvKey: string;
  private readonly config: ConsumerConfig;
  private readonly owner: string;
  private readonly host: string;
  private readonly claimedAt: string;

  private _startSeq: number;
  private _lastDeliveredSeq: number;
  private _lastCommittedSeq: number;
  private _lastCommittedAt: string;
  // Highest sequence handed to a commit call; the release write persists at least this.
  private _lastRequestedSeq: number;
  private _lastRequestedAt: string;
  // Set by close() once the bounded wait expired: queued writes are skipped.
  private abandonPending = false;

  private generation = 0;
  private consuming = false;
  private source: Source | null = null;
  private sourceWaiters: Array<() => void> = [];
  private consumeOpts: { signal?: AbortSignal } | undefined;

  // Tail of the KV write pipeline; writes are chained to keep program order.
  private writeChain: Promise<void> = Promise.resolve();
  // Bumped by seek so that acks of older commits leave the position alone.
  private commitEpoch = 0;

  private _closed = false;
  private kvDeleted = false;
  private autoCommitTimer: ReturnType<typeof setInterval> | null = null;

  constructor(
    nc: NatsConnection,
    js: JetStreamClient,
    jsm: JetStreamManager,
    kv: KV,
    kvKey: string,
    config: ConsumerConfig,
    claimed: OffsetEntry,
  ) {
    this.nc = nc;
    this.js = js;
    this.jsm = jsm;
    this.kv = kv;
    this.kvKey = kvKey;
    this.config = config;
    this.owner = claimed.owner;
    this.host = claimed.host;
    this.claimedAt = claimed.claimed_at;
    this._startSeq = claimed.seq;
    this._lastDeliveredSeq = claimed.seq;
    this._lastCommittedSeq = claimed.seq;
    this._lastCommittedAt = claimed.committed_at;
    this._lastRequestedSeq = claimed.seq;
    this._lastRequestedAt = claimed.committed_at;
  }

  private inactiveThresholdMs(): number {
    return this.config.inactiveThresholdMs ?? DEFAULT_INACTIVE_THRESHOLD_MS;
  }

  private entryFor(seq: number, committedAt: string): OffsetEntry {
    return {
      seq,
      committed_at: committedAt,
      owner: this.owner,
      host: this.host,
      claimed_at: this.claimedAt,
    };
  }

  /** Whether the source of generation gen is still the active one. */
  isCurrentSource(gen: number): boolean {
    return this.source !== null && this.source.gen === gen;
  }

  /** Called by the wrapper to track the last delivered sequence. */
  trackDelivered(seq: number): void {
    this._lastDeliveredSeq = seq;
  }

  /**
   * Resolves with the next message source after generation afterGen, or null
   * once consumption has stopped for good.
   */
  async nextSource(afterGen: number): Promise<Source | null> {
    for (;;) {
      if (this.source && this.source.gen !== afterGen) {
        return this.source;
      }
      if (!this.consuming) {
        return null;
      }
      await new Promise<void>((resolve) => this.sourceWaiters.push(resolve));
    }
  }

  private notifySourceChange(): void {
    const waiters = this.sourceWaiters;
    this.sourceWaiters = [];
    for (const resolve of waiters) {
      resolve();
    }
  }

  async consume(opts?: { signal?: AbortSignal }): Promise<ConsumerMessages> {
    if (this._closed) {
      throw new Error("offset consumer: consumer is closed");
    }
    if (this.consuming) {
      throw new Error("offset consumer: already consuming");
    }
    this.consuming = true;
    this.consumeOpts = opts;
    try {
      await this.startSource();
    } catch (err) {
      this.consuming = false;
      throw err;
    }

    if (opts?.signal) {
      if (opts.signal.aborted) {
        void this.stopConsumption();
      } else {
        opts.signal.addEventListener("abort", () => void this.stopConsumption(), { once: true });
      }
    }

    if (this.config.autoCommit && !this.autoCommitTimer) {
      this.startAutoCommit();
    }

    return wrapMessages(this);
  }

  /**
   * Creates an ordered consumer under a new generation prefix and starts
   * delivering messages from it.
   */
  private async startSource(): Promise<void> {
    const gen = ++this.generation;
    const orderOpts: Partial<OrderedConsumerOptions> = {
      name_prefix: `${this.owner}-g${gen}`,
      inactive_threshold: this.inactiveThresholdMs(),
    };

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

    const consumer = await this.js.consumers.get(this.config.stream, orderOpts);
    let messages: NatsConsumerMessages;
    try {
      messages = await consumer.consume();
    } catch (err) {
      await consumer.delete().catch(() => {});
      throw err;
    }

    this.source = { gen, consumer, messages };
    this.monitorStatus(messages);
    this.notifySourceChange();
  }

  /**
   * Stops the current source and deletes its ordered consumer from the server
   * so that ownership is visibly released. Best effort.
   */
  private async stopSource(): Promise<void> {
    const source = this.source;
    this.source = null;
    this.notifySourceChange();
    if (!source) {
      return;
    }
    source.messages.stop();
    try {
      await source.consumer.delete();
    } catch {
      // already gone, or connection down
    }
  }

  /** User-initiated stop (messages.stop() or abort signal). */
  async stopConsumption(): Promise<void> {
    if (!this.consuming) {
      return;
    }
    this.consuming = false;
    await this.stopSource();
  }

  private monitorStatus(messages: NatsConsumerMessages): void {
    const thresholdNanos = nanos(this.inactiveThresholdMs());
    (async () => {
      for await (const status of messages.status()) {
        if (status.type === "ordered_consumer_recreated") {
          // The JS client recreates ordered consumers with a 5 minute inactive
          // threshold; re-apply ours so a crashed owner is released promptly.
          this.jsm.consumers
            .update(this.config.stream, status.name, { inactive_threshold: thresholdNanos })
            .catch(() => {});
          continue;
        }
        const terminalTypes = [
          "heartbeats_missed",
          "consumer_not_found",
          "stream_not_found",
          "consumer_deleted",
        ];
        if (terminalTypes.includes(status.type) && this.config.errorHandler && this.source?.messages === messages) {
          this.config.errorHandler(this, new Error(`Consumer status: ${status.type}`));
        }
      }
    })().catch(() => {
      // Status iterator ended — normal during shutdown
    });
  }

  private startAutoCommit(): void {
    const intervalMs = this.config.autoCommitIntervalMs ?? DEFAULT_AUTO_COMMIT_INTERVAL_MS;
    this.autoCommitTimer = setInterval(() => {
      if (this._closed || this._lastDeliveredSeq <= this._lastCommittedSeq) {
        return;
      }
      this.commitSync().catch((err) => {
        if (!this._closed && this.config.errorHandler) {
          this.config.errorHandler(this, err instanceof Error ? err : new Error(String(err)));
        }
      });
    }, intervalMs);
  }

  private stopAutoCommit(): void {
    if (this.autoCommitTimer) {
      clearInterval(this.autoCommitTimer);
      this.autoCommitTimer = null;
    }
  }

  /**
   * Appends a KV write to the pipeline and returns a promise for its completion.
   */
  private enqueueWrite(op: () => Promise<void>): Promise<void> {
    const prev = this.writeChain;
    const current = (async () => {
      try {
        await prev;
      } catch {
        // the previous write's error is reported to its own caller
      }
      if (this.abandonPending) {
        return; // close() gave up waiting: the release write persists the position
      }
      await op();
    })();
    this.writeChain = current;
    return current;
  }

  async commitSync(): Promise<void> {
    await this.commitAsync();
  }

  commitAsync(): Promise<void> {
    if (this._closed) {
      return Promise.reject(new Error("offset consumer: consumer is closed"));
    }
    const seq = this._lastDeliveredSeq; // captured at call time
    if (seq === 0) {
      return Promise.resolve();
    }
    const epoch = this.commitEpoch;
    const at = new Date().toISOString();
    if (seq > this._lastRequestedSeq) {
      this._lastRequestedSeq = seq;
      this._lastRequestedAt = at;
    }
    const data = encodeEntry(this.entryFor(seq, at));
    return this.enqueueWrite(async () => {
      await this.kv.put(this.kvKey, data);
      if (epoch === this.commitEpoch && seq > this._lastCommittedSeq) {
        this._lastCommittedSeq = seq;
        this._lastCommittedAt = at;
      }
    });
  }

  async info(): Promise<OffsetConsumerInfo | null> {
    const stored = await readEntry(this.kv, this.kvKey);
    return stored ? entryToInfo(stored.entry) : null;
  }

  async seek(seq: number): Promise<void> {
    if (this._closed) {
      throw new Error("offset consumer: consumer is closed");
    }

    // Stop delivery; consuming stays true so the application's iterable waits
    // for the restarted source.
    await this.stopSource();

    // Move the in-memory position first: any commit queued from now on
    // commits the seek position, and acks of older commits are ignored.
    const previous = {
      startSeq: this._startSeq,
      delivered: this._lastDeliveredSeq,
      committed: this._lastCommittedSeq,
      committedAt: this._lastCommittedAt,
    };
    const at = new Date().toISOString();
    this.commitEpoch++;
    this._startSeq = seq;
    this._lastDeliveredSeq = seq;
    this._lastCommittedSeq = seq;
    this._lastCommittedAt = at;
    this._lastRequestedSeq = seq;
    this._lastRequestedAt = at;

    const data = encodeEntry(this.entryFor(seq, at));
    try {
      await this.enqueueWrite(() => this.kv.put(this.kvKey, data).then(() => {}));
    } catch (err) {
      // Consumer is left stopped; restore the previous position.
      this._startSeq = previous.startSeq;
      this._lastDeliveredSeq = previous.delivered;
      this._lastCommittedSeq = previous.committed;
      this._lastCommittedAt = previous.committedAt;
      this.consuming = false;
      this.notifySourceChange();
      throw err;
    }

    if (this.consuming && !this._closed) {
      try {
        await this.startSource();
      } catch (err) {
        this.consuming = false;
        this.notifySourceChange();
        throw err;
      }
    }
  }

  async seekToBeginning(): Promise<void> {
    return this.seek(0);
  }

  async seekToEnd(): Promise<void> {
    const streamInfo = await this.jsm.streams.info(this.config.stream);
    return this.seek(streamInfo.state.last_seq);
  }

  async deleteConsumer(): Promise<void> {
    await this.enqueueWrite(() => this.kv.delete(this.kvKey));
    this.kvDeleted = true;
  }

  async close(): Promise<void> {
    if (this._closed) {
      return;
    }
    this._closed = true;
    this.stopAutoCommit();

    // Drain pending writes, bounded. Writes still queued after the wait are
    // abandoned; the release write below persists the highest requested
    // position anyway.
    const chain = this.writeChain.then(
      () => true,
      () => true,
    );
    let timer: ReturnType<typeof setTimeout> | undefined;
    const expired = new Promise<boolean>((resolve) => {
      timer = setTimeout(() => resolve(false), internals.closeTimeoutMs);
    });
    const drained = await Promise.race([chain, expired]);
    clearTimeout(timer);
    if (!drained) {
      this.abandonPending = true;
      await chain;
    }

    // Stop delivery and delete the ordered consumer.
    this.consuming = false;
    await this.stopSource();

    // Release ownership with one final write. It persists the highest
    // position handed to a commit call (acknowledged or not) and, with
    // autoCommit, any uncommitted messages.
    let seq = this._lastCommittedSeq;
    let at = this._lastCommittedAt;
    if (this._lastRequestedSeq > seq) {
      seq = this._lastRequestedSeq;
      at = this._lastRequestedAt;
    }
    if (this.config.autoCommit && this._lastDeliveredSeq > seq) {
      seq = this._lastDeliveredSeq;
      at = new Date().toISOString();
    }
    if (this.kvDeleted) {
      return;
    }
    await this.kv.put(this.kvKey, encodeEntry({ seq, committed_at: at, owner: "", host: "", claimed_at: "" }));
    if (seq > this._lastCommittedSeq) {
      this._lastCommittedSeq = seq;
      this._lastCommittedAt = at;
    }
  }
}
