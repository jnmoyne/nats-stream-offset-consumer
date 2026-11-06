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
import { AlreadyInUseError, ConsumerClosedError, StreamRecreatedError } from "./errors.js";
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
  streamCreatedId,
  streamWasRecreated,
  validateNames,
  type OffsetEntry,
} from "./kv.js";

const DEFAULT_AUTO_COMMIT_INTERVAL_MS = 5000;
const DEFAULT_INACTIVE_THRESHOLD_MS = 30_000;

/**
 * Bounds how often close() re-reads the entry when its conditional release
 * write loses a race.
 */
const RELEASE_ATTEMPTS = 3;

/** How often a missing stream is polled after an ordered consumer error. */
const STREAM_WATCH_INTERVAL_MS = 1000;

/** Ordered consumer statuses after which the stream may have been recreated. */
const WATCH_STATUSES = new Set([
  "ordered_consumer_recreated",
  "consumer_deleted",
  "consumer_not_found",
  "stream_not_found",
  "heartbeats_missed",
]);

/** Ordered consumer statuses reported to the errorHandler. */
const REPORTED_STATUSES = new Set([
  "heartbeats_missed",
  "consumer_not_found",
  "stream_not_found",
  "consumer_deleted",
]);

/**
 * Bounds how often a claim is retried after losing a CAS race to a write that
 * did not change the owner (a release, or this instance's own late commit).
 */
const CLAIM_ROUNDS = 3;

/** Internal knobs (not part of the public API); tests shorten them. */
export const internals = {
  /** Bound on how long close() waits for pending async commits. */
  closeTimeoutMs: 5000,
  /**
   * How long a starter waits, measured with its own clock, before taking
   * over an entry whose owner has no consumer on the server. It must exceed
   * the time a live owner can be without one: an ordered consumer reset
   * deletes the old consumer before creating the new one, retrying the
   * creation if it fails. No jitter is needed: concurrent starters are
   * decided by the claim's CAS.
   */
  livenessWaitMs: 5000,
};

function nowISO(): string {
  return new Date().toISOString();
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function toError(err: unknown): Error {
  return err instanceof Error ? err : new Error(String(err));
}

/**
 * Returns the position to start from when there is no usable stored offset:
 * 0 (deliver all) for "beginning", the stream's last sequence for "end".
 */
function initialPosition(config: ConsumerConfig, lastSeq: number): { seq: number; at: string } {
  if (config.startPosition === "end" && lastSeq > 0) {
    return { seq: lastSeq, at: nowISO() };
  }
  return { seq: 0, at: "" };
}

/**
 * Creates a new offset consumer for the given stream and consumer name.
 * Ensures the offset-consumers KV bucket exists and checks the stored offset
 * entry; it writes nothing. Throws AlreadyInUseError if the entry's owner is
 * visibly alive (a consumer whose name starts with its owner prefix exists on
 * the stream). Ownership is claimed when consumption starts (see consume()).
 * startPosition "end" is resolved to the stream's last sequence here; the
 * claim stores it if there is no usable stored offset by then.
 * Does NOT start consuming — call consume() for that.
 */
export async function createConsumer(
  nc: NatsConnection,
  config: ConsumerConfig,
): Promise<OffsetConsumer> {
  validateNames(config.stream, config.consumerName);

  const js = jetstream(nc);
  const jsm = await jetstreamManager(nc);
  const streamInfo = await jsm.streams.info(config.stream);
  const kv = await ensureBucket(nc, jsm, streamInfo.config.num_replicas);

  const kvKey = `${config.stream}.${config.consumerName}`;
  const owner = `oc-${config.stream}-${config.consumerName}-${nuid.next()}`;
  const host = await getHostname();

  // Fail fast if the entry is visibly owned. This check does not wait: the
  // full check runs when consumption starts.
  const stored = await readEntry(kv, kvKey);
  if (stored && stored.entry.owner && (await isOwnerAlive(jsm, config.stream, stored.entry.owner))) {
    throw new AlreadyInUseError(stored.entry.owner, stored.entry.host);
  }

  return new OffsetConsumerImpl(nc, js, jsm, kv, kvKey, config, {
    owner,
    host,
    streamCreated: streamCreatedId(streamInfo.created),
    initial: initialPosition(config, streamInfo.state.last_seq),
  });
}

/** Whether the stream has a consumer whose name starts with owner. */
async function isOwnerAlive(jsm: JetStreamManager, stream: string, owner: string): Promise<boolean> {
  let alive = false;
  for await (const ci of jsm.consumers.list(stream)) {
    if (ci.name.startsWith(owner)) {
      alive = true;
    }
  }
  return alive;
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
 * A yielded message counts as processed when the application asks for the
 * next one.
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
          if (!impl.isActiveSource(gen)) {
            // The source was stopped (seek/stop/close): drop what it still
            // had buffered and move on to the next source, if any.
            break;
          }
          // A message from another server-side consumer means the client
          // reset the ordered consumer: check the stream before delivering.
          if (impl.consumerChanged(gen, msg) && !(await impl.checkAfterReset(gen))) {
            continue; // dropped: the stream was recreated, the consumer repositions
          }
          if (!impl.trackDelivered(gen, msg.seq)) {
            continue; // dropped: stopped, replaced or being repositioned
          }
          yield msg;
          // The application asked for the next message: this one is processed.
          impl.trackProcessed(gen, msg.seq);
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
  // Identifies the stream incarnation (see OffsetConsumerInfo.streamCreated).
  // Updated when a recreation is detected.
  private streamCreated: string;
  // startPosition resolved at construction, used by the claim when there is
  // no usable stored offset.
  private initialSeq: number;
  private initialAt: string;
  // Set once this instance has claimed the entry (at its first consume()).
  // Before that, commits and close() write nothing.
  private claimed = false;
  // Set by a seek while not consuming: the claim at the next consume()
  // stores that position instead of the stored offset.
  private pendingSeek = false;

  private _startSeq = 0;
  private _lastDeliveredSeq = 0;
  // Last sequence the application has moved past. autoCommit and the final
  // commit in close() use it, so a message still being handled is never
  // committed implicitly.
  private _lastProcessedSeq = 0;
  private _lastCommittedSeq = 0;
  private _lastCommittedAt = "";
  // Highest sequence handed to a commit call; the release write persists at least this.
  private _lastRequestedSeq = 0;
  private _lastRequestedAt = "";
  // Set by close() once the bounded wait expired: queued writes are skipped.
  private abandonPending = false;

  private generation = 0;
  private consuming = false;
  private source: Source | null = null;
  private sourceWaiters: Array<() => void> = [];
  // Server-side name of the ordered consumer that delivered the last message;
  // a change means the client reset it.
  private lastConsumerName = "";
  // Set while a detected stream recreation is handled; deliveries are dropped.
  private repositioning = false;
  // Set while a task polls the stream for a recreation.
  private watching = false;

  // Lifecycle lock: serializes consume, seek, stop and close so that at most
  // one ordered consumer is ever active. FIFO; an operation started while the
  // lock is free runs synchronously up to its first await.
  private lifecycleBusy = false;
  private lifecycleWaiters: Array<() => void> = [];

  // Tail of the KV write pipeline; writes are chained to keep program order.
  private writeChain: Promise<void> = Promise.resolve();
  // Bumped by seek so that acks of older commits leave the position alone.
  private commitEpoch = 0;
  // Set while a seek's or claim's write is queued: commits submitted meanwhile
  // resolve their position after it (the new position, or the old one if it
  // failed).
  private positionWritePending = false;

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
    identity: { owner: string; host: string; streamCreated: string; initial: { seq: number; at: string } },
  ) {
    this.nc = nc;
    this.js = js;
    this.jsm = jsm;
    this.kv = kv;
    this.kvKey = kvKey;
    this.config = config;
    this.owner = identity.owner;
    this.host = identity.host;
    this.streamCreated = identity.streamCreated;
    this.initialSeq = identity.initial.seq;
    this.initialAt = identity.initial.at;
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
      stream_created: this.streamCreated,
    };
  }

  /** Passes err to the configured errorHandler, if any. */
  reportError(err: Error): void {
    try {
      this.config.errorHandler?.(this, err);
    } catch {
      // an exception thrown by the application's handler is not ours to handle
    }
  }

  /** Runs fn holding the lifecycle lock, after the operations queued before it. */
  private async withLifecycle<T>(fn: () => Promise<T>): Promise<T> {
    if (this.lifecycleBusy) {
      await new Promise<void>((resolve) => this.lifecycleWaiters.push(resolve));
    } else {
      this.lifecycleBusy = true;
    }
    try {
      return await fn();
    } finally {
      const next = this.lifecycleWaiters.shift();
      if (next) {
        next(); // hand the lock over
      } else {
        this.lifecycleBusy = false;
      }
    }
  }

  /** Whether the source of generation gen is still the active one. */
  isActiveSource(gen: number): boolean {
    return this.source !== null && this.source.gen === gen;
  }

  /** Whether generation gen is the active consumption and not being repositioned. */
  private isCurrent(gen: number): boolean {
    return this.isActiveSource(gen) && !this.repositioning;
  }

  /**
   * Records the server-side consumer name of msg and reports whether it
   * differs from the previous message's (the client reset the ordered consumer).
   */
  consumerChanged(gen: number, msg: JsMsg): boolean {
    if (!this.isCurrent(gen)) {
      return false;
    }
    const name = msg.info.consumer;
    const reset = this.lastConsumerName !== "" && name !== this.lastConsumerName;
    this.lastConsumerName = name;
    return reset;
  }

  /**
   * Called after an ordered consumer reset, before delivering. If the stream
   * was recreated (the client silently resumed at the old position on the new
   * stream), starts repositioning and returns false: the message is dropped.
   */
  async checkAfterReset(gen: number): Promise<boolean> {
    const { created, changed } = await this.streamChanged();
    if (!changed || !this.isCurrent(gen)) {
      return true; // trackDelivered drops it if gen is no longer current
    }
    this.repositioning = true;
    void this.repositionAfterRecreate(created);
    return false;
  }

  /**
   * Records seq as the last delivered sequence, or returns false if the
   * message must be dropped: it comes from a stopped or replaced generation,
   * or the consumer is being repositioned.
   */
  trackDelivered(gen: number, seq: number): boolean {
    if (!this.isCurrent(gen)) {
      return false;
    }
    this._lastDeliveredSeq = seq;
    return true;
  }

  /** Called by the wrapper when the application asks for the message after seq. */
  trackProcessed(gen: number, seq: number): void {
    if (this.isCurrent(gen) && seq > this._lastProcessedSeq) {
      this._lastProcessedSeq = seq;
    }
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

  consume(opts?: { signal?: AbortSignal }): Promise<ConsumerMessages> {
    return this.withLifecycle(async () => {
      if (this._closed) {
        throw new ConsumerClosedError();
      }
      if (this.consuming) {
        throw new Error("offset consumer: already consuming");
      }
      // Claim (or re-validate) ownership every time consumption starts from
      // a stopped state: while not consuming, the instance has no consumer
      // on the server and another instance may have taken the entry over.
      await this.claim();
      this.consuming = true;
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
    });
  }

  /**
   * Takes ownership of the offset entry before consumption starts, and stores
   * the position consumption starts from: the pending seek position if seek()
   * was called while not consuming, else the stored offset, else the position
   * resolved from startPosition at construction (also when the stored offset
   * belongs to an earlier incarnation of the stream).
   *
   * The stored owner, if it is another instance, is alive while a consumer
   * whose name starts with its owner prefix exists. If none exists, the claim
   * waits livenessWaitMs (measured locally) and lists again,
   * which rides out an ordered consumer reset; it then writes with a CAS
   * against the revision read before the wait, so any write during the wait
   * (a commit by the owner, another instance's claim) makes it fail. No
   * clocks are compared. Must be called within the lifecycle lock.
   */
  private async claim(): Promise<void> {
    for (let round = 1; ; round++) {
      const stored = await readEntry(this.kv, this.kvKey);
      if (stored && stored.entry.owner !== "" && stored.entry.owner !== this.owner) {
        await this.checkOwnership(stored.entry);
      }

      let lastSeq: number;
      let currentCreated: string;
      try {
        const info = await this.jsm.streams.info(this.config.stream);
        lastSeq = info.state.last_seq;
        currentCreated = streamCreatedId(info.created);
      } catch (err) {
        const e = toError(err);
        throw new Error(`offset consumer: failed to get stream info: ${e.message}`, { cause: e });
      }
      if (currentCreated !== "" && currentCreated !== this.streamCreated) {
        // The stream was recreated since construction: the start position
        // resolved then refers to the old stream.
        this.streamCreated = currentCreated;
        ({ seq: this.initialSeq, at: this.initialAt } = initialPosition(this.config, lastSeq));
      }
      const recreated = stored !== null && streamWasRecreated(stored.entry, this.streamCreated, lastSeq);

      let seq: number;
      let committedAt: string;
      if (this.pendingSeek) {
        seq = this._startSeq;
        committedAt = this._lastCommittedAt;
      } else if (stored === null || recreated) {
        seq = this.initialSeq;
        committedAt = this.initialAt;
      } else {
        seq = stored.entry.seq;
        committedAt = stored.entry.committed_at;
      }
      const entry: OffsetEntry = {
        seq,
        committed_at: committedAt,
        owner: this.owner,
        host: this.host,
        stream_created: this.streamCreated,
      };

      // In order with the commits (this instance's own, after a stop).
      let conflict: ClaimConflictError | null = null;
      this.positionWritePending = true;
      await this.enqueueWrite(async () => {
        try {
          await claimEntry(this.kv, this.kvKey, entry, stored?.revision ?? 0);
        } catch (err) {
          if (err instanceof ClaimConflictError) {
            conflict = err;
            return;
          }
          throw err;
        } finally {
          this.positionWritePending = false;
        }
        this.claimed = true;
        this.pendingSeek = false;
        this.kvDeleted = false;
        this.commitEpoch++;
        this._startSeq = seq;
        this._lastDeliveredSeq = seq;
        this._lastProcessedSeq = seq;
        this._lastCommittedSeq = seq;
        this._lastCommittedAt = committedAt;
        this._lastRequestedSeq = seq;
        this._lastRequestedAt = committedAt;
      });
      if (conflict === null) {
        if (recreated) {
          this.reportError(new StreamRecreatedError(stored!.entry.seq));
        }
        return;
      }
      // Someone wrote the entry meanwhile. Another owner just proved it is
      // alive; a release (empty owner) or this instance's own late commit is
      // retried.
      const current = await readEntry(this.kv, this.kvKey);
      if (current && current.entry.owner !== "" && current.entry.owner !== this.owner) {
        throw new AlreadyInUseError(current.entry.owner, current.entry.host);
      }
      if (round === CLAIM_ROUNDS) {
        const e = conflict as ClaimConflictError;
        throw new Error(`offset consumer: failed to claim offset entry: ${e.message}`, { cause: e });
      }
    }
  }

  /**
   * Throws AlreadyInUseError if the entry's owner (another instance) is
   * alive: a consumer whose name starts with its owner prefix exists on the
   * stream, now or after the liveness wait.
   */
  private async checkOwnership(entry: OffsetEntry): Promise<void> {
    for (let attempt = 0; attempt < 2; attempt++) {
      if (attempt > 0) {
        await sleep(internals.livenessWaitMs);
      }
      if (await isOwnerAlive(this.jsm, this.config.stream, entry.owner)) {
        throw new AlreadyInUseError(entry.owner, entry.host);
      }
    }
  }

  /**
   * Creates an ordered consumer under a new generation prefix and starts
   * delivering messages from it. Must be called within the lifecycle lock.
   */
  private async startSource(): Promise<void> {
    const gen = ++this.generation;
    this.lastConsumerName = "";
    this.repositioning = false;
    const orderOpts: Partial<OrderedConsumerOptions> = {
      name_prefix: `${this.owner}-g${gen}`,
      inactive_threshold: this.inactiveThresholdMs(),
    };

    // The start position is fully resolved by the claim and seek: 0
    // means the beginning of the stream.
    if (this._startSeq > 0) {
      orderOpts.deliver_policy = DeliverPolicy.StartSequence;
      orderOpts.opt_start_seq = this._startSeq + 1;
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
    this.monitorStatus(gen, messages);
    this.notifySourceChange();
  }

  /**
   * Stops the current source and deletes its ordered consumer from the server
   * so that ownership is visibly released. Best effort.
   */
  private async stopSource(): Promise<void> {
    await this.deleteSource(this.detachSource());
  }

  /**
   * Stops delivery from the current source, keeping its server-side consumer
   * (see deleteSource), and returns it.
   */
  private detachSource(): Source | null {
    const source = this.source;
    this.source = null;
    this.notifySourceChange();
    source?.messages.stop();
    return source;
  }

  /** Deletes a detached source's ordered consumer from the server. Best effort. */
  private async deleteSource(source: Source | null): Promise<void> {
    if (!source) {
      return;
    }
    try {
      await source.consumer.delete();
    } catch {
      // already gone, or connection down
    }
  }

  /** User-initiated stop (messages.stop() or abort signal). */
  stopConsumption(): Promise<void> {
    return this.withLifecycle(async () => {
      if (!this.consuming) {
        return;
      }
      this.consuming = false;
      await this.stopSource();
    });
  }

  private monitorStatus(gen: number, messages: NatsConsumerMessages): void {
    const thresholdNanos = nanos(this.inactiveThresholdMs());
    (async () => {
      for await (const status of messages.status()) {
        // The ordered consumer is about to be (or was) recreated. If the
        // stream itself was recreated, the new ordered consumer waits at the
        // old position, possibly forever: watch for that.
        if (WATCH_STATUSES.has(status.type)) {
          this.watchStream(gen);
        }
        if (status.type === "ordered_consumer_recreated") {
          // The JS client recreates ordered consumers with a 5 minute inactive
          // threshold; re-apply ours so a crashed owner is released promptly.
          this.jsm.consumers
            .update(this.config.stream, status.name, { inactive_threshold: thresholdNanos })
            .catch(() => {});
          continue;
        }
        if (REPORTED_STATUSES.has(status.type) && this.source?.messages === messages) {
          this.reportError(new Error(`Consumer status: ${status.type}`));
        }
      }
    })().catch(() => {
      // Status iterator ended — normal during shutdown
    });
  }

  /**
   * Polls the stream after an ordered consumer error until the stream exists
   * again, and repositions if it is a new incarnation. At most one watcher
   * runs at a time; it gives up when generation gen ends.
   */
  private watchStream(gen: number): void {
    if (this.watching || !this.isCurrent(gen)) {
      return;
    }
    this.watching = true;
    void (async () => {
      try {
        while (this.isCurrent(gen) && !this.nc.isClosed()) {
          let created: string;
          try {
            created = streamCreatedId((await this.jsm.streams.info(this.config.stream)).created);
          } catch {
            await sleep(STREAM_WATCH_INTERVAL_MS);
            continue;
          }
          if (this.identityChanged(created) && this.isCurrent(gen)) {
            this.repositioning = true;
            await this.repositionAfterRecreate(created);
          }
          return;
        }
      } finally {
        this.watching = false;
      }
    })();
  }

  private identityChanged(created: string): boolean {
    return created !== "" && this.streamCreated !== "" && created !== this.streamCreated;
  }

  /**
   * Reports whether the stream's incarnation differs from the one the offsets
   * refer to, and returns the current one.
   */
  private async streamChanged(): Promise<{ created: string; changed: boolean }> {
    try {
      const created = streamCreatedId((await this.jsm.streams.info(this.config.stream)).created);
      return { created, changed: this.identityChanged(created) };
    } catch {
      return { created: "", changed: false };
    }
  }

  /**
   * Moves the consumer to startPosition on the new stream and reports
   * StreamRecreatedError. The new identity is applied by the seek write.
   */
  private async repositionAfterRecreate(created: string): Promise<void> {
    this.reportError(new StreamRecreatedError(this._lastDeliveredSeq));

    let lastSeq = 0;
    try {
      lastSeq = (await this.jsm.streams.info(this.config.stream)).state.last_seq;
    } catch {
      // start from the beginning
    }
    const { seq } = initialPosition(this.config, lastSeq);
    try {
      await this.seekTo(seq, created);
    } catch (err) {
      if (!(err instanceof ConsumerClosedError)) {
        const e = toError(err);
        this.reportError(
          new Error(`offset consumer: failed to reposition after stream recreation: ${e.message}`, { cause: e }),
        );
      }
    }
  }

  /**
   * Commits the last processed sequence on a timer, never a message the
   * application is still handling. close() just stops the timer, so the
   * errorHandler may call close().
   */
  private startAutoCommit(): void {
    const intervalMs = this.config.autoCommitIntervalMs ?? DEFAULT_AUTO_COMMIT_INTERVAL_MS;
    this.autoCommitTimer = setInterval(() => {
      if (this._closed || this._lastProcessedSeq <= this._lastCommittedSeq) {
        return;
      }
      this.submitCommit(true).catch((err) => {
        if (!(err instanceof ConsumerClosedError)) {
          const e = toError(err);
          this.reportError(new Error(`offset consumer: auto-commit failed: ${e.message}`, { cause: e }));
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
    return this.submitCommit(false);
  }

  /**
   * Submits a commit of the last delivered sequence or, with processedOnly,
   * of the last processed sequence (autoCommit).
   */
  private submitCommit(processedOnly: boolean): Promise<void> {
    if (this._closed) {
      return Promise.reject(new ConsumerClosedError());
    }
    if (!this.claimed) {
      // Nothing was delivered, and the entry is not ours to write yet.
      return Promise.resolve();
    }
    if (this.positionWritePending) {
      // Resolve the position (and stream identity) once the seek's or the
      // claim's write has settled.
      return this.enqueueWrite(async () => {
        const write = this.prepareCommit(processedOnly);
        if (write) {
          await write();
        }
      });
    }
    const write = this.prepareCommit(processedOnly);
    return write ? this.enqueueWrite(write) : Promise.resolve();
  }

  /**
   * Captures the position to commit and returns the write, or null when
   * there is nothing to commit.
   */
  private prepareCommit(processedOnly: boolean): (() => Promise<void>) | null {
    let seq = this._lastDeliveredSeq;
    if (processedOnly) {
      seq = this._lastProcessedSeq;
      if (seq <= this._lastRequestedSeq) {
        return null; // an explicit commit already covers it
      }
    }
    if (seq === 0) {
      return null;
    }
    const epoch = this.commitEpoch;
    const at = nowISO();
    if (seq > this._lastRequestedSeq) {
      this._lastRequestedSeq = seq;
      this._lastRequestedAt = at;
    }
    const data = encodeEntry(this.entryFor(seq, at));
    return async () => {
      await this.kv.put(this.kvKey, data);
      if (epoch === this.commitEpoch && seq > this._lastCommittedSeq) {
        this._lastCommittedSeq = seq;
        this._lastCommittedAt = at;
      }
    };
  }

  async info(): Promise<OffsetConsumerInfo | null> {
    const stored = await readEntry(this.kv, this.kvKey);
    return stored ? entryToInfo(stored.entry) : null;
  }

  /**
   * While consuming: stops delivery, writes the new offset, starts a new
   * ordered consumer under the next generation prefix, and only then deletes
   * the previous one, so the instance stays visible on the server throughout.
   * The position only changes once the write succeeded; if it fails the
   * consumer is left stopped with its position unchanged. While not
   * consuming, the position is only recorded: the claim at the next
   * consume() stores it.
   */
  seek(seq: number): Promise<void> {
    return this.seekTo(seq, "");
  }

  /**
   * Implements seek. A non-empty streamCreated switches the consumer to a new
   * stream incarnation atomically with the seek write: commits submitted
   * before it carry the old identity, commits after it the new one.
   */
  private seekTo(seq: number, streamCreated: string): Promise<void> {
    return this.withLifecycle(async () => {
      if (this._closed) {
        throw new ConsumerClosedError();
      }
      const at = nowISO();
      if (!this.consuming) {
        // Not consuming: no consumer protects the entry, so do not write it.
        // The claim at the next consume() stores this position.
        this._startSeq = seq;
        this._lastDeliveredSeq = seq;
        this._lastProcessedSeq = seq;
        this._lastCommittedAt = at;
        this.pendingSeek = true;
        if (streamCreated !== "") {
          this.streamCreated = streamCreated;
        }
        return;
      }

      // Stop delivery but keep the server-side consumer until the new one
      // exists (make before break); consuming stays true so the
      // application's iterable waits for the restarted source.
      const old = this.detachSource();
      try {
        // Persist the new offset, in order with the commits. Bumping the
        // epoch makes acks of older commits leave the committed position
        // alone.
        this.commitEpoch++;
        const entry = this.entryFor(seq, at);
        if (streamCreated !== "") {
          entry.stream_created = streamCreated;
        }
        const data = encodeEntry(entry);
        this.positionWritePending = true;
        try {
          await this.enqueueWrite(async () => {
            try {
              await this.kv.put(this.kvKey, data);
            } finally {
              this.positionWritePending = false;
            }
            // Update the position before the next queued write runs, so that
            // any commit submitted after the seek commits the seek position.
            this._startSeq = seq;
            this._lastDeliveredSeq = seq;
            this._lastProcessedSeq = seq;
            this._lastCommittedSeq = seq;
            this._lastCommittedAt = at;
            this._lastRequestedSeq = seq;
            this._lastRequestedAt = at;
            this.kvDeleted = false; // the seek write recreated the entry
            if (streamCreated !== "") {
              this.streamCreated = streamCreated;
            }
          });
        } catch (err) {
          // Consumer is left stopped with its position unchanged.
          this.consuming = false;
          this.notifySourceChange();
          throw err;
        }

        // The lifecycle lock guarantees nothing else started consuming
        // meanwhile; the source check is a second line of defence against a
        // leaked ordered consumer.
        if (this.consuming && !this._closed && this.source === null) {
          try {
            await this.startSource();
          } catch (err) {
            this.consuming = false;
            this.notifySourceChange();
            throw err;
          }
        }
      } finally {
        // The new generation is visible (or the seek failed): delete the old one.
        await this.deleteSource(old);
      }
    });
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

  close(): Promise<void> {
    return this.withLifecycle(async () => {
      if (this._closed) {
        return;
      }
      this._closed = true;
      // The auto-commit timer is stopped, not waited for: a commit it already
      // submitted settles on its own.
      this.stopAutoCommit();

      // Drain pending writes, bounded. Writes still queued after the wait
      // are abandoned; the release write below persists the highest
      // requested position anyway.
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

      // Release ownership with one final conditional write (releaseEntry). It
      // persists the highest position handed to a commit call (acknowledged
      // or not) and, with autoCommit, the messages processed since the last
      // commit. The message the application is handling now is not included.
      let seq = this._lastCommittedSeq;
      let at = this._lastCommittedAt;
      if (this._lastRequestedSeq > seq) {
        seq = this._lastRequestedSeq;
        at = this._lastRequestedAt;
      }
      if (this.config.autoCommit && this._lastProcessedSeq > seq) {
        seq = this._lastProcessedSeq;
        at = nowISO();
      }
      // Before the first claim the entry is not this instance's to write.
      if (this.kvDeleted || !this.claimed) {
        return;
      }
      const release: OffsetEntry = { ...emptyEntry(), seq, committed_at: at, stream_created: this.streamCreated };
      if (!(await this.releaseEntry(release))) {
        return;
      }
      if (seq > this._lastCommittedSeq) {
        this._lastCommittedSeq = seq;
        this._lastCommittedAt = at;
      }
    });
  }

  /**
   * Writes the release only while the entry still names this instance as its
   * owner, with a compare-and-set against the revision just read. An instance
   * that was displaced without noticing (its ordered consumer expired and
   * another instance claimed the entry) therefore cannot clear the new
   * owner's claim, which would let a third instance in without a liveness
   * check. A revision conflict means a write landed between the read and the
   * release, so the entry is read again (at most RELEASE_ATTEMPTS attempts in
   * total). Returns false, without error, when the entry no longer exists
   * (nothing to release); throws AlreadyInUseError when another instance
   * owns it.
   */
  private async releaseEntry(release: OffsetEntry): Promise<boolean> {
    for (let attempt = 1; ; attempt++) {
      let current: Awaited<ReturnType<typeof readEntry>>;
      try {
        current = await readEntry(this.kv, this.kvKey);
      } catch (err) {
        throw releaseError(err);
      }
      if (current === null) {
        return false;
      }
      const { entry, revision } = current;
      if (entry.owner !== this.owner) {
        throw new AlreadyInUseError(
          entry.owner,
          entry.host,
          "offset consumer: release skipped, the entry is no longer owned by this instance",
        );
      }
      try {
        await claimEntry(this.kv, this.kvKey, release, revision);
        return true;
      } catch (err) {
        if (!(err instanceof ClaimConflictError) || attempt === RELEASE_ATTEMPTS) {
          throw releaseError(err);
        }
      }
    }
  }
}

function releaseError(err: unknown): Error {
  const e = toError(err);
  return new Error(`offset consumer: failed to release offset entry: ${e.message}`, { cause: e });
}
