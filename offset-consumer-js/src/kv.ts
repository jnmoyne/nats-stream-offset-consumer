import type { NatsConnection } from "@nats-io/nats-core";
import { JetStreamApiCodes, JetStreamApiError, type JetStreamManager } from "@nats-io/jetstream";
import { Kvm } from "@nats-io/kv";
import type { KV } from "@nats-io/kv";
import type { OffsetConsumerInfo } from "./types.js";
import { InvalidConsumerNameError, InvalidStreamNameError } from "./errors.js";

export const BUCKET_NAME = "offset-consumers";

/** Name of the stream backing the offset-consumers bucket. */
export const BUCKET_STREAM_NAME = `KV_${BUCKET_NAME}`;

/** Delete-marker TTL (ms) applied when the library creates the bucket. */
export const DELETE_MARKER_TTL_MS = 10 * 60 * 1000;

/** JetStream API error code: stream name already in use with a different configuration. */
const STREAM_NAME_EXIST = 10058;

/**
 * Bound on stream.length + consumerName.length. Ordered consumer names are
 * "oc-{stream}-{consumer}-{22-char nuid}-g{gen}" plus a client suffix, and the
 * server limits names to 255 characters; 32 characters are reserved for the
 * generation and client suffixes.
 */
export const MAX_COMBINED_NAME_LEN = 255 - 32 - "oc-".length - 2 - 22;

/** Names usable both in a KV key token and in a consumer name. */
const VALID_NAME = /^[A-Za-z0-9_=-]+$/;

const VALID_CHARS = "use letters, digits, '-', '_' and '=' only";

/**
 * Validates the stream and consumer names. Throws InvalidStreamNameError or
 * InvalidConsumerNameError.
 */
export function validateNames(stream: string, consumerName: string): void {
  if (!stream) {
    throw new InvalidStreamNameError(stream ?? "", "stream name is required");
  }
  if (typeof consumerName !== "string" || !VALID_NAME.test(consumerName)) {
    throw new InvalidConsumerNameError(consumerName ?? "", VALID_CHARS);
  }
  if (!VALID_NAME.test(stream)) {
    throw new InvalidStreamNameError(stream, VALID_CHARS);
  }
  if (stream.length + consumerName.length > MAX_COMBINED_NAME_LEN) {
    throw new InvalidConsumerNameError(
      consumerName,
      `stream and consumer names together exceed ${MAX_COMBINED_NAME_LEN} characters`,
    );
  }
}

/** Unix seconds of Go's zero time, 0001-01-01T00:00:00Z. */
const ZERO_TIME_UNIX_SECONDS = -62135596800n;

const RFC3339 = /^(\d{4})-(\d{2})-(\d{2})[Tt](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?([Zz]|[+-]\d{2}:\d{2})$/;

/**
 * Identifies a stream incarnation by its creation time, as decimal Unix
 * nanoseconds (the same string in every port). `created` is the RFC3339
 * timestamp of the stream info, with up to 9 fractional digits; it is
 * converted exactly with BigInt (Date only keeps milliseconds). Returns ""
 * when the time is missing, unparseable or the zero time.
 */
export function streamCreatedId(created: string | undefined): string {
  if (!created) {
    return "";
  }
  const m = RFC3339.exec(created);
  if (!m) {
    return "";
  }
  const [, y, mo, d, h, mi, s, frac, zone] = m;
  // setUTCFullYear avoids Date.UTC's mapping of years 0-99 to 1900-1999.
  const date = new Date(0);
  date.setUTCFullYear(Number(y), Number(mo) - 1, Number(d));
  date.setUTCHours(Number(h), Number(mi), Number(s), 0);
  let seconds = BigInt(date.getTime() / 1000);
  if (!/^[Zz]$/.test(zone)) {
    const sign = zone[0] === "-" ? -1n : 1n;
    const offset = BigInt(Number(zone.slice(1, 3)) * 3600 + Number(zone.slice(4, 6)) * 60);
    seconds -= sign * offset;
  }
  const nanos = BigInt((frac ?? "").slice(0, 9).padEnd(9, "0"));
  if (seconds === ZERO_TIME_UNIX_SECONDS && nanos === 0n) {
    return ""; // Go's zero time: unknown
  }
  return (seconds * 1_000_000_000n + nanos).toString();
}

/**
 * The record stored in the offset-consumers KV bucket. An empty owner means
 * the entry is not owned by any live instance.
 */
export interface OffsetEntry {
  seq: number;
  committed_at: string;
  owner: string;
  host: string;
  /**
   * The stream's creation time in Unix nanoseconds (decimal string): the
   * stream incarnation seq refers to. "" when unknown (omitted from the JSON).
   */
  stream_created: string;
}

export function emptyEntry(): OffsetEntry {
  return { seq: 0, committed_at: "", owner: "", host: "", stream_created: "" };
}

/**
 * Reports whether a stored entry refers to an earlier incarnation of the
 * stream. Entries from older versions carry no stream identity; for those a
 * stored sequence beyond the stream's last sequence is the tell-tale sign.
 */
export function streamWasRecreated(entry: OffsetEntry, currentCreated: string, lastSeq: number): boolean {
  if (entry.stream_created !== "" && currentCreated !== "") {
    return entry.stream_created !== currentCreated;
  }
  return entry.seq > lastSeq;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** Encodes the entry as JSON with the same field order as the other ports. */
export function encodeEntry(entry: OffsetEntry): Uint8Array {
  const json: Record<string, unknown> = {
    seq: entry.seq,
    committed_at: entry.committed_at,
    owner: entry.owner,
    host: entry.host,
  };
  if (entry.stream_created) {
    json.stream_created = entry.stream_created;
  }
  return encoder.encode(JSON.stringify(json));
}

/** Decodes an entry. Fields written by older versions (claimed_at) are ignored. */
export function decodeEntry(data: Uint8Array): OffsetEntry {
  const raw = JSON.parse(decoder.decode(data)) as Partial<OffsetEntry>;
  return {
    seq: typeof raw.seq === "number" ? raw.seq : 0,
    committed_at: raw.committed_at ?? "",
    owner: raw.owner ?? "",
    host: raw.host ?? "",
    stream_created: typeof raw.stream_created === "string" ? raw.stream_created : "",
  };
}

export function entryToInfo(entry: OffsetEntry): OffsetConsumerInfo {
  return {
    seq: entry.seq,
    committedAt: entry.committed_at,
    owner: entry.owner,
    host: entry.host,
    streamCreated: entry.stream_created,
  };
}

/**
 * Reads the offset entry. Returns null when the key does not exist or has
 * been deleted (the KV client returns DEL/PURGE markers as entries).
 */
export async function readEntry(
  kv: KV,
  key: string,
): Promise<{ entry: OffsetEntry; revision: number } | null> {
  const kve = await kv.get(key);
  if (!kve || kve.operation !== "PUT") {
    return null;
  }
  return { entry: decodeEntry(kve.value), revision: kve.revision };
}

/** Internal: the CAS write of a claim lost against another instance. */
export class ClaimConflictError extends Error {
  constructor(cause?: unknown) {
    super("offset consumer: claim conflict");
    this.name = "ClaimConflictError";
    this.cause = cause;
  }
}

/**
 * Writes entry with a compare-and-set against revision (0 means the key must
 * not exist). Throws ClaimConflictError if another writer got there first.
 */
export async function claimEntry(
  kv: KV,
  key: string,
  entry: OffsetEntry,
  revision: number,
): Promise<number> {
  const data = encodeEntry(entry);
  try {
    if (revision === 0) {
      return await kv.create(key, data);
    }
    return await kv.update(key, data, revision);
  } catch (err) {
    if (
      err instanceof JetStreamApiError &&
      (err.code === JetStreamApiCodes.StreamWrongLastSequence ||
        err.code === JetStreamApiCodes.StreamWrongLastSequenceUnknown)
    ) {
      throw new ClaimConflictError(err);
    }
    throw err;
  }
}

export function isStreamNotFound(err: unknown): boolean {
  return err instanceof JetStreamApiError && err.code === JetStreamApiCodes.StreamNotFound;
}

function isStreamNameInUse(err: unknown): boolean {
  return err instanceof JetStreamApiError && err.code === STREAM_NAME_EXIST;
}

/**
 * Returns the offset-consumers KV bucket, creating it if it does not exist
 * with replicas matching the target stream (minimum 1), history 1 and a
 * 10-minute delete-marker TTL (so tombstones left by deleteConsumer() are
 * purged). An existing bucket's configuration is never overwritten: Kvm.create
 * opens an existing bucket as is (it only reads its stream info) and adds the
 * stream only when it is missing. Its replica count is only ever raised (best
 * effort) so that offsets of a replicated stream are not stored with fewer
 * replicas than the stream itself.
 */
export async function ensureBucket(
  nc: NatsConnection,
  jsm: JetStreamManager,
  streamReplicas: number,
): Promise<KV> {
  const replicas = Math.max(streamReplicas, 1);
  const kvm = new Kvm(nc);
  const opts = { history: 1, replicas, markerTTL: DELETE_MARKER_TTL_MS };
  let kv: KV;
  try {
    kv = await kvm.create(BUCKET_NAME, { ...opts });
  } catch (err) {
    if (!isStreamNameInUse(err)) {
      throw err;
    }
    // Created concurrently, possibly with another configuration: it exists
    // now, so this opens it as is.
    kv = await kvm.create(BUCKET_NAME, { ...opts });
  }
  await raiseBucketReplicas(jsm, replicas);
  return kv;
}

/**
 * Raises the bucket's replica count to replicas if it is lower. Errors
 * (permissions, too few servers) are ignored: the bucket keeps working with
 * its current replicas.
 */
async function raiseBucketReplicas(jsm: JetStreamManager, replicas: number): Promise<void> {
  try {
    const info = await jsm.streams.info(BUCKET_STREAM_NAME);
    if (info.config.num_replicas >= replicas) {
      return;
    }
    await jsm.streams.update(BUCKET_STREAM_NAME, { num_replicas: replicas });
  } catch {
    // best effort
  }
}
