import type { NatsConnection } from "@nats-io/nats-core";
import { JetStreamApiCodes, JetStreamApiError } from "@nats-io/jetstream";
import { Kvm } from "@nats-io/kv";
import type { KV } from "@nats-io/kv";
import type { OffsetConsumerInfo } from "./types.js";

export const BUCKET_NAME = "offset-consumers";

/** Delete-marker TTL (ms) applied when the library creates the bucket. */
export const DELETE_MARKER_TTL_MS = 10 * 60 * 1000;

/**
 * The record stored in the offset-consumers KV bucket. An empty owner means
 * the entry is not owned by any live instance.
 */
export interface OffsetEntry {
  seq: number;
  committed_at: string;
  owner: string;
  host: string;
  claimed_at: string;
}

export function emptyEntry(): OffsetEntry {
  return { seq: 0, committed_at: "", owner: "", host: "", claimed_at: "" };
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function encodeEntry(entry: OffsetEntry): Uint8Array {
  return encoder.encode(JSON.stringify(entry));
}

export function decodeEntry(data: Uint8Array): OffsetEntry {
  const raw = JSON.parse(decoder.decode(data)) as Partial<OffsetEntry>;
  return {
    seq: typeof raw.seq === "number" ? raw.seq : 0,
    committed_at: raw.committed_at ?? "",
    owner: raw.owner ?? "",
    host: raw.host ?? "",
    claimed_at: raw.claimed_at ?? "",
  };
}

export function entryToInfo(entry: OffsetEntry): OffsetConsumerInfo {
  return {
    seq: entry.seq,
    committedAt: entry.committed_at,
    owner: entry.owner,
    host: entry.host,
    claimedAt: entry.claimed_at,
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

export async function ensureBucket(
  nc: NatsConnection,
  streamReplicas: number,
): Promise<KV> {
  const kvm = new Kvm(nc);
  try {
    // replicas matching the stream (minimum 1), history 1, and a 10-minute
    // delete-marker TTL so tombstones left by deleteConsumer() are purged.
    return await kvm.create(BUCKET_NAME, {
      history: 1,
      replicas: Math.max(streamReplicas, 1),
      markerTTL: DELETE_MARKER_TTL_MS,
    });
  } catch {
    return await kvm.open(BUCKET_NAME);
  }
}
