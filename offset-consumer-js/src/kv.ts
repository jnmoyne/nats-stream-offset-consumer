import type { NatsConnection } from "@nats-io/nats-core";
import { Kvm } from "@nats-io/kv";
import type { KV } from "@nats-io/kv";
import { CASConflictError } from "./errors.js";

const BUCKET_NAME = "offset-consumers";

export interface OffsetEntry {
  seq: number;
  committed_at: string;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function encodeOffset(seq: number, committedAt: string): Uint8Array {
  const entry: OffsetEntry = { seq, committed_at: committedAt };
  return encoder.encode(JSON.stringify(entry));
}

export function decodeOffset(data: Uint8Array): OffsetEntry {
  return JSON.parse(decoder.decode(data)) as OffsetEntry;
}

export async function ensureBucket(
  nc: NatsConnection,
  streamReplicas: number,
): Promise<KV> {
  const kvm = new Kvm(nc);
  try {
    return await kvm.create(BUCKET_NAME, {
      history: 1,
      replicas: streamReplicas,
    });
  } catch {
    return await kvm.open(BUCKET_NAME);
  }
}

export async function casUpdate(
  kv: KV,
  key: string,
  value: Uint8Array,
  revision: number,
): Promise<number> {
  try {
    return await kv.update(key, value, revision);
  } catch (err) {
    throw new CASConflictError(err instanceof Error ? err : undefined);
  }
}
