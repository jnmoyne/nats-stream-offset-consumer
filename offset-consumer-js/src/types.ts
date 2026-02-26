import type { JsMsg } from "@nats-io/jetstream";

export type StartPosition = "beginning" | "end";

export interface ConsumerConfig {
  stream: string;
  consumerName: string;
  autoCommit?: boolean;
  autoCommitIntervalMs?: number;
  filterSubjects?: string[];
  errorHandler?: (consumer: OffsetConsumer, error: Error) => void;
  startPosition?: StartPosition;
}

export interface ConsumerMessages extends AsyncIterable<JsMsg> {
  stop(): void;
}

export interface OffsetConsumer {
  consume(opts?: { signal?: AbortSignal }): Promise<ConsumerMessages>;
  commitSync(): Promise<void>;
  commitAsync(): Promise<void>;
  seek(seq: number): Promise<void>;
  seekToBeginning(): Promise<void>;
  seekToEnd(): Promise<void>;
  deleteConsumer(): Promise<void>;
  close(): Promise<void>;
}
