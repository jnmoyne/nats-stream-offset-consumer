export class InvalidConsumerNameError extends Error {
  constructor(name: string) {
    super(
      `Invalid consumer name: "${name}" — must be non-empty and must not contain '.'`,
    );
    this.name = "InvalidConsumerNameError";
  }
}

/**
 * Thrown by createConsumer when the offset entry for the stream and consumer
 * name is owned by another live instance: either its ordered consumer still
 * exists on the server, or its claim is too recent.
 */
export class AlreadyInUseError extends Error {
  /** Owner prefix recorded in the KV entry. */
  readonly owner: string;
  /** Hostname recorded by the owning instance. */
  readonly host: string;
  /** RFC3339 time at which the owner claimed the entry. */
  readonly claimedAt: string;

  constructor(owner: string, host: string, claimedAt: string) {
    super(
      `Offset consumer already in use by another instance (owner "${owner}" on host "${host}", claimed at ${claimedAt})`,
    );
    this.name = "AlreadyInUseError";
    this.owner = owner;
    this.host = host;
    this.claimedAt = claimedAt;
  }
}
