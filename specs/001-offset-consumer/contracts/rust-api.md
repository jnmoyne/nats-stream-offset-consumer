# Rust API Contract: Offset Consumer Library

**Crate**: `offset-consumer`
**Client**: `async-nats` 0.51.0 (Rust 1.89+), plus `nuid` 0.5 and `hostname` 0.4

---

## Construction

```rust
impl OffsetConsumer {
    /// Opens the offset-consumers KV bucket (creating it only if it does not
    /// exist; an existing bucket's configuration is never overwritten, its
    /// replicas are only raised, best effort, to the stream's), resolves
    /// `start_position` into the initial position (`End` -> the stream's last
    /// sequence now) and reads the entry: if it has an owner and a consumer
    /// whose name starts with that owner exists (one listing, no wait),
    /// returns `OffsetConsumerError::AlreadyInUse`. Writes nothing and creates
    /// nothing on the server; ownership is claimed by `consume()`. Does NOT
    /// start consuming.
    pub async fn new(
        js: jetstream::Context,
        config: ConsumerConfig,
        error_handler: Option<Arc<dyn Fn(&OffsetConsumerError) + Send + Sync>>,
    ) -> Result<Self>;
}

/// All offset consumers of a stream (client-side prefix filter on `keys()`); empty if none.
pub async fn list_consumers(js: &jetstream::Context, stream: &str) -> Result<HashMap<String, OffsetConsumerInfo>>;

/// Inactivity threshold async-nats applies to ordered consumers (not configurable).
pub const INACTIVE_THRESHOLD: Duration = Duration::from_secs(30);

impl OffsetConsumer {
    /// Overrides the claim's liveness wait (default 5s). Tests only.
    #[doc(hidden)]
    pub fn set_liveness_wait(&self, wait: Duration);
}
```

---

## Methods

```rust
impl OffsetConsumer {
    /// Claims ownership of the entry (see Ownership) whenever consumption starts
    /// from a stopped state, then starts delivering messages; the handler is called
    /// serially from a spawned task. Returns `AlreadyInUse` if another instance owns
    /// the entry. The claim stores the start position: the pending seek position if
    /// `seek` was called while not consuming, else the stored offset, else the initial
    /// position resolved at construction (also when the stored offset belongs to an
    /// earlier incarnation of the stream; `StreamRecreated` is then reported after
    /// the claim succeeded).
    /// Errors yielded by the ordered stream are reported to the error handler and
    /// polling continues (backoff 100ms doubling to 5s, reset by a message); if the
    /// message stream ends on its own, `ConsumptionStopped` is reported and
    /// consumption stops. While consuming, the stream identity is checked every 5s
    /// and when a delivered message reveals a client-side reset (async-nats exposes
    /// no reset signal); a recreated stream is reported (`StreamRecreated`) and the
    /// consumer seeks to `start_position` on the new stream, applying the new
    /// identity atomically with the seek write.
    pub async fn consume<F: Fn(Message) + Send + Sync + 'static>(&self, handler: F) -> Result<()>;

    /// Persists the last delivered sequence and waits for the acknowledgement.
    /// From inside the handler (via a clone) it includes the message being handled.
    /// Before the first claim it is a no-op (nothing is written).
    pub async fn commit_sync(&self) -> Result<()>;

    /// Queues the write behind pending writes (commits are applied in call
    /// order) and returns a handle that resolves on acknowledgement. Before the
    /// first claim it returns an already-resolved handle and writes nothing.
    pub async fn commit_async(&self) -> Result<JoinHandle<Result<()>>>;

    /// Fresh read of the offset entry, or `None` if it does not exist.
    pub async fn info(&self) -> Result<Option<OffsetConsumerInfo>>;

    /// After `seek(N)` the next message is N+1. While consuming (make before
    /// break): stops delivery without deleting the server-side ordered
    /// consumer, writes the new offset, starts the next generation, and only
    /// then deletes the old consumer (in all cases, also when the write or
    /// the restart failed; the instance is then left stopped, with its
    /// position fields unchanged after a failed write). While not consuming:
    /// only records the position (start, delivered and processed sequence,
    /// committed_at, and the new stream identity if repositioning after a
    /// recreation); nothing is written, the next claim stores it. consume,
    /// seek and close are serialized by a lifecycle lock.
    pub async fn seek(&self, seq: u64) -> Result<()>;
    /// Seeks to 0 (the beginning), regardless of `start_position`.
    pub async fn seek_to_beginning(&self) -> Result<()>;
    pub async fn seek_to_end(&self) -> Result<()>;

    /// Removes the offset entry; a later `close()` will not recreate it.
    pub async fn delete_consumer(&self) -> Result<()>;

    /// Waits (5s bound) for pending commits (leftovers are abandoned, their
    /// handles resolve), deletes the ordered consumer, and releases ownership
    /// with one final write that persists the highest sequence passed to a
    /// commit call and, with auto-commit, the last sequence whose handler has
    /// returned. Does not wait for a running handler (that message is not
    /// committed and is redelivered on the next start) nor for the auto-commit
    /// task (it is stopped), so the error handler may trigger a close.
    /// The release is conditional: the entry is read; missing -> nothing is
    /// written (Ok); owner != this instance -> nothing is written and close
    /// returns `Err(AlreadyInUse { owner, host })` of the entry
    /// (release skipped, the entry is no longer owned by this instance);
    /// otherwise a CAS update against the read revision, re-read and retried
    /// on a revision conflict, at most 3 attempts in total, then
    /// `Err(Other("failed to release offset entry: ..."))`. Skipped after
    /// `delete_consumer()` and before the first claim (nothing is written and
    /// there is no consumer to delete). All other cleanup happens in every case.
    pub async fn close(&self) -> Result<()>;
}

pub struct OffsetConsumerInfo {
    pub seq: u64,
    pub committed_at: String,
    pub owner: String,      // "" when unowned
    pub host: String,
    pub stream_created: String, // stream creation time, decimal Unix nanoseconds; "" for older entries
}
```

Auto-commit commits the last sequence whose handler has returned (never a
message still being handled) and is a no-op when an explicit commit already
covers it. Messages delivered by a stopped or replaced generation are dropped.

The KV entry is `{"seq","committed_at","owner","host","stream_created"}` (a
`claimed_at` field written by older versions is ignored);
`stream_created` (omitted when empty) is `info.created.unix_timestamp_nanos()` as
a decimal string, byte-identical across ports. At the claim an entry is from an
earlier incarnation if both identities are non-empty and differ, or, for a
legacy entry without identity, if its `seq` exceeds the stream's last sequence.

---

## ConsumerConfig

```rust
pub struct ConsumerConfig {
    pub stream: String,
    pub consumer_name: String,            // [A-Za-z0-9_=-]+; stream.len() + consumer_name.len() <= 196
    pub auto_commit: bool,                // default false
    pub auto_commit_interval: Duration,   // default 5s
    pub filter_subjects: Vec<String>,
    pub start_position: StartPosition,    // Beginning (default) or End (resolved at creation, stored by the claim)
}
```

---

## Errors

```rust
pub enum OffsetConsumerError {
    /// Empty, invalid characters (allowed: [A-Za-z0-9_=-]), or stream + consumer names longer than 196.
    InvalidConsumerName { name: String, reason: String },
    /// Stream name with invalid characters (allowed: [A-Za-z0-9_=-]).
    InvalidStreamName { name: String, reason: String },
    StreamRequired,
    ConsumerClosed,
    AlreadyConsuming,
    /// Another live instance owns the offset entry (returned by `new` and by
    /// `consume`'s claim, and by `close` when the release is skipped because
    /// another instance owns the entry).
    AlreadyInUse { owner: String, host: String },
    /// Reported to the error handler (informational): the stream was deleted and
    /// recreated since the offset was stored; the offset is reset to `start_position`.
    StreamRecreated { stored_seq: u64 },
    /// Reported to the error handler: the ordered stream ended on its own and
    /// consumption stopped (`consume()` may be called again).
    ConsumptionStopped(String),
    Nats(async_nats::Error),
    Other(String),
}
```

## Ownership

Each instance has an owner prefix `oc-{stream}-{consumer}-{nuid}`; its ordered
consumers are named with it. An owner is alive while a consumer whose name
starts with its prefix exists on the stream. No clocks are compared.

Claim (in `consume()`, lifecycle lock held), at most 3 rounds:
1. Read the entry (revision r). If its owner is set and is another instance:
   list the stream's consumer names; a match -> `AlreadyInUse`; otherwise wait
   the liveness wait (5s, local clock; no jitter, the CAS decides between
   concurrent starters) and list
   again; a match -> `AlreadyInUse`.
2. Stream info; the entry is from an earlier incarnation per the rule above.
3. CAS write (`create` if missing, `update` at r) of the start position with
   `owner`, `host`, `stream_created`, holding the state lock (no commit is
   submitted meanwhile). Success: claimed, pending seek cleared, the delete flag
   cleared, commit epoch bumped, all positions set to the claimed sequence.
4. CAS conflict: re-read; another owner -> `AlreadyInUse`; otherwise (a release,
   or this instance's own late commit) the next round; after 3 rounds
   `Other("failed to claim offset entry: ...")`.

The CAS against the revision read before the wait makes any write during the
wait (a commit by a live owner, another instance's claim) fail the claim.
After a crash, a new instance is admitted at `consume()` once the crashed
owner's ordered consumer has expired (30s after its connection went away) and
the liveness wait has passed.

`close()` deletes the ordered consumer and then writes the release entry
(conditionally, see `close()`).

Ordered consumers created by an instance are named `oc-{stream}-{consumer}-{nuid}-g{generation}` (then `..._{n}` after client-side resets).

Known client quirk: if the ordered consumer resets before any message was delivered, async-nats 0.51 recreates it with `DeliverPolicy::All` instead of the requested start sequence (redelivery only; at-least-once is preserved).
