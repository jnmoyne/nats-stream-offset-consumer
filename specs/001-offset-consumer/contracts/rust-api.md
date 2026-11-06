# Rust API Contract: Offset Consumer Library

**Crate**: `offset-consumer`
**Client**: `async-nats` 0.51.0 (Rust 1.89+), plus `nuid` 0.5 and `hostname` 0.4

---

## Construction

```rust
impl OffsetConsumer {
    /// Ensures the offset-consumers KV bucket exists, reads any stored offset,
    /// resolves the start sequence and claims ownership of the offset entry.
    /// Returns `OffsetConsumerError::AlreadyInUse` if another live instance
    /// owns it. Does NOT start consuming — call `consume()` for that.
    pub async fn new(
        js: jetstream::Context,
        config: ConsumerConfig,
        error_handler: Option<Arc<dyn Fn(&OffsetConsumerError) + Send + Sync>>,
    ) -> Result<Self>;
}

/// All offset consumers of a stream (client-side prefix filter on `keys()`); empty if none.
pub async fn list_consumers(js: &jetstream::Context, stream: &str) -> Result<HashMap<String, OffsetConsumerInfo>>;

/// Inactivity threshold async-nats applies to ordered consumers (not configurable); also the claim grace window.
pub const INACTIVE_THRESHOLD: Duration = Duration::from_secs(30);
```

---

## Methods

```rust
impl OffsetConsumer {
    /// Starts delivering messages; the handler is called serially from a spawned task.
    pub async fn consume<F: Fn(Message) + Send + Sync + 'static>(&self, handler: F) -> Result<()>;

    /// Persists the last delivered sequence and waits for the acknowledgement.
    pub async fn commit_sync(&self) -> Result<()>;

    /// Queues the write behind pending writes (commits are applied in call
    /// order) and returns a handle that resolves on acknowledgement.
    pub async fn commit_async(&self) -> Result<JoinHandle<Result<()>>>;

    /// Fresh read of the offset entry, or `None` if it does not exist.
    pub async fn info(&self) -> Result<Option<OffsetConsumerInfo>>;

    /// Stops consumption, deletes its ordered consumer, writes the new offset
    /// and restarts consumption under a new generation name if it was running.
    /// After `seek(N)` the next message is N+1. If the write fails the consumer
    /// is left stopped with its position unchanged.
    pub async fn seek(&self, seq: u64) -> Result<()>;
    pub async fn seek_to_beginning(&self) -> Result<()>;
    pub async fn seek_to_end(&self) -> Result<()>;

    /// Removes the offset entry; a later `close()` will not recreate it.
    pub async fn delete_consumer(&self) -> Result<()>;

    /// Waits (5s bound) for pending commits (leftovers are abandoned, their
    /// handles resolve), deletes the ordered consumer, and releases ownership
    /// with one final write that persists the highest sequence passed to a
    /// commit call and, with auto-commit, the last delivered sequence.
    pub async fn close(&self) -> Result<()>;
}

pub struct OffsetConsumerInfo {
    pub seq: u64,
    pub committed_at: String,
    pub owner: String,      // "" when unowned
    pub host: String,
    pub claimed_at: String,
}
```

---

## ConsumerConfig

```rust
pub struct ConsumerConfig {
    pub stream: String,
    pub consumer_name: String,            // non-empty, no '.'
    pub auto_commit: bool,                // default false
    pub auto_commit_interval: Duration,   // default 5s
    pub filter_subjects: Vec<String>,
    pub start_position: StartPosition,    // Beginning (default) or End
}
```

---

## Errors

```rust
pub enum OffsetConsumerError {
    InvalidConsumerName,
    StreamRequired,
    ConsumerClosed,
    AlreadyConsuming,
    /// Another live instance owns the offset entry (returned by `new`).
    AlreadyInUse { owner: String, host: String, claimed_at: String },
    Nats(async_nats::Error),
    Other(String),
}
```

Ordered consumers created by an instance are named `oc-{stream}-{consumer}-{nuid}-g{generation}` (then `..._{n}` after client-side resets).

Known client quirk: if the ordered consumer resets before any message was delivered, async-nats 0.51 recreates it with `DeliverPolicy::All` instead of the requested start sequence (redelivery only; at-least-once is preserved).
