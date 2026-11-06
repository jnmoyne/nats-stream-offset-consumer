use thiserror::Error;

#[derive(Debug, Error)]
pub enum OffsetConsumerError {
    /// The consumer name is empty, too long (the stream and consumer names
    /// together must not exceed 196 characters), or contains characters
    /// other than letters, digits, '-', '_' and '='.
    #[error("offset consumer: invalid consumer name {name:?}: {reason}")]
    InvalidConsumerName { name: String, reason: String },

    /// The stream name cannot be used in the offset entry's KV key or the
    /// ordered consumer names: it contains characters other than letters,
    /// digits, '-', '_' and '='.
    #[error("offset consumer: invalid stream name {name:?}: {reason}")]
    InvalidStreamName { name: String, reason: String },

    #[error("offset consumer: stream name is required")]
    StreamRequired,

    #[error("offset consumer: consumer is closed")]
    ConsumerClosed,

    #[error("offset consumer: already consuming")]
    AlreadyConsuming,

    /// Another live instance owns the offset entry for this stream and
    /// consumer name: a consumer whose name starts with its owner prefix
    /// exists on the stream (returned by `new()` without waiting, and by
    /// `consume()` also after the liveness wait), or it wrote the entry while
    /// `consume()` was claiming it. `owner` is the prefix of the ordered
    /// consumer names created by the owning instance. Also returned by
    /// `close()` when its release write is skipped because the entry is no
    /// longer owned by this instance.
    #[error("offset consumer: already in use by another instance (owner {owner:?} on host {host:?})")]
    AlreadyInUse { owner: String, host: String },

    /// Reported to the error handler when the stream was deleted and
    /// recreated since the offset was stored (detected at startup or while
    /// consuming). The stored offset (`stored_seq`) refers to the old stream,
    /// so the consumer repositions itself according to `start_position`, like
    /// Kafka's auto.offset.reset. Informational: consumption continues.
    #[error("offset consumer: stream was recreated, offset reset to the start position (stored sequence {stored_seq})")]
    StreamRecreated { stored_seq: u64 },

    /// Reported to the error handler when the ordered consumer's message
    /// stream ended on its own (not through `seek` or `close`). Consumption
    /// is stopped; `consume()` may be called again.
    #[error("offset consumer: consumption stopped: {0}")]
    ConsumptionStopped(String),

    #[error("offset consumer: {0}")]
    Nats(#[from] async_nats::Error),

    #[error("offset consumer: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, OffsetConsumerError>;
