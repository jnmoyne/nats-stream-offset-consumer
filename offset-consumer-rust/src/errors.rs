use thiserror::Error;

#[derive(Debug, Error)]
pub enum OffsetConsumerError {
    #[error("offset consumer: invalid consumer name")]
    InvalidConsumerName,

    #[error("offset consumer: stream name is required")]
    StreamRequired,

    #[error("offset consumer: consumer is closed")]
    ConsumerClosed,

    #[error("offset consumer: already consuming")]
    AlreadyConsuming,

    /// Another live instance owns the offset entry for this stream and
    /// consumer name: either its ordered consumer still exists on the server,
    /// or its claim is too recent. `owner` is the prefix of the ordered
    /// consumer names created by the owning instance.
    #[error("offset consumer: already in use by another instance (owner {owner:?} on host {host:?}, claimed at {claimed_at})")]
    AlreadyInUse {
        owner: String,
        host: String,
        claimed_at: String,
    },

    #[error("offset consumer: {0}")]
    Nats(#[from] async_nats::Error),

    #[error("offset consumer: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, OffsetConsumerError>;
