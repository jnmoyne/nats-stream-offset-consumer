use std::time::Duration;

/// Controls where a new consumer (no stored offset) begins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StartPosition {
    /// Start from the first message in the stream.
    #[default]
    Beginning,
    /// Start after the stream's last message at the time the consumer is
    /// created. The claim at the first `consume()` stores that position, so
    /// messages published while the consumer is down are not skipped on the
    /// next start.
    End,
}

/// Configuration for an offset consumer.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Name of the JetStream stream to consume. Required.
    pub stream: String,

    /// Logical name for this consumer. Required.
    /// Letters, digits, '-', '_' and '=' only; the stream and consumer names
    /// together must not exceed 196 characters (the server limits consumer
    /// names to 255, and the ordered consumer names embed both).
    /// Together with stream forms the KV key: "{stream}.{consumer_name}".
    pub consumer_name: String,

    /// Enable timer-based background commits. Default: false.
    pub auto_commit: bool,

    /// Interval for auto-commit. Default: 5s.
    /// Ignored when auto_commit is false.
    pub auto_commit_interval: Duration,

    /// Optional list of subject filters for the ordered consumer.
    pub filter_subjects: Vec<String>,

    /// Controls where a new consumer (no stored offset) begins.
    /// Default: Beginning (sequence 1). `End` is resolved to the stream's last
    /// sequence when the consumer is created and stored by the claim at the
    /// first `consume()`. It also
    /// applies when the stream is found to have been recreated (see
    /// `OffsetConsumerError::StreamRecreated`).
    pub start_position: StartPosition,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            stream: String::new(),
            consumer_name: String::new(),
            auto_commit: false,
            auto_commit_interval: Duration::from_secs(5),
            filter_subjects: Vec::new(),
            start_position: StartPosition::Beginning,
        }
    }
}
