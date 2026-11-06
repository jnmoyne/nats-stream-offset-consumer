use async_nats::jetstream;
use async_nats::jetstream::context::GetStreamErrorKind;
use async_nats::jetstream::kv::{CreateErrorKind, Operation, UpdateErrorKind};
use async_nats::jetstream::ErrorCode;
use bytes::Bytes;
use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use crate::consumer::OffsetConsumerInfo;
use crate::errors::{OffsetConsumerError, Result};

pub const KV_BUCKET_NAME: &str = "offset-consumers";

/// Name of the stream backing the offset-consumers bucket.
const KV_STREAM_NAME: &str = "KV_offset-consumers";

/// Bound on `stream.len() + consumer_name.len()`. Ordered consumer names are
/// "oc-{stream}-{consumer}-{22-char nuid}-g{gen}" plus a client suffix, and
/// the server limits names to 255 characters; 32 characters are reserved for
/// the generation and client suffixes.
pub const MAX_COMBINED_NAME_LEN: usize = 255 - 32 - "oc-".len() - 2 - 22;

/// Reports whether `name` is usable both in a KV key token and in a consumer
/// name: letters, digits, '-', '_' and '=' only.
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'=')
}

/// Checks the stream and consumer names.
pub fn validate_names(stream: &str, consumer_name: &str) -> Result<()> {
    const CHARSET: &str = "use letters, digits, '-', '_' and '=' only";
    if !valid_name(consumer_name) {
        return Err(OffsetConsumerError::InvalidConsumerName {
            name: consumer_name.to_string(),
            reason: CHARSET.to_string(),
        });
    }
    if !valid_name(stream) {
        return Err(OffsetConsumerError::InvalidStreamName {
            name: stream.to_string(),
            reason: CHARSET.to_string(),
        });
    }
    if stream.len() + consumer_name.len() > MAX_COMBINED_NAME_LEN {
        return Err(OffsetConsumerError::InvalidConsumerName {
            name: consumer_name.to_string(),
            reason: format!("stream and consumer names together exceed {MAX_COMBINED_NAME_LEN} characters"),
        });
    }
    Ok(())
}

/// Unix nanoseconds of Go's zero time (0001-01-01T00:00:00Z), which the
/// reference implementation maps to an empty identity.
const ZERO_TIME_NANOS: i128 = -62_135_596_800_000_000_000;

/// Identifies a stream incarnation by its creation time, as decimal Unix
/// nanoseconds (the same string in every port). Empty for the zero time.
pub fn stream_created_id(info: &jetstream::stream::Info) -> String {
    let nanos = info.created.unix_timestamp_nanos();
    if nanos == ZERO_TIME_NANOS {
        String::new()
    } else {
        nanos.to_string()
    }
}

/// Reports whether a stored entry refers to an earlier incarnation of the
/// stream. Entries from older versions carry no stream identity; for those a
/// stored sequence beyond the stream's last sequence is the tell-tale sign.
pub fn stream_was_recreated(entry: &OffsetEntry, current_created: &str, last_seq: u64) -> bool {
    if !entry.stream_created.is_empty() && !current_created.is_empty() {
        return entry.stream_created != current_created;
    }
    entry.seq > last_seq
}

/// Delete-marker TTL applied when the library creates the bucket.
pub const DELETE_MARKER_TTL: std::time::Duration = std::time::Duration::from_secs(10 * 60);

/// The record stored in the offset-consumers KV bucket.
/// An empty `owner` means the entry is not owned by any live instance.
/// Unknown fields (such as `claimed_at`, written by older versions) are ignored.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OffsetEntry {
    /// Last committed stream sequence.
    pub seq: u64,
    /// RFC3339 time of the last commit.
    #[serde(default)]
    pub committed_at: String,
    /// Owner prefix of the live instance holding this entry.
    #[serde(default)]
    pub owner: String,
    /// Hostname of the owning instance.
    #[serde(default)]
    pub host: String,
    /// The stream's creation time in Unix nanoseconds (decimal string): the
    /// stream incarnation `seq` refers to. Empty for entries written by older
    /// versions.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub stream_created: String,
}

impl OffsetEntry {
    pub fn to_bytes(&self) -> Result<Bytes> {
        serde_json::to_vec(self)
            .map(Bytes::from)
            .map_err(|e| OffsetConsumerError::Other(format!("failed to marshal offset entry: {e}")))
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        serde_json::from_slice(data)
            .map_err(|e| OffsetConsumerError::Other(format!("failed to parse offset entry: {e}")))
    }

    pub fn to_info(&self) -> OffsetConsumerInfo {
        OffsetConsumerInfo {
            seq: self.seq,
            committed_at: self.committed_at.clone(),
            owner: self.owner.clone(),
            host: self.host.clone(),
            stream_created: self.stream_created.clone(),
        }
    }
}

pub fn rfc3339_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Reads the offset entry. Returns `None` when the key does not exist or has
/// been deleted (the KV client returns delete/purge markers as entries).
pub async fn read_entry(kv: &jetstream::kv::Store, key: &str) -> Result<Option<(OffsetEntry, u64)>> {
    let entry = kv
        .entry(key)
        .await
        .map_err(|e| OffsetConsumerError::Other(format!("failed to read offset entry: {e}")))?;
    match entry {
        Some(entry) if entry.operation == Operation::Put => {
            Ok(Some((OffsetEntry::from_bytes(&entry.value)?, entry.revision)))
        }
        _ => Ok(None),
    }
}

/// Outcome of a claim write.
pub enum ClaimOutcome {
    Claimed(u64),
    /// Another instance wrote the entry first.
    Conflict,
}

/// Writes `entry` with a compare-and-set against `revision` (0 means the key
/// must not exist).
pub async fn claim_entry(
    kv: &jetstream::kv::Store,
    key: &str,
    entry: &OffsetEntry,
    revision: u64,
) -> Result<ClaimOutcome> {
    let data = entry.to_bytes()?;
    if revision == 0 {
        match kv.create(key, data).await {
            Ok(rev) => Ok(ClaimOutcome::Claimed(rev)),
            Err(e) if e.kind() == CreateErrorKind::AlreadyExists => Ok(ClaimOutcome::Conflict),
            Err(e) => Err(OffsetConsumerError::Other(format!("failed to claim offset entry: {e}"))),
        }
    } else {
        match kv.update(key, data, revision).await {
            Ok(rev) => Ok(ClaimOutcome::Claimed(rev)),
            Err(e) if e.kind() == UpdateErrorKind::WrongLastRevision => Ok(ClaimOutcome::Conflict),
            Err(e) => Err(OffsetConsumerError::Other(format!("failed to claim offset entry: {e}"))),
        }
    }
}

/// Returns the offset-consumers KV bucket, creating it if it does not exist
/// with replicas matching the stream (minimum 1), history 1 and a 10-minute
/// delete-marker TTL (so tombstones left by `delete_consumer()` are purged).
/// An existing bucket's configuration is never overwritten; its replica count
/// is only ever raised (best effort) so that offsets of a replicated stream
/// are not stored with fewer replicas than the stream itself.
pub async fn ensure_bucket(js: &jetstream::Context, replicas: usize) -> Result<jetstream::kv::Store> {
    let replicas = replicas.max(1);
    let open_err = |e: &dyn std::fmt::Display| OffsetConsumerError::Other(format!("failed to open KV bucket: {e}"));

    let kv = match js.get_stream(KV_STREAM_NAME).await {
        Ok(_) => js.get_key_value(KV_BUCKET_NAME).await.map_err(|e| open_err(&e))?,
        Err(e) if is_stream_not_found(&e.kind()) => {
            let config = jetstream::kv::Config {
                bucket: KV_BUCKET_NAME.to_string(),
                num_replicas: replicas,
                history: 1,
                limit_markers: Some(DELETE_MARKER_TTL),
                ..Default::default()
            };
            match js.create_key_value(config).await {
                Ok(store) => store,
                // Possibly created concurrently, with another configuration.
                Err(create_err) => js
                    .get_key_value(KV_BUCKET_NAME)
                    .await
                    .map_err(|_| open_err(&create_err))?,
            }
        }
        Err(e) => return Err(open_err(&e)),
    };

    raise_bucket_replicas(js, replicas).await;
    Ok(kv)
}

fn is_stream_not_found(kind: &GetStreamErrorKind) -> bool {
    matches!(kind, GetStreamErrorKind::JetStream(e) if e.error_code() == ErrorCode::STREAM_NOT_FOUND)
}

/// Raises the bucket's replica count to `replicas` if it is lower. Errors
/// (permissions, too few servers) are ignored: the bucket keeps working with
/// its current replicas.
async fn raise_bucket_replicas(js: &jetstream::Context, replicas: usize) {
    let Ok(stream) = js.get_stream(KV_STREAM_NAME).await else {
        return;
    };
    let mut config = stream.cached_info().config.clone();
    if config.num_replicas >= replicas {
        return;
    }
    config.num_replicas = replicas;
    let _ = js.update_stream(config).await;
}
