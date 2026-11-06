use async_nats::jetstream;
use async_nats::jetstream::kv::{CreateErrorKind, Operation, UpdateErrorKind};
use bytes::Bytes;
use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use crate::consumer::OffsetConsumerInfo;
use crate::errors::{OffsetConsumerError, Result};

pub const KV_BUCKET_NAME: &str = "offset-consumers";

/// Delete-marker TTL applied when the library creates the bucket.
pub const DELETE_MARKER_TTL: std::time::Duration = std::time::Duration::from_secs(10 * 60);

/// The record stored in the offset-consumers KV bucket.
/// An empty `owner` means the entry is not owned by any live instance.
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
    /// RFC3339 time at which the owner claimed the entry.
    #[serde(default)]
    pub claimed_at: String,
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
            claimed_at: self.claimed_at.clone(),
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

/// Creates the offset-consumers KV bucket if it does not exist (replicas
/// matching the stream, minimum 1; history 1; a 10-minute delete-marker TTL
/// so tombstones left by `delete_consumer()` are purged), or returns the
/// existing one.
pub async fn ensure_bucket(js: &jetstream::Context, replicas: usize) -> Result<jetstream::kv::Store> {
    let config = jetstream::kv::Config {
        bucket: KV_BUCKET_NAME.to_string(),
        num_replicas: replicas.max(1),
        history: 1,
        limit_markers: Some(DELETE_MARKER_TTL),
        ..Default::default()
    };

    match js.create_key_value(config).await {
        Ok(store) => Ok(store),
        Err(_) => js
            .get_key_value(KV_BUCKET_NAME)
            .await
            .map_err(|e| OffsetConsumerError::Other(format!("failed to ensure KV bucket: {e}"))),
    }
}
