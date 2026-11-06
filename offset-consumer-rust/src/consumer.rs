use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::context::KeyValueErrorKind;
use async_nats::jetstream::{self, consumer::DeliverPolicy, Message};
use futures::StreamExt;
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;

use crate::config::{ConsumerConfig, StartPosition};
use crate::errors::{OffsetConsumerError, Result};
use crate::kv::{
    claim_entry, ensure_bucket, read_entry, rfc3339_now, ClaimOutcome, OffsetEntry,
    KV_BUCKET_NAME,
};

/// Inactivity threshold async-nats applies to ordered consumers (it is not
/// configurable). After an instance crashes, its ordered consumer, and with
/// it the ownership of the offset entry, is released by the server once this
/// elapses. It is also the grace window during which a fresh claim is
/// considered alive even if its ordered consumer cannot be found yet.
pub const INACTIVE_THRESHOLD: Duration = Duration::from_secs(30);

/// Bound on how long `close` waits for pending writes and the consume task.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Contains the stored offset information for a consumer,
/// as read from the KV store.
#[derive(Debug, Clone)]
pub struct OffsetConsumerInfo {
    /// The last committed stream sequence number.
    pub seq: u64,
    /// The timestamp of the last commit (RFC3339).
    pub committed_at: String,
    /// Owner prefix of the instance currently holding the entry, or empty
    /// when no instance owns it. Ordered consumers created by the owner have
    /// names starting with this prefix.
    pub owner: String,
    /// Hostname of the owning instance, or empty.
    pub host: String,
    /// Time at which the owner claimed the entry (RFC3339), or empty.
    pub claimed_at: String,
}

type Handler = Arc<dyn Fn(Message) + Send + Sync>;
type ErrorHandler = Arc<dyn Fn(&OffsetConsumerError) + Send + Sync>;

/// Internal mutable state protected by a mutex.
struct ConsumerState {
    start_seq: u64,
    last_delivered_seq: u64,
    last_committed_seq: u64,
    last_committed_at: String,
    /// Highest sequence handed to a commit call; the release write on close
    /// persists at least this position.
    last_requested_seq: u64,
    last_requested_at: String,
    /// Counts consumption (re)starts; each gets its own ordered consumer
    /// name "{owner}-g{generation}".
    generation: u32,
    consuming: bool,
    closed: bool,
    kv_deleted: bool,
    /// Bumped by seek so that completions of older commits leave the
    /// position alone.
    commit_epoch: u64,
    /// Completion of the last queued KV write; writes are chained to keep
    /// program order.
    write_chain: Option<watch::Receiver<bool>>,
    handler: Option<Handler>,
    shutdown_tx: Option<watch::Sender<bool>>,
    consume_task: Option<JoinHandle<()>>,
    /// Name of the current server-side ordered consumer.
    consumer_name: Option<String>,
}

struct Inner {
    config: ConsumerConfig,
    js: jetstream::Context,
    stream: jetstream::stream::Stream,
    kv: jetstream::kv::Store,
    kv_key: String,
    owner: String,
    host: String,
    claimed_at: String,
    state: Mutex<ConsumerState>,
    error_handler: Option<ErrorHandler>,
    /// Set by close() once the bounded wait expired: queued writes are skipped.
    abandon_pending: Arc<AtomicBool>,
    /// Bound (ms) on how long close() waits for pending writes.
    close_timeout_ms: AtomicU64,
}

/// Provides Kafka-style offset-based consumption on a NATS JetStream stream.
/// All public methods are safe for concurrent use from multiple tasks.
/// Cloning is cheap (shared handle to the same consumer), which lets a
/// message handler commit from inside the callback.
#[derive(Clone)]
pub struct OffsetConsumer {
    inner: Arc<Inner>,
}

impl OffsetConsumer {
    /// Creates a new offset consumer for the given stream and consumer name.
    /// Ensures the offset-consumers KV bucket exists, reads any stored offset,
    /// resolves the start sequence and claims ownership of the offset entry.
    /// Returns `OffsetConsumerError::AlreadyInUse` if another live instance
    /// owns it. Does NOT start consuming — call `consume()` for that.
    pub async fn new(
        js: jetstream::Context,
        config: ConsumerConfig,
        error_handler: Option<ErrorHandler>,
    ) -> Result<Self> {
        if config.consumer_name.is_empty() || config.consumer_name.contains('.') {
            return Err(OffsetConsumerError::InvalidConsumerName);
        }
        if config.stream.is_empty() {
            return Err(OffsetConsumerError::StreamRequired);
        }

        let stream = js.get_stream(&config.stream).await.map_err(|e| {
            OffsetConsumerError::Other(format!("stream {:?} not found: {e}", config.stream))
        })?;
        let replicas = stream.cached_info().config.num_replicas;

        let kv = ensure_bucket(&js, replicas).await?;
        let kv_key = format!("{}.{}", config.stream, config.consumer_name);
        let owner = format!("oc-{}-{}-{}", config.stream, config.consumer_name, nuid::next());
        let host = hostname::get()
            .map(|h| h.to_string_lossy().into_owned())
            .unwrap_or_default();

        // Read the stored entry and check that nobody else owns it.
        let (stored, revision) = read_entry(&kv, &kv_key).await?.unwrap_or_default();
        if !stored.owner.is_empty() {
            check_ownership(&stream, &stored).await?;
        }

        // Claim ownership with a single CAS write that keeps the stored offset.
        let claim = OffsetEntry {
            seq: stored.seq,
            committed_at: stored.committed_at.clone(),
            owner: owner.clone(),
            host: host.clone(),
            claimed_at: rfc3339_now(),
        };
        if let ClaimOutcome::Conflict = claim_entry(&kv, &kv_key, &claim, revision).await? {
            let current = read_entry(&kv, &kv_key)
                .await
                .ok()
                .flatten()
                .map(|(e, _)| e)
                .unwrap_or(stored);
            return Err(OffsetConsumerError::AlreadyInUse {
                owner: current.owner,
                host: current.host,
                claimed_at: current.claimed_at,
            });
        }

        Ok(Self {
            inner: Arc::new(Inner {
                config,
                js,
                stream,
                kv,
                kv_key,
                owner,
                host,
                claimed_at: claim.claimed_at,
                state: Mutex::new(ConsumerState {
                    start_seq: stored.seq,
                    last_delivered_seq: stored.seq,
                    last_committed_seq: stored.seq,
                    last_committed_at: stored.committed_at.clone(),
                    last_requested_seq: stored.seq,
                    last_requested_at: stored.committed_at,
                    generation: 0,
                    consuming: false,
                    closed: false,
                    kv_deleted: false,
                    commit_epoch: 0,
                    write_chain: None,
                    handler: None,
                    shutdown_tx: None,
                    consume_task: None,
                    consumer_name: None,
                }),
                error_handler,
                abandon_pending: Arc::new(AtomicBool::new(false)),
                close_timeout_ms: AtomicU64::new(CLOSE_TIMEOUT.as_millis() as u64),
            }),
        })
    }

    /// Overrides the bound on how long `close()` waits for pending commits.
    /// Intended for tests.
    #[doc(hidden)]
    pub fn set_close_timeout(&self, timeout: Duration) {
        self.inner
            .close_timeout_ms
            .store(timeout.as_millis() as u64, Ordering::Relaxed);
    }

    /// Starts delivering messages from the resolved start position.
    /// The handler is called serially for each message.
    pub async fn consume<F>(&self, handler: F) -> Result<()>
    where
        F: Fn(Message) + Send + Sync + 'static,
    {
        {
            let mut state = self.inner.state.lock().await;
            if state.closed {
                return Err(OffsetConsumerError::ConsumerClosed);
            }
            if state.consuming {
                return Err(OffsetConsumerError::AlreadyConsuming);
            }
            state.consuming = true;
            state.handler = Some(Arc::new(handler));
        }

        if let Err(e) = self.inner.start_source().await {
            let mut state = self.inner.state.lock().await;
            state.consuming = false;
            state.handler = None;
            return Err(e);
        }

        if self.inner.config.auto_commit {
            self.inner.clone().start_auto_commit();
        }
        Ok(())
    }

    /// Persists the last delivered sequence number and waits for the KV
    /// write to be acknowledged.
    pub async fn commit_sync(&self) -> Result<()> {
        let handle = self.inner.commit_async().await?;
        handle
            .await
            .map_err(|e| OffsetConsumerError::Other(format!("commit task failed: {e}")))?
    }

    /// Persists the last delivered sequence number asynchronously. The
    /// sequence is captured at call time; the write is queued behind any
    /// pending writes so commits are applied in call order. Returns a
    /// `JoinHandle` that resolves when the write is acknowledged.
    pub async fn commit_async(&self) -> Result<JoinHandle<Result<()>>> {
        self.inner.commit_async().await
    }

    /// Performs a fresh read of the consumer's offset entry from the KV store.
    /// Returns `None` if the entry does not exist.
    pub async fn info(&self) -> Result<Option<OffsetConsumerInfo>> {
        Ok(read_entry(&self.inner.kv, &self.inner.kv_key)
            .await?
            .map(|(entry, _)| entry.to_info()))
    }

    /// Repositions the consumer to a specific sequence number. After
    /// `seek(N)`, the next message delivered will be sequence N+1.
    /// Consumption is stopped, its ordered consumer deleted, the new offset
    /// written and consumption restarted if it was running. If the write
    /// fails the consumer is left stopped.
    pub async fn seek(&self, seq: u64) -> Result<()> {
        if self.inner.state.lock().await.closed {
            return Err(OffsetConsumerError::ConsumerClosed);
        }

        // Stop delivery; `consuming` stays true until we know the outcome.
        self.inner.stop_source().await;

        // Move the in-memory position and queue the write under one lock so
        // that any commit queued from now on commits the seek position.
        let at = rfc3339_now();
        let data = self.inner.entry_for(seq, &at).to_bytes()?;
        let (was_consuming, previous, handle) = {
            let mut state = self.inner.state.lock().await;
            let previous = (
                state.start_seq,
                state.last_delivered_seq,
                state.last_committed_seq,
                state.last_committed_at.clone(),
            );
            state.commit_epoch += 1;
            state.start_seq = seq;
            state.last_delivered_seq = seq;
            state.last_committed_seq = seq;
            state.last_committed_at = at.clone();
            state.last_requested_seq = seq;
            state.last_requested_at = at;
            let kv = self.inner.kv.clone();
            let key = self.inner.kv_key.clone();
            let handle = self.inner.enqueue_write(&mut state, async move {
                kv.put(key, data)
                    .await
                    .map(|_| ())
                    .map_err(|e| OffsetConsumerError::Other(format!("KV write failed: {e}")))
            });
            (state.consuming, previous, handle)
        };

        let result = match handle.await {
            Ok(r) => r,
            Err(e) => Err(OffsetConsumerError::Other(format!("seek write task failed: {e}"))),
        };
        if let Err(e) = result {
            // Consumer is left stopped; restore the previous position.
            let mut state = self.inner.state.lock().await;
            state.start_seq = previous.0;
            state.last_delivered_seq = previous.1;
            state.last_committed_seq = previous.2;
            state.last_committed_at = previous.3;
            state.consuming = false;
            return Err(e);
        }

        if was_consuming {
            if let Err(e) = self.inner.start_source().await {
                self.inner.state.lock().await.consuming = false;
                return Err(e);
            }
        }
        Ok(())
    }

    /// Repositions the consumer to the beginning of the stream.
    pub async fn seek_to_beginning(&self) -> Result<()> {
        self.seek(0).await
    }

    /// Repositions the consumer to the current end of the stream.
    pub async fn seek_to_end(&self) -> Result<()> {
        let stream = self
            .inner
            .js
            .get_stream(&self.inner.config.stream)
            .await
            .map_err(|e| OffsetConsumerError::Other(format!("failed to get stream info: {e}")))?;
        let last_seq = stream.cached_info().state.last_sequence;
        self.seek(last_seq).await
    }

    /// Removes the offset entry from the KV store. Does not stop consumption;
    /// a later `close()` will not recreate the entry.
    pub async fn delete_consumer(&self) -> Result<()> {
        let handle = {
            let mut state = self.inner.state.lock().await;
            let kv = self.inner.kv.clone();
            let key = self.inner.kv_key.clone();
            self.inner.enqueue_write(&mut state, async move {
                kv.delete(key)
                    .await
                    .map_err(|e| OffsetConsumerError::Other(format!("failed to delete offset entry: {e}")))
            })
        };
        handle
            .await
            .map_err(|e| OffsetConsumerError::Other(format!("delete task failed: {e}")))??;
        self.inner.state.lock().await.kv_deleted = true;
        Ok(())
    }

    /// Stops consumption and releases all resources: waits (bounded, 5s) for
    /// pending async commits, deletes the underlying ordered consumer, and
    /// releases ownership of the offset entry with one final write. That
    /// write persists the highest sequence passed to a commit call (commits
    /// still queued after the wait are abandoned, their handles resolve, and
    /// the position is not lost) and, with auto-commit, the last delivered
    /// sequence.
    pub async fn close(&self) -> Result<()> {
        let pending = {
            let mut state = self.inner.state.lock().await;
            if state.closed {
                return Ok(());
            }
            state.closed = true;
            state.write_chain.clone()
        };

        // Drain pending writes, bounded. Writes still queued after the wait
        // are abandoned; the release write below persists the highest
        // requested position anyway.
        let close_timeout = Duration::from_millis(self.inner.close_timeout_ms.load(Ordering::Relaxed));
        if let Some(rx) = pending {
            let mut first = rx.clone();
            if tokio::time::timeout(close_timeout, first.wait_for(|done| *done)).await.is_err() {
                self.inner.abandon_pending.store(true, Ordering::SeqCst);
                let mut second = rx;
                let _ = tokio::time::timeout(close_timeout, second.wait_for(|done| *done)).await;
            }
        }

        // Stop delivery and delete the ordered consumer.
        self.inner.stop_source().await;

        // Release ownership with one final write. It persists the highest
        // position handed to a commit call (acknowledged or not) and, with
        // auto-commit, any uncommitted messages.
        let (seq, at, deleted) = {
            let mut state = self.inner.state.lock().await;
            state.consuming = false;
            let (mut seq, mut at) = (state.last_committed_seq, state.last_committed_at.clone());
            if state.last_requested_seq > seq {
                seq = state.last_requested_seq;
                at = state.last_requested_at.clone();
            }
            if self.inner.config.auto_commit && state.last_delivered_seq > seq {
                seq = state.last_delivered_seq;
                at = rfc3339_now();
            }
            (seq, at, state.kv_deleted)
        };
        if deleted {
            return Ok(());
        }

        let release = OffsetEntry {
            seq,
            committed_at: at.clone(),
            ..Default::default()
        };
        self.inner
            .kv
            .put(&self.inner.kv_key, release.to_bytes()?)
            .await
            .map_err(|e| OffsetConsumerError::Other(format!("failed to release offset entry: {e}")))?;

        let mut state = self.inner.state.lock().await;
        if seq > state.last_committed_seq {
            state.last_committed_seq = seq;
            state.last_committed_at = at;
        }
        Ok(())
    }
}

impl Inner {
    fn entry_for(&self, seq: u64, committed_at: &str) -> OffsetEntry {
        OffsetEntry {
            seq,
            committed_at: committed_at.to_string(),
            owner: self.owner.clone(),
            host: self.host.clone(),
            claimed_at: self.claimed_at.clone(),
        }
    }

    /// Queues a KV write behind the previous one. Must be called with the
    /// state lock held (passed in) so that submissions are serialized.
    fn enqueue_write<F>(&self, state: &mut ConsumerState, op: F) -> JoinHandle<Result<()>>
    where
        F: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let prev = state.write_chain.take();
        let (done_tx, done_rx) = watch::channel(false);
        state.write_chain = Some(done_rx);
        let abandon = self.abandon_pending.clone();
        tokio::spawn(async move {
            if let Some(mut rx) = prev {
                let _ = rx.wait_for(|done| *done).await;
            }
            // close() gave up waiting: the release write persists the position.
            let result = if abandon.load(Ordering::SeqCst) { Ok(()) } else { op.await };
            let _ = done_tx.send(true);
            result
        })
    }

    async fn commit_async(self: &Arc<Self>) -> Result<JoinHandle<Result<()>>> {
        let mut state = self.state.lock().await;
        if state.closed {
            return Err(OffsetConsumerError::ConsumerClosed);
        }
        let seq = state.last_delivered_seq;
        if seq == 0 {
            return Ok(tokio::spawn(async { Ok(()) }));
        }
        let epoch = state.commit_epoch;
        let at = rfc3339_now();
        if seq > state.last_requested_seq {
            state.last_requested_seq = seq;
            state.last_requested_at = at.clone();
        }
        let data = self.entry_for(seq, &at).to_bytes()?;
        let inner = self.clone();
        Ok(self.enqueue_write(&mut state, async move {
            inner
                .kv
                .put(&inner.kv_key, data)
                .await
                .map_err(|e| OffsetConsumerError::Other(format!("KV write failed: {e}")))?;
            let mut s = inner.state.lock().await;
            if epoch == s.commit_epoch && seq > s.last_committed_seq {
                s.last_committed_seq = seq;
                s.last_committed_at = at;
            }
            Ok(())
        }))
    }

    fn build_ordered_config(&self, state: &ConsumerState, name: String) -> jetstream::consumer::pull::OrderedConfig {
        let deliver_policy = if state.start_seq > 0 {
            DeliverPolicy::ByStartSequence {
                start_sequence: state.start_seq + 1,
            }
        } else if self.config.start_position == StartPosition::End {
            DeliverPolicy::New
        } else {
            DeliverPolicy::All
        };

        jetstream::consumer::pull::OrderedConfig {
            name: Some(name),
            deliver_policy,
            filter_subjects: self.config.filter_subjects.clone(),
            ..Default::default()
        }
    }

    /// Creates an ordered consumer under a new generation name and spawns the
    /// delivery task.
    async fn start_source(self: &Arc<Self>) -> Result<()> {
        let (ordered_config, handler, shutdown_rx) = {
            let mut state = self.state.lock().await;
            state.generation += 1;
            let name = format!("{}-g{}", self.owner, state.generation);
            let config = self.build_ordered_config(&state, name.clone());
            let handler = state.handler.clone().ok_or_else(|| {
                OffsetConsumerError::Other("no message handler installed".to_string())
            })?;
            let (tx, rx) = watch::channel(false);
            state.shutdown_tx = Some(tx);
            state.consumer_name = Some(name);
            (config, handler, rx)
        };

        let consumer = self
            .stream
            .create_consumer(ordered_config)
            .await
            .map_err(|e| OffsetConsumerError::Other(format!("failed to create ordered consumer: {e}")))?;
        let created_name = consumer.cached_info().name.clone();

        let mut messages = match consumer.messages().await {
            Ok(m) => m,
            Err(e) => {
                let _ = self.stream.delete_consumer(&created_name).await;
                return Err(OffsetConsumerError::Other(format!("failed to start consuming: {e}")));
            }
        };

        let inner = self.clone();
        let task = tokio::spawn(async move {
            let mut shutdown_rx = shutdown_rx;
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    next = messages.next() => match next {
                        Some(Ok(msg)) => {
                            if let Ok(info) = msg.info() {
                                let mut s = inner.state.lock().await;
                                s.last_delivered_seq = info.stream_sequence;
                                // The client renames the consumer on every reset.
                                s.consumer_name = Some(info.consumer.to_string());
                            }
                            handler(msg);
                        }
                        Some(Err(e)) => {
                            if let Some(eh) = &inner.error_handler {
                                eh(&OffsetConsumerError::Other(format!("consumer error: {e}")));
                            }
                            break;
                        }
                        None => break,
                    },
                }
            }
        });

        let mut state = self.state.lock().await;
        state.consume_task = Some(task);
        Ok(())
    }

    /// Stops the delivery task and deletes the ordered consumer from the
    /// server so that ownership is visibly released. Best effort.
    async fn stop_source(&self) {
        let (shutdown_tx, task, name) = {
            let mut state = self.state.lock().await;
            (
                state.shutdown_tx.take(),
                state.consume_task.take(),
                state.consumer_name.take(),
            )
        };
        if let Some(tx) = shutdown_tx {
            let _ = tx.send(true);
        }
        if let Some(task) = task {
            if tokio::time::timeout(CLOSE_TIMEOUT, task).await.is_err() {
                // the handler is stuck; the consumer below is deleted anyway
            }
        }
        if let Some(name) = name {
            let _ = self.stream.delete_consumer(&name).await;
        }
    }

    fn start_auto_commit(self: Arc<Self>) {
        let interval = self.config.auto_commit_interval;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // skip first tick
            loop {
                ticker.tick().await;
                let needs_commit = {
                    let s = self.state.lock().await;
                    if s.closed {
                        break;
                    }
                    s.last_delivered_seq > s.last_committed_seq
                };
                if !needs_commit {
                    continue;
                }
                let result = match self.commit_async().await {
                    Ok(handle) => match handle.await {
                        Ok(r) => r,
                        Err(e) => Err(OffsetConsumerError::Other(format!("commit task failed: {e}"))),
                    },
                    Err(e) => Err(e),
                };
                if let Err(e) = result {
                    if matches!(e, OffsetConsumerError::ConsumerClosed) {
                        break;
                    }
                    if let Some(eh) = &self.error_handler {
                        eh(&e);
                    }
                }
            }
        });
    }
}

/// Returns `AlreadyInUse` if the entry's owner is still alive: either its
/// claim is younger than the inactive threshold, or a consumer with its
/// prefix still exists on the stream.
async fn check_ownership(stream: &jetstream::stream::Stream, stored: &OffsetEntry) -> Result<()> {
    let in_use = || OffsetConsumerError::AlreadyInUse {
        owner: stored.owner.clone(),
        host: stored.host.clone(),
        claimed_at: stored.claimed_at.clone(),
    };

    if let Ok(claimed) = chrono::DateTime::parse_from_rfc3339(&stored.claimed_at) {
        let age = chrono::Utc::now().signed_duration_since(claimed.with_timezone(&chrono::Utc));
        if age < chrono::Duration::from_std(INACTIVE_THRESHOLD).unwrap_or(chrono::Duration::zero()) {
            return Err(in_use());
        }
    }

    let mut names = stream.consumer_names();
    while let Some(name) = names.next().await {
        let name = name.map_err(|e| OffsetConsumerError::Other(format!("failed to list consumers: {e}")))?;
        if name.starts_with(&stored.owner) {
            return Err(in_use());
        }
    }
    Ok(())
}

/// Lists all offset consumers for the given stream.
/// The Rust async-nats client does not support server-side filtered key listing,
/// so this fetches all keys and filters client-side by the "{stream}." prefix.
/// Returns a map of consumer name to OffsetConsumerInfo, empty if none exist.
pub async fn list_consumers(
    js: &jetstream::Context,
    stream: &str,
) -> Result<HashMap<String, OffsetConsumerInfo>> {
    let mut result = HashMap::new();

    let kv = match js.get_key_value(KV_BUCKET_NAME).await {
        Ok(store) => store,
        Err(e) if e.kind() == KeyValueErrorKind::GetBucket => return Ok(result),
        Err(e) => return Err(OffsetConsumerError::Other(format!("failed to open KV bucket: {e}"))),
    };

    let prefix = format!("{stream}.");
    let mut keys = kv
        .keys()
        .await
        .map_err(|e| OffsetConsumerError::Other(format!("failed to list keys: {e}")))?;

    while let Some(key) = keys.next().await {
        let key = key.map_err(|e| OffsetConsumerError::Other(format!("failed to list keys: {e}")))?;
        let Some(consumer_name) = key.strip_prefix(&prefix) else {
            continue;
        };
        if consumer_name.is_empty() {
            continue;
        }
        if let Some((entry, _)) = read_entry(&kv, &key).await? {
            result.insert(consumer_name.to_string(), entry.to_info());
        }
    }

    Ok(result)
}
