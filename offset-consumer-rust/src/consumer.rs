use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::consumer::pull::{Ordered, OrderedErrorKind};
use async_nats::jetstream::context::KeyValueErrorKind;
use async_nats::jetstream::{self, consumer::DeliverPolicy, Message};
use futures::future::BoxFuture;
use futures::StreamExt;
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;

use crate::config::{ConsumerConfig, StartPosition};
use crate::errors::{OffsetConsumerError, Result};
use crate::kv::{
    claim_entry, ensure_bucket, read_entry, rfc3339_now, stream_created_id, stream_was_recreated,
    validate_names, ClaimOutcome, OffsetEntry, KV_BUCKET_NAME,
};

/// Inactivity threshold async-nats applies to ordered consumers (it is not
/// configurable). After an instance crashes, its ordered consumer, and with
/// it the ownership of the offset entry, is released by the server once this
/// elapses.
pub const INACTIVE_THRESHOLD: Duration = Duration::from_secs(30);

/// How long a starter waits, measured with its own clock, before taking over
/// an entry whose owner has no consumer on the server. It must exceed the
/// time a live owner can be without one (an ordered consumer reset deletes
/// the old consumer before creating the new one). No jitter is needed:
/// concurrent starters are decided by the claim's CAS.
const LIVENESS_WAIT: Duration = Duration::from_secs(5);

/// Bounds how often a claim is retried after losing a CAS race to a write
/// that did not change the owner (a release, or this instance's own late
/// commit).
const CLAIM_ROUNDS: u32 = 3;

/// Bound on how long `close` waits for pending writes.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Bounds how often `close` re-reads the entry when its conditional release
/// write loses a race.
const RELEASE_ATTEMPTS: u32 = 3;

/// How often the stream identity is checked while consuming. async-nats
/// handles ordered consumer resets internally and exposes no reset signal: a
/// stream deleted and recreated makes the ordered stream silently recreate
/// its consumer at the old position on the new stream (skipping messages, or
/// waiting forever if the new stream is shorter). A changed consumer name on
/// a delivered message reveals a reset that delivers; this periodic check
/// covers the one that waits.
const STREAM_CHECK_INTERVAL: Duration = Duration::from_secs(5);

/// Backoff after an error yielded by the ordered stream, doubled on each
/// consecutive error up to `ERROR_BACKOFF_MAX` and reset by a message.
const ERROR_BACKOFF_MIN: Duration = Duration::from_millis(100);
const ERROR_BACKOFF_MAX: Duration = Duration::from_secs(5);

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
    /// Identifies the incarnation of the stream the offset refers to: the
    /// stream's creation time in Unix nanoseconds, as a decimal string. Empty
    /// for entries written by older versions.
    pub stream_created: String,
}

type Handler = Arc<dyn Fn(Message) + Send + Sync>;
type ErrorHandler = Arc<dyn Fn(&OffsetConsumerError) + Send + Sync>;

/// Internal mutable state protected by a mutex.
struct ConsumerState {
    start_seq: u64,
    last_delivered_seq: u64,
    /// Last sequence whose handler has returned. Auto-commit and the final
    /// commit in close use it, so a message still being handled is never
    /// committed implicitly.
    last_processed_seq: u64,
    last_committed_seq: u64,
    last_committed_at: String,
    /// Highest sequence handed to a commit call; the release write on close
    /// persists at least this position.
    last_requested_seq: u64,
    last_requested_at: String,
    /// Identifies the stream incarnation (see `OffsetConsumerInfo::stream_created`).
    /// Updated when a recreation is detected.
    stream_created: String,
    /// Counts consumption (re)starts; each gets its own ordered consumer
    /// name "{owner}-g{generation}".
    generation: u32,
    consuming: bool,
    closed: bool,
    kv_deleted: bool,
    /// Set once this instance has claimed the entry (at its first consume);
    /// until then commits and close write nothing.
    claimed: bool,
    /// Set by a seek while not consuming: the claim at the next consume
    /// stores `start_seq` (and `last_committed_at`).
    pending_seek: bool,
    /// The start position resolved from `start_position` at construction
    /// (re-resolved by the claim if the stream was recreated since), stored
    /// by the claim when there is no usable stored offset.
    initial_seq: u64,
    initial_at: String,
    /// Set while a detected stream recreation is handled; messages delivered
    /// meanwhile are dropped.
    repositioning: bool,
    /// Bumped by seek so that completions of older commits leave the
    /// position alone.
    commit_epoch: u64,
    /// Completion of the last queued KV write; writes are chained to keep
    /// program order.
    write_chain: Option<watch::Receiver<bool>>,
    handler: Option<Handler>,
    shutdown_tx: Option<watch::Sender<bool>>,
    consume_task: Option<JoinHandle<()>>,
    auto_commit_stop: Option<watch::Sender<bool>>,
    /// Name of the current server-side ordered consumer: the created name,
    /// then the name carried by the last delivered message. The client
    /// renames the consumer on every reset, so a change reveals one.
    consumer_name: Option<String>,
}

impl ConsumerState {
    /// Reports whether generation `generation` is the active consumption.
    fn is_current(&self, generation: u32) -> bool {
        self.shutdown_tx.is_some() && self.generation == generation && !self.repositioning && !self.closed
    }
}

struct Inner {
    config: ConsumerConfig,
    js: jetstream::Context,
    stream: jetstream::stream::Stream,
    kv: jetstream::kv::Store,
    kv_key: String,
    owner: String,
    host: String,
    /// Serializes consume, seek and close so that at most one ordered
    /// consumer is ever active. Lock order: `life`, then `state`.
    life: Mutex<()>,
    state: Mutex<ConsumerState>,
    error_handler: Option<ErrorHandler>,
    /// Set by close() once the bounded wait expired: queued writes are skipped.
    abandon_pending: Arc<AtomicBool>,
    /// Bound (ms) on how long close() waits for pending writes.
    close_timeout_ms: AtomicU64,
    /// Liveness wait (ms) of the claim, see `LIVENESS_WAIT`.
    liveness_wait_ms: AtomicU64,
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
    /// Ensures the offset-consumers KV bucket exists and checks the stored
    /// offset entry; it writes nothing and creates nothing on the server.
    /// Returns `OffsetConsumerError::AlreadyInUse` if the entry's owner is
    /// visibly alive (a consumer whose name starts with its owner prefix
    /// exists on the stream). Ownership is claimed when consumption starts
    /// (see `consume()`). `StartPosition::End` is resolved to the stream's
    /// last sequence here; the claim stores it if there is no usable stored
    /// offset by then. Does NOT start consuming — call `consume()` for that.
    pub async fn new(
        js: jetstream::Context,
        config: ConsumerConfig,
        error_handler: Option<ErrorHandler>,
    ) -> Result<Self> {
        if config.stream.is_empty() {
            return Err(OffsetConsumerError::StreamRequired);
        }
        validate_names(&config.stream, &config.consumer_name)?;

        let stream = js.get_stream(&config.stream).await.map_err(|e| {
            OffsetConsumerError::Other(format!("stream {:?} not found: {e}", config.stream))
        })?;
        let info = stream.cached_info();
        let replicas = info.config.num_replicas;
        let stream_created = stream_created_id(info);
        let last_seq = info.state.last_sequence;

        let kv = ensure_bucket(&js, replicas).await?;
        let kv_key = format!("{}.{}", config.stream, config.consumer_name);
        let owner = format!("oc-{}-{}-{}", config.stream, config.consumer_name, nuid::next());
        let host = hostname::get()
            .map(|h| h.to_string_lossy().into_owned())
            .unwrap_or_default();

        let (initial_seq, initial_at) = initial_position(config.start_position, last_seq);

        // Fail fast if the entry is visibly owned. This check does not wait:
        // the full check runs when consumption starts.
        if let Some((stored, _)) = read_entry(&kv, &kv_key).await? {
            if !stored.owner.is_empty() && owner_alive(&stream, &stored.owner).await? {
                return Err(OffsetConsumerError::AlreadyInUse {
                    owner: stored.owner,
                    host: stored.host,
                });
            }
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
                life: Mutex::new(()),
                state: Mutex::new(ConsumerState {
                    start_seq: 0,
                    last_delivered_seq: 0,
                    last_processed_seq: 0,
                    last_committed_seq: 0,
                    last_committed_at: String::new(),
                    last_requested_seq: 0,
                    last_requested_at: String::new(),
                    stream_created,
                    generation: 0,
                    consuming: false,
                    closed: false,
                    kv_deleted: false,
                    claimed: false,
                    pending_seek: false,
                    initial_seq,
                    initial_at,
                    repositioning: false,
                    commit_epoch: 0,
                    write_chain: None,
                    handler: None,
                    shutdown_tx: None,
                    consume_task: None,
                    auto_commit_stop: None,
                    consumer_name: None,
                }),
                error_handler,
                abandon_pending: Arc::new(AtomicBool::new(false)),
                close_timeout_ms: AtomicU64::new(CLOSE_TIMEOUT.as_millis() as u64),
                liveness_wait_ms: AtomicU64::new(LIVENESS_WAIT.as_millis() as u64),
            }),
        })
    }

    /// Overrides the liveness wait of the claim made by `consume()`.
    /// Intended for tests.
    #[doc(hidden)]
    pub fn set_liveness_wait(&self, wait: Duration) {
        self.inner.liveness_wait_ms.store(wait.as_millis() as u64, Ordering::Relaxed);
    }

    /// Overrides the bound on how long `close()` waits for pending commits.
    /// Intended for tests.
    #[doc(hidden)]
    pub fn set_close_timeout(&self, timeout: Duration) {
        self.inner
            .close_timeout_ms
            .store(timeout.as_millis() as u64, Ordering::Relaxed);
    }

    /// Claims ownership of the offset entry (see Ownership in the contract)
    /// and starts delivering messages from the resolved start position: the
    /// pending seek position if `seek` was called while not consuming, else
    /// the stored offset, else the position resolved from `start_position`
    /// at construction (also when the stored offset belongs to an earlier
    /// incarnation of the stream, which is reported as `StreamRecreated`).
    /// The claim runs every time consumption starts from a stopped state and
    /// returns `OffsetConsumerError::AlreadyInUse` if another instance owns
    /// the entry. The handler is called serially for each message.
    pub async fn consume<F>(&self, handler: F) -> Result<()>
    where
        F: Fn(Message) + Send + Sync + 'static,
    {
        let _life = self.inner.life.lock().await;
        {
            let state = self.inner.state.lock().await;
            if state.closed {
                return Err(OffsetConsumerError::ConsumerClosed);
            }
            if state.consuming {
                return Err(OffsetConsumerError::AlreadyConsuming);
            }
        }

        // Claim (or re-validate) ownership every time consumption starts
        // from a stopped state: while not consuming, the instance has no
        // consumer on the server and another instance may have taken the
        // entry over.
        self.inner.claim().await?;

        {
            let mut state = self.inner.state.lock().await;
            if state.closed {
                return Err(OffsetConsumerError::ConsumerClosed);
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
            let mut state = self.inner.state.lock().await;
            if state.auto_commit_stop.is_none() {
                let (tx, rx) = watch::channel(false);
                state.auto_commit_stop = Some(tx);
                self.inner.clone().start_auto_commit(rx);
            }
        }
        Ok(())
    }

    /// Persists the last delivered sequence number and waits for the KV
    /// write to be acknowledged. Called from inside the handler (through a
    /// clone of the consumer), it includes the message being handled.
    pub async fn commit_sync(&self) -> Result<()> {
        await_commit(self.inner.commit_async(false).await?).await
    }

    /// Persists the last delivered sequence number asynchronously. The
    /// sequence is captured at call time; the write is queued behind any
    /// pending writes so commits are applied in call order. Returns a
    /// `JoinHandle` that resolves when the write is acknowledged.
    pub async fn commit_async(&self) -> Result<JoinHandle<Result<()>>> {
        self.inner.commit_async(false).await
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
    /// While consuming, it stops delivery, writes the new offset, starts a
    /// new ordered consumer under the next generation name, and only then
    /// deletes the previous one, so the instance stays visible on the server
    /// throughout. If the write fails the consumer is left stopped with its
    /// position unchanged. While not consuming, the position is only
    /// recorded: the claim at the next `consume()` stores it.
    pub async fn seek(&self, seq: u64) -> Result<()> {
        self.inner.seek(seq, String::new()).await
    }

    /// Repositions the consumer to the beginning of the stream, regardless
    /// of `start_position`.
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
    /// the position is not lost) and, with auto-commit, the last sequence
    /// whose handler has returned. A message whose handler is still running
    /// is not committed by close (and close does not wait for it): it is
    /// redelivered on the next start. Close does not wait for the
    /// auto-commit task either, so the error handler may trigger a close.
    /// The release write is conditional: if the entry no longer names this
    /// instance as its owner (another instance claimed it after this one's
    /// ordered consumer expired), it is left untouched and close returns
    /// `OffsetConsumerError::AlreadyInUse` describing the entry's owner; if
    /// the entry no longer exists, nothing is written. All other cleanup
    /// happens in every case. Before the first claim (no `consume()` yet)
    /// close writes nothing.
    pub async fn close(&self) -> Result<()> {
        let _life = self.inner.life.lock().await;
        let pending = {
            let mut state = self.inner.state.lock().await;
            if state.closed {
                return Ok(());
            }
            state.closed = true;
            if let Some(stop) = state.auto_commit_stop.take() {
                let _ = stop.send(true);
            }
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

        // Release ownership with one final conditional write (see
        // `Inner::release`). It persists the highest position handed to a
        // commit call (acknowledged or not) and, with auto-commit, the
        // messages processed since the last commit.
        let (release, deleted) = {
            let mut state = self.inner.state.lock().await;
            state.consuming = false;
            let (mut seq, mut at) = (state.last_committed_seq, state.last_committed_at.clone());
            if state.last_requested_seq > seq {
                seq = state.last_requested_seq;
                at = state.last_requested_at.clone();
            }
            if self.inner.config.auto_commit && state.last_processed_seq > seq {
                seq = state.last_processed_seq;
                at = rfc3339_now();
            }
            let release = OffsetEntry {
                seq,
                committed_at: at,
                stream_created: state.stream_created.clone(),
                ..Default::default()
            };
            // Before the first claim the entry is not ours to write.
            (release, state.kv_deleted || !state.claimed)
        };

        if deleted || !self.inner.release(&release).await? {
            return Ok(());
        }

        let mut state = self.inner.state.lock().await;
        if release.seq > state.last_committed_seq {
            state.last_committed_seq = release.seq;
            state.last_committed_at = release.committed_at;
        }
        Ok(())
    }
}

/// Returns the position to start from when there is no usable stored
/// offset: 0 (deliver all) for `Beginning`, the stream's last sequence for `End`.
fn initial_position(start_position: StartPosition, last_seq: u64) -> (u64, String) {
    if start_position == StartPosition::End && last_seq > 0 {
        (last_seq, rfc3339_now())
    } else {
        (0, String::new())
    }
}

async fn await_commit(handle: JoinHandle<Result<()>>) -> Result<()> {
    handle
        .await
        .map_err(|e| OffsetConsumerError::Other(format!("commit task failed: {e}")))?
}

impl Inner {
    /// Writes the release entry only while the entry still names this
    /// instance as its owner, with a compare-and-set against the revision
    /// just read. An instance that was displaced without noticing (its
    /// ordered consumer expired and another instance claimed the entry)
    /// therefore cannot clear the new owner's claim, which would let a third
    /// instance in without a liveness check. A revision conflict means a
    /// write landed between the read and the release, most likely one of this
    /// instance's own late commits, so the entry is read again (at most
    /// `RELEASE_ATTEMPTS` attempts in total). Returns `Ok(false)` when the
    /// entry no longer exists (nothing to release) and `AlreadyInUse` when
    /// another instance owns it.
    async fn release(&self, release: &OffsetEntry) -> Result<bool> {
        let failed = |e: OffsetConsumerError| {
            let reason = match e {
                OffsetConsumerError::Other(msg) => msg,
                e => e.to_string(),
            };
            OffsetConsumerError::Other(format!("failed to release offset entry: {reason}"))
        };
        for attempt in 1..=RELEASE_ATTEMPTS {
            let Some((current, revision)) = read_entry(&self.kv, &self.kv_key).await.map_err(failed)? else {
                return Ok(false);
            };
            if current.owner != self.owner {
                // Release skipped: the entry is no longer owned by this instance.
                return Err(OffsetConsumerError::AlreadyInUse {
                    owner: current.owner,
                    host: current.host,
                });
            }
            match claim_entry(&self.kv, &self.kv_key, release, revision).await.map_err(failed)? {
                ClaimOutcome::Claimed(_) => return Ok(true),
                ClaimOutcome::Conflict if attempt < RELEASE_ATTEMPTS => {}
                ClaimOutcome::Conflict => break,
            }
        }
        Err(OffsetConsumerError::Other(
            "failed to release offset entry: wrong last sequence (the entry kept changing)".to_string(),
        ))
    }

    /// Passes `err` to the configured error handler, if any.
    fn report(&self, err: &OffsetConsumerError) {
        if let Some(eh) = &self.error_handler {
            eh(err);
        }
    }

    /// Builds the entry to write for a commit at `seq`, carrying the
    /// instance's ownership fields and the stream identity.
    fn entry_for(&self, state: &ConsumerState, seq: u64, committed_at: &str) -> OffsetEntry {
        OffsetEntry {
            seq,
            committed_at: committed_at.to_string(),
            owner: self.owner.clone(),
            host: self.host.clone(),
            stream_created: state.stream_created.clone(),
        }
    }

    /// Takes ownership of the offset entry before consumption starts, and
    /// stores the position consumption starts from: the pending seek
    /// position if `seek` was called while not consuming, else the stored
    /// offset, else the position resolved from `start_position` at
    /// construction (also when the stored offset belongs to an earlier
    /// incarnation of the stream).
    ///
    /// The stored owner, if it is another instance, is alive while a consumer
    /// whose name starts with its owner prefix exists. If none exists, the
    /// claim waits the liveness wait (measured locally) and
    /// lists again, which rides out an ordered consumer reset; it then writes
    /// with a CAS against the revision read before the wait, so any write
    /// during the wait (a commit by the owner, another instance's claim)
    /// makes it fail. No clocks are compared. Must be called with the
    /// lifecycle lock held and the state lock not held.
    async fn claim(&self) -> Result<()> {
        for round in 1..=CLAIM_ROUNDS {
            let stored = read_entry(&self.kv, &self.kv_key).await?;
            let found = stored.is_some();
            let (entry, revision) = stored.unwrap_or_default();
            if found && !entry.owner.is_empty() && entry.owner != self.owner {
                self.check_ownership(&entry).await?;
            }

            let info = self
                .stream
                .get_info()
                .await
                .map_err(|e| OffsetConsumerError::Other(format!("failed to get stream info: {e}")))?;

            // The state lock is held across the CAS write so that no commit
            // is submitted between the write and the position update.
            let mut state = self.state.lock().await;
            let current = stream_created_id(&info);
            if !current.is_empty() && current != state.stream_created {
                // The stream was recreated since construction: the start
                // position resolved then refers to the old stream.
                state.stream_created = current;
                (state.initial_seq, state.initial_at) =
                    initial_position(self.config.start_position, info.state.last_sequence);
            }
            let recreated = found && stream_was_recreated(&entry, &state.stream_created, info.state.last_sequence);
            let (seq, committed_at) = if state.pending_seek {
                (state.start_seq, state.last_committed_at.clone())
            } else if !found || recreated {
                (state.initial_seq, state.initial_at.clone())
            } else {
                (entry.seq, entry.committed_at.clone())
            };
            let claim = OffsetEntry {
                seq,
                committed_at: committed_at.clone(),
                owner: self.owner.clone(),
                host: self.host.clone(),
                stream_created: state.stream_created.clone(),
            };
            let revision = if found { revision } else { 0 };
            match claim_entry(&self.kv, &self.kv_key, &claim, revision).await? {
                ClaimOutcome::Claimed(_) => {
                    state.claimed = true;
                    state.pending_seek = false;
                    state.kv_deleted = false;
                    state.commit_epoch += 1;
                    state.start_seq = seq;
                    state.last_delivered_seq = seq;
                    state.last_processed_seq = seq;
                    state.last_committed_seq = seq;
                    state.last_committed_at = committed_at.clone();
                    state.last_requested_seq = seq;
                    state.last_requested_at = committed_at;
                    drop(state);
                    if recreated {
                        self.report(&OffsetConsumerError::StreamRecreated { stored_seq: entry.seq });
                    }
                    return Ok(());
                }
                ClaimOutcome::Conflict => drop(state),
            }

            // Someone wrote the entry meanwhile. Another owner just proved it
            // is alive; a release (empty owner) or this instance's own late
            // commit is retried.
            if let Some((current, _)) = read_entry(&self.kv, &self.kv_key).await? {
                if !current.owner.is_empty() && current.owner != self.owner {
                    return Err(OffsetConsumerError::AlreadyInUse {
                        owner: current.owner,
                        host: current.host,
                    });
                }
            }
            if round == CLAIM_ROUNDS {
                break;
            }
        }
        Err(OffsetConsumerError::Other(
            "failed to claim offset entry: wrong last sequence (the entry kept changing)".to_string(),
        ))
    }

    /// Returns `AlreadyInUse` if the entry's owner (another instance) is
    /// alive: a consumer whose name starts with its owner prefix exists on
    /// the stream, now or after the liveness wait.
    async fn check_ownership(&self, entry: &OffsetEntry) -> Result<()> {
        for attempt in 0..2 {
            if attempt > 0 {
                let wait = Duration::from_millis(self.liveness_wait_ms.load(Ordering::Relaxed));
                tokio::time::sleep(wait).await;
            }
            if owner_alive(&self.stream, &entry.owner).await? {
                return Err(OffsetConsumerError::AlreadyInUse {
                    owner: entry.owner.clone(),
                    host: entry.host.clone(),
                });
            }
        }
        Ok(())
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

    /// Queues a commit of the last delivered sequence, or with
    /// `processed_only` of the last processed sequence (auto-commit).
    async fn commit_async(self: &Arc<Self>, processed_only: bool) -> Result<JoinHandle<Result<()>>> {
        let mut state = self.state.lock().await;
        if state.closed {
            return Err(OffsetConsumerError::ConsumerClosed);
        }
        if !state.claimed {
            // Nothing was delivered, and the entry is not ours to write yet.
            return Ok(tokio::spawn(async { Ok(()) }));
        }
        let mut seq = state.last_delivered_seq;
        if processed_only {
            seq = state.last_processed_seq;
            if seq <= state.last_requested_seq {
                // An explicit commit already covers it.
                return Ok(tokio::spawn(async { Ok(()) }));
            }
        }
        if seq == 0 {
            return Ok(tokio::spawn(async { Ok(()) }));
        }
        let epoch = state.commit_epoch;
        let at = rfc3339_now();
        if seq > state.last_requested_seq {
            state.last_requested_seq = seq;
            state.last_requested_at = at.clone();
        }
        let data = self.entry_for(&state, seq, &at).to_bytes()?;
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

    /// Implements `seek`. A non-empty `stream_created` switches the consumer
    /// to a new stream incarnation atomically with the seek write: commits
    /// queued before it carry the old identity, commits queued after it the
    /// new one (with the seek position). Like the position fields, the
    /// identity is restored if the write fails.
    async fn seek(self: &Arc<Self>, seq: u64, stream_created: String) -> Result<()> {
        let _life = self.life.lock().await;
        let at = rfc3339_now();
        {
            let mut state = self.state.lock().await;
            if state.closed {
                return Err(OffsetConsumerError::ConsumerClosed);
            }
            if !state.consuming {
                // Not consuming: no consumer protects the entry, so do not
                // write it. The claim at the next consume stores the position.
                state.start_seq = seq;
                state.last_delivered_seq = seq;
                state.last_processed_seq = seq;
                state.last_committed_at = at;
                state.pending_seek = true;
                if !stream_created.is_empty() {
                    state.stream_created = stream_created;
                }
                return Ok(());
            }
        }

        // Stop delivery but keep the server-side consumer until the new one
        // exists (make before break). `consuming` stays true until we know
        // the outcome.
        let old = self.stop_delivery().await;
        let result = self.seek_write(seq, stream_created, at).await;
        // The new generation is visible (or the seek failed): delete the old one.
        if let Some(name) = old {
            let _ = self.stream.delete_consumer(&name).await;
        }
        result
    }

    /// The part of a seek while consuming after delivery stopped: writes the
    /// offset and starts the next generation.
    async fn seek_write(self: &Arc<Self>, seq: u64, stream_created: String, at: String) -> Result<()> {
        // Move the in-memory position and queue the write under one lock so
        // that any commit queued from now on commits the seek position.
        let (previous, handle) = {
            let mut state = self.state.lock().await;
            let mut entry = self.entry_for(&state, seq, &at);
            if !stream_created.is_empty() {
                entry.stream_created = stream_created.clone();
            }
            let data = match entry.to_bytes() {
                Ok(data) => data,
                Err(e) => {
                    state.consuming = false;
                    return Err(e);
                }
            };
            let previous = (
                state.start_seq,
                state.last_delivered_seq,
                state.last_processed_seq,
                state.last_committed_seq,
                state.last_committed_at.clone(),
                state.last_requested_seq,
                state.last_requested_at.clone(),
                state.stream_created.clone(),
            );
            state.commit_epoch += 1;
            if !stream_created.is_empty() {
                state.stream_created = stream_created;
            }
            state.start_seq = seq;
            state.last_delivered_seq = seq;
            state.last_processed_seq = seq;
            state.last_committed_seq = seq;
            state.last_committed_at = at.clone();
            state.last_requested_seq = seq;
            state.last_requested_at = at;
            let kv = self.kv.clone();
            let key = self.kv_key.clone();
            let handle = self.enqueue_write(&mut state, async move {
                kv.put(key, data)
                    .await
                    .map(|_| ())
                    .map_err(|e| OffsetConsumerError::Other(format!("KV write failed: {e}")))
            });
            (previous, handle)
        };

        let result = match handle.await {
            Ok(r) => r,
            Err(e) => Err(OffsetConsumerError::Other(format!("seek write task failed: {e}"))),
        };
        let mut state = self.state.lock().await;
        if let Err(e) = result {
            // Consumer is left stopped; restore every position field, so
            // that close() does not persist the failed seek target.
            state.start_seq = previous.0;
            state.last_delivered_seq = previous.1;
            state.last_processed_seq = previous.2;
            state.last_committed_seq = previous.3;
            state.last_committed_at = previous.4;
            state.last_requested_seq = previous.5;
            state.last_requested_at = previous.6;
            state.stream_created = previous.7;
            state.consuming = false;
            return Err(e);
        }
        state.kv_deleted = false; // the seek write recreated the entry

        // The lifecycle lock guarantees nothing else started consuming
        // meanwhile; the shutdown_tx check is a second line of defence
        // against a leaked ordered consumer.
        let restart = state.consuming && !state.closed && state.shutdown_tx.is_none();
        drop(state);
        if restart {
            if let Err(e) = self.start_source().await {
                self.state.lock().await.consuming = false;
                return Err(e);
            }
        }
        Ok(())
    }

    fn build_ordered_config(&self, state: &ConsumerState, name: String) -> jetstream::consumer::pull::OrderedConfig {
        // The start position is fully resolved by the claim and seek(): 0 means
        // the beginning of the stream.
        let deliver_policy = if state.start_seq > 0 {
            DeliverPolicy::ByStartSequence {
                start_sequence: state.start_seq + 1,
            }
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
    /// delivery task. Must be called with the lifecycle lock held.
    async fn start_source(self: &Arc<Self>) -> Result<()> {
        let (ordered_config, handler, shutdown_rx, generation) = {
            let mut state = self.state.lock().await;
            let handler = state.handler.clone().ok_or_else(|| {
                OffsetConsumerError::Other("no message handler installed".to_string())
            })?;
            state.generation += 1;
            state.repositioning = false;
            let name = format!("{}-g{}", self.owner, state.generation);
            let config = self.build_ordered_config(&state, name.clone());
            let (tx, rx) = watch::channel(false);
            state.shutdown_tx = Some(tx);
            state.consumer_name = Some(name);
            (config, handler, rx, state.generation)
        };

        let started = async {
            let consumer = self
                .stream
                .create_consumer(ordered_config)
                .await
                .map_err(|e| OffsetConsumerError::Other(format!("failed to create ordered consumer: {e}")))?;
            let created_name = consumer.cached_info().name.clone();
            match consumer.messages().await {
                Ok(m) => Ok(m),
                Err(e) => {
                    let _ = self.stream.delete_consumer(&created_name).await;
                    Err(OffsetConsumerError::Other(format!("failed to start consuming: {e}")))
                }
            }
        }
        .await;
        let messages = match started {
            Ok(m) => m,
            Err(e) => {
                let mut state = self.state.lock().await;
                state.shutdown_tx = None;
                state.consumer_name = None;
                return Err(e);
            }
        };

        let inner = self.clone();
        let task = tokio::spawn(async move {
            inner.run_delivery(generation, messages, handler, shutdown_rx).await;
        });

        let mut state = self.state.lock().await;
        state.consume_task = Some(task);
        Ok(())
    }

    /// The delivery loop of one generation. Errors yielded by the ordered
    /// stream are reported and polling continues (async-nats keeps the
    /// stream usable and recreates its consumer as needed), with a backoff
    /// so that a persistent error cannot spin. The end of the stream is
    /// reported as `ConsumptionStopped` and stops consumption.
    async fn run_delivery(
        self: Arc<Self>,
        generation: u32,
        mut messages: Ordered,
        handler: Handler,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        let mut backoff = ERROR_BACKOFF_MIN;
        let mut check = tokio::time::interval_at(
            tokio::time::Instant::now() + STREAM_CHECK_INTERVAL,
            STREAM_CHECK_INTERVAL,
        );
        check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => return,
                _ = check.tick() => self.check_stream(generation).await,
                next = messages.next() => match next {
                    Some(Ok(msg)) => {
                        backoff = ERROR_BACKOFF_MIN;
                        self.deliver(generation, &handler, msg).await;
                    }
                    // The recreation retries are exhausted; the ordered
                    // stream cannot be polled any further.
                    Some(Err(e)) if e.kind() == OrderedErrorKind::Recreate => {
                        self.stopped(generation, format!("consumer error: {e}")).await;
                        return;
                    }
                    Some(Err(e)) => {
                        if !self.state.lock().await.is_current(generation) {
                            return;
                        }
                        self.report(&OffsetConsumerError::Other(format!("consumer error: {e}")));
                        tokio::select! {
                            biased;
                            _ = shutdown_rx.changed() => return,
                            _ = tokio::time::sleep(backoff) => {}
                        }
                        backoff = (backoff * 2).min(ERROR_BACKOFF_MAX);
                    }
                    None => {
                        self.stopped(generation, "the ordered consumer's message stream ended".to_string()).await;
                        return;
                    }
                },
            }
        }
    }

    /// Tracks positions around one handler call. Messages from a stopped or
    /// replaced generation are dropped, so a seek is never undone by a late
    /// message of the previous ordered consumer.
    async fn deliver(self: &Arc<Self>, generation: u32, handler: &Handler, msg: Message) {
        let (seq, consumer) = match msg.info() {
            Ok(info) => (info.stream_sequence, info.consumer.to_string()),
            Err(_) => {
                handler(msg);
                return;
            }
        };

        let reset = {
            let mut s = self.state.lock().await;
            if !s.is_current(generation) {
                return;
            }
            let reset = s.consumer_name.as_deref() != Some(consumer.as_str());
            s.consumer_name = Some(consumer);
            reset
        };

        // The client recreated the ordered consumer. If that is because the
        // stream was deleted and recreated, the ordered consumer silently
        // resumes at the old position on the new stream: reposition instead.
        if reset {
            if let Some(created) = self.stream_changed().await {
                self.begin_reposition(generation, created).await;
                return;
            }
        }

        {
            let mut s = self.state.lock().await;
            if !s.is_current(generation) {
                return;
            }
            s.last_delivered_seq = seq;
        }

        handler(msg);

        let mut s = self.state.lock().await;
        if s.is_current(generation) && seq > s.last_processed_seq {
            s.last_processed_seq = seq;
        }
    }

    /// Periodic check of the stream identity while consuming.
    async fn check_stream(self: &Arc<Self>, generation: u32) {
        if let Some(created) = self.stream_changed().await {
            self.begin_reposition(generation, created).await;
        }
    }

    /// Returns the stream's current identity if it differs from the one the
    /// offsets refer to.
    async fn stream_changed(&self) -> Option<String> {
        let info = self.stream.get_info().await.ok()?;
        let created = stream_created_id(&info);
        let state = self.state.lock().await;
        (!created.is_empty() && !state.stream_created.is_empty() && created != state.stream_created)
            .then_some(created)
    }

    /// Marks generation `generation` as repositioning (its deliveries are
    /// dropped) and repositions in a separate task, since the seek stops
    /// this generation.
    async fn begin_reposition(self: &Arc<Self>, generation: u32, created: String) {
        {
            let mut s = self.state.lock().await;
            if !s.is_current(generation) {
                return;
            }
            s.repositioning = true;
        }
        let inner = self.clone();
        tokio::spawn(inner.reposition_after_recreate(created));
    }

    /// Moves the consumer to `start_position` on the new stream and reports
    /// `StreamRecreated`. Boxed: it seeks, which restarts consumption, which
    /// spawns the delivery task that may call this again.
    fn reposition_after_recreate(self: Arc<Self>, created: String) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            // The new identity is applied by the seek, atomically with its
            // write, so no commit stores an old-stream sequence under it.
            let old = self.state.lock().await.last_delivered_seq;
            self.report(&OffsetConsumerError::StreamRecreated { stored_seq: old });

            let last_seq = self
                .stream
                .get_info()
                .await
                .map(|info| info.state.last_sequence)
                .unwrap_or(0);
            let (seq, _) = initial_position(self.config.start_position, last_seq);
            match self.seek(seq, created).await {
                Ok(()) | Err(OffsetConsumerError::ConsumerClosed) => {}
                Err(e) => self.report(&OffsetConsumerError::Other(format!(
                    "failed to reposition after stream recreation: {e}"
                ))),
            }
        })
    }

    /// Ends consumption after the ordered stream ended on its own.
    async fn stopped(&self, generation: u32, reason: String) {
        let name = {
            let mut s = self.state.lock().await;
            if !s.is_current(generation) {
                return;
            }
            s.consuming = false;
            s.shutdown_tx = None;
            s.consume_task = None; // this task; dropping the handle detaches it
            s.consumer_name.take()
        };
        if let Some(name) = name {
            let _ = self.stream.delete_consumer(&name).await;
        }
        self.report(&OffsetConsumerError::ConsumptionStopped(reason));
    }

    /// Stops the delivery task and deletes the ordered consumer from the
    /// server so that ownership is visibly released. Best effort. Does not
    /// wait for a running handler: the delivery task drops whatever it
    /// delivers from now on and ends at its next poll.
    async fn stop_source(&self) {
        if let Some(name) = self.stop_delivery().await {
            let _ = self.stream.delete_consumer(&name).await;
        }
    }

    /// Stops the delivery task without deleting the ordered consumer, and
    /// returns the name of its current server-side consumer.
    async fn stop_delivery(&self) -> Option<String> {
        let (shutdown_tx, name) = {
            let mut state = self.state.lock().await;
            state.consume_task.take();
            (state.shutdown_tx.take(), state.consumer_name.take())
        };
        if let Some(tx) = shutdown_tx {
            let _ = tx.send(true);
        }
        name
    }

    /// Commits the last processed sequence every `auto_commit_interval`,
    /// never a message whose handler is still running. Close does not wait
    /// for this task: it stops it, and a commit submitted after close started
    /// fails with `ConsumerClosed`, which is not reported.
    fn start_auto_commit(self: Arc<Self>, mut stop: watch::Receiver<bool>) {
        let interval = self.config.auto_commit_interval;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
            loop {
                tokio::select! {
                    biased;
                    _ = stop.changed() => return,
                    _ = ticker.tick() => {}
                }
                let needs_commit = {
                    let s = self.state.lock().await;
                    if s.closed {
                        return;
                    }
                    s.last_processed_seq > s.last_committed_seq
                };
                if !needs_commit {
                    continue;
                }
                let result = match self.commit_async(true).await {
                    Ok(handle) => await_commit(handle).await,
                    Err(e) => Err(e),
                };
                match result {
                    Ok(()) => {}
                    Err(OffsetConsumerError::ConsumerClosed) => return,
                    Err(e) => self.report(&OffsetConsumerError::Other(format!("auto-commit failed: {e}"))),
                }
            }
        });
    }
}

/// Reports whether a consumer named with the `owner` prefix exists on the stream.
async fn owner_alive(stream: &jetstream::stream::Stream, owner: &str) -> Result<bool> {
    let mut names = stream.consumer_names();
    while let Some(name) = names.next().await {
        let name = name.map_err(|e| OffsetConsumerError::Other(format!("failed to list consumers: {e}")))?;
        if name.starts_with(owner) {
            return Ok(true);
        }
    }
    Ok(false)
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
