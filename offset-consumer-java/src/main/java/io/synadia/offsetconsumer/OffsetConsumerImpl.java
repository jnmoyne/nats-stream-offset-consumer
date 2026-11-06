package io.synadia.offsetconsumer;

import io.nats.client.ConsumeOptions;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.Message;
import io.nats.client.MessageConsumer;
import io.nats.client.MessageHandler;
import io.nats.client.OrderedConsumerContext;
import io.nats.client.StreamContext;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.OrderedConsumerConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.NatsJetStreamMetaData;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Package-private implementation of OffsetConsumer.
 * <p>
 * State is guarded by {@code lock}. KV writes (claims, commits, seeks) are executed
 * on a single-thread {@code commitExecutor}, which keeps them in program order;
 * their submission is serialized by {@code submitLock}. consume(), seek() and close()
 * are serialized by {@code lifeLock} so that at most one ordered consumer is
 * ever active. Lock order: lifeLock, then submitLock, then lock.
 */
class OffsetConsumerImpl implements OffsetConsumer {

    /** Bound on how long close() waits for pending commits. Not final so tests can shorten it. */
    static volatile long closeTimeoutMillis = 5000;

    /**
     * How long a starter waits, measured with its own clock, before taking
     * over an entry whose owner has no consumer on the server. It must exceed
     * the time a live owner can be without one: an ordered consumer reset
     * deletes the old consumer before creating the new one, and retries the
     * creation if it fails. No jitter is needed: concurrent starters are
     * decided by the claim's CAS. Not final so tests can shorten it.
     */
    static volatile long livenessWaitMillis = 5000;

    /**
     * Bounds how often a claim is retried after losing a CAS race to a write
     * that did not change the owner (a release, or this instance's own late commit).
     */
    static final int CLAIM_ROUNDS = 3;

    /** Attempts of the conditional release write in close() on revision conflicts. */
    static final int RELEASE_ATTEMPTS = 3;

    /**
     * How often, while consuming, the ordered consumer's server-side name is
     * checked for a change (jnats recreated it), and how often a missing
     * stream is polled after such a change.
     */
    static final long STREAM_CHECK_INTERVAL_MILLIS = 1000;

    /**
     * Pull expiry of the ordered consumer. jnats derives the idle heartbeat
     * from it (half of it) and recreates the ordered consumer after three
     * missed heartbeats; that recreation is the only sign of a deleted stream
     * the library gets (jnats does not surface "consumer deleted" to it). With
     * the jnats default of 30s it takes 45 to 90 seconds to notice a deleted
     * and recreated stream; 5s brings that down to 7.5 to 15 seconds, at the
     * cost of an idle pull request every 5s.
     */
    static final ConsumeOptions CONSUME_OPTIONS = ConsumeOptions.builder().expiresIn(5000).build();

    private final ConsumerConfig config;
    private final StreamContext streamContext;
    private final KeyValue kv;
    private final String kvKey;
    private final String owner;
    private final String host;
    /**
     * StartPosition resolved at construction (re-resolved by the claim if the
     * stream was recreated since), used by the claim without a usable stored
     * offset. Guarded by lock.
     */
    private long initialSeq;
    private String initialAt;

    private final Object lifeLock = new Object();
    private final Object lock = new Object();
    private final Object submitLock = new Object();
    private final ExecutorService commitExecutor;

    /** Identifies the stream incarnation (see OffsetConsumerInfo.getStreamCreated()). */
    private String streamCreated;
    private long startSeq;
    private long lastDeliveredSeq;
    /**
     * Last sequence whose handler has returned. Auto-commit and the final
     * commit in close() use it, so a message still being handled is never
     * committed implicitly.
     */
    private long lastProcessedSeq;
    private long lastCommittedSeq;
    private String lastCommittedAt;
    /** Highest sequence handed to a commit call; the release write persists at least this. */
    private long lastRequestedSeq;
    private String lastRequestedAt;
    /** Set by close() once the bounded wait expired: queued commits complete without writing. */
    private volatile boolean abandonPending;
    private int generation;
    private MessageConsumer messageConsumer;
    private OrderedConsumerContext orderedConsumer;
    private MessageHandler currentHandler;
    /** Server-side name of the ordered consumer that delivered the last message. */
    private String lastConsumerName;
    /** Last non-null server-side name reported by the ordered consumer context. */
    private String lastContextName;
    /** Set while a detected stream recreation is handled; messages delivered meanwhile are dropped. */
    private boolean repositioning;
    /** Set while a thread polls the stream for a recreation. */
    private boolean watching;
    private ScheduledExecutorService autoCommitScheduler;
    private ScheduledExecutorService monitorScheduler;
    private boolean closed;
    private boolean kvDeleted;
    /** Set once this instance has claimed the entry (at its first consume()). */
    private boolean claimed;
    /** Set by a seek while not consuming: the claim at the next consume() stores startSeq. */
    private boolean pendingSeek;

    /**
     * Package-private constructor. Use OffsetConsumerFactory.create() to obtain instances.
     */
    OffsetConsumerImpl(ConsumerConfig config, StreamContext streamContext, KeyValue kv, String kvKey,
                       String owner, String host, String streamCreated, long initialSeq, String initialAt) {
        this.config = config;
        this.streamContext = streamContext;
        this.kv = kv;
        this.kvKey = kvKey;
        this.owner = owner;
        this.host = host;
        this.streamCreated = streamCreated;
        this.initialSeq = initialSeq;
        this.initialAt = initialAt;
        this.lastCommittedAt = "";
        this.lastRequestedAt = "";
        this.commitExecutor = Executors.newSingleThreadExecutor(r -> daemon(r, "offset-consumer-commit"));
    }

    private static Thread daemon(Runnable r, String name) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }

    /** Passes the error to the configured ErrorHandler, if any. */
    void reportError(Exception e) {
        ErrorHandler handler = config.getErrorHandler();
        if (handler != null) {
            handler.onError(this, e);
        }
    }

    /** Package-private for tests: the position consumption (re)starts from. */
    long startSeq() {
        synchronized (lock) {
            return startSeq;
        }
    }

    /** Package-private for tests: the ordered consumer name prefix of this instance. */
    String owner() {
        return owner;
    }

    @Override
    public void consume(MessageHandler handler) throws IOException, JetStreamApiException, AlreadyInUseException {
        synchronized (lifeLock) {
            synchronized (lock) {
                if (closed) {
                    throw new IllegalStateException("Consumer is closed");
                }
                if (messageConsumer != null) {
                    throw new IllegalStateException("Already consuming");
                }
            }

            // Claim (or re-validate) ownership every time consumption starts
            // from a stopped state: while not consuming, the instance has no
            // consumer on the server and another instance may have taken the
            // entry over.
            claim();

            synchronized (lock) {
                if (closed) {
                    throw new IllegalStateException("Consumer is closed");
                }
                startConsuming(handler);
            }
        }
    }

    /**
     * Takes ownership of the offset entry before consumption starts, and
     * stores the position consumption starts from: the pending seek position
     * if seek() was called while not consuming, else the stored offset, else
     * the position resolved from StartPosition at construction (also when the
     * stored offset belongs to an earlier incarnation of the stream).
     * <p>
     * The stored owner, if it is another instance, is alive while a consumer
     * whose name starts with its owner prefix exists. If none exists, the claim
     * waits {@link #livenessWaitMillis} (measured locally) and
     * lists again, which rides out an ordered consumer reset; it then writes
     * with a CAS against the revision read before the wait, so any write during
     * the wait (a commit by the owner, another instance's claim) makes it fail.
     * No clocks are compared. Must be called while holding lifeLock (and not lock).
     */
    private void claim() throws IOException, JetStreamApiException, AlreadyInUseException {
        for (int round = 1; ; round++) {
            KeyValueEntry kve = kv.get(kvKey);
            OffsetEntry entry = kve == null ? OffsetEntry.empty() : OffsetEntry.fromJson(kve.getValue());
            if (kve != null && !entry.getOwner().isEmpty() && !entry.getOwner().equals(owner)) {
                checkOwnership(entry);
            }

            StreamInfo info = streamContext.getStreamInfo();
            long lastSeq = info.getStreamState().getLastSequence();
            String currentIdentity = OffsetConsumerFactory.streamCreatedId(info.getCreateTime());

            boolean recreated;
            long seq;
            String committedAt;
            byte[] data;
            synchronized (lock) {
                if (!currentIdentity.isEmpty() && !currentIdentity.equals(streamCreated)) {
                    // The stream was recreated since construction: the start
                    // position resolved then refers to the old stream.
                    streamCreated = currentIdentity;
                    if (config.getStartPosition() == StartPosition.END && lastSeq > 0) {
                        initialSeq = lastSeq;
                        initialAt = Instant.now().toString();
                    } else {
                        initialSeq = 0;
                        initialAt = "";
                    }
                }
                recreated = kve != null && OffsetConsumerFactory.streamWasRecreated(entry, streamCreated, lastSeq);
                if (pendingSeek) {
                    seq = startSeq;
                    committedAt = lastCommittedAt;
                } else if (kve == null || recreated) {
                    seq = initialSeq;
                    committedAt = initialAt;
                } else {
                    seq = entry.getSeq();
                    committedAt = entry.getCommittedAt();
                }
                data = entryFor(seq, committedAt).toJson();
            }

            long revision = kve == null ? 0 : kve.getRevision();
            JetStreamApiException conflict = null;
            synchronized (submitLock) {
                try {
                    onCommitThread(() -> {
                        if (revision == 0) {
                            kv.create(kvKey, data);
                        } else {
                            kv.update(kvKey, data, revision);
                        }
                    });
                    synchronized (lock) {
                        claimed = true;
                        pendingSeek = false;
                        kvDeleted = false;
                        startSeq = seq;
                        lastDeliveredSeq = seq;
                        lastProcessedSeq = seq;
                        lastCommittedSeq = seq;
                        lastCommittedAt = committedAt;
                        lastRequestedSeq = seq;
                        lastRequestedAt = committedAt;
                    }
                } catch (JetStreamApiException e) {
                    if (!OffsetConsumerFactory.isRevisionConflict(e)) {
                        throw e;
                    }
                    conflict = e;
                }
            }
            if (conflict == null) {
                if (recreated) {
                    reportError(new StreamRecreatedException(entry.getSeq()));
                }
                return;
            }

            // Someone wrote the entry meanwhile. Another owner just proved it
            // is alive; a release (empty owner) or this instance's own late
            // commit is retried.
            KeyValueEntry now = kv.get(kvKey);
            if (now != null) {
                OffsetEntry current = OffsetEntry.fromJson(now.getValue());
                if (!current.getOwner().isEmpty() && !current.getOwner().equals(owner)) {
                    throw new AlreadyInUseException(current.getOwner(), current.getHost());
                }
            }
            if (round == CLAIM_ROUNDS) {
                throw conflict;
            }
        }
    }

    /**
     * Throws AlreadyInUseException if the entry's owner (another instance) is
     * alive: a consumer whose name starts with its owner prefix exists on the
     * stream, now or after the liveness wait.
     */
    private void checkOwnership(OffsetEntry entry) throws IOException, JetStreamApiException, AlreadyInUseException {
        for (int attempt = 0; attempt < 2; attempt++) {
            if (attempt > 0) {
                try {
                    Thread.sleep(livenessWaitMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while checking ownership", e);
                }
            }
            if (OffsetConsumerFactory.isOwnerAlive(streamContext, entry.getOwner())) {
                throw new AlreadyInUseException(entry.getOwner(), entry.getHost());
            }
        }
    }

    /** A KV operation run on the commit thread. */
    @FunctionalInterface
    private interface KvWrite {
        void run() throws IOException, JetStreamApiException;
    }

    /**
     * Runs a KV write on the commit thread, after the commits already queued,
     * and waits for it. Must be called while holding submitLock.
     */
    private void onCommitThread(KvWrite write) throws IOException, JetStreamApiException {
        CompletableFuture<Void> done = new CompletableFuture<>();
        try {
            commitExecutor.execute(() -> {
                try {
                    write.run();
                    done.complete(null);
                } catch (Exception e) {
                    done.completeExceptionally(e);
                }
            });
        } catch (RejectedExecutionException e) {
            throw new IllegalStateException("Consumer is closed", e);
        }
        try {
            done.get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            if (cause instanceof JetStreamApiException) {
                throw (JetStreamApiException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new IOException(cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while writing the offset entry", e);
        }
    }

    /**
     * Commits the last processed sequence. Never commits a message whose
     * handler is still running. close() does not wait for this task, so the
     * ErrorHandler may call close().
     */
    private void autoCommit() {
        try {
            synchronized (lock) {
                if (closed || lastProcessedSeq <= lastCommittedSeq) {
                    return;
                }
            }
            try {
                awaitCommit(commitAsync(true));
            } catch (CommitException e) {
                boolean report;
                synchronized (lock) {
                    report = !closed; // commits submitted once close() started fail with "closed"
                }
                if (report) {
                    reportError(e);
                }
            }
        } catch (RuntimeException e) {
            // keep the schedule alive (an ErrorHandler exception would cancel it)
        }
    }

    /**
     * Creates an ordered consumer under a new generation prefix and starts
     * delivering messages. Must be called while holding lifeLock and lock.
     */
    private void startConsuming(MessageHandler handler) throws IOException, JetStreamApiException {
        generation++;
        final int gen = generation;
        lastConsumerName = null;
        lastContextName = null;
        repositioning = false;
        OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration()
                .consumerNamePrefix(owner + "-g" + gen);

        // The start position is fully resolved by the factory and seek(): 0
        // means the beginning of the stream.
        if (startSeq > 0) {
            occ.deliverPolicy(DeliverPolicy.ByStartSequence);
            occ.startSequence(startSeq + 1);
        } else {
            occ.deliverPolicy(DeliverPolicy.All);
        }

        if (config.getFilterSubjects() != null && !config.getFilterSubjects().isEmpty()) {
            occ.filterSubjects(config.getFilterSubjects().toArray(new String[0]));
        }

        OrderedConsumerContext ctx = streamContext.createOrderedConsumer(occ);
        MessageConsumer mc = ctx.consume(CONSUME_OPTIONS, msg -> deliver(gen, handler, msg));

        messageConsumer = mc;
        orderedConsumer = ctx;
        currentHandler = handler;
        lastContextName = ctx.getConsumerName();

        if (config.isAutoCommit() && autoCommitScheduler == null) {
            autoCommitScheduler = Executors.newSingleThreadScheduledExecutor(r -> daemon(r, "offset-consumer-autocommit"));
            long intervalMs = config.getAutoCommitInterval().toMillis();
            autoCommitScheduler.scheduleAtFixedRate(this::autoCommit, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        }
        if (monitorScheduler == null) {
            monitorScheduler = Executors.newSingleThreadScheduledExecutor(r -> daemon(r, "offset-consumer-monitor"));
            monitorScheduler.scheduleWithFixedDelay(this::checkConsumerName,
                    STREAM_CHECK_INTERVAL_MILLIS, STREAM_CHECK_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Reports whether generation gen is the active consumption. Must be called while holding lock.
     */
    private boolean isCurrent(int gen) {
        return messageConsumer != null && generation == gen && !repositioning;
    }

    /**
     * Tracks positions around one handler call. Messages from a stopped or
     * replaced generation are dropped, so a seek is never undone by a late
     * message of the previous ordered consumer.
     */
    private void deliver(int gen, MessageHandler handler, Message msg) throws InterruptedException {
        long seq;
        String consumerName;
        try {
            NatsJetStreamMetaData md = msg.metaData();
            seq = md.streamSequence();
            consumerName = md.getConsumer();
        } catch (RuntimeException e) {
            handler.onMessage(msg);
            return;
        }

        boolean reset;
        synchronized (lock) {
            if (!isCurrent(gen)) {
                return;
            }
            reset = lastConsumerName != null && !lastConsumerName.equals(consumerName);
            lastConsumerName = consumerName;
        }

        // The client recreated the ordered consumer. If that is because the
        // stream was deleted and recreated, the ordered consumer silently
        // resumes at the old position on the new stream: reposition instead.
        if (reset) {
            String created = changedStreamIdentity();
            if (created != null) {
                synchronized (lock) {
                    if (!isCurrent(gen)) {
                        return;
                    }
                    repositioning = true;
                }
                daemon(() -> repositionAfterRecreate(created), "offset-consumer-reposition").start();
                return;
            }
        }

        synchronized (lock) {
            if (!isCurrent(gen)) {
                return;
            }
            lastDeliveredSeq = seq;
        }

        handler.onMessage(msg);

        synchronized (lock) {
            if (isCurrent(gen) && seq > lastProcessedSeq) {
                lastProcessedSeq = seq;
            }
        }
    }

    /**
     * Periodic local check of the ordered consumer's server-side name. jnats
     * does not surface ordered consumer resets (consumer deleted, missed
     * heartbeats) to the library, but a reset gives the consumer a new name.
     * When the stream was deleted and recreated, the recreated ordered consumer
     * waits at the old position on the new stream, possibly forever, without
     * delivering anything that would reveal the change: watch the stream.
     */
    private void checkConsumerName() {
        try {
            int gen;
            OrderedConsumerContext ctx;
            synchronized (lock) {
                if (messageConsumer == null || repositioning || orderedConsumer == null) {
                    return;
                }
                gen = generation;
                ctx = orderedConsumer;
            }
            String name = ctx.getConsumerName();
            if (name == null) {
                return;
            }
            boolean changed;
            synchronized (lock) {
                if (!isCurrent(gen)) {
                    return;
                }
                changed = lastContextName != null && !lastContextName.equals(name);
                lastContextName = name;
            }
            if (changed) {
                watchStream(gen);
            }
        } catch (RuntimeException e) {
            // keep the schedule alive
        }
    }

    /**
     * Polls the stream until it exists again, and repositions if it is a new
     * incarnation. At most one watcher runs at a time; it gives up when
     * generation gen ends.
     */
    private void watchStream(int gen) {
        synchronized (lock) {
            if (watching || !isCurrent(gen)) {
                return;
            }
            watching = true;
        }
        daemon(() -> {
            try {
                while (true) {
                    synchronized (lock) {
                        if (!isCurrent(gen)) {
                            return;
                        }
                    }
                    StreamInfo info;
                    try {
                        info = streamContext.getStreamInfo();
                    } catch (Exception e) {
                        try {
                            Thread.sleep(STREAM_CHECK_INTERVAL_MILLIS);
                        } catch (InterruptedException ie) {
                            return;
                        }
                        continue;
                    }
                    String created = OffsetConsumerFactory.streamCreatedId(info.getCreateTime());
                    boolean changed;
                    synchronized (lock) {
                        changed = !created.isEmpty() && !streamCreated.isEmpty()
                                && !created.equals(streamCreated) && isCurrent(gen);
                        if (changed) {
                            repositioning = true;
                        }
                    }
                    if (changed) {
                        repositionAfterRecreate(created);
                    }
                    return;
                }
            } finally {
                synchronized (lock) {
                    watching = false;
                }
            }
        }, "offset-consumer-stream-watch").start();
    }

    /**
     * Returns the stream's current identity if it differs from the one the
     * offsets refer to, otherwise null.
     */
    private String changedStreamIdentity() {
        StreamInfo info;
        try {
            info = streamContext.getStreamInfo();
        } catch (Exception e) {
            return null;
        }
        String created = OffsetConsumerFactory.streamCreatedId(info.getCreateTime());
        synchronized (lock) {
            boolean changed = !created.isEmpty() && !streamCreated.isEmpty() && !created.equals(streamCreated);
            return changed ? created : null;
        }
    }

    /**
     * Moves the consumer to its StartPosition on the new stream and reports a
     * StreamRecreatedException. The new stream identity is applied by the seek
     * write itself, so no commit can store an old-stream position under it.
     */
    private void repositionAfterRecreate(String created) {
        long old;
        synchronized (lock) {
            old = lastDeliveredSeq;
        }

        reportError(new StreamRecreatedException(old));

        long lastSeq = 0;
        try {
            lastSeq = streamContext.getStreamInfo().getStreamState().getLastSequence();
        } catch (Exception ignored) {
            // treat as empty
        }
        long seq = config.getStartPosition() == StartPosition.END && lastSeq > 0 ? lastSeq : 0;
        try {
            seek(seq, created);
        } catch (IllegalStateException e) {
            // closed meanwhile
        } catch (Exception e) {
            reportError(new CommitException("Failed to reposition after stream recreation", e));
        }
    }

    /**
     * Stops delivery and deletes the ordered consumer from the server so that
     * ownership is visibly released. Best effort. Call without holding the lock.
     */
    private void stopOrderedConsumer(MessageConsumer mc, OrderedConsumerContext ctx) {
        closeMessageConsumer(mc);
        deleteServerConsumer(ctx);
    }

    /** Stops delivery without deleting the server-side consumer. Best effort. */
    private static void closeMessageConsumer(MessageConsumer mc) {
        if (mc != null) {
            try {
                mc.close();
            } catch (Exception ignored) {
                // best effort
            }
        }
    }

    /** Deletes the ordered consumer's current server-side consumer. Best effort. */
    private void deleteServerConsumer(OrderedConsumerContext ctx) {
        if (ctx != null) {
            String name = ctx.getConsumerName();
            if (name != null && !name.isEmpty()) {
                try {
                    streamContext.deleteConsumer(name);
                } catch (Exception ignored) {
                    // best effort (already gone, or connection down)
                }
            }
        }
    }

    /** Builds the entry to write for a commit at seq. Must be called while holding lock. */
    private OffsetEntry entryFor(long seq, String committedAt) {
        return new OffsetEntry(seq, committedAt, owner, host, streamCreated);
    }

    @Override
    public void commitSync() throws CommitException {
        awaitCommit(commitAsync(false));
    }

    private static void awaitCommit(CompletableFuture<Void> future) throws CommitException {
        try {
            future.get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof CommitException) {
                throw (CommitException) cause;
            }
            throw new CommitException("Failed to commit offset", cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CommitException("Interrupted while committing offset", e);
        }
    }

    @Override
    public CompletableFuture<Void> commitAsync() {
        return commitAsync(false);
    }

    /**
     * Submits a commit of the last delivered sequence, or with processedOnly of
     * the last processed sequence (auto-commit).
     */
    private CompletableFuture<Void> commitAsync(boolean processedOnly) {
        synchronized (submitLock) {
            long seq;
            String at = Instant.now().toString();
            byte[] data;
            synchronized (lock) {
                if (closed) {
                    return failed(new CommitException("Consumer is closed"));
                }
                if (!claimed) {
                    // Nothing was delivered, and the entry is not ours to write yet.
                    return CompletableFuture.completedFuture(null);
                }
                seq = lastDeliveredSeq;
                if (processedOnly) {
                    seq = lastProcessedSeq;
                    if (seq <= lastRequestedSeq) {
                        // An explicit commit already covers it.
                        return CompletableFuture.completedFuture(null);
                    }
                }
                if (seq == 0) {
                    return CompletableFuture.completedFuture(null);
                }
                if (seq > lastRequestedSeq) {
                    lastRequestedSeq = seq;
                    lastRequestedAt = at;
                }
                data = entryFor(seq, at).toJson();
            }
            return submit(seq, at, data);
        }
    }

    /**
     * Queues the write of seq on the commit pipeline. Must be called while holding submitLock.
     */
    private CompletableFuture<Void> submit(long seq, String committedAt, byte[] data) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            commitExecutor.execute(() -> {
                if (abandonPending) {
                    // close() gave up waiting: the release write persists this position.
                    future.complete(null);
                    return;
                }
                try {
                    kv.put(kvKey, data);
                    synchronized (lock) {
                        if (seq > lastCommittedSeq) {
                            lastCommittedSeq = seq;
                            lastCommittedAt = committedAt;
                        }
                    }
                    future.complete(null);
                } catch (Exception e) {
                    future.completeExceptionally(new CommitException("Failed to commit offset", e));
                }
            });
        } catch (RejectedExecutionException e) {
            future.completeExceptionally(new CommitException("Consumer is closed", e));
        }
        return future;
    }

    private static CompletableFuture<Void> failed(Throwable t) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        f.completeExceptionally(t);
        return f;
    }

    @Override
    public void seek(long seq) throws CommitException, IOException, JetStreamApiException {
        seek(seq, "");
    }

    /**
     * Implements seek(). A non-empty newStreamCreated switches the consumer to a
     * new stream incarnation atomically with the seek write: commits submitted
     * before it carry the old identity, commits after it the new one.
     */
    private void seek(long seq, String newStreamCreated) throws CommitException, IOException, JetStreamApiException {
        synchronized (lifeLock) {
            MessageConsumer mc;
            OrderedConsumerContext old;
            MessageHandler handler;
            String at = Instant.now().toString();

            synchronized (lock) {
                if (closed) {
                    throw new IllegalStateException("Consumer is closed");
                }
                if (messageConsumer == null) {
                    // Not consuming: no consumer protects the entry, so do not
                    // write it; the claim at the next consume() stores it.
                    startSeq = seq;
                    lastDeliveredSeq = seq;
                    lastProcessedSeq = seq;
                    lastCommittedAt = at;
                    pendingSeek = true;
                    if (!newStreamCreated.isEmpty()) {
                        streamCreated = newStreamCreated;
                    }
                    return;
                }
                mc = messageConsumer;
                old = orderedConsumer;
                messageConsumer = null;
                orderedConsumer = null;
                handler = currentHandler;
            }

            // Stop delivery but keep the server-side consumer until the new one
            // exists (make before break): the instance stays visible throughout.
            closeMessageConsumer(mc);
            try {
                // Write the new position through the commit pipeline, holding submitLock
                // so that no commit can be queued between the write and the state update.
                // The position is only updated once the write succeeded.
                synchronized (submitLock) {
                    byte[] data;
                    synchronized (lock) {
                        String identity = newStreamCreated.isEmpty() ? streamCreated : newStreamCreated;
                        data = new OffsetEntry(seq, at, owner, host, identity).toJson();
                    }
                    try {
                        onCommitThread(() -> kv.put(kvKey, data));
                    } catch (IOException | JetStreamApiException e) {
                        throw new CommitException("Failed to persist seek offset", e);
                    } catch (IllegalStateException e) {
                        throw new CommitException("Consumer is closed", e);
                    }
                    synchronized (lock) {
                        startSeq = seq;
                        lastDeliveredSeq = seq;
                        lastProcessedSeq = seq;
                        lastCommittedSeq = seq;
                        lastCommittedAt = at;
                        lastRequestedSeq = seq;
                        lastRequestedAt = at;
                        kvDeleted = false; // the seek write recreated the entry
                        if (!newStreamCreated.isEmpty()) {
                            streamCreated = newStreamCreated;
                        }
                    }
                }

                synchronized (lock) {
                    // lifeLock guarantees nothing else started consuming meanwhile; the
                    // messageConsumer check is a second line of defence against a leaked consumer.
                    if (handler != null && !closed && messageConsumer == null) {
                        startConsuming(handler);
                    }
                }
            } finally {
                // The new generation is visible (or the seek failed and the
                // consumer is left stopped): delete the old one.
                deleteServerConsumer(old);
            }
        }
    }

    @Override
    public void seekToBeginning() throws CommitException, IOException, JetStreamApiException {
        seek(0);
    }

    @Override
    public void seekToEnd() throws CommitException, IOException, JetStreamApiException {
        StreamInfo info = streamContext.getStreamInfo();
        seek(info.getStreamState().getLastSequence());
    }

    @Override
    public OffsetConsumerInfo info() throws IOException, JetStreamApiException {
        KeyValueEntry entry = kv.get(kvKey);
        if (entry == null) {
            return null;
        }
        return OffsetEntry.fromJson(entry.getValue()).toInfo();
    }

    @Override
    public void deleteConsumer() throws IOException, JetStreamApiException {
        synchronized (submitLock) {
            kv.delete(kvKey);
        }
        synchronized (lock) {
            kvDeleted = true;
        }
    }

    @Override
    public void close() {
        synchronized (lifeLock) {
            ScheduledExecutorService autoCommits;
            ScheduledExecutorService monitor;
            synchronized (lock) {
                if (closed) {
                    return;
                }
                closed = true;
                autoCommits = autoCommitScheduler;
                monitor = monitorScheduler;
                autoCommitScheduler = null;
                monitorScheduler = null;
            }

            // Stop the timers without waiting for them: close() may be called by
            // the ErrorHandler from the auto-commit thread, and any commit the
            // auto-commit task submits from now on fails with "closed".
            if (autoCommits != null) {
                autoCommits.shutdown();
            }
            if (monitor != null) {
                monitor.shutdown();
            }

            // Drain pending commits (bounded). Commits still queued after the
            // wait are abandoned: the release write below persists the highest
            // requested position anyway.
            commitExecutor.shutdown();
            if (!awaitTermination(commitExecutor)) {
                abandonPending = true;
                awaitTermination(commitExecutor);
            }

            // Stop delivery and delete the ordered consumer. A handler still
            // running is not waited for: its message is not committed and is
            // redelivered on the next start.
            MessageConsumer mc;
            OrderedConsumerContext ctx;
            synchronized (lock) {
                mc = messageConsumer;
                ctx = orderedConsumer;
                messageConsumer = null;
                orderedConsumer = null;
            }
            stopOrderedConsumer(mc, ctx);

            // Release ownership with one final conditional write (releaseEntry).
            // It persists the highest position handed to a commit call
            // (acknowledged or not) and, with auto-commit, the messages
            // processed since the last commit.
            long seq;
            String at;
            boolean deleted;
            byte[] release;
            synchronized (lock) {
                seq = lastCommittedSeq;
                at = lastCommittedAt;
                if (lastRequestedSeq > seq) {
                    seq = lastRequestedSeq;
                    at = lastRequestedAt;
                }
                if (config.isAutoCommit() && lastProcessedSeq > seq) {
                    seq = lastProcessedSeq;
                    at = Instant.now().toString();
                }
                // Before the first claim the entry is not ours: write nothing.
                deleted = kvDeleted || !claimed;
                release = new OffsetEntry(seq, at, "", "", streamCreated).toJson();
            }
            try {
                if (!deleted) {
                    boolean released;
                    synchronized (submitLock) {
                        released = releaseEntry(release);
                    }
                    if (released) {
                        synchronized (lock) {
                            if (seq > lastCommittedSeq) {
                                lastCommittedSeq = seq;
                                lastCommittedAt = at;
                            }
                        }
                    }
                }
            } catch (AlreadyInUseException e) {
                reportError(new CommitException(
                        "Release skipped: the offset entry is no longer owned by this instance", e));
            } catch (Exception e) {
                reportError(new CommitException("Failed to release offset entry on close", e));
            }
        }
    }

    /**
     * Writes the release only while the entry still names this instance as
     * its owner, with a compare-and-set against the revision just read. An
     * instance that was displaced without noticing (its ordered consumer
     * expired and another instance claimed the entry) therefore cannot clear
     * the new owner's claim, which would let a third instance in without a
     * liveness check. A revision conflict means a write landed between the
     * read and the release, so the entry is read again, at most
     * {@link #RELEASE_ATTEMPTS} times in total. Must be called while holding
     * submitLock.
     *
     * @return true if the release was written, false if the entry no longer
     * exists (nothing to release)
     * @throws AlreadyInUseException if another instance owns the entry
     */
    private boolean releaseEntry(byte[] release)
            throws IOException, JetStreamApiException, AlreadyInUseException {
        for (int attempt = 1; ; attempt++) {
            KeyValueEntry current = kv.get(kvKey);
            if (current == null) {
                return false;
            }
            OffsetEntry entry = OffsetEntry.fromJson(current.getValue());
            if (!entry.getOwner().equals(owner)) {
                throw new AlreadyInUseException(entry.getOwner(), entry.getHost());
            }
            try {
                kv.update(kvKey, release, current.getRevision());
                return true;
            } catch (JetStreamApiException e) {
                if (!OffsetConsumerFactory.isRevisionConflict(e) || attempt == RELEASE_ATTEMPTS) {
                    throw e;
                }
            }
        }
    }

    private static boolean awaitTermination(ExecutorService executor) {
        try {
            return executor.awaitTermination(closeTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
