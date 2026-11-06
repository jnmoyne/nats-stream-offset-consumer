package io.synadia.offsetconsumer;

import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.MessageConsumer;
import io.nats.client.MessageHandler;
import io.nats.client.OrderedConsumerContext;
import io.nats.client.StreamContext;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.OrderedConsumerConfiguration;
import io.nats.client.api.StreamInfo;

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
 * State is guarded by {@code lock}. KV writes (commits, seeks) are executed on a
 * single-thread {@code commitExecutor}, which keeps them in program order; their
 * submission is serialized by {@code submitLock} (lock order: submitLock, then lock).
 */
class OffsetConsumerImpl implements OffsetConsumer {

    /** Bound on how long close() waits for pending commits. Not final so tests can shorten it. */
    static volatile long closeTimeoutMillis = 5000;

    private final ConsumerConfig config;
    private final StreamContext streamContext;
    private final KeyValue kv;
    private final String kvKey;
    private final String owner;
    private final String host;
    private final String claimedAt;

    private final Object lock = new Object();
    private final Object submitLock = new Object();
    private final ExecutorService commitExecutor;

    private long startSeq;
    private long lastDeliveredSeq;
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
    private ScheduledExecutorService autoCommitScheduler;
    private boolean closed;
    private boolean kvDeleted;

    /**
     * Package-private constructor. Use OffsetConsumerFactory.create() to obtain instances.
     */
    OffsetConsumerImpl(ConsumerConfig config, StreamContext streamContext, KeyValue kv, String kvKey, OffsetEntry claimed) {
        this.config = config;
        this.streamContext = streamContext;
        this.kv = kv;
        this.kvKey = kvKey;
        this.owner = claimed.getOwner();
        this.host = claimed.getHost();
        this.claimedAt = claimed.getClaimedAt();
        this.startSeq = claimed.getSeq();
        this.lastDeliveredSeq = claimed.getSeq();
        this.lastCommittedSeq = claimed.getSeq();
        this.lastCommittedAt = claimed.getCommittedAt();
        this.lastRequestedSeq = claimed.getSeq();
        this.lastRequestedAt = claimed.getCommittedAt();
        this.commitExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "offset-consumer-commit");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public void consume(MessageHandler handler) throws IOException, JetStreamApiException {
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException("Consumer is closed");
            }
            if (messageConsumer != null) {
                throw new IllegalStateException("Already consuming");
            }

            startConsuming(handler);

            if (config.isAutoCommit() && autoCommitScheduler == null) {
                autoCommitScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "offset-consumer-autocommit");
                    t.setDaemon(true);
                    return t;
                });
                long intervalMs = config.getAutoCommitInterval().toMillis();
                autoCommitScheduler.scheduleAtFixedRate(this::autoCommit, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
            }
        }
    }

    private void autoCommit() {
        synchronized (lock) {
            if (closed || lastDeliveredSeq <= lastCommittedSeq) {
                return;
            }
        }
        try {
            commitSync();
        } catch (Exception e) {
            boolean report;
            synchronized (lock) {
                report = !closed;
            }
            if (report && config.getErrorHandler() != null) {
                config.getErrorHandler().onError(this, e);
            }
        }
    }

    /**
     * Creates an ordered consumer under a new generation prefix and starts
     * delivering messages. Must be called while holding the lock.
     */
    private void startConsuming(MessageHandler handler) throws IOException, JetStreamApiException {
        generation++;
        OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration()
                .consumerNamePrefix(owner + "-g" + generation);

        if (startSeq > 0) {
            occ.deliverPolicy(DeliverPolicy.ByStartSequence);
            occ.startSequence(startSeq + 1);
        } else if (config.getStartPosition() == StartPosition.END) {
            occ.deliverPolicy(DeliverPolicy.New);
        } else {
            occ.deliverPolicy(DeliverPolicy.All);
        }

        if (config.getFilterSubjects() != null && !config.getFilterSubjects().isEmpty()) {
            occ.filterSubjects(config.getFilterSubjects().toArray(new String[0]));
        }

        OrderedConsumerContext ctx = streamContext.createOrderedConsumer(occ);
        MessageConsumer mc = ctx.consume(msg -> {
            synchronized (lock) {
                lastDeliveredSeq = msg.metaData().streamSequence();
            }
            handler.onMessage(msg);
        });

        messageConsumer = mc;
        orderedConsumer = ctx;
        currentHandler = handler;
    }

    /**
     * Stops delivery and deletes the ordered consumer from the server so that
     * ownership is visibly released. Best effort. Call without holding the lock.
     */
    private void stopOrderedConsumer(MessageConsumer mc, OrderedConsumerContext ctx) {
        if (mc != null) {
            try {
                mc.close();
            } catch (Exception ignored) {
                // best effort
            }
        }
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

    private OffsetEntry entryFor(long seq, String committedAt) {
        return new OffsetEntry(seq, committedAt, owner, host, claimedAt);
    }

    @Override
    public void commitSync() throws CommitException {
        try {
            commitAsync().get();
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
        synchronized (submitLock) {
            long seq;
            String at = Instant.now().toString();
            synchronized (lock) {
                if (closed) {
                    return failed(new CommitException("Consumer is closed"));
                }
                seq = lastDeliveredSeq;
                if (seq > lastRequestedSeq) {
                    lastRequestedSeq = seq;
                    lastRequestedAt = at;
                }
            }
            if (seq == 0) {
                return CompletableFuture.completedFuture(null);
            }
            return submit(seq, at);
        }
    }

    /**
     * Queues the write of seq on the commit pipeline. Must be called while holding submitLock.
     */
    private CompletableFuture<Void> submit(long seq, String committedAt) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        byte[] data = entryFor(seq, committedAt).toJson();
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
        MessageConsumer mc;
        OrderedConsumerContext ctx;
        MessageHandler handler;

        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException("Consumer is closed");
            }
            mc = messageConsumer;
            ctx = orderedConsumer;
            messageConsumer = null;
            orderedConsumer = null;
            handler = currentHandler;
        }

        stopOrderedConsumer(mc, ctx);

        // Write the new position through the commit pipeline, holding submitLock
        // so that no commit can be queued between the write and the state update.
        String at = Instant.now().toString();
        byte[] data = entryFor(seq, at).toJson();
        synchronized (submitLock) {
            CompletableFuture<Void> write = new CompletableFuture<>();
            try {
                commitExecutor.execute(() -> {
                    try {
                        kv.put(kvKey, data);
                        write.complete(null);
                    } catch (Exception e) {
                        write.completeExceptionally(e);
                    }
                });
                write.get();
            } catch (ExecutionException e) {
                throw new CommitException("Failed to persist seek offset", e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CommitException("Interrupted while seeking", e);
            } catch (RejectedExecutionException e) {
                throw new CommitException("Consumer is closed", e);
            }
            synchronized (lock) {
                startSeq = seq;
                lastDeliveredSeq = seq;
                lastCommittedSeq = seq;
                lastCommittedAt = at;
                lastRequestedSeq = seq;
                lastRequestedAt = at;
            }
        }

        synchronized (lock) {
            if (handler != null && !closed) {
                startConsuming(handler);
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
        ScheduledExecutorService scheduler;
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            scheduler = autoCommitScheduler;
            autoCommitScheduler = null;
        }

        // Stop the auto-commit timer, then drain pending commits (bounded).
        // Commits still queued after the wait are abandoned: the release write
        // below persists the highest requested position anyway.
        if (scheduler != null) {
            scheduler.shutdown();
            awaitTermination(scheduler);
        }
        commitExecutor.shutdown();
        if (!awaitTermination(commitExecutor)) {
            abandonPending = true;
            awaitTermination(commitExecutor);
        }

        // Stop delivery and delete the ordered consumer.
        MessageConsumer mc;
        OrderedConsumerContext ctx;
        synchronized (lock) {
            mc = messageConsumer;
            ctx = orderedConsumer;
            messageConsumer = null;
            orderedConsumer = null;
        }
        stopOrderedConsumer(mc, ctx);

        // Release ownership with one final write. It persists the highest
        // position handed to a commit call (acknowledged or not) and, with
        // auto-commit, any uncommitted messages.
        long seq;
        String at;
        boolean deleted;
        synchronized (lock) {
            seq = lastCommittedSeq;
            at = lastCommittedAt;
            if (lastRequestedSeq > seq) {
                seq = lastRequestedSeq;
                at = lastRequestedAt;
            }
            if (config.isAutoCommit() && lastDeliveredSeq > seq) {
                seq = lastDeliveredSeq;
                at = Instant.now().toString();
            }
            deleted = kvDeleted;
        }
        if (deleted) {
            return;
        }
        try {
            kv.put(kvKey, new OffsetEntry(seq, at, "", "", "").toJson());
            synchronized (lock) {
                if (seq > lastCommittedSeq) {
                    lastCommittedSeq = seq;
                    lastCommittedAt = at;
                }
            }
        } catch (Exception e) {
            if (config.getErrorHandler() != null) {
                config.getErrorHandler().onError(this, new CommitException("Failed to release offset entry on close", e));
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
