package io.synadia.offsetconsumer;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.MessageConsumer;
import io.nats.client.MessageHandler;
import io.nats.client.StreamContext;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.OrderedConsumerConfiguration;
import io.nats.client.api.StreamInfo;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Package-private implementation of OffsetConsumer.
 * All public methods are thread-safe using synchronized blocks on the lock object.
 */
class OffsetConsumerImpl implements OffsetConsumer {

    private final Connection nc;
    private final ConsumerConfig config;
    private final StreamContext streamContext;
    private final KeyValue kv;
    private final String kvKey;
    private final Object lock = new Object();

    private long lastRevision;
    private long startSeq;
    private long lastDeliveredSeq;
    private long lastCommittedSeq;
    private MessageConsumer messageConsumer;
    private MessageHandler currentHandler;
    private boolean asyncCommitInFlight;
    private ScheduledExecutorService autoCommitScheduler;
    private boolean closed;

    /**
     * Package-private constructor. Use OffsetConsumerFactory.create() to obtain instances.
     */
    OffsetConsumerImpl(Connection nc, ConsumerConfig config, StreamContext streamContext,
                       KeyValue kv, String kvKey, long lastRevision, long startSeq) {
        this.nc = nc;
        this.config = config;
        this.streamContext = streamContext;
        this.kv = kv;
        this.kvKey = kvKey;
        this.lastRevision = lastRevision;
        this.startSeq = startSeq;

        // If we have a stored offset, initialize delivered and committed to match
        if (startSeq > 0) {
            this.lastDeliveredSeq = startSeq;
            this.lastCommittedSeq = startSeq;
        }
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

            // Start auto-commit scheduler if enabled
            if (config.isAutoCommit() && autoCommitScheduler == null) {
                autoCommitScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "offset-consumer-autocommit");
                    t.setDaemon(true);
                    return t;
                });
                long intervalMs = config.getAutoCommitInterval().toMillis();
                autoCommitScheduler.scheduleAtFixedRate(() -> {
                    try {
                        commitSync();
                    } catch (Exception e) {
                        // Best effort — swallow auto-commit errors
                    }
                }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * Creates an ordered consumer and starts delivering messages.
     * Must be called while holding the lock.
     */
    private void startConsuming(MessageHandler handler) throws IOException, JetStreamApiException {
        OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration();

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

        io.nats.client.OrderedConsumerContext orderedConsumer = streamContext.createOrderedConsumer(occ);

        messageConsumer = orderedConsumer.consume(msg -> {
            synchronized (lock) {
                lastDeliveredSeq = msg.metaData().streamSequence();
            }
            handler.onMessage(msg);
        });

        currentHandler = handler;
    }

    @Override
    public void commitSync() throws CommitException {
        long seq;
        long lastRev;

        synchronized (lock) {
            if (closed) {
                throw new CommitException("Consumer is closed");
            }
            seq = lastDeliveredSeq;
            lastRev = lastRevision;
        }

        if (seq == 0) {
            return; // nothing to commit
        }

        String json = String.format("{\"seq\":%d,\"committed_at\":\"%s\"}", seq, Instant.now().toString());
        byte[] data = json.getBytes();

        try {
            long newRev = kvUpdate(data, lastRev);
            synchronized (lock) {
                lastRevision = newRev;
                lastCommittedSeq = seq;
            }
        } catch (CASConflictException e) {
            throw e;
        } catch (Exception e) {
            throw new CommitException("Failed to commit offset", e);
        }
    }

    @Override
    public CompletableFuture<Void> commitAsync() throws AsyncCommitInFlightException {
        synchronized (lock) {
            if (asyncCommitInFlight) {
                throw new AsyncCommitInFlightException();
            }
            asyncCommitInFlight = true;
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                commitSync();
                return null;
            } catch (Exception e) {
                throw new CompletionException(e);
            } finally {
                synchronized (lock) {
                    asyncCommitInFlight = false;
                }
            }
        });
    }

    @Override
    public void seek(long seq) throws CommitException, IOException, JetStreamApiException {
        MessageConsumer mc;
        MessageHandler handler;
        long lastRev;

        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException("Consumer is closed");
            }

            mc = messageConsumer;
            messageConsumer = null;
            handler = currentHandler;
            lastRev = lastRevision;
        }

        // Stop current consumption
        if (mc != null) {
            try {
                mc.stop();
            } catch (Exception e) {
                // best effort
            }
        }

        // Persist new offset via CAS
        String json = String.format("{\"seq\":%d,\"committed_at\":\"%s\"}", seq, Instant.now().toString());
        byte[] data = json.getBytes();

        long newRev;
        try {
            newRev = kvUpdate(data, lastRev);
        } catch (CASConflictException e) {
            // Consumer left stopped on CAS failure
            throw e;
        } catch (Exception e) {
            throw new CommitException("Failed to persist seek offset", e);
        }

        // Update internal state and restart consumption
        synchronized (lock) {
            lastRevision = newRev;
            startSeq = seq;
            lastDeliveredSeq = seq;
            lastCommittedSeq = seq;

            if (handler != null) {
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
        try {
            StreamInfo info = streamContext.getStreamInfo();
            long lastSeq = info.getStreamState().getLastSequence();
            seek(lastSeq);
        } catch (CommitException | JetStreamApiException e) {
            throw e;
        } catch (IOException e) {
            throw e;
        }
    }

    @Override
    public void deleteConsumer() throws IOException, JetStreamApiException {
        kv.delete(kvKey);
        synchronized (lock) {
            lastRevision = 0;
        }
    }

    @Override
    public void close() {
        ScheduledExecutorService scheduler;
        MessageConsumer mc;
        boolean needsFinalCommit;

        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            scheduler = autoCommitScheduler;
            autoCommitScheduler = null;
            mc = messageConsumer;
            messageConsumer = null;
            needsFinalCommit = config.isAutoCommit() && lastDeliveredSeq > lastCommittedSeq;
        }

        // Shut down auto-commit scheduler
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Stop the message consumer
        if (mc != null) {
            try {
                mc.stop();
            } catch (Exception e) {
                // best effort
            }
        }

        // Final commit if auto-commit enabled and there are uncommitted messages
        if (needsFinalCommit) {
            try {
                commitSyncInternal();
            } catch (Exception e) {
                // best effort — swallow exceptions on final commit
            }
        }
    }

    /**
     * Internal commit that does not check the closed flag.
     * Used during close() for the final commit.
     */
    private void commitSyncInternal() throws CommitException {
        long seq;
        long lastRev;

        synchronized (lock) {
            seq = lastDeliveredSeq;
            lastRev = lastRevision;
        }

        if (seq == 0) {
            return;
        }

        String json = String.format("{\"seq\":%d,\"committed_at\":\"%s\"}", seq, Instant.now().toString());
        byte[] data = json.getBytes();

        try {
            long newRev = kvUpdate(data, lastRev);
            synchronized (lock) {
                lastRevision = newRev;
                lastCommittedSeq = seq;
            }
        } catch (CASConflictException e) {
            throw e;
        } catch (Exception e) {
            throw new CommitException("Failed to commit offset", e);
        }
    }

    /**
     * Performs a CAS update on the KV store. If lastRev is 0, creates the key;
     * otherwise updates with the expected revision.
     *
     * @return the new revision
     * @throws CASConflictException if the revision does not match
     */
    private long kvUpdate(byte[] value, long lastRev) throws Exception {
        try {
            if (lastRev == 0) {
                return kv.create(kvKey, value);
            } else {
                return kv.update(kvKey, value, lastRev);
            }
        } catch (JetStreamApiException e) {
            // Error code 10071 = wrong last sequence (CAS conflict)
            if (e.getApiErrorCode() == 10071) {
                throw new CASConflictException(e);
            }
            throw e;
        }
    }
}
