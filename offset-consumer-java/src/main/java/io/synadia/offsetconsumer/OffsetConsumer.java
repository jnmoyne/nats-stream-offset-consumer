package io.synadia.offsetconsumer;

import io.nats.client.JetStreamApiException;
import io.nats.client.MessageHandler;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Kafka-style offset-based consumption on a NATS JetStream stream.
 * All methods are thread-safe.
 */
public interface OffsetConsumer extends AutoCloseable {

    /**
     * Starts delivering messages from the resolved start position.
     * The handler is called serially for each message.
     */
    void consume(MessageHandler handler) throws IOException, JetStreamApiException;

    /**
     * Persists the last delivered sequence number and waits for the KV write
     * to be acknowledged.
     */
    void commitSync() throws CommitException;

    /**
     * Persists the last delivered sequence number asynchronously. Never blocks
     * on the server: the write is queued on the consumer's commit pipeline and
     * the returned future completes when it is acknowledged. Several commits
     * may be pending at once; they are applied in call order. A future that
     * fails carries a {@link CommitException}.
     */
    CompletableFuture<Void> commitAsync();

    /**
     * Repositions the consumer to a specific sequence number. After seek(N),
     * the next message delivered will be sequence N+1. Consumption is stopped,
     * its ordered consumer deleted, the new offset written, and consumption
     * restarted if it was running. If the write fails the consumer is left stopped.
     */
    void seek(long seq) throws CommitException, IOException, JetStreamApiException;

    void seekToBeginning() throws CommitException, IOException, JetStreamApiException;

    void seekToEnd() throws CommitException, IOException, JetStreamApiException;

    /**
     * Performs a fresh read of the consumer's offset entry from the KV store.
     *
     * @return the offset info, or null if the entry does not exist
     * @throws IOException           if KV operations fail
     * @throws JetStreamApiException if JetStream API calls fail
     */
    OffsetConsumerInfo info() throws IOException, JetStreamApiException;

    /**
     * Removes the offset entry from the KV store. Does not stop consumption;
     * a later {@link #close()} will not recreate the entry.
     */
    void deleteConsumer() throws IOException, JetStreamApiException;

    /**
     * Stops consumption and releases all resources: waits (bounded, 5s) for
     * pending async commits, deletes the underlying ordered consumer, and
     * releases ownership of the offset entry with one final write. That write
     * persists the highest sequence passed to a commit call (commits still
     * queued after the wait are abandoned, their futures complete, and the
     * position is not lost) and, with auto-commit, the last delivered
     * sequence. Failures of the final write are reported to the configured
     * {@link ErrorHandler}.
     */
    @Override
    void close();
}
