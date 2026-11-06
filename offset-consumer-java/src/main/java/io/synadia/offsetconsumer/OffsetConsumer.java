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
     * Claims ownership of the offset entry and starts delivering messages from
     * the resolved start position. The handler is called serially for each
     * message.
     * <p>
     * The claim stores the position consumption starts from: a position set by
     * {@link #seek} while not consuming, else the stored offset, else the
     * StartPosition resolved at creation (also when the stored offset belongs
     * to an earlier incarnation of the stream, in which case a
     * {@link StreamRecreatedException} is reported). If the entry names
     * another owner without a consumer on the server, the claim waits 5s
     * (measured locally) and checks again before
     * taking it over with a compare-and-set write, so this call can block for
     * several seconds.
     *
     * @throws AlreadyInUseException if another live instance owns the offset entry
     *                               (its consumer exists, or it wrote the entry during the claim)
     * @throws IllegalStateException if the consumer is closed or already consuming
     */
    void consume(MessageHandler handler) throws IOException, JetStreamApiException, AlreadyInUseException;

    /**
     * Persists the last delivered sequence number and waits for the KV write
     * to be acknowledged. Called from inside the handler, it includes the
     * message being handled. Before the first {@link #consume} it is a no-op:
     * nothing has been delivered and the entry is not claimed yet.
     */
    void commitSync() throws CommitException;

    /**
     * Persists the last delivered sequence number asynchronously. Never blocks
     * on the server: the write is queued on the consumer's commit pipeline and
     * the returned future completes when it is acknowledged. Several commits
     * may be pending at once; they are applied in call order. A future that
     * fails carries a {@link CommitException}. Before the first
     * {@link #consume} it returns a completed future and writes nothing.
     */
    CompletableFuture<Void> commitAsync();

    /**
     * Repositions the consumer to a specific sequence number. After seek(N),
     * the next message delivered will be sequence N+1. While consuming,
     * delivery is stopped, the new offset written, a new ordered consumer
     * started under the next generation prefix, and only then the previous one
     * deleted, so the instance stays visible on the server throughout. If the
     * write fails the consumer is left stopped. While not consuming, the
     * position is only recorded: the claim at the next {@link #consume} stores it.
     */
    void seek(long seq) throws CommitException, IOException, JetStreamApiException;

    /** Repositions the consumer to the beginning of the stream, regardless of StartPosition. */
    void seekToBeginning() throws CommitException, IOException, JetStreamApiException;

    /** Repositions the consumer to the current end of the stream. */

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
     * position is not lost) and, with auto-commit, the last sequence whose
     * handler has returned. A message whose handler is still running is not
     * waited for and not committed: it is redelivered on the next start.
     * close() does not wait for the auto-commit task, so the ErrorHandler may
     * call it. Failures of the final write are reported to the configured
     * {@link ErrorHandler}. Before the first {@link #consume} nothing is written.
     * <p>
     * The release write is conditional (compare-and-set on the revision just
     * read, retried up to 3 times on a conflict). If the entry no longer names
     * this instance as its owner (another instance claimed it after this one's
     * ordered consumer expired), it is left untouched and a
     * {@link CommitException} whose cause is an {@link AlreadyInUseException}
     * naming the current owner is reported to the ErrorHandler. If the entry no
     * longer exists, nothing is written.
     */
    @Override
    void close();
}
