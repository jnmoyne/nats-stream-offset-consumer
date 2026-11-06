# Java API Contract: Offset Consumer Library

**Package**: `io.synadia.offsetconsumer`
**Artifact**: `io.synadia:offset-consumer`
**Client**: `io.nats:jnats` 2.26.4 (Java 11+)

---

## Construction

```java
public final class OffsetConsumerFactory {
    /**
     * Ensures the offset-consumers KV bucket exists, reads any stored offset,
     * resolves the start sequence and claims ownership of the offset entry.
     * Does NOT start consuming — call consume() for that.
     *
     * @throws IllegalArgumentException if consumerName is empty or contains '.'
     * @throws AlreadyInUseException    if another live instance owns the offset entry
     */
    public static OffsetConsumer create(Connection nc, ConsumerConfig config)
        throws IOException, JetStreamApiException, AlreadyInUseException;

    /** All offset consumers of a stream (KV keys "{stream}.*"); empty if none. */
    public static Map<String, OffsetConsumerInfo> listConsumers(Connection nc, String stream)
        throws IOException, JetStreamApiException;
}
```

---

## OffsetConsumer Interface

```java
public interface OffsetConsumer extends AutoCloseable {
    /** Starts delivering messages; the handler is called serially. */
    void consume(MessageHandler handler) throws IOException, JetStreamApiException;

    /** Persists the last delivered sequence and waits for the acknowledgement. */
    void commitSync() throws CommitException;

    /**
     * Queues the write on the consumer's single commit thread and returns
     * immediately; several commits may be pending and are applied in call order.
     * The future completes on acknowledgement or fails with CommitException.
     */
    CompletableFuture<Void> commitAsync();

    /**
     * Stops consumption, deletes its ordered consumer, writes the new offset and
     * restarts consumption under a new generation prefix if it was running.
     * After seek(N) the next message is N+1. If the write fails the consumer is
     * left stopped with its position unchanged.
     */
    void seek(long seq) throws CommitException, IOException, JetStreamApiException;
    void seekToBeginning() throws CommitException, IOException, JetStreamApiException;
    void seekToEnd() throws CommitException, IOException, JetStreamApiException;

    /** Fresh read of the offset entry, or null if it does not exist. */
    OffsetConsumerInfo info() throws IOException, JetStreamApiException;

    /** Removes the offset entry; a later close() will not recreate it. */
    void deleteConsumer() throws IOException, JetStreamApiException;

    /**
     * Waits (5s bound) for pending commits (leftovers are abandoned, their
     * futures complete), deletes the ordered consumer, and releases ownership
     * with one final write that persists the highest sequence passed to a
     * commit call and, with auto-commit, the last delivered sequence. Failures
     * of the final write go to the ErrorHandler.
     */
    @Override
    void close();
}

public final class OffsetConsumerInfo {
    public long getSeq();
    public String getCommittedAt();
    public String getOwner();     // "" when unowned
    public String getHost();
    public String getClaimedAt();
}
```

---

## ConsumerConfig (Builder Pattern)

```java
new ConsumerConfig.Builder(String stream, String consumerName)
    .autoCommit(boolean)                 // default false
    .autoCommitInterval(Duration)        // default 5s
    .filterSubjects(List<String>)
    .errorHandler(ErrorHandler)          // (OffsetConsumer, Exception) -> void
    .startPosition(StartPosition)        // BEGINNING (default) or END
    .build();
```

jnats does not expose the ordered consumer's inactive threshold; the server default for ephemeral consumers (5s) applies, and the same value is the claim grace window.

---

## Exceptions

```java
public class CommitException extends Exception { ... }

/** Another live instance owns the offset entry. Thrown by OffsetConsumerFactory.create. */
public class AlreadyInUseException extends Exception {
    public String getOwner();
    public String getHost();
    public String getClaimedAt();
}
```

Ordered consumers created by an instance are named `oc-{stream}-{consumer}-{nuid}-g{generation}-{NUID}`.
