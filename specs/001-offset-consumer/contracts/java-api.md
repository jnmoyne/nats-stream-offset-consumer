# Java API Contract: Offset Consumer Library

**Package**: `io.synadia.offsetconsumer`
**Artifact**: `io.synadia:offset-consumer`
**Client**: `io.nats:jnats` 2.26.4 (Java 11+)

---

## Construction

```java
public final class OffsetConsumerFactory {
    /**
     * Opens the offset-consumers KV bucket (creating it only if it does not
     * exist; an existing bucket's configuration is never overwritten, its
     * replicas are only raised, best effort, to the stream's), resolves
     * StartPosition (END = the stream's current last sequence, used by the claim
     * if there is no usable stored offset by then) and reads the stored entry.
     * Writes nothing and creates nothing on the server. Ownership is claimed by
     * consume(). Does NOT start consuming.
     *
     * @throws IllegalArgumentException if the stream or consumer name is invalid
     *         (letters, digits, '-', '_', '=' only; together at most 196 characters)
     * @throws AlreadyInUseException    if the entry's owner is visibly alive: a consumer
     *         whose name starts with its owner prefix exists (one listing, no wait)
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
    /**
     * Claims ownership of the offset entry (see Ownership) every time
     * consumption starts from a stopped state, then starts delivering
     * messages; the handler is called serially. The claim stores the start
     * position: a pending seek, else the stored offset, else the StartPosition
     * resolved at creation (also when the stored offset belongs to an earlier
     * incarnation of the stream; a StreamRecreatedException is then reported to
     * the ErrorHandler after the claim). May block for the liveness wait (5s)
     * when the entry names another owner without a consumer.
     *
     * @throws AlreadyInUseException if another live instance owns the entry
     * @throws IllegalStateException if closed or already consuming
     */
    void consume(MessageHandler handler) throws IOException, JetStreamApiException, AlreadyInUseException;

    /**
     * Persists the last delivered sequence and waits for the acknowledgement.
     * Called from inside the handler, it includes the message being handled.
     * A no-op before the first claim.
     */
    void commitSync() throws CommitException;

    /**
     * Queues the write on the consumer's single commit thread and returns
     * immediately; several commits may be pending and are applied in call order.
     * The future completes on acknowledgement or fails with CommitException.
     * Before the first claim it returns a completed future and writes nothing.
     */
    CompletableFuture<Void> commitAsync();

    /**
     * After seek(N) the next message is N+1. While consuming (make before
     * break): stops delivery without deleting the server-side ordered consumer,
     * writes the new offset, starts the next generation, then deletes the old
     * server-side consumer in all cases (also when the write or the restart
     * failed; the consumer is then left stopped, position unchanged on a write
     * failure). While not consuming: only records the position (pending seek,
     * with the new stream identity if any); nothing is written until the claim
     * at the next consume() stores it.
     */
    void seek(long seq) throws CommitException, IOException, JetStreamApiException;
    void seekToBeginning() throws CommitException, IOException, JetStreamApiException; // seek(0), regardless of StartPosition
    void seekToEnd() throws CommitException, IOException, JetStreamApiException;       // seek(stream last seq)

    /** Fresh read of the offset entry, or null if it does not exist. */
    OffsetConsumerInfo info() throws IOException, JetStreamApiException;

    /** Removes the offset entry; a later close() will not recreate it. */
    void deleteConsumer() throws IOException, JetStreamApiException;

    /**
     * Waits (5s bound) for pending commits (leftovers are abandoned, their
     * futures complete), deletes the ordered consumer, and releases ownership
     * with one final write (none before the first claim) that persists the highest sequence passed to a
     * commit call and, with auto-commit, the last sequence whose handler has
     * returned (a message still in its handler is not waited for and is
     * redelivered on the next start). Does not wait for the auto-commit task,
     * so the ErrorHandler may call close(). Failures of the final write go to
     * the ErrorHandler. The release write is conditional: it is a CAS update on
     * the revision just read, done only while the entry still names this
     * instance as owner (a revision conflict re-reads, at most 3 attempts in
     * total, then the failure is reported). If another instance owns the entry it
     * is left untouched and a CommitException ("Release skipped: the offset entry
     * is no longer owned by this instance") whose cause is an
     * AlreadyInUseException carrying the entry's owner/host is
     * reported; if the entry no longer exists nothing is written and nothing is
     * reported. close() cannot throw the checked AlreadyInUseException, so it is
     * reported as that CommitException's cause.
     */
    @Override
    void close();
}

public final class OffsetConsumerInfo {
    public long getSeq();
    public String getCommittedAt();
    public String getOwner();     // "" when unowned
    public String getHost();
    public String getStreamCreated(); // stream creation time in Unix ns (decimal string); "" for older entries
}
```

consume(), seek() and close() are serialized: at most one ordered consumer
delivers at a time (during a seek the previous one, already stopped, is deleted
just after the next one is created). Auto-commit commits the last sequence whose handler has returned,
never a message still being handled. Messages delivered by a stopped or
replaced ordered consumer are dropped.

### Stream recreation

Every KV write carries `stream_created`. At the claim an entry is from an
earlier incarnation if both identities are set and differ, or (legacy entry
without identity) if its seq is past the stream's last sequence. While
consuming, jnats does not report ordered consumer resets to the library, so the
consumer checks the ordered consumer's server-side name every second (and the
consumer name of each delivered message); when it changes, the stream is polled
(every 1s until it exists) and, if its identity changed, the consumer reports a
StreamRecreatedException and seeks to its StartPosition on the new stream (the
new identity is applied atomically with that seek write: commits submitted
before it carry the old identity, commits after it the new one). The
ordered consumer pulls with a 5s expiry (2.5s idle heartbeat), so jnats
recreates it, and the change is noticed, within about 7.5 to 15 seconds of the
stream being deleted.

---

## ConsumerConfig (Builder Pattern)

```java
new ConsumerConfig.Builder(String stream, String consumerName)
    .autoCommit(boolean)                 // default false
    .autoCommitInterval(Duration)        // default 5s
    .filterSubjects(List<String>)
    .errorHandler(ErrorHandler)          // (OffsetConsumer, Exception) -> void
    .startPosition(StartPosition)        // BEGINNING (default) or END, resolved and stored at creation
    .build();
```

jnats does not expose the ordered consumer's inactive threshold; the server default for ephemeral consumers (5s) applies.

---

## Exceptions

```java
public class CommitException extends Exception { ... }

/**
 * Reported to the ErrorHandler (informational, consumption continues) when the
 * stream was deleted and recreated since the offset was stored.
 */
public class StreamRecreatedException extends Exception {
    public long getStoredSeq();
}

/**
 * Another live instance owns the offset entry. Checked; thrown by
 * OffsetConsumerFactory.create (visible owner) and declared on consume() (the
 * claim), like the factory already does, so callers handle "in use" at both
 * points with the same type.
 */
public class AlreadyInUseException extends Exception {
    public String getOwner();
    public String getHost();
}
```

Ordered consumers created by an instance are named `oc-{stream}-{consumer}-{nuid}-g{generation}-{NUID}`.

---

## Ownership

- **No claimed_at.** Entries are `{seq, committed_at, owner, host,
  stream_created}`; `claimed_at` in entries written by older versions is
  ignored. No timestamps are compared.
- **At create.** If the stored entry names an owner and a consumer whose name
  starts with that owner prefix exists on the stream (one listing, no wait),
  create throws AlreadyInUseException(owner, host). Nothing is written.
- **Claim (at consume(), lifecycle lock held).** Up to 3 rounds: read the entry
  (revision r). If it names another owner: list consumers; a match is
  AlreadyInUseException; otherwise wait 5s (local
  sleep; no jitter, the CAS decides between concurrent starters) and list again; a match is AlreadyInUseException. Read the stream
  info; if its identity is set and differs from the one recorded at creation
  (the stream was recreated in between), record the current identity and
  re-resolve the StartPosition against the current stream's last sequence
  (under the state lock), then check the entry for recreation; compute the position (pending seek, else stored
  offset, else the StartPosition resolved at creation when the entry is missing
  or from an earlier stream incarnation), and write with a CAS (create if
  missing, update at r) on the commit thread under the commit-submission lock.
  On success the instance is claimed (pending seek cleared, delete flag
  cleared, positions set to the claimed seq) and StreamRecreatedException is
  reported if applicable. On a revision conflict the entry is re-read: another
  owner is AlreadyInUseException (it wrote during the wait, so it is alive);
  otherwise (a release, or this instance's own late commit) the claim is
  retried, and the conflict is thrown after the third round. A live owner is
  never without a consumer for longer than the wait: seeks are
  make-before-break, and an ordered consumer reset recreates its consumer
  within seconds. After a crash a new instance is admitted once the crashed
  owner's ordered consumer has expired (5s after its connection went away) and
  the wait has passed.
- **Before the first claim** commits are no-ops, a seek only records the
  position, and close() writes nothing.
- **Close order.** Stop and delete the ordered consumer, then write the release
  entry (only if claimed; conditionally, see close() above; serialized with commit submission;
  skipped after deleteConsumer(), when the entry is gone, or when another
  instance owns it), so an instance displaced without noticing cannot clear the
  new owner's claim.
