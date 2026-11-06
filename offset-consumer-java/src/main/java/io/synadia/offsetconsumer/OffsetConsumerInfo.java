package io.synadia.offsetconsumer;

/**
 * Contains the stored offset information for a consumer,
 * as read from the KV store.
 */
public final class OffsetConsumerInfo {

    private final long seq;
    private final String committedAt;
    private final String owner;
    private final String host;
    private final String streamCreated;

    public OffsetConsumerInfo(long seq, String committedAt, String owner, String host) {
        this(seq, committedAt, owner, host, "");
    }

    public OffsetConsumerInfo(long seq, String committedAt, String owner, String host, String streamCreated) {
        this.seq = seq;
        this.committedAt = committedAt == null ? "" : committedAt;
        this.owner = owner == null ? "" : owner;
        this.host = host == null ? "" : host;
        this.streamCreated = streamCreated == null ? "" : streamCreated;
    }

    /**
     * Returns the last committed stream sequence number.
     */
    public long getSeq() {
        return seq;
    }

    /**
     * Returns the timestamp of the last commit (RFC3339/ISO-8601).
     */
    public String getCommittedAt() {
        return committedAt;
    }

    /**
     * Returns the owner prefix of the instance currently holding the entry, or
     * an empty string when no instance owns it. Ordered consumers created by
     * the owner have names starting with this prefix.
     */
    public String getOwner() {
        return owner;
    }

    /**
     * Returns the hostname of the owning instance, or an empty string.
     */
    public String getHost() {
        return host;
    }

    /**
     * Returns the identity of the stream incarnation the offset refers to: the
     * stream's creation time in Unix nanoseconds, as a decimal string. Empty
     * for entries written by older versions.
     */
    public String getStreamCreated() {
        return streamCreated;
    }

    @Override
    public String toString() {
        return "OffsetConsumerInfo{seq=" + seq + ", committedAt='" + committedAt + "', owner='" + owner
                + "', host='" + host + "', streamCreated='" + streamCreated + "'}";
    }
}
