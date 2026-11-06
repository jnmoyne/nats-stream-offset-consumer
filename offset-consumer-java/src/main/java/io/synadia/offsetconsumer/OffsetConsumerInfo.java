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
    private final String claimedAt;

    public OffsetConsumerInfo(long seq, String committedAt, String owner, String host, String claimedAt) {
        this.seq = seq;
        this.committedAt = committedAt == null ? "" : committedAt;
        this.owner = owner == null ? "" : owner;
        this.host = host == null ? "" : host;
        this.claimedAt = claimedAt == null ? "" : claimedAt;
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
     * Returns the timestamp at which the owner claimed the entry (RFC3339/ISO-8601), or an empty string.
     */
    public String getClaimedAt() {
        return claimedAt;
    }

    @Override
    public String toString() {
        return "OffsetConsumerInfo{seq=" + seq + ", committedAt='" + committedAt + "', owner='" + owner
                + "', host='" + host + "', claimedAt='" + claimedAt + "'}";
    }
}
