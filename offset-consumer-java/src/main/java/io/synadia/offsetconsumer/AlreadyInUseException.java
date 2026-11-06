package io.synadia.offsetconsumer;

/**
 * Thrown by {@link OffsetConsumerFactory#create} when the offset entry for the
 * stream and consumer name is owned by another live instance: either its
 * ordered consumer still exists on the server, or its claim is too recent.
 */
public class AlreadyInUseException extends Exception {

    private final String owner;
    private final String host;
    private final String claimedAt;

    public AlreadyInUseException(String owner, String host, String claimedAt) {
        super("Offset consumer already in use by another instance (owner \"" + owner
                + "\" on host \"" + host + "\", claimed at " + claimedAt + ")");
        this.owner = owner;
        this.host = host;
        this.claimedAt = claimedAt;
    }

    /**
     * Returns the owner prefix recorded in the KV entry. Ordered consumers
     * created by the owning instance have names starting with this prefix.
     */
    public String getOwner() {
        return owner;
    }

    /**
     * Returns the hostname recorded by the owning instance.
     */
    public String getHost() {
        return host;
    }

    /**
     * Returns the RFC3339 timestamp at which the owner claimed the entry.
     */
    public String getClaimedAt() {
        return claimedAt;
    }
}
