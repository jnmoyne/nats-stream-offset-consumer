package io.synadia.offsetconsumer;

/**
 * Thrown by {@link OffsetConsumerFactory#create} and {@link OffsetConsumer#consume}
 * when the offset entry for the stream and consumer name is owned by another
 * live instance: a consumer whose name starts with its owner prefix exists on
 * the stream (at consume(), also after the liveness wait), or another instance
 * wrote the entry while this one was claiming it.
 */
public class AlreadyInUseException extends Exception {

    private final String owner;
    private final String host;

    public AlreadyInUseException(String owner, String host) {
        super("Offset consumer already in use by another instance (owner \"" + owner
                + "\" on host \"" + host + "\")");
        this.owner = owner;
        this.host = host;
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
}
