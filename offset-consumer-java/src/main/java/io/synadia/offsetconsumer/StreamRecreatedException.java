package io.synadia.offsetconsumer;

/**
 * Reported to the {@link ErrorHandler} when the stream was deleted and
 * recreated since the offset was stored (detected when the consumer is created
 * or while it is consuming). The stored offset refers to the old stream, so the
 * consumer repositions itself according to its {@link StartPosition}, like
 * Kafka's {@code auto.offset.reset}. It is informational: consumption continues.
 */
public class StreamRecreatedException extends Exception {

    private final long storedSeq;

    public StreamRecreatedException(long storedSeq) {
        super("Stream was recreated, offset reset to the start position (stored sequence " + storedSeq + ")");
        this.storedSeq = storedSeq;
    }

    /**
     * Returns the sequence that was stored (or last delivered) for the old
     * incarnation of the stream.
     */
    public long getStoredSeq() {
        return storedSeq;
    }
}
