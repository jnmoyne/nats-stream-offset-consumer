package io.synadia.offsetconsumer;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.KeyValue;
import io.nats.client.NUID;
import io.nats.client.StreamContext;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;

import java.io.IOException;
import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Factory for creating OffsetConsumer instances.
 */
public final class OffsetConsumerFactory {

    static final String KV_BUCKET_NAME = "offset-consumers";
    static final String KV_STREAM_NAME = "KV_" + KV_BUCKET_NAME;

    /**
     * Upper bound for {@code stream.length() + consumerName.length()}. Ordered
     * consumer names are "oc-{stream}-{consumer}-{22-char nuid}-g{gen}" plus a
     * client suffix, and the server limits names to 255 characters; 32
     * characters are reserved for the generation and client suffixes.
     */
    static final int MAX_COMBINED_NAME_LENGTH = 255 - 32 - "oc-".length() - 2 - 22;

    /** Names usable both in a KV key token and in a consumer name. */
    private static final Pattern VALID_NAME = Pattern.compile("^[A-Za-z0-9_=-]+$");

    /** Delete-marker TTL applied when the library creates the bucket. */
    static final Duration DELETE_MARKER_TTL = Duration.ofMinutes(10);

    private static final int JS_WRONG_LAST_SEQUENCE = 10071;
    private static final int JS_WRONG_LAST_SEQUENCE_CONSTANT = 10164;
    private static final int JS_STREAM_NAME_IN_USE = 10058;
    private static final int JS_STREAM_NOT_FOUND = 10059;

    private OffsetConsumerFactory() {
        // utility class
    }

    /**
     * Creates a new offset consumer for the given stream and consumer name.
     * Ensures the offset-consumers KV bucket exists and checks the stored offset
     * entry; it writes nothing and creates nothing on the server. If the entry's
     * owner is visibly alive (a consumer whose name starts with its owner prefix
     * exists on the stream; checked once, without waiting), an
     * {@link AlreadyInUseException} is thrown. Ownership is claimed when
     * consumption starts (see {@link OffsetConsumer#consume}).
     * {@link StartPosition#END} is resolved to the stream's last sequence here;
     * the claim stores it if there is no usable stored offset by then.
     * Does NOT start consuming — call consume() for that.
     *
     * @param nc     NATS connection (must have JetStream capability)
     * @param config consumer configuration
     * @return a new OffsetConsumer instance
     * @throws IllegalArgumentException if the stream or consumer name is invalid: names may only
     *                                  contain letters, digits, '-', '_' and '=', and together
     *                                  must not exceed {@value #MAX_COMBINED_NAME_LENGTH} characters
     * @throws AlreadyInUseException    if another live instance visibly owns the offset entry
     * @throws IOException              if the target stream does not exist or KV operations fail
     * @throws JetStreamApiException    if JetStream API calls fail
     */
    public static OffsetConsumer create(Connection nc, ConsumerConfig config)
            throws IOException, JetStreamApiException, AlreadyInUseException {

        String consumerName = config.getConsumerName();
        validateNames(config.getStream(), consumerName);

        StreamContext streamContext = nc.getStreamContext(config.getStream());

        StreamInfo streamInfo = streamContext.getStreamInfo();
        int replicas = streamInfo.getConfiguration().getReplicas();
        KeyValue kv = ensureBucket(nc, replicas);
        String streamCreated = streamCreatedId(streamInfo.getCreateTime());
        long lastSeq = streamInfo.getStreamState().getLastSequence();

        String kvKey = config.getStream() + "." + consumerName;
        String owner = "oc-" + config.getStream() + "-" + consumerName + "-" + NUID.nextGlobal();
        String host = hostname();

        // Resolve StartPosition now (not when consumption starts): the claim
        // stores it if there is no usable stored offset by then.
        long initialSeq = 0;
        String initialAt = "";
        if (config.getStartPosition() == StartPosition.END && lastSeq > 0) {
            initialSeq = lastSeq;
            initialAt = Instant.now().toString();
        }

        // Fail fast if the entry is visibly owned. This check does not wait:
        // the full check runs when consumption starts.
        KeyValueEntry kve = kv.get(kvKey);
        if (kve != null) {
            OffsetEntry stored = OffsetEntry.fromJson(kve.getValue());
            if (!stored.getOwner().isEmpty() && isOwnerAlive(streamContext, stored.getOwner())) {
                throw new AlreadyInUseException(stored.getOwner(), stored.getHost());
            }
        }

        return new OffsetConsumerImpl(config, streamContext, kv, kvKey, owner, host, streamCreated,
                initialSeq, initialAt);
    }

    /**
     * Reports whether a KV create/update failed because the entry's revision
     * changed (a compare-and-set conflict).
     */
    static boolean isRevisionConflict(JetStreamApiException e) {
        return e.getApiErrorCode() == JS_WRONG_LAST_SEQUENCE || e.getApiErrorCode() == JS_WRONG_LAST_SEQUENCE_CONSTANT;
    }

    /**
     * Checks the stream and consumer names: letters, digits, '-', '_' and '='
     * only (they are used in a KV key token and in consumer names), and
     * together at most {@link #MAX_COMBINED_NAME_LENGTH} characters.
     */
    static void validateNames(String stream, String consumerName) {
        if (consumerName == null || !VALID_NAME.matcher(consumerName).matches()) {
            throw new IllegalArgumentException("Invalid consumer name \"" + consumerName
                    + "\": use letters, digits, '-', '_' and '=' only");
        }
        if (stream == null || !VALID_NAME.matcher(stream).matches()) {
            throw new IllegalArgumentException("Invalid stream name \"" + stream
                    + "\": use letters, digits, '-', '_' and '=' only");
        }
        if (stream.length() + consumerName.length() > MAX_COMBINED_NAME_LENGTH) {
            throw new IllegalArgumentException("Invalid consumer name \"" + consumerName
                    + "\": stream and consumer names together exceed " + MAX_COMBINED_NAME_LENGTH + " characters");
        }
    }

    /**
     * Identifies a stream incarnation by its creation time, as decimal Unix
     * nanoseconds (the same string in every port). Empty when unknown.
     */
    static String streamCreatedId(ZonedDateTime created) {
        if (created == null || created.getYear() <= 1) {
            return "";
        }
        Instant i = created.toInstant();
        return Long.toString(Math.addExact(Math.multiplyExact(i.getEpochSecond(), 1_000_000_000L), i.getNano()));
    }

    /**
     * Reports whether a stored entry refers to an earlier incarnation of the
     * stream. Entries from older versions carry no stream identity; for those
     * a stored sequence beyond the stream's last sequence is the tell-tale sign.
     */
    static boolean streamWasRecreated(OffsetEntry entry, String currentCreated, long lastSeq) {
        if (!entry.getStreamCreated().isEmpty() && !currentCreated.isEmpty()) {
            return !entry.getStreamCreated().equals(currentCreated);
        }
        return entry.getSeq() > lastSeq;
    }

    /**
     * Reports whether a consumer whose name starts with the owner prefix
     * exists on the stream (the owner's ordered consumer).
     */
    static boolean isOwnerAlive(StreamContext streamContext, String owner)
            throws IOException, JetStreamApiException {
        for (String name : streamContext.getConsumerNames()) {
            if (name.startsWith(owner)) {
                return true;
            }
        }
        return false;
    }

    static String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            String h = System.getenv("HOSTNAME");
            return h == null ? "" : h;
        }
    }

    /**
     * Lists all offset consumers for the given stream by scanning KV keys with the stream name prefix.
     *
     * @param nc     NATS connection
     * @param stream stream name to list consumers for
     * @return a map of consumer name to OffsetConsumerInfo, empty if none exist
     * @throws IOException           if KV operations fail
     * @throws JetStreamApiException if JetStream API calls fail
     */
    public static Map<String, OffsetConsumerInfo> listConsumers(Connection nc, String stream)
            throws IOException, JetStreamApiException {
        Map<String, OffsetConsumerInfo> result = new HashMap<>();
        KeyValue kv;
        try {
            kv = nc.keyValue(KV_BUCKET_NAME);
        } catch (IOException e) {
            if (isStreamNotFound(e)) {
                return result; // bucket does not exist: no consumers
            }
            throw e;
        }

        String prefix = stream + ".";
        List<String> keys;
        try {
            keys = kv.keys(stream + ".*");
        } catch (JetStreamApiException e) {
            if (e.getApiErrorCode() == JS_STREAM_NOT_FOUND) {
                return result; // bucket does not exist: no consumers
            }
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while listing keys", e);
        }

        for (String key : keys) {
            KeyValueEntry entry = kv.get(key);
            if (entry == null) {
                continue; // deleted between listing and read
            }
            result.put(key.substring(prefix.length()), OffsetEntry.fromJson(entry.getValue()).toInfo());
        }
        return result;
    }

    /**
     * Returns the offset-consumers KV bucket, creating it if it does not exist
     * with replicas matching the stream (minimum 1), history 1, and a 10-minute
     * delete-marker TTL so tombstones left by deleteConsumer() are purged. An
     * existing bucket's configuration is never overwritten; its replica count
     * is only ever raised (best effort) so that offsets of a replicated stream
     * are not stored with fewer replicas than the stream itself.
     */
    private static KeyValue ensureBucket(Connection nc, int replicas)
            throws IOException, JetStreamApiException {
        int wanted = Math.max(replicas, 1);
        KeyValue kv;
        try {
            kv = nc.keyValue(KV_BUCKET_NAME);
        } catch (IOException e) {
            if (!isStreamNotFound(e)) {
                throw e;
            }
            KeyValueConfiguration kvConfig = KeyValueConfiguration.builder()
                    .name(KV_BUCKET_NAME)
                    .replicas(wanted)
                    .maxHistoryPerKey(1)
                    .limitMarker(DELETE_MARKER_TTL)
                    .build();
            try {
                nc.keyValueManagement().create(kvConfig);
            } catch (JetStreamApiException ce) {
                if (ce.getApiErrorCode() != JS_STREAM_NAME_IN_USE) {
                    throw ce;
                }
                // created concurrently, possibly with another configuration
            }
            kv = nc.keyValue(KV_BUCKET_NAME);
        }
        raiseBucketReplicas(nc, wanted);
        return kv;
    }

    /**
     * Raises the bucket's replica count to replicas if it is lower. Errors
     * (permissions, too few servers) are ignored: the bucket keeps working with
     * its current replicas.
     */
    private static void raiseBucketReplicas(Connection nc, int replicas) {
        try {
            JetStreamManagement jsm = nc.jetStreamManagement();
            StreamConfiguration sc = jsm.getStreamInfo(KV_STREAM_NAME).getConfiguration();
            if (sc.getReplicas() >= replicas) {
                return;
            }
            jsm.updateStream(StreamConfiguration.builder(sc).replicas(replicas).build());
        } catch (Exception ignored) {
            // best effort
        }
    }

    /** jnats reports a missing bucket as an IOException wrapping a "stream not found" API error. */
    private static boolean isStreamNotFound(IOException e) {
        Throwable cause = e.getCause();
        return cause instanceof JetStreamApiException
                && ((JetStreamApiException) cause).getApiErrorCode() == JS_STREAM_NOT_FOUND;
    }
}
