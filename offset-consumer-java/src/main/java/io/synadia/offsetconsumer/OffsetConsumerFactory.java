package io.synadia.offsetconsumer;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.KeyValueManagement;
import io.nats.client.NUID;
import io.nats.client.StreamContext;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.StreamInfo;

import java.io.IOException;
import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory for creating OffsetConsumer instances.
 */
public final class OffsetConsumerFactory {

    static final String KV_BUCKET_NAME = "offset-consumers";

    /**
     * Grace window during which a fresh ownership claim is considered alive
     * even if its ordered consumer cannot be found on the server yet. jnats
     * does not let the ordered consumer's inactive threshold be configured, so
     * the server default for ephemeral consumers (5 seconds) applies, and the
     * same value is used here.
     */
    static final Duration CLAIM_GRACE = Duration.ofSeconds(5);

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
     * Ensures the offset-consumers KV bucket exists, reads any stored offset,
     * resolves the start sequence and claims ownership of the offset entry.
     * Does NOT start consuming — call consume() for that.
     *
     * @param nc     NATS connection (must have JetStream capability)
     * @param config consumer configuration
     * @return a new OffsetConsumer instance
     * @throws IllegalArgumentException if consumerName is empty or contains '.'
     * @throws AlreadyInUseException    if another live instance owns the offset entry
     * @throws IOException              if the target stream does not exist or KV operations fail
     * @throws JetStreamApiException    if JetStream API calls fail
     */
    public static OffsetConsumer create(Connection nc, ConsumerConfig config)
            throws IOException, JetStreamApiException, AlreadyInUseException {

        String consumerName = config.getConsumerName();
        if (consumerName == null || consumerName.isEmpty()) {
            throw new IllegalArgumentException("consumerName must not be null or empty");
        }
        if (consumerName.contains(".")) {
            throw new IllegalArgumentException("consumerName must not contain '.'");
        }

        StreamContext streamContext = nc.getStreamContext(config.getStream());

        StreamInfo streamInfo = streamContext.getStreamInfo();
        int replicas = streamInfo.getConfiguration().getReplicas();
        KeyValue kv = ensureBucket(nc, replicas);

        String kvKey = config.getStream() + "." + consumerName;
        String owner = "oc-" + config.getStream() + "-" + consumerName + "-" + NUID.nextGlobal();
        String host = hostname();

        // Read the stored entry and check that nobody else owns it.
        KeyValueEntry kve = kv.get(kvKey);
        OffsetEntry stored = kve == null ? OffsetEntry.empty() : OffsetEntry.fromJson(kve.getValue());
        long revision = kve == null ? 0 : kve.getRevision();
        if (!stored.getOwner().isEmpty()) {
            checkOwnership(streamContext, stored);
        }

        // Claim ownership with a single CAS write that keeps the stored offset.
        OffsetEntry claim = new OffsetEntry(stored.getSeq(), stored.getCommittedAt(),
                owner, host, Instant.now().toString());
        try {
            if (revision == 0) {
                kv.create(kvKey, claim.toJson());
            } else {
                kv.update(kvKey, claim.toJson(), revision);
            }
        } catch (JetStreamApiException e) {
            if (e.getApiErrorCode() == JS_WRONG_LAST_SEQUENCE || e.getApiErrorCode() == JS_WRONG_LAST_SEQUENCE_CONSTANT) {
                OffsetEntry current = stored;
                KeyValueEntry now = kv.get(kvKey);
                if (now != null) {
                    current = OffsetEntry.fromJson(now.getValue());
                }
                throw new AlreadyInUseException(current.getOwner(), current.getHost(), current.getClaimedAt());
            }
            throw e;
        }

        return new OffsetConsumerImpl(config, streamContext, kv, kvKey, claim);
    }

    /**
     * Throws AlreadyInUseException if the entry's owner is still alive: either
     * its claim is younger than the grace window, or a consumer with its
     * prefix still exists on the stream.
     */
    private static void checkOwnership(StreamContext streamContext, OffsetEntry stored)
            throws IOException, JetStreamApiException, AlreadyInUseException {
        AlreadyInUseException inUse = new AlreadyInUseException(stored.getOwner(), stored.getHost(), stored.getClaimedAt());
        try {
            Instant claimed = Instant.parse(stored.getClaimedAt());
            if (Duration.between(claimed, Instant.now()).compareTo(CLAIM_GRACE) < 0) {
                throw inUse;
            }
        } catch (DateTimeParseException ignored) {
            // unparsable claim time: rely on the liveness check
        }
        for (String name : streamContext.getConsumerNames()) {
            if (name.startsWith(stored.getOwner())) {
                throw inUse;
            }
        }
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
        KeyValue kv = nc.keyValue(KV_BUCKET_NAME);

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
     * Ensures the offset-consumers KV bucket exists with the correct configuration:
     * replicas matching the stream (minimum 1), history 1, and a 10-minute
     * delete-marker TTL so tombstones left by deleteConsumer() are purged.
     */
    private static KeyValue ensureBucket(Connection nc, int replicas)
            throws IOException, JetStreamApiException {
        KeyValueConfiguration kvConfig = KeyValueConfiguration.builder()
                .name(KV_BUCKET_NAME)
                .replicas(Math.max(replicas, 1))
                .maxHistoryPerKey(1)
                .limitMarker(DELETE_MARKER_TTL)
                .build();

        KeyValueManagement kvMgmt = nc.keyValueManagement();
        try {
            kvMgmt.create(kvConfig);
        } catch (JetStreamApiException e) {
            if (e.getApiErrorCode() != JS_STREAM_NAME_IN_USE) {
                throw e;
            }
        }
        return nc.keyValue(KV_BUCKET_NAME);
    }
}
