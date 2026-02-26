package io.synadia.offsetconsumer;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.KeyValueManagement;
import io.nats.client.StreamContext;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.StreamInfo;

import java.io.IOException;

/**
 * Factory for creating OffsetConsumer instances.
 */
public final class OffsetConsumerFactory {

    private static final String KV_BUCKET_NAME = "offset-consumers";

    private OffsetConsumerFactory() {
        // utility class
    }

    /**
     * Creates a new offset consumer for the given stream and consumer name.
     * Ensures the offset-consumers KV bucket exists, reads any stored offset,
     * and resolves the start sequence. Does NOT start consuming — call consume() for that.
     *
     * @param nc     NATS connection (must have JetStream capability)
     * @param config consumer configuration
     * @return a new OffsetConsumer instance
     * @throws IllegalArgumentException if consumerName is empty or contains '.'
     * @throws IOException              if the target stream does not exist or KV operations fail
     * @throws JetStreamApiException    if JetStream API calls fail
     */
    public static OffsetConsumer create(Connection nc, ConsumerConfig config)
            throws IOException, JetStreamApiException {

        String consumerName = config.getConsumerName();
        if (consumerName == null || consumerName.isEmpty()) {
            throw new IllegalArgumentException("consumerName must not be null or empty");
        }
        if (consumerName.contains(".")) {
            throw new IllegalArgumentException("consumerName must not contain '.'");
        }

        // Get StreamContext for the configured stream
        StreamContext streamContext = nc.getStreamContext(config.getStream());

        // Get stream info to determine replicas
        StreamInfo streamInfo = streamContext.getStreamInfo();
        int replicas = streamInfo.getConfiguration().getReplicas();
        if (replicas < 1) {
            replicas = 1;
        }

        // Create or get the KV bucket
        KeyValue kv = ensureBucket(nc, replicas);

        // Build the KV key
        String kvKey = config.getStream() + "." + consumerName;

        // Read stored offset
        long lastRevision = 0;
        long startSeq = 0;

        KeyValueEntry entry = kv.get(kvKey);
        if (entry != null) {
            // Parse JSON to extract seq
            String json = new String(entry.getValue());
            long seq = parseSeqFromJson(json);
            lastRevision = entry.getRevision();
            startSeq = seq;
        }
        // If entry is null, no stored offset — use configured start position (startSeq stays 0)

        return new OffsetConsumerImpl(nc, config, streamContext, kv, kvKey, lastRevision, startSeq);
    }

    /**
     * Ensures the offset-consumers KV bucket exists with the correct configuration.
     */
    private static KeyValue ensureBucket(Connection nc, int replicas)
            throws IOException, JetStreamApiException {
        KeyValueConfiguration kvConfig = KeyValueConfiguration.builder()
                .name(KV_BUCKET_NAME)
                .replicas(replicas)
                .maxHistoryPerKey(1)
                .build();

        KeyValueManagement kvMgmt = nc.keyValueManagement();
        try {
            kvMgmt.create(kvConfig);
        } catch (JetStreamApiException e) {
            // Bucket may already exist — error code 10058 means stream name already in use
            if (e.getApiErrorCode() != 10058) {
                throw e;
            }
        }
        return nc.keyValue(KV_BUCKET_NAME);
    }

    /**
     * Parses the "seq" field from a JSON string like {"seq":123,"committed_at":"..."}.
     */
    static long parseSeqFromJson(String json) {
        // Find "seq": and extract the number
        int seqIdx = json.indexOf("\"seq\"");
        if (seqIdx < 0) {
            return 0;
        }
        int colonIdx = json.indexOf(':', seqIdx + 5);
        if (colonIdx < 0) {
            return 0;
        }
        int start = colonIdx + 1;
        // Skip whitespace
        while (start < json.length() && Character.isWhitespace(json.charAt(start))) {
            start++;
        }
        int end = start;
        while (end < json.length() && Character.isDigit(json.charAt(end))) {
            end++;
        }
        if (start == end) {
            return 0;
        }
        return Long.parseLong(json.substring(start, end));
    }
}
