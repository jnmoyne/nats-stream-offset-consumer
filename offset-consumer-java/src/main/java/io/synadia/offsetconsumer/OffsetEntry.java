package io.synadia.offsetconsumer;

import io.nats.client.support.Encoding;
import io.nats.client.support.JsonParseException;
import io.nats.client.support.JsonParser;
import io.nats.client.support.JsonValue;
import io.nats.client.support.JsonValueUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * The record stored in the offset-consumers KV bucket.
 * <pre>
 * {"seq":123,"committed_at":"...","owner":"oc-...","host":"...","claimed_at":"..."}
 * </pre>
 * An empty owner means the entry is not owned by any live instance.
 */
final class OffsetEntry {

    private final long seq;
    private final String committedAt;
    private final String owner;
    private final String host;
    private final String claimedAt;

    OffsetEntry(long seq, String committedAt, String owner, String host, String claimedAt) {
        this.seq = seq;
        this.committedAt = committedAt == null ? "" : committedAt;
        this.owner = owner == null ? "" : owner;
        this.host = host == null ? "" : host;
        this.claimedAt = claimedAt == null ? "" : claimedAt;
    }

    static OffsetEntry empty() {
        return new OffsetEntry(0, "", "", "", "");
    }

    long getSeq() {
        return seq;
    }

    String getCommittedAt() {
        return committedAt;
    }

    String getOwner() {
        return owner;
    }

    String getHost() {
        return host;
    }

    String getClaimedAt() {
        return claimedAt;
    }

    OffsetConsumerInfo toInfo() {
        return new OffsetConsumerInfo(seq, committedAt, owner, host, claimedAt);
    }

    byte[] toJson() {
        StringBuilder sb = new StringBuilder(160);
        sb.append("{\"seq\":").append(seq);
        appendString(sb, "committed_at", committedAt);
        appendString(sb, "owner", owner);
        appendString(sb, "host", host);
        appendString(sb, "claimed_at", claimedAt);
        sb.append('}');
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void appendString(StringBuilder sb, String name, String value) {
        sb.append(",\"").append(name).append("\":\"");
        Encoding.jsonEncode(sb, value);
        sb.append('"');
    }

    static OffsetEntry fromJson(byte[] data) throws IOException {
        try {
            JsonValue jv = JsonParser.parse(new String(data, StandardCharsets.UTF_8));
            return new OffsetEntry(
                    JsonValueUtils.readLong(jv, "seq", 0),
                    JsonValueUtils.readString(jv, "committed_at", ""),
                    JsonValueUtils.readString(jv, "owner", ""),
                    JsonValueUtils.readString(jv, "host", ""),
                    JsonValueUtils.readString(jv, "claimed_at", ""));
        } catch (JsonParseException e) {
            throw new IOException("Failed to parse offset entry", e);
        }
    }
}
