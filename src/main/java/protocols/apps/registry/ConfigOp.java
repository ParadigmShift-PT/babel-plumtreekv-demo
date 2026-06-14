package protocols.apps.registry;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.UUID;

/**
 * A single registry operation: "key {@code key} becomes {@code value}" (a set)
 * or "key {@code key} is removed" (a delete, {@code deleted == true}). This is
 * the atomic unit PlumtreeKV disseminates by gossip and stores per key.
 *
 * <h2>Last-writer-wins (why the registry converges)</h2>
 * Each op carries a {@code timestamp} (the writer's wall clock at write time)
 * and the writer's {@code originId}. A key keeps whichever op is the
 * <em>winner</em> under the total order in {@link #isNewerThan}: higher
 * timestamp first, ties broken by {@code originId} then {@code opId}. Because
 * that order is total and deterministic, any two nodes that have applied the
 * <em>same set</em> of ops hold the identical winner for every key —
 * independent of the order gossip delivered them. A delete is just an op that
 * wins and marks the key not-live; it is retained as a tombstone so a late,
 * older set cannot resurrect the key.
 *
 * <p>{@code opId} is a globally-unique id assigned once at the writer; it is the
 * final tie-breaker and the stable key the telemetry uses to correlate a write
 * on one node with its delivery on another.
 */
public final class ConfigOp {

    private final String key;
    private final String value;   // "" when deleted
    private final boolean deleted;
    private final long timestamp;
    private final String originId;
    private final UUID opId;

    public ConfigOp(String key, String value, boolean deleted, long timestamp, String originId, UUID opId) {
        this.key = key;
        this.value = value == null ? "" : value;
        this.deleted = deleted;
        this.timestamp = timestamp;
        this.originId = originId;
        this.opId = opId;
    }

    public String getKey() { return key; }
    public String getValue() { return value; }
    public boolean isDeleted() { return deleted; }
    public long getTimestamp() { return timestamp; }
    public String getOriginId() { return originId; }
    public UUID getOpId() { return opId; }

    /**
     * Total, deterministic last-writer-wins order: this op beats {@code other}
     * iff it has a strictly higher timestamp, or an equal timestamp and a greater
     * {@code originId}, or equal on both and a greater {@code opId}. {@code other}
     * may be {@code null} (no current winner), which this always beats.
     */
    public boolean isNewerThan(ConfigOp other) {
        if (other == null) {
            return true;
        }
        if (timestamp != other.timestamp) {
            return timestamp > other.timestamp;
        }
        int byOrigin = originId.compareTo(other.originId);
        if (byOrigin != 0) {
            return byOrigin > 0;
        }
        return opId.compareTo(other.opId) > 0;
    }

    /** Append this op to {@code out} (see {@link #readFrom} for the inverse). */
    public void writeTo(DataOutputStream out) throws IOException {
        out.writeUTF(key);
        out.writeBoolean(deleted);
        out.writeUTF(value);
        out.writeLong(timestamp);
        out.writeUTF(originId);
        out.writeLong(opId.getMostSignificantBits());
        out.writeLong(opId.getLeastSignificantBits());
    }

    /** Read one op previously written by {@link #writeTo}. */
    public static ConfigOp readFrom(DataInputStream in) throws IOException {
        String key = in.readUTF();
        boolean deleted = in.readBoolean();
        String value = in.readUTF();
        long timestamp = in.readLong();
        String originId = in.readUTF();
        long hi = in.readLong();
        long lo = in.readLong();
        return new ConfigOp(key, value, deleted, timestamp, originId, new UUID(hi, lo));
    }

    @Override
    public String toString() {
        return "ConfigOp{" + (deleted ? "DEL " : "SET ") + key
                + (deleted ? "" : "=" + value) + " ts=" + timestamp
                + " origin=" + originId + " op=" + opId + '}';
    }
}
