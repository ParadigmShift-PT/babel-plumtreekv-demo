package protocols.apps.registry.messages;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import protocols.apps.registry.ConfigOp;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

/**
 * A point-to-point registry snapshot exchange, sent <b>directly</b> to one peer
 * (not broadcast) over HyParView's shared channel via {@code sendMessage}. Two
 * kinds:
 * <ul>
 *   <li>{@link Kind#REQUEST} — "send me your current registry", sent once a node
 *       joins so a late joiner sees existing state immediately instead of waiting
 *       for new gossip;</li>
 *   <li>{@link Kind#REPLY} — the responder's full set of winning {@link ConfigOp}s
 *       (including tombstones), which the requester merges under last-writer-wins.</li>
 * </ul>
 *
 * <p>Rides HyParView's channel specifically: the peers a {@code NeighborUp}
 * reports are HyParView active-view members reachable there with no port
 * translation, so the notification {@code Host} is exactly the {@code sendMessage}
 * destination.
 *
 * <p>Handler class: message. <b>ID:</b> {@value #MSG_ID}. Owning protocol:
 * {@code PlumtreeKVApp} (id 300).
 */
public class RegistrySyncMessage extends ProtoMessage {

    /** PlumtreeKVApp owns protocol id 300; its messages start at 301. */
    public static final short MSG_ID = 301;

    public enum Kind { REQUEST, REPLY }

    private final Kind kind;
    private final List<ConfigOp> ops; // empty for REQUEST

    public RegistrySyncMessage(Kind kind, List<ConfigOp> ops) {
        super(MSG_ID);
        this.kind = kind;
        this.ops = ops == null ? List.of() : ops;
    }

    public Kind getKind() { return kind; }
    public List<ConfigOp> getOps() { return ops; }

    // Netty's ByteBuf has no writeUTF, so we length-prefix UTF-8 bytes ourselves.
    private static void writeString(String s, ByteBuf out) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.writeBytes(b);
    }

    private static String readString(ByteBuf in) {
        int n = in.readInt();
        byte[] b = new byte[n];
        in.readBytes(b);
        return new String(b, StandardCharsets.UTF_8);
    }

    private static void writeOp(ConfigOp op, ByteBuf out) {
        writeString(op.getKey(), out);
        out.writeBoolean(op.isDeleted());
        writeString(op.getValue(), out);
        out.writeLong(op.getTimestamp());
        writeString(op.getOriginId(), out);
        out.writeLong(op.getOpId().getMostSignificantBits());
        out.writeLong(op.getOpId().getLeastSignificantBits());
    }

    private static ConfigOp readOp(ByteBuf in) {
        String key = readString(in);
        boolean deleted = in.readBoolean();
        String value = readString(in);
        long ts = in.readLong();
        String originId = readString(in);
        long hi = in.readLong();
        long lo = in.readLong();
        return new ConfigOp(key, value, deleted, ts, originId, new UUID(hi, lo));
    }

    public static final ISerializer<RegistrySyncMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(RegistrySyncMessage m, ByteBuf out) throws IOException {
            out.writeByte(m.kind.ordinal());
            out.writeInt(m.ops.size());
            for (ConfigOp op : m.ops) {
                writeOp(op, out);
            }
        }

        @Override
        public RegistrySyncMessage deserialize(ByteBuf in) throws IOException {
            Kind kind = Kind.values()[in.readByte()];
            int n = in.readInt();
            List<ConfigOp> ops = new ArrayList<>(Math.max(0, n));
            for (int i = 0; i < n; i++) {
                ops.add(readOp(in));
            }
            return new RegistrySyncMessage(kind, ops);
        }
    };
}
