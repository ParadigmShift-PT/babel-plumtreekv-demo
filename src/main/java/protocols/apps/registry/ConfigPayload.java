package protocols.apps.registry;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * The registry's "application protocol" that rides <em>inside</em> the gossip
 * broadcast's opaque {@code byte[]} payload. The dissemination layer doesn't
 * know or care what these bytes mean — that is the point of layering: gossip
 * moves bytes; PlumtreeKV decides they are a {@link ConfigOp}.
 *
 * <p>One operation travels per broadcast. The encoding (via {@link ConfigOp#writeTo})
 * matches the snapshot encoding used for sync.
 */
public final class ConfigPayload {

    private ConfigPayload() {
        // Static encode/decode helpers only.
    }

    /** Serialize a registry op to the bytes carried inside a broadcast message. */
    public static byte[] encode(ConfigOp op) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos)) {
            op.writeTo(out);
        } catch (IOException e) {
            throw new UncheckedIOException(e); // in-memory buffer cannot fail
        }
        return baos.toByteArray();
    }

    /** Parse the bytes delivered by the gossip layer back into a registry op. */
    public static ConfigOp decode(byte[] bytes) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            return ConfigOp.readFrom(in);
        } catch (IOException | RuntimeException e) {
            throw new UncheckedIOException(new IOException("Malformed registry payload", e));
        }
    }
}
