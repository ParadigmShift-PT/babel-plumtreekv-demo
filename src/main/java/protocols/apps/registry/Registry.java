package protocols.apps.registry;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The replicated key→value store: one {@link ConfigOp} winner per key under
 * last-writer-wins (see {@link ConfigOp#isNewerThan}). All mutation goes through
 * {@link #apply}; reads ({@link #liveEntries}, {@link #snapshotOps},
 * {@link #digest}) are used by the web UI thread and the periodic digest timer,
 * so every method is synchronized on this instance.
 *
 * <p><b>Tombstones.</b> A delete is retained as a winning op with
 * {@code deleted == true} so it cannot be undone by a re-delivered older set;
 * such keys are simply not <em>live</em> (hidden from the UI and the digest).
 *
 * <p><b>{@link #digest} is the convergence oracle:</b> a stable 64-bit FNV-1a
 * hash over the {@code (key, value)} of every <em>live</em> entry in
 * key-sorted order. Two nodes that have applied the same set of ops show the
 * same visible registry and therefore the same digest — the experiments harness
 * asserts all live nodes agree once gossip quiesces. (Sorting matters: map
 * iteration order isn't stable across nodes, so we always fold in key order.)
 */
public final class Registry {

    private static final long FNV_OFFSET = 0xcbf29ce484222325L;
    private static final long FNV_PRIME = 0x100000001b3L;

    private final Map<String, ConfigOp> winners = new HashMap<>();

    /**
     * Apply an op under last-writer-wins. Returns {@code true} if it became the
     * new winner for its key (the visible registry may have changed),
     * {@code false} if an older op the current winner already supersedes.
     * Idempotent and order-independent.
     */
    public synchronized boolean apply(ConfigOp op) {
        ConfigOp current = winners.get(op.getKey());
        if (!op.isNewerThan(current)) {
            return false;
        }
        winners.put(op.getKey(), op);
        return true;
    }

    /** Number of live (non-deleted) keys. */
    public synchronized int liveCount() {
        int n = 0;
        for (ConfigOp op : winners.values()) {
            if (!op.isDeleted()) {
                n++;
            }
        }
        return n;
    }

    /** Live entries (winning, non-deleted ops), sorted by key — for the UI. */
    public synchronized List<ConfigOp> liveEntries() {
        List<ConfigOp> out = new ArrayList<>();
        for (ConfigOp op : winners.values()) {
            if (!op.isDeleted()) {
                out.add(op);
            }
        }
        out.sort((a, b) -> a.getKey().compareTo(b.getKey()));
        return out;
    }

    /** All current winners (incl. tombstones) — used to seed a joining peer. */
    public synchronized List<ConfigOp> snapshotOps() {
        return new ArrayList<>(winners.values());
    }

    /** Merge a peer's snapshot under LWW. Returns how many ops became winners. */
    public synchronized int mergeSnapshot(List<ConfigOp> ops) {
        int changed = 0;
        for (ConfigOp op : ops) {
            if (apply(op)) {
                changed++;
            }
        }
        return changed;
    }

    /**
     * Stable 64-bit digest of the <em>visible</em> registry: FNV-1a over
     * {@code (key, value)} of every live entry, in key-sorted order. Equal
     * across nodes that have converged; the convergence oracle for the harness.
     */
    public synchronized long digest() {
        long h = FNV_OFFSET;
        for (ConfigOp op : liveEntries()) { // already key-sorted
            h = foldBytes(h, op.getKey().getBytes(StandardCharsets.UTF_8));
            h = fold(h, 0x1f); // separator so {ab,c} != {a,bc}
            h = foldBytes(h, op.getValue().getBytes(StandardCharsets.UTF_8));
            h = fold(h, 0x1e); // record separator
        }
        return h;
    }

    private static long foldBytes(long h, byte[] bytes) {
        for (byte b : bytes) {
            h ^= (b & 0xff);
            h *= FNV_PRIME;
        }
        return h;
    }

    private static long fold(long h, int b) {
        h ^= (b & 0xff);
        h *= FNV_PRIME;
        return h;
    }
}
