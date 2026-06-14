package protocols.apps.registry.telemetry;

import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Structured, machine-readable event emitter — the data contract between the
 * PlumtreeKV demo and the {@code babel-plumtree-experiments} correctness harness.
 *
 * <p>Each method writes exactly one line to the dedicated {@code plumtreekv.telemetry}
 * logger, which log4j2 routes to a per-node telemetry file with an epoch-millis
 * prefix ({@code %d{UNIX_MILLIS}}, see {@code log4j2.xml}). So the event's time is
 * the line prefix — methods here never stamp their own. The line body is
 * {@code EVENT key=value …} with no spaces inside values, which the harness parses
 * by splitting on whitespace then on {@code =}. (The headless workload generates
 * space-free keys; interactively-typed keys are not parsed by the harness.)
 *
 * <p>What the harness derives:
 * <ul>
 *   <li><b>reliability</b> — {@code DELIVER} count per {@code op} across nodes ÷ live nodes;</li>
 *   <li><b>latency / LDH</b> — a node's {@code DELIVER} prefix-time minus the op's {@code SET} prefix-time;</li>
 *   <li><b>convergence</b> — all live nodes' final {@code DIGEST hash} agree;</li>
 *   <li><b>overlay health</b> — {@code view} size + {@code NEIGHBOR_*} churn.</li>
 * </ul>
 */
public final class Telemetry {

    private static final Logger log = LogManager.getLogger("plumtreekv.telemetry");

    private final String nodeId;

    public Telemetry(String nodeId) {
        this.nodeId = nodeId;
    }

    /** Once at startup: this node's identity and the dissemination protocol in use. */
    public void start(String disseminationProtocol) {
        log.info("START node={} dissemination={}", nodeId, disseminationProtocol);
    }

    /** Emitted by the writer when it issues a registry op (set or delete). */
    public void wrote(UUID opId, String key, boolean deleted) {
        log.info("SET node={} op={} key={} deleted={}", nodeId, opId, key, deleted);
    }

    /** Emitted by every node (including the writer) when an op is delivered locally. */
    public void delivered(UUID opId, String key, String originId) {
        log.info("DELIVER node={} op={} key={} origin={}", nodeId, opId, key, originId);
    }

    /**
     * Periodic convergence digest of the local registry. {@code delivered} is this
     * node's in-process count of ops delivered <b>via gossip</b> — a robust coverage
     * signal the analyzer cross-checks against the (higher-volume, loss-prone) DELIVER
     * lines. {@code peers} is the active-view set ({@code ip:port} joined by {@code ;},
     * empty when the view is empty): a time-stamped membership snapshot from which the
     * analyzer reconstructs active-view symmetry at a common instant, robust to churn.
     * {@code sent} (omitted when {@code < 0}) is the dissemination protocol's cumulative
     * {@code SentMessages} counter for this node — a read-only observation the analyzer
     * sums fleet-wide and divides by deliveries to get sends-per-delivery (redundancy).
     */
    public void digest(long tick, long hash, int liveKeys, int activeView, long delivered, String peers, long sent) {
        if (sent >= 0) {
            log.info("DIGEST node={} tick={} hash={} keys={} view={} delivered={} sent={} peers={}",
                    nodeId, tick, Long.toHexString(hash), liveKeys, activeView, delivered, sent, peers);
        } else {
            log.info("DIGEST node={} tick={} hash={} keys={} view={} delivered={} peers={}",
                    nodeId, tick, Long.toHexString(hash), liveKeys, activeView, delivered, peers);
        }
    }

    public void neighborUp(String peer, int activeView) {
        log.info("NEIGHBOR_UP node={} peer={} view={}", nodeId, peer, activeView);
    }

    public void neighborDown(String peer, int activeView) {
        log.info("NEIGHBOR_DOWN node={} peer={} view={}", nodeId, peer, activeView);
    }

    /**
     * The local node began its write workload. {@code trigger} is {@code control}
     * (the orchestrator's RUN signal) or {@code timer} (legacy start-delay). In
     * coordinated mode all nodes emit this together once the overlay has settled —
     * the analyzer uses it as the writing-window start.
     */
    public void writeStart(int activeView, long sinceBootMs, String trigger) {
        log.info("WRITE_START node={} view={} sinceBootMs={} trigger={}",
                nodeId, activeView, sinceBootMs, trigger);
    }

    /** The local node stopped generating writes (on STOP / duration cap); deliveries keep draining. */
    public void workloadStop(long writingMs) {
        log.info("WORKLOAD_STOP node={} writingMs={}", nodeId, writingMs);
    }

    /**
     * This node merged a snapshot reply from {@code from}: {@code ops} received,
     * {@code applied} of them won LWW (state obtained by reconciliation rather than
     * gossip — invisible to the {@code delivered} counter). Lets the analyzer split
     * coverage into the gossip share and the snapshot-repair share.
     */
    public void syncMerge(String from, int ops, int applied, int activeView) {
        log.info("SYNC_MERGE node={} from={} ops={} applied={} view={}",
                nodeId, from, ops, applied, activeView);
    }
}
