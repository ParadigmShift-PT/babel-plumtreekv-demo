package protocols.apps.registry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.apps.registry.messages.RegistrySyncMessage;
import protocols.apps.registry.telemetry.Telemetry;
import protocols.apps.registry.timers.ControlTimer;
import protocols.apps.registry.timers.DigestTimer;
import protocols.apps.registry.timers.WorkloadTimer;
import protocols.apps.registry.ui.WebUi;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.babel.protocols.dissemination.notifications.BroadcastDelivery;
import pt.unl.fct.di.novasys.babel.protocols.dissemination.requests.BroadcastRequest;
import pt.unl.fct.di.novasys.babel.protocols.general.notifications.ChannelAvailableNotification;
import pt.unl.fct.di.novasys.babel.protocols.membership.notifications.NeighborDown;
import pt.unl.fct.di.novasys.babel.protocols.membership.notifications.NeighborUp;
import pt.unl.fct.di.novasys.network.data.Host;

/**
 * PlumtreeKV — the application at the top of the stack, and the only protocol a
 * user (or experiment) drives directly. A multi-writer, eventually-consistent
 * replicated key-value store:
 * <ul>
 *   <li><b>Writes</b> (set / delete) go out as a {@link BroadcastRequest} to
 *       MultiPlumtree and come back — to everyone, including us — as
 *       {@link BroadcastDelivery} notifications. Each op is applied to a
 *       {@link Registry} under last-writer-wins, so all nodes converge and read
 *       locally with no network round-trip.</li>
 *   <li><b>Snapshot sync</b> ({@link RegistrySyncMessage}) is point-to-point over
 *       HyParView's channel: on joining, a node asks one neighbour for the current
 *       registry so a late joiner sees existing state immediately.</li>
 *   <li><b>Presence</b> — this node's HyParView active view — is tracked from
 *       {@link NeighborUp}/{@link NeighborDown} and surfaced in the web UI.</li>
 * </ul>
 *
 * <h2>Two faces: interactive and headless</h2>
 * With {@code plumtreekv.ui.enabled} (default) an embedded {@link WebUi} lets a
 * human set/delete keys. With {@code plumtreekv.workload.enabled} the node instead
 * writes random keys from a keyspace — how {@code babel-plumtree-experiments} drives
 * load. Both funnel through {@link #doWrite}.
 *
 * <h2>Workload start/stop: orchestrator-coordinated</h2>
 * A locally-settled view is NOT a safe start trigger — a node whose own view has
 * settled would still emit ops that nodes still joining miss. So in experiments the
 * workload is driven <b>externally</b>: when {@code plumtreekv.workload.controlFile}
 * is set, a {@link ControlTimer} polls that shared file and writes only between the
 * orchestrator's {@code RUN} and {@code STOP}. This lets the harness (a) start every
 * node together once the whole overlay has formed, and (b) stop them together and
 * <b>drain</b> in-flight messages before reading digests or killing nodes — so a
 * measurement is never taken while messages are in flux. With no control file the
 * node falls back to a single start-delay timer (interactive/demo use).
 *
 * <h2>Threading note</h2>
 * The web server runs on its own threads; {@link #setFromUi}, {@link #deleteFromUi}
 * and {@link #stateJson} are called from there and only touch {@code sendRequest},
 * the thread-safe {@link Telemetry}, the synchronized {@link Registry} and the
 * {@link CopyOnWriteArraySet} neighbours. All other state is event-loop-only.
 */
public class PlumtreeKVApp extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(PlumtreeKVApp.class);

    public static final short PROTO_ID = 300;
    public static final String PROTO_NAME = "PlumtreeKVApp";

    /** Property key — enable the embedded web UI. */
    public static final String PAR_UI_ENABLED = "plumtreekv.ui.enabled";
    public static final String DEFAULT_UI_ENABLED = "true";
    /** Property key — web UI port. Defaults to {@code babel.port + 2000} when unset. */
    public static final String PAR_UI_PORT = "plumtreekv.ui.port";
    public static final int DEFAULT_UI_PORT_OFFSET = 2000;
    /** Property key — open the system browser at the UI on startup (best-effort). */
    public static final String PAR_UI_OPEN = "plumtreekv.ui.open";
    public static final String DEFAULT_UI_OPEN = "true";

    /** Property key — request a registry snapshot from a neighbour on join. */
    public static final String PAR_SNAPSHOT_SYNC = "plumtreekv.snapshot.sync";
    public static final String DEFAULT_SNAPSHOT_SYNC = "true";

    /** Property key — period (ms) between convergence-digest telemetry lines; {@code <= 0} disables. */
    public static final String PAR_DIGEST_INTERVAL = "plumtreekv.digest.interval";
    public static final String DEFAULT_DIGEST_INTERVAL = "5000";

    /** Property key — enable the headless random-write workload driver. */
    public static final String PAR_WORKLOAD_ENABLED = "plumtreekv.workload.enabled";
    public static final String DEFAULT_WORKLOAD_ENABLED = "false";
    /** Property key — workload writes per second. */
    public static final String PAR_WORKLOAD_RATE = "plumtreekv.workload.rate";
    public static final String DEFAULT_WORKLOAD_RATE = "2";
    /**
     * Property key — workload duration (ms). In coordinated mode this is a SAFETY cap;
     * the primary stop is the orchestrator's {@code STOP} signal. {@code <= 0} = unbounded.
     */
    public static final String PAR_WORKLOAD_DURATION = "plumtreekv.workload.duration";
    public static final String DEFAULT_WORKLOAD_DURATION = "0";
    /** Property key — legacy single start-delay (ms) before writing, when no control file is set. */
    public static final String PAR_WORKLOAD_START_DELAY = "plumtreekv.workload.startDelay";
    public static final String DEFAULT_WORKLOAD_START_DELAY = "5000";
    /** Property key — number of distinct keys the workload writes to. */
    public static final String PAR_WORKLOAD_KEYSPACE = "plumtreekv.workload.keyspace";
    public static final String DEFAULT_WORKLOAD_KEYSPACE = "16";

    /** Property key — shared control file (WAIT/RUN/STOP) the experiment drives; empty = legacy timer. */
    public static final String PAR_WORKLOAD_CONTROL_FILE = "plumtreekv.workload.controlFile";
    public static final String DEFAULT_WORKLOAD_CONTROL_FILE = "";
    /** Property key — how often (ms) to poll the control file in coordinated mode. */
    public static final String PAR_WORKLOAD_CONTROL_POLL_MS = "plumtreekv.workload.controlPollMs";
    public static final String DEFAULT_WORKLOAD_CONTROL_POLL_MS = "250";

    private final Host myself;
    private final String nodeId;
    private final short broadcastProtoId;
    private final short membershipProtoId;

    private final Registry registry;
    private final Telemetry telemetry;

    private final boolean snapshotSync;
    private final long digestInterval;
    private final boolean workloadEnabled;
    private final long workloadPeriodMs;
    private final long workloadDuration;
    private final long workloadStartDelay;
    private final int workloadKeyspace;
    private final String controlFile;
    private final long controlPollMs;
    private final boolean controlMode;
    private final boolean uiEnabled;
    private final int uiPort;
    private final boolean uiOpen;

    /** This node's HyParView active view; read by the UI thread, so concurrent. */
    private final CopyOnWriteArraySet<Host> neighbours = new CopyOnWriteArraySet<>();

    private int channelId = -1;
    private boolean channelReady = false;
    private boolean snapshotRequested = false;
    private long digestTick = 0;
    private long bootMillis = -1;            // init() wall-clock — origin for write-start timing
    private boolean workloadStarted = false;
    private boolean workloadStopped = false;
    private long workloadTimerId = -1;
    private long workloadStartMillis = -1;
    private long deliveredOps = 0;           // distinct ops delivered via gossip (robust coverage signal)

    private WebUi ui;

    /**
     * @param props             protocol configuration; see the {@code PAR_*} constants
     * @param myself            this node's membership-space {@link Host} (HyParView's bind endpoint)
     * @param broadcastProtoId  the dissemination protocol id (writes are sent there — MultiPlumtree/Plumtree)
     * @param membershipProtoId the HyParView protocol id (its channel is reused for snapshot sync)
     */
    public PlumtreeKVApp(Properties props, Host myself, short broadcastProtoId, short membershipProtoId)
            throws HandlerRegistrationException, IOException {
        super(PROTO_NAME, PROTO_ID);
        this.myself = myself;
        this.nodeId = myself.getAddress().getHostAddress() + ":" + myself.getPort();
        this.broadcastProtoId = broadcastProtoId;
        this.membershipProtoId = membershipProtoId;

        this.registry = new Registry();
        this.telemetry = new Telemetry(nodeId);

        this.snapshotSync = readBool(props, PAR_SNAPSHOT_SYNC, DEFAULT_SNAPSHOT_SYNC);
        this.digestInterval = readLong(props, PAR_DIGEST_INTERVAL, DEFAULT_DIGEST_INTERVAL);
        this.uiEnabled = readBool(props, PAR_UI_ENABLED, DEFAULT_UI_ENABLED);
        this.uiPort = readInt(props, PAR_UI_PORT, Integer.toString(myself.getPort() + DEFAULT_UI_PORT_OFFSET));
        this.uiOpen = readBool(props, PAR_UI_OPEN, DEFAULT_UI_OPEN);

        this.workloadEnabled = readBool(props, PAR_WORKLOAD_ENABLED, DEFAULT_WORKLOAD_ENABLED);
        int rate = Math.max(1, readInt(props, PAR_WORKLOAD_RATE, DEFAULT_WORKLOAD_RATE));
        this.workloadPeriodMs = Math.max(1, 1000L / rate);
        this.workloadDuration = readLong(props, PAR_WORKLOAD_DURATION, DEFAULT_WORKLOAD_DURATION);
        this.workloadStartDelay = readLong(props, PAR_WORKLOAD_START_DELAY, DEFAULT_WORKLOAD_START_DELAY);
        this.workloadKeyspace = Math.max(1, readInt(props, PAR_WORKLOAD_KEYSPACE, DEFAULT_WORKLOAD_KEYSPACE));
        this.controlFile = props.getProperty(PAR_WORKLOAD_CONTROL_FILE, DEFAULT_WORKLOAD_CONTROL_FILE).trim();
        this.controlPollMs = Math.max(1, readLong(props, PAR_WORKLOAD_CONTROL_POLL_MS, DEFAULT_WORKLOAD_CONTROL_POLL_MS));
        this.controlMode = !controlFile.isEmpty();

        subscribeNotification(BroadcastDelivery.NOTIFICATION_ID, this::uponDeliver);
        subscribeNotification(NeighborUp.NOTIFICATION_ID, this::uponNeighborUp);
        subscribeNotification(NeighborDown.NOTIFICATION_ID, this::uponNeighborDown);
        subscribeNotification(ChannelAvailableNotification.NOTIFICATION_ID, this::uponChannelAvailable);

        registerTimerHandler(DigestTimer.TIMER_ID, this::uponDigestTimer);
        registerTimerHandler(WorkloadTimer.TIMER_ID, this::uponWorkloadTimer);
        registerTimerHandler(ControlTimer.TIMER_ID, this::uponControlTimer);
    }

    @Override
    public void init(Properties props) {
        bootMillis = System.currentTimeMillis();
        String proto = props.getProperty("plumtreekv.protocol", "multi").trim();
        telemetry.start(proto.equalsIgnoreCase("single") ? "Plumtree" : "MultiPlumtree");

        if (digestInterval > 0) {
            setupPeriodicTimer(new DigestTimer(), digestInterval, digestInterval);
        }
        if (workloadEnabled) {
            logger.info("Headless workload: 1 write every {} ms over {} keys, cap {}{}",
                    workloadPeriodMs, workloadKeyspace,
                    workloadDuration > 0 ? workloadDuration + " ms" : "unbounded",
                    controlMode ? " — coordinated via control file " + controlFile
                            : " — legacy start +" + workloadStartDelay + " ms");
            if (controlMode) {
                // Poll the shared control file; uponControlTimer starts on RUN, stops on STOP.
                setupPeriodicTimer(new ControlTimer(), 0, controlPollMs);
            } else {
                // Legacy/demo path: one-shot — begin writing at the start-delay floor.
                setupTimer(new ControlTimer(), workloadStartDelay);
            }
        }
        if (uiEnabled) {
            ui = new WebUi(uiPort, this);
            try {
                ui.start();
                String url = "http://localhost:" + uiPort + "/";
                logger.info("PlumtreeKV web UI on {}", url);
                if (uiOpen) {
                    openInBrowser(url);
                }
            } catch (IOException e) {
                logger.error("Failed to start web UI on port {} — continuing headless", uiPort, e);
                ui = null;
            }
        }
    }

    /* ─────────── Attach snapshot-sync to HyParView's channel ─────────── */

    private void uponChannelAvailable(ChannelAvailableNotification notification, short sourceProto) {
        if (channelReady || notification.getProtoSource() != membershipProtoId) {
            return;
        }
        this.channelId = notification.getChannelID();
        registerSharedChannel(channelId);
        registerMessageSerializer(channelId, RegistrySyncMessage.MSG_ID, RegistrySyncMessage.serializer);
        try {
            registerMessageHandler(channelId, RegistrySyncMessage.MSG_ID, this::uponSync, this::uponSyncFail);
        } catch (HandlerRegistrationException e) {
            logger.error("Failed to register registry sync handler", e);
            return;
        }
        channelReady = true;
        logger.debug("Attached snapshot sync to membership channel {}", channelId);
        maybeRequestSnapshot();
    }

    /* ───────────────────── Membership notifications ───────────────────── */

    private void uponNeighborUp(NeighborUp notification, short sourceProto) {
        Host h = notification.getPeer();
        neighbours.add(h);
        telemetry.neighborUp(hostId(h), neighbours.size());
        maybeRequestSnapshot();
    }

    private void uponNeighborDown(NeighborDown notification, short sourceProto) {
        Host h = notification.getPeer();
        neighbours.remove(h);
        telemetry.neighborDown(hostId(h), neighbours.size());
    }

    /** {@code ip:port} identity of a peer — matches this node's own {@link #nodeId} format. */
    private static String hostId(Host h) {
        return h.getAddress().getHostAddress() + ":" + h.getPort();
    }

    /** Active-view set as {@code ip:port} joined by {@code ;} (empty string when no neighbours). */
    private String peersString() {
        StringBuilder b = new StringBuilder();
        for (Host h : neighbours) {
            if (b.length() > 0) {
                b.append(';');
            }
            b.append(hostId(h));
        }
        return b.toString();
    }

    /* ───────────────────── Snapshot sync (point-to-point) ─────────────── */

    private void maybeRequestSnapshot() {
        if (!snapshotSync || snapshotRequested || !channelReady) {
            return;
        }
        Host peer = neighbours.stream().findFirst().orElse(null);
        if (peer == null) {
            return;
        }
        snapshotRequested = true;
        sendMessage(channelId, new RegistrySyncMessage(RegistrySyncMessage.Kind.REQUEST, null), peer);
        logger.debug("Requested registry snapshot from {}", peer);
    }

    private void uponSync(RegistrySyncMessage msg, Host from, short sourceProto, int channelId) {
        switch (msg.getKind()) {
            case REQUEST -> {
                RegistrySyncMessage reply = new RegistrySyncMessage(
                        RegistrySyncMessage.Kind.REPLY, registry.snapshotOps());
                sendMessage(this.channelId, reply, from);
                logger.debug("Served registry snapshot ({} ops) to {}", reply.getOps().size(), from);
            }
            case REPLY -> {
                // mergeSnapshot applies ops via LWW WITHOUT going through uponDeliver, so
                // `applied` is state obtained by reconciliation, not gossip — it does NOT bump
                // deliveredOps. SYNC_MERGE lets the analyzer separate the gossip-delivered
                // share of convergence from the snapshot-repair share.
                int ops = msg.getOps().size();
                int applied = registry.mergeSnapshot(msg.getOps());
                telemetry.syncMerge(hostId(from), ops, applied, neighbours.size());
                logger.debug("Merged snapshot from {}: {} of {} ops became winners (LWW)", from, applied, ops);
            }
        }
    }

    private void uponSyncFail(ProtoMessage msg, Host host, short destProto, Throwable t, int channelId) {
        logger.warn("Registry sync message to {} failed: {}", host, t.toString());
        snapshotRequested = false; // allow a later neighbour to be asked
    }

    /* ─────────────────────── Write deliveries (broadcast) ─────────────── */

    private void uponDeliver(BroadcastDelivery notification, short sourceProto) {
        ConfigOp op = ConfigPayload.decode(notification.getPayload());
        registry.apply(op);
        deliveredOps++;
        telemetry.delivered(op.getOpId(), op.getKey(), op.getOriginId());
    }

    /* ─────────────────────────────── Timers ──────────────────────────── */

    private void uponDigestTimer(DigestTimer timer, long timerId) {
        digestTick++;
        telemetry.digest(digestTick, registry.digest(), registry.liveCount(),
                neighbours.size(), deliveredOps, peersString());
    }

    /**
     * Workload control (see {@link ControlTimer}). In coordinated mode this fires
     * periodically and reads the shared control file: it begins writing on {@code RUN}
     * (all nodes together, once the whole system has settled) and ceases on {@code STOP}.
     * In legacy/demo mode it is a one-shot that just begins writing at the start-delay floor.
     */
    private void uponControlTimer(ControlTimer timer, long timerId) {
        if (!controlMode) {
            beginWorkload("timer");
            return;
        }
        String state = readControlState();
        if ("RUN".equals(state) && !workloadStarted) {
            beginWorkload("control");
        } else if ("STOP".equals(state) && workloadStarted && !workloadStopped) {
            stopWorkload();
            cancelTimer(timerId); // nothing more to watch for
        }
    }

    /** Read the control file's trimmed contents, or {@code ""} on any error (treated as WAIT). */
    private String readControlState() {
        try {
            return Files.readString(Path.of(controlFile)).trim();
        } catch (IOException | RuntimeException e) {
            return ""; // not written yet / transient — keep waiting
        }
    }

    /** Emit WRITE_START and kick off the write workload (idempotent). */
    private void beginWorkload(String trigger) {
        if (workloadStarted) {
            return;
        }
        workloadStarted = true;
        long sinceBoot = bootMillis < 0 ? -1 : System.currentTimeMillis() - bootMillis;
        telemetry.writeStart(neighbours.size(), sinceBoot, trigger);
        logger.info("Writing begins ({}): view={} sinceBoot={}ms", trigger, neighbours.size(), sinceBoot);
        workloadTimerId = setupPeriodicTimer(new WorkloadTimer(), 0, workloadPeriodMs);
    }

    /** Stop generating writes (deliveries keep flowing during drain); idempotent. */
    private void stopWorkload() {
        if (!workloadStarted || workloadStopped) {
            return;
        }
        workloadStopped = true;
        if (workloadTimerId >= 0) {
            cancelTimer(workloadTimerId);
        }
        long writingMs = workloadStartMillis < 0 ? -1 : System.currentTimeMillis() - workloadStartMillis;
        telemetry.workloadStop(writingMs);
        logger.info("Writing stopped after {} ms (draining)", writingMs);
    }

    private void uponWorkloadTimer(WorkloadTimer timer, long timerId) {
        long now = System.currentTimeMillis();
        if (workloadStartMillis < 0) {
            workloadStartMillis = now;
        }
        // A duration is a SAFETY cap; the primary stop is the orchestrator's STOP signal.
        if (workloadDuration > 0 && now - workloadStartMillis > workloadDuration) {
            stopWorkload();
            return;
        }
        String key = "k" + ThreadLocalRandom.current().nextInt(workloadKeyspace);
        String value = "v" + ThreadLocalRandom.current().nextInt(1_000_000);
        doWrite(key, value, false);
    }

    /* ─────────────────────────── Write path (shared) ─────────────────── */

    private void doWrite(String key, String value, boolean deleted) {
        long ts = System.currentTimeMillis();
        UUID opId = UUID.randomUUID();
        ConfigOp op = new ConfigOp(key, value, deleted, ts, nodeId, opId);
        telemetry.wrote(opId, key, deleted);
        sendRequest(new BroadcastRequest(myself, ConfigPayload.encode(op), PROTO_ID), broadcastProtoId);
    }

    /* ───────────────────────────── Web UI surface ────────────────────── */

    public void setFromUi(String key, String value) {
        if (key == null || key.isBlank()) {
            return;
        }
        doWrite(key.trim(), value == null ? "" : value, false);
    }

    public void deleteFromUi(String key) {
        if (key == null || key.isBlank()) {
            return;
        }
        doWrite(key.trim(), "", true);
    }

    /** Graceful "leave": let the node exit so neighbours see it go down and the trees heal. */
    public void requestLeave() {
        logger.info("Leave requested via UI — shutting down this node");
        Thread t = new Thread(() -> {
            try {
                Thread.sleep(150); // let the HTTP response flush first
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            System.exit(0);
        });
        t.setDaemon(true);
        t.start();
    }

    /** A JSON snapshot of the registry + neighbours for the web UI's polling loop. */
    public String stateJson() {
        List<ConfigOp> entries = registry.liveEntries();
        StringBuilder b = new StringBuilder(256 + entries.size() * 64);
        b.append("{\"node\":\"").append(jsonEscape(nodeId)).append('"')
                .append(",\"keys\":").append(entries.size())
                .append(",\"digest\":\"").append(Long.toHexString(registry.digest())).append('"')
                .append(",\"neighbours\":[");
        boolean first = true;
        for (Host h : neighbours) {
            if (!first) {
                b.append(',');
            }
            first = false;
            b.append('"').append(hostId(h)).append('"');
        }
        b.append("],\"entries\":[");
        for (int i = 0; i < entries.size(); i++) {
            ConfigOp op = entries.get(i);
            if (i > 0) {
                b.append(',');
            }
            b.append("{\"key\":\"").append(jsonEscape(op.getKey())).append('"')
                    .append(",\"value\":\"").append(jsonEscape(op.getValue())).append('"')
                    .append(",\"owner\":\"").append(jsonEscape(op.getOriginId())).append('"')
                    .append(",\"version\":").append(op.getTimestamp())
                    .append('}');
        }
        b.append("]}");
        return b.toString();
    }

    public int getUiPort() {
        return uiPort;
    }

    public boolean isUiEnabled() {
        return uiEnabled;
    }

    /* ────────────────────────────── Helpers ──────────────────────────── */

    private void openInBrowser(String url) {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        String[] cmd;
        if (os.contains("mac")) {
            cmd = new String[] {"open", url};
        } else if (os.contains("win")) {
            cmd = new String[] {"rundll32", "url.dll,FileProtocolHandler", url};
        } else {
            cmd = new String[] {"xdg-open", url};
        }
        try {
            new ProcessBuilder(cmd).start();
        } catch (IOException e) {
            logger.debug("Could not auto-open a browser ({}); open {} manually", e.getMessage(), url);
        }
    }

    private static String jsonEscape(String s) {
        StringBuilder b = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> b.append("\\\"");
                case '\\' -> b.append("\\\\");
                case '\n' -> b.append("\\n");
                case '\r' -> b.append("\\r");
                case '\t' -> b.append("\\t");
                default -> {
                    if (c < 0x20) {
                        b.append(String.format("\\u%04x", (int) c));
                    } else {
                        b.append(c);
                    }
                }
            }
        }
        return b.toString();
    }

    private static int readInt(Properties p, String key, String def) {
        return Integer.parseInt(p.getProperty(key, def).trim());
    }

    private static long readLong(Properties p, String key, String def) {
        return Long.parseLong(p.getProperty(key, def).trim());
    }

    private static boolean readBool(Properties p, String key, String def) {
        return Boolean.parseBoolean(p.getProperty(key, def).trim());
    }
}
