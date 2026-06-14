import java.net.InetAddress;
import java.net.NetworkInterface;
import java.security.InvalidParameterException;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.apps.registry.PlumtreeKVApp;
import pt.paradigmshift.babel.hyparview.HyParView;
import pt.paradigmshift.babel.plumtree.MultiPlumtree;
import pt.paradigmshift.babel.plumtree.Plumtree;
import pt.unl.fct.di.novasys.babel.core.Babel;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.core.protocols.discovery.MulticastDiscoveryProtocol;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.InterfaceToIp;

/**
 * Entry point for PlumtreeKV — a peer-to-peer, multi-writer replicated key-value
 * store.
 *
 * <p>The demo composes the real ParadigmShift overlay stack and lets the Babel
 * runtime wire it together through asynchronous events:
 * <ol>
 *   <li>{@link HyParView} — partial-view membership (the gossip overlay), which
 *       auto-discovers peers on the LAN (it's a {@code DiscoverableProtocol}) and
 *       owns the TCP channel;</li>
 *   <li>{@link MultiPlumtree} — disseminates registry writes along one spanning
 *       tree per writer, <b>sharing HyParView's channel</b>;</li>
 *   <li>{@link PlumtreeKVApp} — the replicated key-value store + web UI on top.</li>
 * </ol>
 *
 * <p>Launch: {@code java -jar babel-plumtree-demo.jar [babel.port=<port>]
 * [babel.interface=<nic>] [babel.address=<ip>] [HyParView.contact=<host>:<port>]
 * [plumtreekv.ui.port=<port>]}. To run several nodes on one machine, give each a
 * distinct {@code babel.port} spaced by &ge; 10 and a distinct
 * {@code babel.discovery.unicast.port}; the web UI defaults to
 * {@code babel.port + 2000}.
 */
public class Main {

    // Point log4j at our bundled configuration before any logger is created.
    static {
        System.setProperty("log4j.configurationFile", "log4j2.xml");
    }

    /** Default Babel configuration file (overridable with the "-config" launch arg). */
    private static final String DEFAULT_CONF = "babel_config.properties";

    /** Default {@code babel.port} HyParView binds (babel-core owns the key, defines no default). */
    public static final String PAR_DEFAULT_BABEL_PORT = "6000";

    /** Property key — dissemination protocol: {@code multi} (MultiPlumtree) or {@code single} (Plumtree). */
    public static final String PAR_PROTOCOL = "plumtreekv.protocol";
    /** Default dissemination protocol: {@value}. */
    public static final String DEFAULT_PROTOCOL = "multi";

    public static void main(String[] args) throws Exception {

        // Per-node log files (so two nodes on one machine don't clash). Must happen
        // BEFORE any logger is created.
        String port = argValue(args, Babel.PAR_DEFAULT_PORT, PAR_DEFAULT_BABEL_PORT);
        System.setProperty("plumtreekv.logfile", "babel-plumtree-demo-" + port + ".log");
        System.setProperty("plumtreekv.telemetryfile", "babel-plumtree-demo-telemetry-" + port + ".log");

        Logger logger = LogManager.getLogger(Main.class);

        Babel babel = Babel.getInstance();
        Properties props = Babel.loadConfig(args, DEFAULT_CONF);

        // Resolve a reachable bind address into babel.address (explicit wins, else
        // babel.interface, else auto-detect the sole physical NIC). Never silently
        // default to loopback; fail loudly with guidance.
        String addressSource;
        try {
            addressSource = InterfaceToIp.resolveBindAddress(props);
        } catch (InvalidParameterException e) {
            System.err.println("PlumtreeKV — cannot determine a bind address.\n");
            System.err.println(e.getMessage());
            System.exit(1);
            return; // unreachable
        }

        String bindAddress = props.getProperty(Babel.PAR_DEFAULT_ADDRESS);
        int bindPort = Integer.parseInt(props.getProperty(Babel.PAR_DEFAULT_PORT, PAR_DEFAULT_BABEL_PORT));
        Host myself = new Host(InetAddress.getByName(bindAddress), bindPort);

        logger.info("PlumtreeKV starting — host={}", myself);
        logger.info(InterfaceToIp.describeInterfaces());
        printStartupBanner(props, myself, addressSource);

        // Build the stack: HyParView owns the membership channel and (with discovery)
        // auto-finds peers; the dissemination protocol shares that same channel; the
        // registry app rides on top.
        //
        // plumtreekv.protocol selects the dissemination protocol: "multi" (default) =
        // MultiPlumtree (one tree per writer); "single" = Plumtree (one shared tree).
        // Both share HyParView's channel and expose the same Broadcast interface, so
        // the experiments harness compares them by flipping one property.
        HyParView membership = new HyParView(TCPChannel.NAME, props, myself);

        String protocol = props.getProperty(PAR_PROTOCOL, DEFAULT_PROTOCOL).trim().toLowerCase();
        GenericProtocol broadcast;
        short broadcastId;
        if (protocol.equals("single")) {
            props.putIfAbsent(Plumtree.PAR_PEER_ADDRESS_RESOLUTION, Plumtree.RESOLUTION_SHARED);
            broadcast = new Plumtree(props, myself);
            broadcastId = Plumtree.PROTOCOL_ID;
        } else {
            props.putIfAbsent(MultiPlumtree.PAR_PEER_ADDRESS_RESOLUTION, MultiPlumtree.RESOLUTION_SHARED);
            broadcast = new MultiPlumtree(props, myself);
            broadcastId = MultiPlumtree.PROTOCOL_ID;
        }
        PlumtreeKVApp app = new PlumtreeKVApp(props, myself, broadcastId, HyParView.PROTOCOL_ID);

        babel.registerProtocol(membership);
        babel.registerProtocol(broadcast);
        babel.registerProtocol(app);

        membership.init(props);
        broadcast.init(props);
        app.init(props);

        babel.start();

        logger.info("PlumtreeKV up ({})", myself);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> logger.info("PlumtreeKV shutting down")));
    }

    /** Concise startup summary to stdout. */
    private static void printStartupBanner(Properties props, Host myself, String addressSource) {
        String iface;
        try {
            NetworkInterface nif = NetworkInterface.getByInetAddress(myself.getAddress());
            iface = (nif != null) ? nif.getName() : "?";
        } catch (Exception e) {
            iface = "?";
        }

        int uiPort = Integer.parseInt(props.getProperty(PlumtreeKVApp.PAR_UI_PORT,
                Integer.toString(myself.getPort() + PlumtreeKVApp.DEFAULT_UI_PORT_OFFSET)));
        boolean uiEnabled = Boolean.parseBoolean(
                props.getProperty(PlumtreeKVApp.PAR_UI_ENABLED, PlumtreeKVApp.DEFAULT_UI_ENABLED));

        StringBuilder b = new StringBuilder(System.lineSeparator());
        b.append("  PlumtreeKV — replicated key-value store").append(System.lineSeparator());
        b.append("  network     : ").append(iface).append("  →  ").append(myself.getAddress().getHostAddress())
                .append("   (").append(addressSource).append(')').append(System.lineSeparator());
        b.append("  membership  : ").append(myself.getAddress().getHostAddress()).append(':').append(myself.getPort())
                .append("  (HyParView, TCP)").append(System.lineSeparator());
        String proto = props.getProperty(PAR_PROTOCOL, DEFAULT_PROTOCOL);
        String protoLabel = proto.equalsIgnoreCase("single")
                ? "Plumtree (single shared tree)" : "MultiPlumtree (per-writer trees)";
        b.append("  dissemination: ").append(protoLabel).append(", shared HyParView channel")
                .append(System.lineSeparator());
        b.append("  web UI      : ").append(uiEnabled ? "http://localhost:" + uiPort + "/" : "disabled")
                .append(System.lineSeparator());

        boolean discoveryOn = props.getProperty(Babel.PAR_DISCOVERY_PROTOCOL) != null;
        if (discoveryOn) {
            String group = props.getProperty(MulticastDiscoveryProtocol.PAR_DISCOVERY_MULTICAST_ADDRESS,
                    MulticastDiscoveryProtocol.MULTICAST_ADDRESS);
            b.append("  discovery   : multicast ").append(group).append(System.lineSeparator());
        } else {
            b.append("  discovery   : off (pass babel.discovery=… to enable multicast)")
                    .append(System.lineSeparator());
        }

        String contact = props.getProperty(HyParView.PAR_CONTACT);
        String bootstrap;
        if (contact != null && !contact.isBlank() && !contact.trim().equalsIgnoreCase("none")) {
            bootstrap = "seed from contact " + contact.trim();
        } else if (contact != null && contact.trim().equalsIgnoreCase("none")) {
            bootstrap = "first node (HyParView.contact=none) — I don't probe, but I reply to others";
        } else if (discoveryOn) {
            bootstrap = "auto-discovery — probe the LAN and connect to whoever answers";
        } else {
            bootstrap = "none — set HyParView.contact=<host>:<port> to join, or babel.discovery=… for multicast";
        }
        b.append("  bootstrap   : ").append(bootstrap).append(System.lineSeparator());
        System.out.println(b);
    }

    /** Tiny helper: find {@code key=value} in the launch args, else return a default. */
    private static String argValue(String[] args, String key, String def) {
        String prefix = key + "=";
        for (String a : args) {
            if (a.startsWith(prefix)) {
                return a.substring(prefix.length());
            }
        }
        return def;
    }
}
