package protocols.apps.registry.ui;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import protocols.apps.registry.PlumtreeKVApp;

/**
 * A tiny embedded web UI for PlumtreeKV, built on the JDK's bundled
 * {@link HttpServer} so the demo pulls in no HTTP dependency. It serves the
 * static assets from the classpath ({@code /web/index.html}, {@code app.js},
 * {@code style.css}) and these JSON endpoints:
 * <ul>
 *   <li>{@code GET /api/state} — this node's converged registry + active-view
 *       neighbours + digest, which the page polls a few times a second;</li>
 *   <li>{@code POST /api/set} — {@code {"key":"..","value":".."}} → set a key
 *       (which then disseminates by gossip like any other write);</li>
 *   <li>{@code POST /api/delete} — {@code {"key":".."}} → delete a key;</li>
 *   <li>{@code POST /api/leave} — gracefully shut this node down (to demo churn).</li>
 * </ul>
 *
 * <p>Handlers run on the server's own small thread pool and call back into
 * {@link PlumtreeKVApp}, whose mutators are safe off the event loop (see that
 * class's threading note).
 */
public final class WebUi {

    private static final Logger logger = LogManager.getLogger(WebUi.class);

    // "field":"value" — value may contain escaped quotes/backslashes (the page JSON-stringifies).
    private static final Pattern STRING_FIELD =
            Pattern.compile("\"(key|value)\"\\s*:\\s*\"((?:[^\"\\\\]|\\\\.)*)\"");

    private final int port;
    private final PlumtreeKVApp app;
    private HttpServer server;

    public WebUi(int port, PlumtreeKVApp app) {
        this.port = port;
        this.app = app;
    }

    /** Bind and start serving. Throws if the port is unavailable. */
    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newFixedThreadPool(2));

        server.createContext("/api/state", this::handleState);
        server.createContext("/api/set", this::handleSet);
        server.createContext("/api/delete", this::handleDelete);
        server.createContext("/api/leave", this::handleLeave);
        server.createContext("/", this::handleStatic);

        server.start();
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    /* ───────────────────────────── Handlers ─────────────────────────────── */

    private void handleState(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            respond(exchange, 405, "text/plain", bytes("method not allowed"));
            return;
        }
        respond(exchange, 200, "application/json", bytes(app.stateJson()));
    }

    private void handleSet(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            respond(exchange, 405, "text/plain", bytes("method not allowed"));
            return;
        }
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        String key = null;
        String value = "";
        Matcher m = STRING_FIELD.matcher(body);
        while (m.find()) {
            String field = m.group(1);
            String val = unescape(m.group(2));
            if ("key".equals(field)) {
                key = val;
            } else {
                value = val;
            }
        }
        if (key == null || key.isBlank()) {
            respond(exchange, 400, "text/plain", bytes("expected {key,value}"));
            return;
        }
        app.setFromUi(key, value);
        respond(exchange, 204, "text/plain", new byte[0]);
    }

    private void handleDelete(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            respond(exchange, 405, "text/plain", bytes("method not allowed"));
            return;
        }
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        String key = null;
        Matcher m = STRING_FIELD.matcher(body);
        while (m.find()) {
            if ("key".equals(m.group(1))) {
                key = unescape(m.group(2));
            }
        }
        if (key == null || key.isBlank()) {
            respond(exchange, 400, "text/plain", bytes("expected {key}"));
            return;
        }
        app.deleteFromUi(key);
        respond(exchange, 204, "text/plain", new byte[0]);
    }

    private void handleLeave(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            respond(exchange, 405, "text/plain", bytes("method not allowed"));
            return;
        }
        respond(exchange, 204, "text/plain", new byte[0]);
        app.requestLeave();
    }

    private void handleStatic(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        if (path == null || path.equals("/")) {
            path = "/index.html";
        }
        if (path.contains("..")) {
            respond(exchange, 400, "text/plain", bytes("bad path"));
            return;
        }
        try (InputStream in = WebUi.class.getResourceAsStream("/web" + path)) {
            if (in == null) {
                respond(exchange, 404, "text/plain", bytes("not found"));
                return;
            }
            respond(exchange, 200, contentType(path), in.readAllBytes());
        }
    }

    /* ───────────────────────────── Helpers ──────────────────────────────── */

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** Minimal JSON string unescape for the values the page sends (\" \\ \n \r \t). */
    private static String unescape(String s) {
        if (s.indexOf('\\') < 0) {
            return s;
        }
        StringBuilder b = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char n = s.charAt(++i);
                switch (n) {
                    case 'n' -> b.append('\n');
                    case 'r' -> b.append('\r');
                    case 't' -> b.append('\t');
                    default -> b.append(n); // \" \\ \/ and anything else → literal
                }
            } else {
                b.append(c);
            }
        }
        return b.toString();
    }

    private static void respond(HttpExchange exchange, int status, String contentType, byte[] body)
            throws IOException {
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        if (body.length > 0) {
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        } else {
            exchange.close();
        }
    }

    private static String contentType(String path) {
        if (path.endsWith(".html")) {
            return "text/html; charset=utf-8";
        }
        if (path.endsWith(".js")) {
            return "text/javascript; charset=utf-8";
        }
        if (path.endsWith(".css")) {
            return "text/css; charset=utf-8";
        }
        if (path.endsWith(".svg")) {
            return "image/svg+xml";
        }
        return "application/octet-stream";
    }
}
