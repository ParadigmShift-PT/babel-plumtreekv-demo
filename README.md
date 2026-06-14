# PlumtreeKV — a replicated key-value store built on Babel

A **software-only** demo of the [Babel](https://ieeexplore.ieee.org/document/9996836)
distributed-protocols framework: a peer-to-peer, multi-writer **replicated
key-value store** where every participant is a peer. Set a key in your browser
and watch it **spread to every node by gossip**; because each key is
last-writer-wins, all nodes **converge to the identical registry** no matter what
order the updates arrive in — and each node **reads its copy locally**, with no
coordinator. This is the canonical use of Plumtree (cf. Riak's cluster-metadata
store, Leapsight's `plum_db`, the c12s edge platform).

Unlike the chat demo ([`babel-demo`](https://github.com/ParadigmShift-PT/babel-demo))
which runs on simplified teaching protocols, PlumtreeKV composes the **real
ParadigmShift overlay stack** — so it doubles as a live showcase of those
protocols working together:

1. **HyParView** — partial-view membership (the gossip overlay) with LAN auto-discovery;
2. **MultiPlumtree / Plumtree** — *push-lazy-push multicast tree* dissemination: each
   update is eager-pushed along an embedded spanning tree and lazy-pushed (`IHAVE`)
   along the rest of the overlay, which both recovers losses and heals the tree;
3. **PlumtreeKVApp** — the replicated registry + web UI on top.

> **A ParadigmShift artefact.** PlumtreeKV is an internal ParadigmShift tech demo,
> free for non-commercial use — see [License](#license).

---

## Quickstart

Requires **Java 17+**. Grab `babel-plumtree-demo.jar` from the
[latest release](https://github.com/ParadigmShift-PT/babel-plumtreekv-demo/releases/latest)
(or [build it](#building-from-source)), then run one node — it auto-opens its UI:

```bash
java -jar babel-plumtree-demo.jar          # one node; auto-opens its UI at http://localhost:8000/
```

That gives you a registry you can write to locally. To connect more nodes,
bootstrap the overlay one of two ways (a joining node only needs to reach one node
that's already in it):

**Explicit contact** — works on one machine, across networks, anywhere TCP
connects; no discovery to configure:

```bash
# first node — the one others contact
java -jar babel-plumtree-demo.jar babel.address=127.0.0.1 babel.port=6000 HyParView.contact=none
# second node — dials the first to join
java -jar babel-plumtree-demo.jar babel.address=127.0.0.1 babel.port=6010 HyParView.contact=127.0.0.1:6000
```

**Multicast auto-discovery** — no addresses to type, but same-LAN only and opt-in
(name the discovery protocol on the command line):

```bash
DISC=babel.discovery=pt.unl.fct.di.novasys.babel.core.protocols.discovery.MulticastDiscoveryProtocol
java -jar babel-plumtree-demo.jar $DISC                                                  # machine A
java -jar babel-plumtree-demo.jar $DISC babel.port=6010 babel.discovery.unicast.port=1027   # 2nd node, same host
```

Each node serves a web UI and opens your browser at it on startup
(`plumtreekv.ui.open=false` suppresses that). Run several nodes on one machine with
a distinct `babel.port` **spaced by ≥ 10**; the dissemination protocol **shares
HyParView's channel** (no second port), and the web-UI port follows automatically
(`babel.port + 2000`). Multicast can be blocked locally (VPN, firewall, multiple
NICs, macOS Local Network permission) — if nodes don't find each other that way,
use explicit contact.

Open two nodes' UIs side by side, write keys on each, and watch the registry
converge — each row is colour-coded by the **writer** that owns it, so the
per-writer trees are visible at a glance.

---

## How it works

A write is one `ConfigOp` — *key `k` becomes value `v`* (or a delete) — stamped
with the writer's wall-clock time, its origin id, and a unique op id. The flow:

1. You set/delete a key in the UI (or the headless workload picks one).
   `PlumtreeKVApp` issues a `BroadcastRequest` to the dissemination protocol.
2. The update is **eager-pushed** down the spanning tree to neighbours and
   **lazy-announced** (`IHAVE`) along the rest of the overlay; a node missing an
   announced update **grafts** the announcer to fetch it, repairing the tree.
3. On every node (including the origin) the op arrives as a `BroadcastDelivery`
   and is applied to the `Registry` under **last-writer-wins**: a key keeps
   whichever op has the highest `(timestamp, originId, opId)`; a delete is a
   tombstone that an older set cannot resurrect.

Because that order is **total and deterministic**, any two nodes that have applied
the *same set* of ops hold the identical value for every key — **convergence is
purely a function of dissemination completeness**, which makes the registry a clean
correctness oracle (a 64-bit digest over the live entries).

**Two trees, one binary.** `plumtreekv.protocol` selects the dissemination protocol:

- **`multi`** (default) — **MultiPlumtree**: one spanning tree per *writer* (root),
  so every source converges to its own latency-optimal tree. Ideal when many nodes
  write.
- **`single`** — **Plumtree**: one shared tree, optimized for the first broadcaster
  and reused by all. Cheaper in state and signalling when one node dominates writes.

**Self-healing, no anti-entropy.** Plumtree's lazy push *is* its recovery: a missed
update is announced via `IHAVE`, and the receiver grafts it — so a late joiner, or
the survivors after a node dies, recover what they missed and reach full
convergence on a connected overlay **without** a separate reconciliation protocol.

**Snapshot sync.** On joining, a node asks one neighbour (point-to-point over
HyParView's channel) for the current registry, so a late joiner sees existing state
immediately instead of waiting for new writes.

---

## Configuration

Every value can come from `babel_config.properties` (bundled) or be overridden on
the command line as `key=value` — which also makes the demo easy to script for
automated, headless runs.

### Process-wide & overlay

| Property | Default | Description |
|---|---|---|
| `babel.port` | `6000` | TCP port HyParView binds (the dissemination protocol shares it). Space local nodes by ≥ 10. |
| `babel.interface` / `babel.address` | auto / — | NIC to bind/announce on, or an explicit IP. No loopback default — use `babel.address=127.0.0.1` for several nodes on one disconnected machine. |
| `babel.discovery` | (unset) | Opt-in **multicast** LAN auto-discovery — set on the command line to `pt.unl.fct.di.novasys.babel.core.protocols.discovery.MulticastDiscoveryProtocol`. Off by default; bootstrap is via `HyParView.contact`. |
| `babel.discovery.unicast.port` | `1026` | Per-process discovery socket — only when multicast is enabled; **distinct per local node**. |
| `HyParView.contact` | (absent) | Bootstrap: `none` = first node; `host:port` = dial that node to join; absent = wait for discovery (only useful with multicast on). |
| `HyParView.ActiveView` / `PassiveView` / … | 4 / 7 / … | HyParView view sizes and walk lengths — see the config file. |
| `MultiPlumtree.LazyTickPeriod` | `1000` | Period (ms) at which lazy `IHAVE` announcements are flushed/retried — bounds tree-repair latency. |
| `MultiPlumtree.UseSharedChannel` | `true` | Disseminate over HyParView's channel rather than opening a second one. |

### PlumtreeKV application (`plumtreekv.*`)

| Property | Default | Description |
|---|---|---|
| `plumtreekv.protocol` | `multi` | Dissemination protocol: `multi` (MultiPlumtree, per-writer trees) or `single` (Plumtree, one shared tree). |
| `plumtreekv.ui.enabled` | `true` | Serve the web UI. |
| `plumtreekv.ui.port` | `babel.port + 2000` | Web UI port. |
| `plumtreekv.ui.open` | `true` | Open the system browser at the UI on startup (best-effort; set `false` to suppress, e.g. when running many local nodes). |
| `plumtreekv.snapshot.sync` | `true` | Fetch the current registry from a neighbour on join. |
| `plumtreekv.digest.interval` | `5000` | Period (ms) of convergence-digest telemetry; `≤ 0` disables. |
| `plumtreekv.workload.enabled` | `false` | Headless random-write driver (no UI needed). |
| `plumtreekv.workload.rate` / `.keyspace` / `.duration` | `2` / `16` / `0` | Writes/sec, number of distinct keys, run length (ms; `0` = unbounded). |
| `plumtreekv.workload.controlFile` | (unset) | Path to a shared `WAIT`/`RUN`/`STOP` control file for scripted runs — lets an external driver coordinate when all nodes start and stop writing (and drain in-flight messages before reading results). |

---

## Telemetry & validation

Alongside the human-readable protocol log (`babel-plumtree-demo-<port>.log`), each
node writes a small machine-readable telemetry file
(`babel-plumtree-demo-telemetry-<port>.log`) — one structured event per line
(`SET` / `DELIVER` / `DIGEST` / `NEIGHBOR_*` / `WRITE_START` / `SYNC_MERGE`). That's
enough for an external driver to verify, automatically, that the demo really does
what it claims: that every write reaches every node (coverage), how fast (latency,
last-delivery hop), and that all nodes converge to the same registry — including,
after a node failure, that the **survivors** still converge (tree repair).

---

## Building from source

```bash
mvn package          # → target/babel-plumtree-demo.jar (fat JAR, mainClass Main)
```

Depends on the ParadigmShift Babel libraries — `babel-core`,
`babel-protocols-common`, `hyparview`, and
**[`plumtree`](https://github.com/ParadigmShift-PT/babel-plumtree)** (the Plumtree /
MultiPlumtree protocol library) — from the ParadigmShift Maven repository, which the
local build and CI resolve directly.

## Project layout

```
src/main/java/
  Main.java                              wiring: HyParView + MultiPlumtree|Plumtree + PlumtreeKVApp
  protocols/apps/registry/
    PlumtreeKVApp.java                    the application protocol (slot 300)
    ConfigOp.java                         one set/delete op + last-writer-wins order
    ConfigPayload.java                    ConfigOp ⇄ broadcast bytes
    Registry.java                         LWW key→value store + convergence digest + snapshot
    messages/RegistrySyncMessage.java     point-to-point snapshot request/reply
    telemetry/Telemetry.java              structured telemetry events
    timers/{DigestTimer,WorkloadTimer,ControlTimer}.java
    ui/WebUi.java                         embedded HTTP server (JDK built-in)
  utils/InterfaceToIp.java                bind-address resolution (shared with babel-demo)
src/main/resources/
  babel_config.properties, log4j2.xml, web/{index.html,app.js,style.css}
```

## Distribution

PlumtreeKV is a **runnable demo, not a library** — it is **never** deployed to the
ParadigmShift Maven repository. CI builds the fat JAR and attaches it to a GitHub
Release on a `v*.*.*` tag, and publishes the API docs to GitHub Pages.

## Credits & further reading

PlumtreeKV is a **ParadigmShift** tech demo. The Babel framework and the protocol
implementations it builds on — Babel itself, the HyParView membership protocol, and
the Plumtree epidemic-broadcast-tree — were originally developed at
[NOVA LINCS](https://nova-lincs.di.fct.unl.pt), in the
[TaRDIS](https://www.project-tardis.eu) European project, by the
[Computer Systems Group](https://novasys.di.fct.unl.pt) at
[NOVA FCT](https://www.fct.unl.pt). The versions used here are ParadigmShift's own,
provided and evolved independently of that original work.

The protocols underpinning this demo are described in:

- J. Leitão, J. Pereira, and L. Rodrigues, “Epidemic Broadcast Trees,” in *Proc.
  26th IEEE Int'l Symp. on Reliable Distributed Systems (SRDS'07)*, Beijing, China,
  Oct. 2007, pp. 301–310.
  doi: [10.1109/SRDS.2007.27](https://doi.org/10.1109/SRDS.2007.27) ·
  [PDF](https://asc.di.fct.unl.pt/~jleitao/pdf/srds07-leitao.pdf)
- J. Leitão, J. Pereira, and L. Rodrigues, “HyParView: A Membership Protocol for
  Reliable Gossip-Based Broadcast,” in *Proc. 37th Annual IEEE/IFIP Int'l Conf. on
  Dependable Systems and Networks (DSN'07)*, Edinburgh, UK, Jun. 2007, pp. 419–429.
  doi: [10.1109/DSN.2007.56](https://doi.org/10.1109/DSN.2007.56) ·
  [PDF](https://asc.di.fct.unl.pt/~jleitao/pdf/dsn07-leitao.pdf)
- J. Leitão, *Gossip-Based Broadcast Protocols*, MSc thesis, Faculdade de Ciências
  da Universidade de Lisboa, 2007.
- P. Fouto, P. Á. Costa, N. Preguiça, and J. Leitão, “Babel: A Framework for
  Developing Performant and Dependable Distributed Protocols,” in *Proc. 41st Int'l
  Symp. on Reliable Distributed Systems (SRDS)*, Vienna, Austria, Sep. 2022,
  pp. 146–155.
  doi: [10.1109/SRDS55811.2022.00022](https://doi.org/10.1109/SRDS55811.2022.00022) ·
  [PDF](https://asc.di.fct.unl.pt/~jleitao/pdf/fouto-srds22.pdf)

The **MultiPlumtree** (per-writer-tree) variant additionally adopts design elements —
per-root trees and the lazy re-advertisement / acknowledgement model — from Riak's
`riak_core_broadcast`, originally implemented by **Jordan West**
([repo](https://github.com/basho/riak_core/blob/develop/src/riak_core_broadcast.erl)).
No code from that project is included; see the
[`plumtree`](https://github.com/ParadigmShift-PT/babel-plumtree) library for the full
acknowledgement.

## License

ParadigmShift Proprietary License — non-commercial use permitted; commercial use
requires a written licence from ParadigmShift, Lda. See [LICENSE](LICENSE).
