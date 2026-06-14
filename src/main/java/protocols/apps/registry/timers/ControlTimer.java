package protocols.apps.registry.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

/**
 * Polls the shared control file in orchestrator-coordinated workload mode (see
 * {@code PlumtreeKVApp.uponControlTimer}). The experiment script lets the whole system
 * join and settle, then writes {@code RUN} to begin the write workload on every node at
 * once, and later {@code STOP} to cease — so writing starts only once ALL nodes are
 * present (a node's own view being stable is not enough; late joiners would otherwise
 * miss its early ops) and stops together so the orchestrator can drain in-flight messages
 * before measuring or killing. Set up with {@code setupPeriodicTimer(...)} when
 * {@code plumtreekv.workload.controlFile} is configured; otherwise the node uses the
 * legacy single start-delay timer.
 *
 * <p>Stateless, so {@link #clone()} returns {@code this} — the Babel convention.
 *
 * <p>Handler class: timer. <b>ID:</b> {@value #TIMER_ID}. Owning protocol:
 * {@code PlumtreeKVApp} (id 300).
 */
public class ControlTimer extends ProtoTimer {

    public static final short TIMER_ID = 303;

    public ControlTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
