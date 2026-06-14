package protocols.apps.registry.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

/**
 * Periodic timer that drives the headless workload: on each tick
 * {@code PlumtreeKVApp} writes a random key from the configured keyspace, at the
 * configured rate, with no UI or human — how {@code babel-plumtree-experiments}
 * generates multi-writer load.
 *
 * <p>Handler class: timer. <b>ID:</b> {@value #TIMER_ID}. Owning protocol:
 * {@code PlumtreeKVApp} (id 300).
 */
public class WorkloadTimer extends ProtoTimer {

    public static final short TIMER_ID = 302;

    public WorkloadTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
