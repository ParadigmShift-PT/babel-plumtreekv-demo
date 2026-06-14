package protocols.apps.registry.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

/**
 * Periodic timer that drives the convergence-digest telemetry line: on each tick
 * {@code PlumtreeKVApp} emits a {@code DIGEST} event with the local registry's
 * hash, which the experiments harness uses to assert all live nodes converge.
 *
 * <p>Handler class: timer. <b>ID:</b> {@value #TIMER_ID}. Owning protocol:
 * {@code PlumtreeKVApp} (id 300).
 */
public class DigestTimer extends ProtoTimer {

    public static final short TIMER_ID = 301;

    public DigestTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
