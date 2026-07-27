package io.milvus.talon;

import java.util.Collections;
import java.util.List;

/**
 * Where a block lives: owners in preference order, and the epoch they were
 * computed at.
 *
 * <p>The epoch matters. Observing a different one means the cached placement may
 * be stale, and continuing to use it is the quiet failure mode — reads keep
 * succeeding, from the wrong worker, until something else forces a refresh.
 */
public final class Placement {

    private final List<String> owners;
    private final long epoch;

    public Placement(List<String> owners, long epoch) {
        this.owners = Collections.unmodifiableList(owners);
        this.epoch = epoch;
    }

    /** Owner node ids, highest-weight first. Resolve to addresses via membership. */
    public List<String> owners() {
        return owners;
    }

    public long epoch() {
        return epoch;
    }
}
