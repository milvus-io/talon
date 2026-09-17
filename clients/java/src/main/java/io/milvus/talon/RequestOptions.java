package io.milvus.talon;

/** Per-call parent selection. An invalid explicit carrier means ROOT, never INHERIT. */
public final class RequestOptions {
    public static final RequestOptions INHERIT = new RequestOptions(true, null);
    public static final RequestOptions ROOT = new RequestOptions(false, null);
    final boolean inherit;
    final TraceContext parent;

    private RequestOptions(boolean inherit, TraceContext parent) {
        this.inherit = inherit;
        this.parent = parent;
    }

    public static RequestOptions explicit(TraceContext parent) { return new RequestOptions(false, parent); }
}
