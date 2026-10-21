package io.milvus.talon;

/** Immutable per-call tracing and cache namespace selection.
 * An invalid explicit trace carrier means ROOT, never INHERIT. */
public final class RequestOptions {
    public static final RequestOptions INHERIT = new RequestOptions(true, null, null);
    public static final RequestOptions ROOT = new RequestOptions(false, null, null);
    final boolean inherit;
    final TraceContext parent;
    final String namespace;

    private RequestOptions(boolean inherit, TraceContext parent, String namespace) {
        if (namespace == null) namespace = "default";
        if (!namespace.matches("[A-Za-z0-9._-]{1,255}")
                || namespace.equals(".") || namespace.equals("..")
                || namespace.equals("worker_identity") || namespace.equals(".worker.lock")) {
            throw new IllegalArgumentException("invalid cache namespace: " + namespace);
        }
        this.inherit = inherit;
        this.parent = parent;
        this.namespace = namespace;
    }

    /** Select an explicit trace parent; null means ROOT, never INHERIT. */
    public static RequestOptions explicit(TraceContext parent) {
        return new RequestOptions(false, parent, null);
    }

    /**
     * Return new options with this namespace, preserving the trace parent policy.
     *
     * @param namespace null selects {@code default}; otherwise 1..255 ASCII letters,
     *     digits, dots, underscores or hyphens, excluding {@code .}, {@code ..},
     *     {@code worker_identity} and {@code .worker.lock}
     */
    public RequestOptions withNamespace(String namespace) {
        return new RequestOptions(inherit, parent, namespace);
    }

    /** Return new options with an explicit trace parent, preserving the namespace.
     * A null parent means ROOT, never INHERIT. */
    public RequestOptions withParent(TraceContext parent) {
        return new RequestOptions(false, parent, namespace);
    }
}
