package io.milvus.talon;

/** Immutable per-call trace, namespace and block-planning options. */
public final class RequestOptions {
    public static final RequestOptions INHERIT = new RequestOptions(true, null, "default", null);
    public static final RequestOptions ROOT = new RequestOptions(false, null, "default", null);
    final boolean inherit;
    final TraceContext parent;
    final String namespace;
    final Integer blockSize;

    private RequestOptions(boolean inherit, TraceContext parent, String namespace, Integer blockSize) {
        this.inherit = inherit;
        this.parent = parent;
        this.namespace = namespace;
        this.blockSize = blockSize;
    }

    public static RequestOptions explicit(TraceContext parent) {
        return new RequestOptions(false, parent, "default", null);
    }

    public RequestOptions withNamespace(String namespace) {
        if (namespace == null || namespace.length() > 255 || !namespace.matches("[A-Za-z0-9_.-]+") || namespace.equals(".") || namespace.equals("..")) {
            throw new IllegalArgumentException("invalid namespace name");
        }
        if (namespace.equals("worker_identity") || namespace.equals(".worker.lock")) {
            throw new IllegalArgumentException("namespace name is reserved for worker metadata");
        }
        return new RequestOptions(inherit, parent, namespace, blockSize);
    }

    /** Match the selected namespace's worker policy without changing the client. */
    public RequestOptions withBlockSize(int blockSize) {
        if (blockSize <= 0) throw new IllegalArgumentException("blockSize must be positive");
        return new RequestOptions(inherit, parent, namespace, blockSize);
    }
}
