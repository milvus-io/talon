package io.milvus.talon;

/** Immutable per-call trace, tenant and block-planning options. */
public final class RequestOptions {
    public static final RequestOptions INHERIT = new RequestOptions(true, null, "default", null);
    public static final RequestOptions ROOT = new RequestOptions(false, null, "default", null);
    final boolean inherit;
    final TraceContext parent;
    final String tenant;
    final Integer blockSize;

    private RequestOptions(boolean inherit, TraceContext parent, String tenant, Integer blockSize) {
        this.inherit = inherit;
        this.parent = parent;
        this.tenant = tenant;
        this.blockSize = blockSize;
    }

    public static RequestOptions explicit(TraceContext parent) {
        return new RequestOptions(false, parent, "default", null);
    }

    public RequestOptions withTenant(String tenant) {
        if (tenant == null || tenant.length() > 255 || !tenant.matches("[A-Za-z0-9_.-]+") || tenant.equals(".") || tenant.equals("..")) {
            throw new IllegalArgumentException("invalid tenant name");
        }
        return new RequestOptions(inherit, parent, tenant, blockSize);
    }

    /** Match the selected tenant's worker policy without changing the client. */
    public RequestOptions withBlockSize(int blockSize) {
        if (blockSize <= 0) throw new IllegalArgumentException("blockSize must be positive");
        return new RequestOptions(inherit, parent, tenant, blockSize);
    }
}
