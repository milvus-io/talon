package io.milvus.talon;

import java.io.IOException;

/** Immutable per-call trace, cache namespace and credential properties.
 * Credential properties never inherit ambient state. */
public final class RequestOptions {
    private static final ThreadLocal<BrokerDescriptor> CURRENT_BROKER = new ThreadLocal<>();
    public static final RequestOptions INHERIT = new RequestOptions(true, null);
    public static final RequestOptions ROOT = new RequestOptions(false, null);
    final boolean inherit;
    final TraceContext parent;
    final String namespace;
    private final BrokerDescriptor broker;

    private RequestOptions(boolean inherit, TraceContext parent) {
        this(inherit, parent, null, null);
    }

    private RequestOptions(boolean inherit, TraceContext parent, String namespace, BrokerDescriptor broker) {
        this.inherit = inherit;
        this.parent = parent;
        this.namespace = validateNamespace(namespace);
        this.broker = broker;
    }

    private static String validateNamespace(String namespace) {
        if (namespace == null) return "default";
        if (!namespace.matches("[A-Za-z0-9._-]{1,255}")
                || namespace.equals(".") || namespace.equals("..")
                || namespace.equals("worker_identity") || namespace.equals(".worker.lock")) {
            throw new IllegalArgumentException("invalid cache namespace: " + namespace);
        }
        return namespace;
    }

    /** Select an explicit trace parent; null means ROOT, never INHERIT. */
    public static RequestOptions explicit(TraceContext parent) { return new RequestOptions(false, parent); }

    /**
     * Return new options with this namespace, preserving tracing and credential properties.
     *
     * @param namespace null selects {@code default}; otherwise 1..255 ASCII letters,
     *     digits, dots, underscores or hyphens, excluding {@code .}, {@code ..},
     *     {@code worker_identity} and {@code .worker.lock}
     */
    public RequestOptions withNamespace(String namespace) {
        return new RequestOptions(inherit, parent, namespace, broker);
    }

    /** Return new options with an explicit trace parent, preserving namespace and properties.
     * A null parent means ROOT, never INHERIT. */
    public RequestOptions withParent(TraceContext parent) {
        return new RequestOptions(false, parent, namespace, broker);
    }

    /**
     * Select a credential broker using the complete JSON descriptor
     * {@code {"url": "...", "request": {...}, "origin": {"endpoint": "..."}}}, preserving namespace and tracing.
     * Every cloud requires an origin object with a nonblank endpoint. AWS, Alibaba Cloud,
     * Tencent Cloud and Huawei Cloud also require a nonblank {@code origin.region};
     * {@code request.region} only selects the Broker region and cannot substitute for it.
     * GCP and Azure may omit {@code origin.region}. Other origin fields optionally select
     * SSL and supported addressing settings.
     * Its {@code request_timeout_ms} controls each Broker HTTP exchange (default 3 seconds).
     * The {@code tls_ca_pem}, {@code tls_min_version} and {@code max_connections} fields
     * are reserved and do not affect request transport settings.
     * Accepting TLS version strings 1.0 through 1.3 does not imply transport support for them.
     * A null descriptor clears the broker; credentials never inherit ambient state.
     */
    public RequestOptions withCredentialBroker(String json) {
        return new RequestOptions(inherit, parent, namespace, json == null ? null : BrokerDescriptor.parse(json));
    }

    static BrokerDescriptor currentBroker() { return CURRENT_BROKER.get(); }

    static boolean hasBroker() { return CURRENT_BROKER.get() != null; }

    /** Include all four initial Broker exchanges and backoff before origin I/O starts. */
    static int requestTimeoutMillis(int base) {
        BrokerDescriptor broker = CURRENT_BROKER.get();
        return broker == null ? base : (int) Math.min(Integer.MAX_VALUE,
                (long) base + broker.acquisitionTimeoutMillis());
    }

    static <T> T call(RequestOptions options, Telemetry.IOCall<T> call) throws IOException {
        BrokerDescriptor previous = CURRENT_BROKER.get();
        CURRENT_BROKER.set(options.broker);
        try { return Telemetry.call(options, call); }
        finally {
            if (previous == null) CURRENT_BROKER.remove();
            else CURRENT_BROKER.set(previous);
        }
    }
}
