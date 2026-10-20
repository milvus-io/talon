package io.milvus.talon;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

/** Lightweight propagation bridge. A host adapter supplies W3C context from its SDK.
 * This class never installs a provider or exports spans. */
public final class Telemetry {
    private Telemetry() {}
    private static volatile Set<String> v2Endpoints = Set.of();
    private static volatile Supplier<TraceContext> contextSupplier = () -> null;
    private static final ThreadLocal<CallContext> CURRENT = new ThreadLocal<>();

    /** Explicit deployment capabilities. Empty means off; no network probing or fallback. */
    public static void configure(Set<String> endpoints, Supplier<TraceContext> supplier) {
        v2Endpoints = Set.copyOf(endpoints);
        contextSupplier = supplier == null ? () -> null : supplier;
    }

    static final class CallContext {
        final TraceContext parent;
        final byte[] readId;
        final String namespace;
        CallContext(TraceContext parent, String namespace) {
            this.parent = parent;
            this.namespace = namespace;
            if (parent != null && (Character.digit(parent.traceparent.charAt(54), 16) & 1) != 0) {
                UUID id = UUID.randomUUID();
                readId = ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array();
            } else {
                readId = null;
            }
        }
    }
    interface IOCall<T> { T run() throws IOException; }

    static <T> T call(RequestOptions options, IOCall<T> call) throws IOException {
        CallContext previous = CURRENT.get();
        // Internal calls keep the whole scope only for the unmodified INHERIT singleton.
        // Derived options must apply their namespace, including an explicit default reset.
        if (options == RequestOptions.INHERIT && previous != null) return call.run();
        TraceContext parent = null;
        if (!v2Endpoints.isEmpty()) {
            parent = options.parent;
            if (options.inherit) {
                if (previous != null) parent = previous.parent;
                else {
                    try { parent = contextSupplier.get(); } catch (RuntimeException ignored) { parent = null; }
                }
            }
        }
        // Namespace routing is required even when trace propagation is disabled.
        CURRENT.set(new CallContext(parent, options.namespace));
        try { return call.run(); }
        finally { if (previous == null) CURRENT.remove(); else CURRENT.set(previous); }
    }

    static boolean canSend(String endpoint) {
        CallContext current = CURRENT.get();
        return current != null && current.parent != null && (v2Endpoints.contains(endpoint) || v2Endpoints.contains("*"));
    }

    static CallContext currentContext() { return CURRENT.get(); }
}
