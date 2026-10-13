package io.milvus.talon;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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

    private static final class CallContext {
        final TraceContext parent;
        final byte[] readId;
        CallContext(TraceContext parent) {
            this.parent = parent;
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
        if (v2Endpoints.isEmpty()) return call.run();
        CallContext previous = CURRENT.get();
        if (options.inherit && previous != null) return call.run();
        TraceContext parent = options.parent;
        if (options.inherit) {
            try { parent = contextSupplier.get(); } catch (RuntimeException ignored) { parent = null; }
        }
        CURRENT.set(new CallContext(parent));
        try { return call.run(); }
        finally { if (previous == null) CURRENT.remove(); else CURRENT.set(previous); }
    }

    static boolean canSend(String endpoint) {
        CallContext current = CURRENT.get();
        return current != null && current.parent != null && (v2Endpoints.contains(endpoint) || v2Endpoints.contains("*"));
    }

    static byte[] envelope(byte[] frame, String endpoint) {
        CallContext current = CURRENT.get();
        if (current == null || current.parent == null || !(v2Endpoints.contains(endpoint) || v2Endpoints.contains("*"))) return frame;
        return envelope(frame, current.parent, current.readId);
    }

    static byte[] envelope(byte[] frame, TraceContext parent, byte[] readId) {
        ByteArrayOutputStream metadata = new ByteArrayOutputStream();
        tlv(metadata, 1, parent.traceparent.getBytes(StandardCharsets.US_ASCII));
        if (!parent.tracestate.isEmpty()) tlv(metadata, 2, parent.tracestate.getBytes(StandardCharsets.US_ASCII));
        if (readId != null) tlv(metadata, 3, readId);
        byte[] values = metadata.toByteArray();
        byte[] result = new byte[frame.length + 2 + values.length];
        System.arraycopy(frame, 0, result, 0, Frame.HEADER_LEN);
        result[2] = 2;
        ByteBuffer.wrap(result).putInt(12, result.length - Frame.HEADER_LEN).putShort(16, (short) values.length);
        System.arraycopy(values, 0, result, 18, values.length);
        System.arraycopy(frame, 16, result, 18 + values.length, frame.length - 16);
        return result;
    }

    private static void tlv(ByteArrayOutputStream out, int key, byte[] value) {
        out.write(key); out.write(value.length >>> 8); out.write(value.length & 255); out.writeBytes(value);
    }
}
