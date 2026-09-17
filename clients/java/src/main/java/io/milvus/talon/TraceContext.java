package io.milvus.talon;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

/** Owned W3C carrier. No dependency on a particular host OpenTelemetry SDK. */
public final class TraceContext {
    final String traceparent;
    final String tracestate;

    private TraceContext(String parent, String state) {
        traceparent = parent;
        tracestate = state;
    }

    /** Invalid parent returns null; invalid tracestate is independently discarded. */
    public static TraceContext fromW3c(String parent, String state) {
        if (parent == null || parent.length() > 1024 || !parent.matches(
                "[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}(-[!-~]*)?")) return null;
        if (parent.startsWith("ff") || (parent.startsWith("00") && parent.length() != 55)
                || parent.substring(3, 35).equals("00000000000000000000000000000000")
                || parent.substring(36, 52).equals("0000000000000000")) return null;
        return new TraceContext("00" + parent.substring(2, 55), validState(state) ? state : "");
    }

    private static boolean validState(String state) {
        if (state == null || state.isEmpty() || state.length() > 512) return false;
        Set<String> keys = new HashSet<>();
        for (String part : state.split(",", -1)) {
            String[] kv = part.trim().split("=", -1);
            if (kv.length != 2 || !keys.add(kv[0]) || keys.size() > 32
                    || kv[0].length() > 256 || kv[1].isEmpty() || kv[1].length() > 256
                    || kv[1].endsWith(" ") || !kv[1].matches("[ -<>-~]+")
                    || !kv[0].matches("([a-z][a-z0-9_*/-]{0,255}|[a-z0-9][a-z0-9_*/-]{0,240}@[a-z][a-z0-9_*/-]{0,13})")) return false;
        }
        return state.getBytes(StandardCharsets.US_ASCII).length == state.length();
    }

    public String traceparent() { return traceparent; }
    public String tracestate() { return tracestate; }
}
