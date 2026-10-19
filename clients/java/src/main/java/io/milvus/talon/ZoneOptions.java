package io.milvus.talon;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.Supplier;

/** Immutable per-client AZ policy. Affinity defaults off. */
public record ZoneOptions(String zone, boolean affinity) {
    public ZoneOptions {
        if (zone != null && (zone.isBlank() || zone.getBytes(StandardCharsets.UTF_8).length > 256)) {
            throw new IllegalArgumentException("zone must contain 1..=256 bytes");
        }
    }

    /** Read settings once; if TALON_ZONE is unset, query the Kubernetes Node zone.
     * Requires TALON_NODE_NAME and a mounted ServiceAccount with get nodes RBAC.
     * The startup HTTP request has a five-second timeout; lookup failure leaves
     * the zone unknown and preserves the requested affinity policy. */
    public static ZoneOptions fromEnvironment() {
        return fromEnvironment(System::getenv);
    }

    static ZoneOptions fromEnvironment(Function<String, String> get) {
        return fromEnvironment(get, () -> KubernetesZone.resolve(get));
    }

    static ZoneOptions fromEnvironment(Function<String, String> get, Supplier<String> discover) {
        String zone = get.apply("TALON_ZONE");
        if (zone != null && zone.isBlank()) zone = null;
        boolean affinity = booleanValue(get.apply("TALON_ZONE_AFFINITY"));
        return new ZoneOptions(zone == null ? discover.get() : zone, affinity);
    }

    private static boolean booleanValue(String value) {
        if (value == null || value.isBlank()) return false;
        return switch (value.toLowerCase(Locale.ROOT)) {
            case "true", "1", "yes" -> true;
            case "false", "0", "no" -> false;
            default -> throw new IllegalArgumentException("invalid TALON zone boolean");
        };
    }
}
