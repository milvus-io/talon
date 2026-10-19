package io.milvus.talon;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.function.Function;

/** Immutable per-client AZ policy. Affinity defaults off. */
public record ZoneOptions(String zone, boolean affinity) {
    public ZoneOptions {
        if (zone != null && (zone.isBlank() || zone.getBytes(StandardCharsets.UTF_8).length > 256)) {
            throw new IllegalArgumentException("zone must contain 1..=256 bytes");
        }
    }

    /** Read deployment settings once. SDKs do not query the Kubernetes API. */
    public static ZoneOptions fromEnvironment() {
        return fromEnvironment(System::getenv);
    }

    static ZoneOptions fromEnvironment(Function<String, String> get) {
        String zone = get.apply("TALON_ZONE");
        if (zone != null && zone.isBlank()) zone = null;
        return new ZoneOptions(zone, booleanValue(get.apply("TALON_ZONE_AFFINITY")));
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
