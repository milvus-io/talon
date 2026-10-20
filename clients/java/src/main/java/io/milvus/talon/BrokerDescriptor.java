package io.milvus.talon;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

/** Immutable, validated wire descriptor. Credential exchange belongs to the backend. */
final class BrokerDescriptor {
    static final int MAX_BYTES = 16 * 1024;
    private static final int DEFAULT_EXCHANGE_TIMEOUT_MS = 3_000;
    private static final int INITIAL_ATTEMPTS = 4;
    private static final int INITIAL_BACKOFF_MS = 100 + 200 + 400;
    private static final Set<String> S3_CLOUDS = Set.of("aws", "ali", "tc", "hwc");
    private final byte[] encoded;
    private final int acquisitionTimeoutMillis;

    private BrokerDescriptor(byte[] encoded, int acquisitionTimeoutMillis) {
        this.encoded = encoded;
        this.acquisitionTimeoutMillis = acquisitionTimeoutMillis;
    }

    static BrokerDescriptor parse(String json) {
        if (json.length() > MAX_BYTES) {
            throw new IllegalArgumentException("credential_broker exceeds size limit");
        }
        if (json.isBlank()) {
            throw new IllegalArgumentException("credential_broker must contain JSON");
        }
        byte[] broker = json.getBytes(StandardCharsets.UTF_8);
        if (broker.length > MAX_BYTES) {
            throw new IllegalArgumentException("credential_broker exceeds size limit");
        }
        BrokerJson parsed = new BrokerJson(json);
        try {
            URI endpoint = new URI(parsed.descriptor());
            if (!("http".equalsIgnoreCase(endpoint.getScheme()) || "https".equalsIgnoreCase(endpoint.getScheme()))
                    || endpoint.getHost() == null || endpoint.getRawUserInfo() != null
                    || endpoint.getRawFragment() != null || endpoint.getPort() > 65535) {
                throw invalidBroker();
            }
        } catch (URISyntaxException | IllegalArgumentException ignored) {
            // URI/parser exceptions can embed the URL or credential body.
            // Never attach the original exception as a cause.
            throw invalidBroker();
        }
        return new BrokerDescriptor(broker, parsed.acquisitionTimeoutMillis);
    }

    int acquisitionTimeoutMillis() { return acquisitionTimeoutMillis; }

    int encodedLength() { return encoded.length; }

    void writeTo(byte[] destination, int offset) {
        System.arraycopy(encoded, 0, destination, offset, encoded.length);
    }

    @Override
    public String toString() { return "BrokerDescriptor([REDACTED])"; }

    private static IllegalArgumentException invalidBroker() {
        return new IllegalArgumentException("invalid credential_broker descriptor");
    }

    /** Validate JSON without a runtime dependency or retaining a second object tree. */
    private static final class BrokerJson {
        private final String input;
        private int position;
        // One initial attempt plus three retries, including 100/200/400ms backoff.
        private int acquisitionTimeoutMillis = DEFAULT_EXCHANGE_TIMEOUT_MS * INITIAL_ATTEMPTS + INITIAL_BACKOFF_MS;

        BrokerJson(String input) { this.input = input; }

        String descriptor() throws URISyntaxException {
            expect('{');
            String url = null;
            String csp = null;
            boolean request = false;
            boolean origin = false;
            boolean hasRegion = false;
            if (!take('}')) {
                do {
                    String key = string();
                    expect(':');
                    if ("url".equals(key) && url == null) url = string();
                    else if ("request".equals(key) && !request) {
                        csp = request();
                        request = true;
                    } else if ("origin".equals(key) && !origin) {
                        hasRegion = origin();
                        origin = true;
                    } else throw invalidBroker();
                } while (take(','));
                expect('}');
            }
            peek();
            if (url == null || !request || !origin || position != input.length()) throw invalidBroker();
            if (csp != null && S3_CLOUDS.contains(csp) && !hasRegion) throw invalidBroker();
            return url;
        }

        private String request() {
            expect('{');
            String csp = null;
            if (!take('}')) {
                do {
                    String key = string();
                    expect(':');
                    if ("csp".equals(key) && peek() == '"') csp = string();
                    else {
                        // The request object occupies depth 0, as in value(0).
                        value(1);
                        // Match serde_json: the last duplicate csp wins, including nonstrings.
                        if ("csp".equals(key)) csp = null;
                    }
                } while (take(','));
                expect('}');
            }
            return csp;
        }

        /** Validate origin settings and report whether a signing region is present. */
        private boolean origin() throws URISyntaxException {
            expect('{');
            String endpoint = null;
            boolean hasRegion = false;
            Boolean useSsl = null;
            Set<String> keys = new HashSet<>();
            if (!take('}')) {
                do {
                    String key = string();
                    if (!keys.add(key)) throw invalidBroker();
                    expect(':');
                    switch (key) {
                        case "endpoint" -> endpoint = nullableString();
                        case "use_ssl" -> useSsl = nullableBoolean();
                        case "use_virtual_host" -> nullableBoolean();
                        case "region", "tls_ca_pem" -> {
                            String text = nullableString();
                            if (text != null && text.codePoints().allMatch(c ->
                                    Character.isWhitespace(c) || Character.isSpaceChar(c) || c == 0x85)) {
                                throw invalidBroker();
                            }
                            if ("region".equals(key)) hasRegion = text != null;
                        }
                        case "tls_min_version" -> {
                            String version = nullableString();
                            if (version != null && !"1.0".equals(version) && !"1.1".equals(version)
                                    && !"1.2".equals(version) && !"1.3".equals(version)) {
                                throw invalidBroker();
                            }
                        }
                        case "request_timeout_ms" -> {
                            String timeout = positiveInteger("18446744073709551615");
                            if (timeout != null) {
                                // Parsing has already validated the full unsigned range.
                                // Clamp before conversion/multiplication to avoid signed overflow.
                                acquisitionTimeoutMillis = timeout.length() > 9 ? Integer.MAX_VALUE
                                        : (int) Math.min(Integer.MAX_VALUE, Long.parseLong(timeout) * INITIAL_ATTEMPTS + INITIAL_BACKOFF_MS);
                            }
                        }
                        case "max_connections" -> positiveInteger("4294967295");
                        default -> throw invalidBroker();
                    }
                } while (take(','));
                expect('}');
            }
            if (endpoint == null) throw invalidBroker();
            int separator = endpoint.indexOf("://");
            String scheme = separator < 0 ? null : endpoint.substring(0, separator);
            String authority = separator < 0 ? endpoint : endpoint.substring(separator + 3);
            if (scheme != null && !"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
                throw invalidBroker();
            }
            // Check raw input before URI parsing can hide unsafe URL parts.
            if (authority.endsWith("/")) authority = authority.substring(0, authority.length() - 1);
            if (authority.isEmpty() || endpoint.codePoints().anyMatch(c -> Character.isWhitespace(c)
                    || Character.isSpaceChar(c) || Character.isISOControl(c))
                    || authority.chars().anyMatch(c -> c > 127 || "/\\@?#%".indexOf(c) >= 0)) {
                throw invalidBroker();
            }
            URI parsed = new URI("https://" + authority);
            if (parsed.getHost() == null || parsed.getPort() > 65535
                    || (scheme != null && useSsl != null && "https".equalsIgnoreCase(scheme) != useSsl)) {
                throw invalidBroker();
            }
            String host = parsed.getHost();
            if (host.matches("(?i)0x[0-9a-f]+")) throw invalidBroker();
            if (host.matches("[0-9.]+")) {
                // Keep IPv4 literal spelling unambiguous across URL parsers.
                String[] octets = host.split("\\.", -1);
                if (octets.length != 4) throw invalidBroker();
                for (String octet : octets) {
                    if (octet.isEmpty() || octet.length() > 3
                            || (octet.length() > 1 && octet.charAt(0) == '0')
                            || Integer.parseInt(octet) > 255) throw invalidBroker();
                }
            }
            return hasRegion;
        }

        private String nullableString() {
            if (peek() != 'n') return string();
            literal("null");
            return null;
        }

        private Boolean nullableBoolean() {
            return switch (peek()) {
                case 't' -> { literal("true"); yield true; }
                case 'f' -> { literal("false"); yield false; }
                case 'n' -> { literal("null"); yield null; }
                default -> throw invalidBroker();
            };
        }

        private String positiveInteger(String maximum) {
            if (peek() == 'n') {
                literal("null");
                return null;
            }
            int start = position;
            digits();
            String number = input.substring(start, position);
            if (number.charAt(0) == '0' || number.length() > maximum.length()
                    || (number.length() == maximum.length() && number.compareTo(maximum) > 0)) {
                throw invalidBroker();
            }
            return number;
        }

        private void value(int depth) {
            if (depth > 64) throw invalidBroker();
            switch (peek()) {
                case '{' -> {
                    position++;
                    if (take('}')) return;
                    do {
                        string();
                        expect(':');
                        value(depth + 1);
                    } while (take(','));
                    expect('}');
                }
                case '[' -> {
                    position++;
                    if (take(']')) return;
                    do { value(depth + 1); } while (take(','));
                    expect(']');
                }
                case '"' -> string();
                case 't' -> literal("true");
                case 'f' -> literal("false");
                case 'n' -> literal("null");
                default -> number();
            }
        }

        private String string() {
            expect('"');
            StringBuilder decoded = new StringBuilder();
            while (position < input.length()) {
                char next = input.charAt(position++);
                if (next == '"') return decoded.toString();
                if (next < 0x20) throw invalidBroker();
                if (next == '\\') {
                    if (position == input.length()) throw invalidBroker();
                    char escape = input.charAt(position++);
                    switch (escape) {
                        case '"', '\\', '/' -> decoded.append(escape);
                        case 'b' -> decoded.append('\b');
                        case 'f' -> decoded.append('\f');
                        case 'n' -> decoded.append('\n');
                        case 'r' -> decoded.append('\r');
                        case 't' -> decoded.append('\t');
                        case 'u' -> {
                            char unit = unicode();
                            decoded.append(unit);
                            if (Character.isHighSurrogate(unit)) {
                                if (!input.startsWith("\\u", position)) throw invalidBroker();
                                position += 2;
                                char low = unicode();
                                if (!Character.isLowSurrogate(low)) throw invalidBroker();
                                decoded.append(low);
                            } else if (Character.isLowSurrogate(unit)) throw invalidBroker();
                        }
                        default -> throw invalidBroker();
                    }
                } else {
                    decoded.append(next);
                    if (Character.isHighSurrogate(next)) {
                        if (position == input.length() || !Character.isLowSurrogate(input.charAt(position))) throw invalidBroker();
                        decoded.append(input.charAt(position++));
                    } else if (Character.isLowSurrogate(next)) throw invalidBroker();
                }
            }
            throw invalidBroker();
        }

        private char unicode() {
            int unit = 0;
            for (int i = 0; i < 4; i++) {
                if (position == input.length()) throw invalidBroker();
                char ch = input.charAt(position++);
                int digit = ch >= '0' && ch <= '9' ? ch - '0'
                        : ch >= 'a' && ch <= 'f' ? ch - 'a' + 10
                        : ch >= 'A' && ch <= 'F' ? ch - 'A' + 10 : -1;
                if (digit < 0) throw invalidBroker();
                unit = unit * 16 + digit;
            }
            return (char) unit;
        }

        private void number() {
            int start = position;
            if (position < input.length() && input.charAt(position) == '-') position++;
            if (position < input.length() && input.charAt(position) == '0') position++;
            else digits();
            if (position < input.length() && input.charAt(position) == '.') {
                position++;
                digits();
            }
            if (position < input.length() && (input.charAt(position) == 'e' || input.charAt(position) == 'E')) {
                position++;
                if (position < input.length() && (input.charAt(position) == '+' || input.charAt(position) == '-')) position++;
                digits();
            }
            if (!Double.isFinite(Double.parseDouble(input.substring(start, position)))) throw invalidBroker();
        }

        private void digits() {
            int start = position;
            while (position < input.length() && input.charAt(position) >= '0' && input.charAt(position) <= '9') position++;
            if (position == start) throw invalidBroker();
        }

        private void literal(String text) {
            if (!input.startsWith(text, position)) throw invalidBroker();
            position += text.length();
        }

        private char peek() {
            while (position < input.length() && " \t\r\n".indexOf(input.charAt(position)) >= 0) position++;
            return position == input.length() ? 0 : input.charAt(position);
        }

        private boolean take(char expected) {
            if (peek() != expected) return false;
            position++;
            return true;
        }

        private void expect(char expected) {
            if (!take(expected)) throw invalidBroker();
        }
    }

}
