package io.milvus.talon;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/** Startup-only Node lookup, matching talon-backend's Worker/Gateway resolver. */
final class KubernetesZone {
    private static final Path SERVICE_ACCOUNT = Path.of("/var/run/secrets/kubernetes.io/serviceaccount");
    private static final System.Logger LOG = System.getLogger(KubernetesZone.class.getName());

    private KubernetesZone() { }

    static String resolve(Function<String, String> env) {
        return resolve(env, SERVICE_ACCOUNT);
    }

    static String resolve(Function<String, String> env, Path credentials) {
        String host = env.apply("KUBERNETES_SERVICE_HOST");
        String node = env.apply("TALON_NODE_NAME");
        if (host == null || host.isBlank() || node == null || node.isBlank()) return null;
        try {
            String port = env.apply("KUBERNETES_SERVICE_PORT");
            int apiPort = port == null || port.isBlank() ? 443 : Integer.parseInt(port);
            URI uri = new URI("https", null, host, apiPort, "/api/v1/nodes/" + node, null, null);
            String token = Files.readString(credentials.resolve("token")).trim();
            if (token.isEmpty()) throw new IOException("missing service account token");
            KeyStore trust = KeyStore.getInstance(KeyStore.getDefaultType());
            trust.load(null, null);
            try (var ca = Files.newInputStream(credentials.resolve("ca.crt"))) {
                int index = 0;
                for (var certificate : CertificateFactory.getInstance("X.509").generateCertificates(ca)) {
                    trust.setCertificateEntry("ca-" + index++, certificate);
                }
                if (index == 0) throw new IOException("missing cluster CA");
            }
            TrustManagerFactory managers = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            managers.init(trust);
            SSLContext tls = SSLContext.getInstance("TLS");
            tls.init(null, managers.getTrustManagers(), null);
            HttpClient client = HttpClient.newBuilder().sslContext(tls)
                    .connectTimeout(Duration.ofSeconds(2))
                    .followRedirects(HttpClient.Redirect.NEVER).build();
            HttpRequest request = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(5))
                    .header("Authorization", "Bearer " + token).GET().build();
            HttpResponse<String> response = request(client, request);
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                LOG.log(System.Logger.Level.WARNING, "zone lookup returned HTTP {0}; zone remains unknown", response.statusCode());
                return null;
            }
            return parseZone(response.body());
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            LOG.log(System.Logger.Level.WARNING, "zone lookup interrupted; zone remains unknown");
        } catch (IOException | GeneralSecurityException | URISyntaxException | IllegalArgumentException error) {
            // Never log the response, credentials, or exception message from HTTP/TLS.
            LOG.log(System.Logger.Level.WARNING, "zone lookup failed ({0}); zone remains unknown", error.getClass().getSimpleName());
        }
        return null;
    }

    private static HttpResponse<String> request(HttpClient client, HttpRequest request)
            throws IOException, InterruptedException {
        var pending = client.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        try {
            // Bound the complete response, including a stalled or trickling body.
            return pending.get(5, TimeUnit.SECONDS);
        } catch (ExecutionException | TimeoutException error) {
            pending.cancel(true);
            throw new IOException("Node lookup failed", error);
        } catch (InterruptedException error) {
            pending.cancel(true);
            throw error;
        }
    }

    static String parseZone(String json) throws IOException {
        Object value = new Json(json).parse();
        for (String key : new String[] {"metadata", "labels", "topology.kubernetes.io/zone"}) {
            if (!(value instanceof Map<?, ?> object)) return null;
            value = object.get(key);
        }
        return value instanceof String zone ? zone : null;
    }

    // The SDK has no JSON dependency. Parse structure and escapes, rather than
    // matching a label-like string in annotations or nested status objects.
    private static final class Json {
        private final String input;
        private int pos;
        Json(String input) throws IOException {
            if (input.length() > 2 * 1024 * 1024) throw invalid();
            this.input = input;
        }
        Object parse() throws IOException {
            Object value = value(0);
            space();
            if (pos != input.length()) throw invalid();
            return value;
        }
        private Object value(int depth) throws IOException {
            if (depth > 64) throw invalid();
            space();
            if (pos == input.length()) throw invalid();
            char c = input.charAt(pos);
            if (c == '"') return string();
            if (c == '{') {
                pos++;
                Map<String, Object> object = new HashMap<>();
                if (take('}')) return object;
                do {
                    space();
                    String key = string();
                    if (!take(':') || object.containsKey(key)) throw invalid();
                    object.put(key, value(depth + 1));
                } while (take(','));
                if (!take('}')) throw invalid();
                return object;
            }
            if (c == '[') {
                pos++;
                if (!take(']')) {
                    do { value(depth + 1); } while (take(','));
                    if (!take(']')) throw invalid();
                }
                return null;
            }
            int start = pos;
            while (pos < input.length() && ",]} \t\r\n".indexOf(input.charAt(pos)) < 0) pos++;
            String literal = input.substring(start, pos);
            if (!literal.equals("true") && !literal.equals("false") && !literal.equals("null")
                    && !literal.matches("-?(0|[1-9][0-9]*)(\\.[0-9]+)?([eE][+-]?[0-9]+)?")) throw invalid();
            return null;
        }
        private String string() throws IOException {
            if (pos == input.length() || input.charAt(pos++) != '"') throw invalid();
            StringBuilder result = new StringBuilder();
            while (pos < input.length()) {
                char c = input.charAt(pos++);
                if (c == '"') return result.toString();
                if (c < 0x20) throw invalid();
                if (c == '\\') {
                    if (pos == input.length()) throw invalid();
                    c = input.charAt(pos++);
                    switch (c) {
                        case '"', '\\', '/' -> result.append(c);
                        case 'b' -> result.append('\b');
                        case 'f' -> result.append('\f');
                        case 'n' -> result.append('\n');
                        case 'r' -> result.append('\r');
                        case 't' -> result.append('\t');
                        case 'u' -> {
                            if (input.length() - pos < 4) throw invalid();
                            int code = 0;
                            for (int i = 0; i < 4; i++) {
                                char hex = input.charAt(pos++);
                                int digit = "0123456789abcdef".indexOf(Character.toLowerCase(hex));
                                if (digit < 0) throw invalid();
                                code = code * 16 + digit;
                            }
                            result.append((char) code);
                        }
                        default -> throw invalid();
                    }
                } else result.append(c);
            }
            throw invalid();
        }
        private boolean take(char c) {
            space();
            if (pos < input.length() && input.charAt(pos) == c) { pos++; return true; }
            return false;
        }
        private void space() {
            while (pos < input.length() && " \t\r\n".indexOf(input.charAt(pos)) >= 0) pos++;
        }
        private static IOException invalid() { return new IOException("invalid Node JSON"); }
    }
}
