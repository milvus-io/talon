package io.milvus.talon;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

/** Real HTTPS Node lookup with a test CA, bearer token, and strict routing. */
public final class KubernetesZoneTest {
    public static void main(String[] args) throws Exception {
        parser();
        equal(KubernetesZone.resolve(k -> null, Path.of("nonexistent-credentials")), null);
        ZoneOptions explicit = ZoneOptions.fromEnvironment(
                Map.of("TALON_ZONE", "override", "TALON_ZONE_AFFINITY", "true")::get,
                () -> { throw new AssertionError("explicit zone queried Kubernetes"); });
        equal(explicit, new ZoneOptions("override", true));
        try {
            ZoneOptions.fromEnvironment(k -> k.equals("TALON_ZONE_AFFINITY") ? "typo" : null,
                    () -> { throw new AssertionError("invalid options queried Kubernetes"); });
            throw new AssertionError("invalid boolean accepted");
        } catch (IllegalArgumentException expected) { }

        Path dir = Files.createTempDirectory("talon-zone-test-");
        Path storePath = dir.resolve("server.p12");
        Process keytool = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
                "-genkeypair", "-alias", "server", "-keyalg", "RSA", "-storetype", "PKCS12",
                "-keystore", storePath.toString(), "-storepass", "changeit", "-keypass", "changeit",
                "-dname", "CN=localhost", "-ext", "SAN=DNS:localhost,IP:127.0.0.1", "-validity", "1")
                .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
        if (keytool.waitFor() != 0) throw new AssertionError("keytool failed");
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (var input = Files.newInputStream(storePath)) { store.load(input, "changeit".toCharArray()); }
        String pem = "-----BEGIN CERTIFICATE-----\n" + Base64.getMimeEncoder(64, new byte[] {'\n'})
                .encodeToString(store.getCertificate("server").getEncoded()) + "\n-----END CERTIFICATE-----\n";
        Files.writeString(dir.resolve("ca.crt"), pem);
        Files.writeString(dir.resolve("token"), "test-token\n");
        KeyManagerFactory keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keys.init(store, "changeit".toCharArray());
        SSLContext tls = SSLContext.getInstance("TLS");
        tls.init(keys.getKeyManagers(), null, null);
        HttpsServer server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(tls));
        var executor = Executors.newCachedThreadPool();
        server.setExecutor(executor);
        AtomicInteger hits = new AtomicInteger();
        AtomicInteger status = new AtomicInteger(200);
        AtomicInteger delay = new AtomicInteger();
        AtomicBoolean delayBody = new AtomicBoolean();
        AtomicReference<String> body = new AtomicReference<>(node("a"));
        AtomicReference<Throwable> serverFailure = new AtomicReference<>();
        server.createContext("/api/v1/nodes/node-1", exchange -> {
            try {
                equal(exchange.getRequestURI().getPath(), "/api/v1/nodes/node-1");
                equal(exchange.getRequestMethod(), "GET");
                equal(exchange.getRequestHeaders().getFirst("Authorization"), "Bearer test-token");
                hits.incrementAndGet();
                if (delay.get() > 0 && !delayBody.get()) Thread.sleep(delay.get());
                byte[] response = body.get().getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(status.get(), response.length);
                if (delay.get() > 0 && delayBody.get()) Thread.sleep(delay.get());
                exchange.getResponseBody().write(response);
            } catch (InterruptedException expected) {
                Thread.currentThread().interrupt();
            } catch (IOException expected) {
                if (delay.get() == 0) serverFailure.set(expected);
            } catch (Throwable error) { serverFailure.set(error); }
            finally { exchange.close(); }
        });
        server.start();
        Function<String, String> env = Map.of("KUBERNETES_SERVICE_HOST", "127.0.0.1",
                "KUBERNETES_SERVICE_PORT", Integer.toString(server.getAddress().getPort()),
                "TALON_NODE_NAME", "node-1", "TALON_ZONE_AFFINITY", "true")::get;
        try {
            ZoneOptions discovered = ZoneOptions.fromEnvironment(env, () -> KubernetesZone.resolve(env, dir));
            equal(discovered, new ZoneOptions("a", true));
            // Feed the discovered zone into a real coordinator/worker cluster.
            try (var cluster = new ZoneAffinityTest.Cluster(2, null);
                 var client = TalonClient.connect(ZoneAffinityTest.Cluster.address(cluster.coordinator), 64, 8, discovered)) {
                ZoneAffinityTest.assertBytes(client, 1);
                equal(cluster.remoteHits.get(), 0);
                equal(hits.get(), 1); // Reads never query the Kubernetes API.
            }
            for (int code : new int[] {403, 500, 302}) {
                status.set(code);
                equal(KubernetesZone.resolve(env, dir), null);
            }
            status.set(200);
            body.set("{malformed}");
            equal(KubernetesZone.resolve(env, dir), null);
            body.set("{\"metadata\":{\"labels\":{}}}");
            equal(KubernetesZone.resolve(env, dir), null);
            body.set(node("a"));
            Files.writeString(dir.resolve("ca.crt"), "invalid CA");
            int before = hits.get();
            ZoneOptions unknown = ZoneOptions.fromEnvironment(env, () -> KubernetesZone.resolve(env, dir));
            equal(unknown, new ZoneOptions(null, true));
            equal(hits.get(), before);
            try (var cluster = new ZoneAffinityTest.Cluster(2, null);
                 var client = TalonClient.connect(ZoneAffinityTest.Cluster.address(cluster.coordinator), 64, 8, unknown)) {
                ZoneAffinityTest.expectUnavailable(client);
                equal(cluster.localHits.get() + cluster.remoteHits.get(), 0);
            }
            Files.writeString(dir.resolve("ca.crt"), pem);
            // A syntactically valid but untrusted certificate must fail TLS too.
            KeyStore system = KeyStore.getInstance(KeyStore.getDefaultType());
            try (var input = Files.newInputStream(Path.of(System.getProperty("java.home"), "lib", "security", "cacerts"))) {
                system.load(input, "changeit".toCharArray());
            }
            Files.writeString(dir.resolve("ca.crt"), "-----BEGIN CERTIFICATE-----\n"
                    + Base64.getMimeEncoder().encodeToString(system.getCertificate(system.aliases().nextElement()).getEncoded())
                    + "\n-----END CERTIFICATE-----\n");
            equal(KubernetesZone.resolve(env, dir), null);
            equal(hits.get(), before);
            Files.writeString(dir.resolve("ca.crt"), pem);
            delay.set(6000);
            for (boolean bodyDelay : new boolean[] {false, true}) {
                delayBody.set(bodyDelay);
                long started = System.nanoTime();
                equal(KubernetesZone.resolve(env, dir), null);
                long elapsedMs = (System.nanoTime() - started) / 1_000_000;
                if (elapsedMs < 4000 || elapsedMs > 9000) throw new AssertionError("timeout: " + elapsedMs);
            }
            if (serverFailure.get() != null) throw new AssertionError(serverFailure.get());
        } finally {
            server.stop(0);
            executor.shutdownNow();
            try (var paths = Files.walk(dir)) {
                for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) Files.delete(path);
            }
        }
        System.out.println("Kubernetes zone: HTTPS, credentials, precedence, strict reads, errors and timeout passed");
    }

    private static String node(String zone) {
        return "{\"metadata\":{\"labels\":{\"topology.kubernetes.io/zone\":\"" + zone + "\"}},\"status\":{\"conditions\":[true,null,1.2e3]}}";
    }
    private static void parser() throws IOException {
        equal(KubernetesZone.parseZone(node("a")), "a");
        equal(KubernetesZone.parseZone(node("\\u0061")), "a");
        equal(KubernetesZone.parseZone("{\"other\":" + node("wrong") + ",\"metadata\":{\"labels\":{}}}"), null);
        equal(KubernetesZone.parseZone("{\"metadata\":{\"annotations\":{\"topology.kubernetes.io/zone\":\"wrong\"}}}"), null);
        for (String invalid : new String[] {"", node("a") + " trailing", "{\"a\":1,}", "{\"a\":01}",
                "{\"metadata\":null,\"metadata\":{}}", node("\\x"), "[".repeat(66) + "]".repeat(66)}) {
            try { KubernetesZone.parseZone(invalid); throw new AssertionError("invalid JSON accepted"); }
            catch (IOException expected) { }
        }
    }
    private static void equal(Object actual, Object expected) {
        if (!Objects.equals(actual, expected)) throw new AssertionError("expected " + expected + ", got " + actual);
    }
}
