package io.milvus.talon;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * End-to-end tests for the Java client against a real distributed Talon cluster
 * backed by MinIO.
 *
 * <p>Same dependency-free style as {@link E2ETest} (a hand-rolled {@code main}
 * with {@code check()/assertBytes()} instead of JUnit, so the client jar keeps
 * zero test-only transitive dependencies). Unlike {@code E2ETest}, this targets
 * the stack deployed by {@code test/stack/deploy.sh}: 3 HA coordinators, workers,
 * and a real MinIO origin seeded with a deterministic {@code i % 251} ramp object.
 * The version is resolved automatically on each read rather than hard-coded,
 * because the MinIO ETag is content-derived and must not be pinned.
 *
 * <p>Expects a coordinator address as {@code args[0]} and a block size as
 * {@code args[1]}. The harness in {@code test/sdk/java/run.sh} compiles and runs
 * this against the deployed stack.
 */
public final class MinioE2ETest {

    private static int passed = 0;
    private static final List<String> failures = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        String coordinator = args.length > 0 ? args[0] : "127.0.0.1:17000";
        int blockSize = args.length > 1 ? Integer.parseInt(args[1]) : (8 << 20);
        String bucket = args.length > 2 ? args[2]
                : envOr("TALON_E2E_BUCKET", "talon-e2e");
        String key = args.length > 3 ? args[3] : envOr("TALON_E2E_KEY", "bench");
        String uri = "s3://" + bucket + "/" + key;

        try (TalonClient client = TalonClient.connect(coordinator, blockSize)) {
            check("stat returns size and version", () -> {
                ObjectStat s = client.stat(uri);
                if (s.version() == null || s.version().isEmpty()) {
                    throw new AssertionError("version should be the MinIO ETag, was "
                            + s.version());
                }
                if (s.size() <= 0) {
                    throw new AssertionError("size should be positive, was " + s.size());
                }
            });

            // The common case after #318: no version supplied, resolved via stat.
            check("read resolves the version automatically", () -> {
                byte[] got = client.read(uri, 0, 4096);
                assertBytes(ramp(0, 4096), got);
            });

            check("reads exact bytes at offset 0", () -> {
                byte[] got = client.read(uri, 0, 4096);
                assertBytes(ramp(0, 4096), got);
            });

            check("reads exact bytes at a non-zero offset", () -> {
                byte[] got = client.read(uri, 1000, 8192);
                assertBytes(ramp(1000, 8192), got);
            });

            // Wrong boundary arithmetic yields a plausible-looking buffer with
            // the wrong bytes in the middle, so assert content and not length.
            check("reassembles a range spanning block boundaries", () -> {
                int length = blockSize + (4 << 20);
                byte[] got = client.read(uri, 0, length);
                assertBytes(ramp(0, length), got);
            });

            check("reads across exactly one block edge", () -> {
                long offset = blockSize - 2048;
                byte[] got = client.read(uri, offset, 4096);
                assertBytes(ramp(offset, 4096), got);
            });

            check("zero-length read returns empty", () -> {
                byte[] got = client.read(uri, 0, 0);
                assertEquals(0, got.length, "length");
            });

            check("placement cache serves a repeated read", () -> {
                byte[] first = client.read(uri, 0, 4096);
                byte[] second = client.read(uri, 0, 4096);
                assertBytes(first, second);
            });

            check("concurrent reads from threads are correct", () -> {
                int n = 8;
                byte[][] results = new byte[n][];
                Throwable[] errors = new Throwable[n];
                List<Thread> threads = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    final int idx = i;
                    Thread t = new Thread(() -> {
                        try {
                            results[idx] = client.read(uri, idx * 65536L, 65536);
                        } catch (Throwable e) {
                            errors[idx] = e;
                        }
                    });
                    threads.add(t);
                    t.start();
                }
                for (Thread t : threads) {
                    t.join();
                }
                for (int i = 0; i < n; i++) {
                    if (errors[i] != null) {
                        throw new AssertionError("thread " + i + " failed: " + errors[i]);
                    }
                    assertBytes(ramp(i * 65536L, 65536), results[i]);
                }
            });

            check("malformed URIs are rejected before any I/O", () -> {
                for (String bad : Arrays.asList(
                        "no-scheme", "ftp://bucket/key", "s3://bucket", "s3:///key")) {
                    try {
                        client.read(bad, 0, 1);
                        throw new AssertionError("should have rejected " + bad);
                    } catch (IllegalArgumentException expected) {
                        if (expected.getMessage() == null || expected.getMessage().isEmpty()) {
                            throw new AssertionError("empty error message for " + bad);
                        }
                    }
                }
            });
        }

        System.out.println();
        if (failures.isEmpty()) {
            System.out.println("minio e2e: " + passed + " passed");
            return;
        }
        System.out.println("minio e2e: " + passed + " passed, " + failures.size() + " FAILED");
        failures.forEach(f -> System.out.println("  " + f));
        System.exit(1);
    }

    /** The deterministic bytes the MinIO seed object contains (i % 251). */
    private static byte[] ramp(long start, int length) {
        byte[] out = new byte[length];
        for (int i = 0; i < length; i++) {
            out[i] = (byte) ((start + i) % 251);
        }
        return out;
    }

    private interface Check {
        void run() throws Exception;
    }

    private static String envOr(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isEmpty() ? fallback : value;
    }

    private static void check(String name, Check c) {
        try {
            c.run();
            passed++;
            System.out.println("  ok   " + name);
        } catch (Throwable t) {
            failures.add(name + ": " + t.getMessage());
            System.out.println("  FAIL " + name + ": " + t.getMessage());
        }
    }

    private static void assertEquals(long expected, long actual, String what) {
        if (expected != actual) {
            throw new AssertionError(what + " expected " + expected + " but was " + actual);
        }
    }

    private static void assertBytes(byte[] expected, byte[] actual) {
        if (expected.length != actual.length) {
            throw new AssertionError(
                    "length expected " + expected.length + " but was " + actual.length);
        }
        for (int i = 0; i < expected.length; i++) {
            if (expected[i] != actual[i]) {
                throw new AssertionError(
                        "bytes differ at index " + i + ": expected " + expected[i]
                                + " but was " + actual[i]);
            }
        }
    }
}
