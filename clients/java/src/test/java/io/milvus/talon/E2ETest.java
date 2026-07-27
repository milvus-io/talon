package io.milvus.talon;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * End-to-end tests against a live cluster.
 *
 * <p>Conformance vectors prove the codec matches Rust; these prove the client
 * actually reads bytes. Both are needed — an encoder can be byte-perfect and
 * still fail at block-boundary arithmetic or replica resolution.
 *
 * <p>Expects a coordinator address as {@code args[0]}, a block size as
 * {@code args[1]}, and a version as {@code args[2]}. The harness in
 * {@code scripts/java_client_e2e.sh} starts the cluster.
 */
public final class E2ETest {

    private static int passed = 0;
    private static final List<String> failures = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        String coordinator = args.length > 0 ? args[0] : "127.0.0.1:17600";
        int blockSize = args.length > 1 ? Integer.parseInt(args[1]) : (8 << 20);
        String version = args.length > 2 ? args[2] : "0x8LOADTEST";
        String uri = "az://container/bench";

        try (TalonClient client = TalonClient.connect(coordinator, blockSize)) {
            check("stat returns size and version", () -> {
                ObjectStat s = client.stat(uri);
                assertEquals(version.length(), s.version().length(), "version length");
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
                byte[] got = client.read(uri, version, 0, 4096);
                assertBytes(ramp(0, 4096), got);
            });

            check("reads exact bytes at a non-zero offset", () -> {
                byte[] got = client.read(uri, version, 1000, 8192);
                assertBytes(ramp(1000, 8192), got);
            });

            // Wrong boundary arithmetic yields a plausible-looking buffer with
            // the wrong bytes in the middle, so assert content and not length.
            check("reassembles a range spanning block boundaries", () -> {
                int length = blockSize + (4 << 20);
                byte[] got = client.read(uri, version, 0, length);
                assertBytes(ramp(0, length), got);
            });

            check("reads across exactly one block edge", () -> {
                long offset = blockSize - 2048;
                byte[] got = client.read(uri, version, offset, 4096);
                assertBytes(ramp(offset, 4096), got);
            });

            check("zero-length read returns empty", () -> {
                byte[] got = client.read(uri, version, 0, 0);
                assertEquals(0, got.length, "length");
            });

            check("placement cache serves a repeated read", () -> {
                byte[] first = client.read(uri, version, 0, 4096);
                byte[] second = client.read(uri, version, 0, 4096);
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
                            results[idx] = client.read(uri, version, idx * 65536L, 65536);
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
                        "no-scheme", "ftp://bucket/key", "az://bucket", "az:///key", "az://bucket/")) {
                    try {
                        client.read(bad, version, 0, 1);
                        throw new AssertionError("should have rejected " + bad);
                    } catch (IllegalArgumentException expected) {
                        // The message must name the problem, not just fail.
                        if (expected.getMessage() == null || expected.getMessage().isEmpty()) {
                            throw new AssertionError("empty error message for " + bad);
                        }
                    }
                }
            });
        }

        System.out.println();
        if (failures.isEmpty()) {
            System.out.println("e2e: " + passed + " passed");
            return;
        }
        System.out.println("e2e: " + passed + " passed, " + failures.size() + " FAILED");
        failures.forEach(f -> System.out.println("  " + f));
        System.exit(1);
    }

    /** The deterministic bytes the test origin serves. */
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
