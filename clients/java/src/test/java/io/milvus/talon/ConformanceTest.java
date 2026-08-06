package io.milvus.talon;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Validates this client's codec against the conformance vectors.
 *
 * <p>This is the test that makes a pure-JVM implementation defensible. The wire
 * protocol is implemented twice — here and in Rust — and the failure mode of
 * drift is subtle: a client that occasionally reads a stale version rather than
 * one that crashes. The vectors are generated from the Rust implementation, so
 * asserting against them turns "these agree today" into "a Rust change that
 * alters the wire fails a test".
 *
 * <p>Deliberately dependency-free: a hand-rolled JSON reader and assertions
 * rather than JUnit and Jackson, so the client jar has no test-only transitive
 * dependencies and the suite runs with nothing but a JDK.
 */
public final class ConformanceTest {

    private static int passed = 0;
    private static final List<String> failures = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        Path vectors = locateVectors(args);
        Map<String, byte[]> byName = parseVectors(Files.readString(vectors));
        System.out.println("loaded " + byName.size() + " vectors from " + vectors);

        frameHeaderDecodes(byName);
        frameHeaderEncodesIdentically(byName);
        zeroLengthPayloadIsNotEof(byName);
        placementResponseDecodes(byName);
        emptyOwnersDecodesAsEmptyList(byName);
        membershipListDecodes(byName);
        objectStatSurvivesValuesAbove2Pow32(byName);
        objectListPreservesMultiByteUtf8(byName);
        placementLookupEncodesIdentically(byName);
        membershipQueryEncodesIdentically(byName);
        localPlacementMatchesRust();
        statObjectEncodesIdentically(byName);
        listObjectsEncodesEmptyPrefix(byName);
        errorResponseIsFlaggedAndCarriesAMessage(byName);

        System.out.println();
        if (failures.isEmpty()) {
            System.out.println("conformance: " + passed + " passed");
            return;
        }
        System.out.println("conformance: " + passed + " passed, " + failures.size() + " FAILED");
        failures.forEach(f -> System.out.println("  " + f));
        System.exit(1);
    }

    // --- the checks --------------------------------------------------------

    private static void localPlacementMatchesRust() {
        check("client-side HRW ranking matches Rust", () -> {
            BlockId block =
                    new BlockId(
                            new ObjectId(
                                    ObjectId.Backend.S3,
                                    "datasets",
                                    "training/part-0001"),
                            268_435_456L,
                            256 << 20,
                            "etag-v7");
            List<NodeInfo> nodes =
                    Arrays.asList(
                            new NodeInfo("worker-a", "10.0.0.1:7001", true),
                            new NodeInfo("worker-b", "10.0.0.2:7001", true),
                            new NodeInfo("worker-c", "10.0.0.3:7001", true));
            List<NodeInfo> ranked = Placement.rank(block, nodes, 3);
            assertEquals("worker-b", ranked.get(0).id(), "primary");
            assertEquals("worker-a", ranked.get(1).id(), "secondary");
            assertEquals("worker-c", ranked.get(2).id(), "tertiary");
        });
    }

    /** A header decodes to the fields the generator encoded. */
    private static void frameHeaderDecodes(Map<String, byte[]> v) {
        check("frame_header decodes", () -> {
            Frame f = Frame.decode(v.get("frame_header.get_range"));
            assertEquals(Frame.MsgType.GET_RANGE, f.type(), "type");
            assertEquals(7, f.requestId(), "requestId");
            assertEquals(42, f.length(), "length");
            assertTrue(!f.isError(), "should not be flagged as an error");
        });
    }

    /** Encoding reproduces the reference bytes exactly. */
    private static void frameHeaderEncodesIdentically(Map<String, byte[]> v) {
        check("frame_header round-trips byte-exactly", () -> {
            byte[] expected = v.get("frame_header.get_range");
            byte[] actual = new Frame(Frame.MsgType.GET_RANGE, 0, 7, 42).encode();
            assertBytes(expected, actual);
        });
    }

    /**
     * A zero-length payload is legal. A decoder that treats it as EOF hangs or
     * drops a valid frame.
     */
    private static void zeroLengthPayloadIsNotEof(Map<String, byte[]> v) {
        check("zero-length payload is a valid frame", () -> {
            Frame f = Frame.decode(v.get("frame_header.zero_length"));
            assertEquals(0, f.length(), "length");
            assertEquals(Frame.MsgType.PING, f.type(), "type");
        });
    }

    private static void placementResponseDecodes(Map<String, byte[]> v) {
        check("PlacementResponse decodes owners and epoch", () -> {
            Messages.Response r = body(v.get("control.placement_response"));
            assertEquals(Messages.TAG_PLACEMENT_RESPONSE, r.tag, "variant tag");
            Placement p = Messages.readPlacementResponse(r.body);
            assertEquals(Arrays.asList("worker-a", "worker-b"), p.owners(), "owners");
            assertEquals(42L, p.epoch(), "epoch");
        });
    }

    /** An empty Vec is a u64 zero, not an absent field. */
    private static void emptyOwnersDecodesAsEmptyList(Map<String, byte[]> v) {
        check("empty owners decodes as an empty list", () -> {
            Messages.Response r = body(v.get("control.placement_response.empty_owners"));
            Placement p = Messages.readPlacementResponse(r.body);
            assertEquals(0, p.owners().size(), "owner count");
            assertEquals(0L, p.epoch(), "epoch");
        });
    }

    private static void membershipListDecodes(Map<String, byte[]> v) {
        check("MembershipList decodes node id, address, and role", () -> {
            Messages.Response r = body(v.get("control.membership_list"));
            assertEquals(Messages.TAG_MEMBERSHIP_LIST, r.tag, "variant tag");
            List<NodeInfo> nodes = Messages.readMembershipList(r.body);
            assertEquals(1, nodes.size(), "node count");
            assertEquals("worker-a", nodes.get(0).id(), "id");
            assertEquals("10.0.0.1:7001", nodes.get(0).address(), "address");
            assertTrue(nodes.get(0).isWorker(), "should be a worker");
        });
    }

    /**
     * The case a u32-reading decoder gets wrong silently: a size above 2^32
     * truncates rather than failing.
     */
    private static void objectStatSurvivesValuesAbove2Pow32(Map<String, byte[]> v) {
        check("ObjectStat size above 2^32 is not truncated", () -> {
            Messages.Response r = body(v.get("control.object_stat.large_size"));
            assertEquals(Messages.TAG_OBJECT_STAT, r.tag, "variant tag");
            long size = r.body.u64();
            assertEquals(5_000_000_000L, size, "size");
            assertEquals("0x8DABCDEF", r.body.string(), "version");
        });
    }

    /** Length prefixes count bytes; a char-counting decoder desynchronises here. */
    private static void objectListPreservesMultiByteUtf8(Map<String, byte[]> v) {
        check("ObjectList preserves multi-byte UTF-8 keys", () -> {
            Messages.Response r = body(v.get("control.object_list.utf8"));
            int n = r.body.seqLen();
            assertEquals(2, n, "entry count");
            String first = r.body.string();
            long firstSize = r.body.u64();
            assertEquals("az/container/数据/文件.parquet", first, "first path");
            assertEquals(1024L, firstSize, "first size");
            // Decoding the second entry proves the first consumed exactly the
            // right number of bytes.
            assertEquals("az/container/empty", r.body.string(), "second path");
            assertEquals(0L, r.body.u64(), "second size");
            assertEquals(0, r.body.remaining(), "trailing bytes");
        });
    }

    private static void placementLookupEncodesIdentically(Map<String, byte[]> v) {
        check("PlacementLookup encodes byte-exactly", () -> {
            BlockId block =
                    new BlockId(
                            new ObjectId(ObjectId.Backend.AZURE, "container", "path/to/object"),
                            268_435_456L,
                            256 << 20,
                            "v1");
            assertBytes(v.get("control.placement_lookup"), Messages.placementLookup(1, block, 1));
        });
    }

    /** A unit variant is the tag and nothing after it. */
    private static void membershipQueryEncodesIdentically(Map<String, byte[]> v) {
        check("MembershipQuery encodes byte-exactly", () ->
                assertBytes(v.get("control.membership_query"), Messages.membershipQuery(2)));
    }

    private static void statObjectEncodesIdentically(Map<String, byte[]> v) {
        check("StatObject encodes byte-exactly", () -> {
            ObjectId object =
                    new ObjectId(ObjectId.Backend.AZURE, "container", "path/to/object");
            assertBytes(v.get("control.stat_object"), Messages.statObject(3, object));
        });
    }

    /** An empty string is a u64 zero followed by nothing. */
    private static void listObjectsEncodesEmptyPrefix(Map<String, byte[]> v) {
        check("ListObjects encodes an empty prefix byte-exactly", () ->
                assertBytes(v.get("control.list_objects.empty_prefix"), Messages.listObjects(4, "")));
    }

    private static void errorResponseIsFlaggedAndCarriesAMessage(Map<String, byte[]> v) {
        check("error response sets the flag and carries UTF-8", () -> {
            byte[] bytes = v.get("data.error_response");
            Frame f = Frame.decode(bytes);
            assertTrue(f.isError(), "ERROR flag should be set");
            String message =
                    new String(
                            Arrays.copyOfRange(bytes, Frame.HEADER_LEN, bytes.length),
                            StandardCharsets.UTF_8);
            assertEquals("worker is not ready", message, "error message");
        });
    }

    // --- harness -----------------------------------------------------------

    private static Messages.Response body(byte[] framed) {
        Frame header = Frame.decode(framed);
        byte[] payload = Arrays.copyOfRange(framed, Frame.HEADER_LEN, framed.length);
        assertEquals(header.length(), payload.length, "declared vs actual payload length");
        return Messages.decodeBody(payload);
    }

    private interface Check {
        void run();
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

    private static void assertEquals(Object expected, Object actual, String what) {
        if (!expected.equals(actual)) {
            throw new AssertionError(what + " expected " + expected + " but was " + actual);
        }
    }

    private static void assertTrue(boolean condition, String what) {
        if (!condition) {
            throw new AssertionError(what);
        }
    }

    private static void assertBytes(byte[] expected, byte[] actual) {
        if (!Arrays.equals(expected, actual)) {
            throw new AssertionError(
                    "bytes differ\n      expected: " + hex(expected) + "\n      actual:   " + hex(actual));
        }
    }

    private static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) {
            sb.append(String.format("%02x", x));
        }
        return sb.toString();
    }

    private static Path locateVectors(String[] args) {
        if (args.length > 0) {
            return Paths.get(args[0]);
        }
        // clients/java -> repository root
        return Paths.get("crates", "talon-transport", "tests", "conformance_vectors.json");
    }

    /**
     * Minimal reader for the vector file's fixed shape, so the client jar needs
     * no JSON dependency for its tests.
     */
    private static Map<String, byte[]> parseVectors(String json) throws IOException {
        Map<String, byte[]> out = new LinkedHashMap<>();
        int i = 0;
        while ((i = json.indexOf("\"name\":", i)) >= 0) {
            String name = quoted(json, json.indexOf('"', i + 7));
            int hexAt = json.indexOf("\"hex\":", i);
            if (hexAt < 0) {
                throw new IOException("vector " + name + " has no hex field");
            }
            String hex = quoted(json, json.indexOf('"', hexAt + 6));
            out.put(name, unhex(hex));
            i = hexAt;
        }
        if (out.isEmpty()) {
            throw new IOException("no vectors parsed; is the file the expected shape?");
        }
        return out;
    }

    private static String quoted(String s, int openQuote) {
        int end = s.indexOf('"', openQuote + 1);
        String raw = s.substring(openQuote + 1, end);
        // The generator emits plain ASCII names and hex, so no unescaping is
        // required beyond this.
        return raw;
    }

    private static byte[] unhex(String hex) {
        byte[] out = new byte[hex.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
    }
}
