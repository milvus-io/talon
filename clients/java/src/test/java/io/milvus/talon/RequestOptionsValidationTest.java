package io.milvus.talon;

import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Shared input-schema checks; descriptor JSON is never printed in diagnostics. */
public final class RequestOptionsValidationTest {
    private static final String PRIVATE_MARKER = "PRIVATE_FIXTURE_MARKER";
    // Fixed fixture envelope, not a parser for the descriptor under test. Hex preserves
    // malformed JSON, duplicate fields and byte-size boundaries in both consumers.
    private static final Pattern VECTOR = Pattern.compile(
            "\\{\"id\":\"([a-z0-9_.-]+)\",\"java_valid\":(true|false),\"rust_valid\":(true|false),"
                    + "\"compatibility\":\"([a-z0-9_]+)\",\"utf8_hex\":\"([0-9a-f]*)\"\\}");
    private static final byte[] REQUEST = {
        0x54, 0x4c, 1, 1, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 1, 42
    };

    public static void main(String[] args) throws Exception {
        // Unpaired raw Java UTF-16 has no valid UTF-8 representation for the corpus.
        for (char unit : new char[] {'\uD800', '\uDC00'}) {
            try {
                RequestOptions.ROOT.withCredentialBroker("{\"url\":\"https://broker\",\"request\":{\"x\":\"" + unit + "\"},\"origin\":{\"endpoint\":\"storage.example\"}}");
                throw new AssertionError("accepted malformed raw UTF-16 descriptor");
            } catch (IllegalArgumentException expected) {
                assertRedacted(expected, "java.raw_utf16");
            }
        }
        Path corpus = args.length == 0 ? Path.of("test/sdk/credential_broker_validity.jsonl") : Path.of(args[0]);
        Set<String> ids = new HashSet<>();
        int edges = 0;
        int lineNumber = 0;
        for (String line : Files.readAllLines(corpus, StandardCharsets.UTF_8)) {
            lineNumber++;
            Matcher vector = VECTOR.matcher(line);
            if (!vector.matches()) throw new AssertionError("invalid corpus record at line " + lineNumber);
            String id = vector.group(1);
            if (!ids.add(id)) throw new AssertionError("duplicate corpus id " + id);
            boolean expected = Boolean.parseBoolean(vector.group(2));
            boolean rustExpected = Boolean.parseBoolean(vector.group(3));
            String compatibility = vector.group(4);
            if (!Set.of("portable", "java_uri", "java_nesting_64", "rust_canonical_output_size", "rust_struct_sequence").contains(compatibility)
                    || compatibility.equals("portable") != (expected == rustExpected)) {
                throw new AssertionError("unlabeled codec expectation for " + id);
            }
            if (!compatibility.equals("portable")) edges++;
            String hex = vector.group(5);
            if ((hex.length() & 1) != 0) throw new AssertionError("invalid corpus hex for " + id);
            byte[] bytes = new byte[hex.length() / 2];
            for (int i = 0; i < bytes.length; i++) bytes[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
            String json = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
            try {
                BrokerDescriptor descriptor = BrokerDescriptor.parse(json);
                if (!expected) throw new AssertionError("accepted invalid corpus case " + id);
                byte[] encoded = Frame.envelope(REQUEST, null, null, "default", descriptor);
                assertBytes(bytes, property(encoded, 6), "descriptor bytes for " + id);
                if (descriptor.toString().contains(PRIVATE_MARKER)) throw new AssertionError("descriptor diagnostic leaked for " + id);
                // Namespace derivation must never influence validation or rewrite JSON.
                RequestOptions options = RequestOptions.ROOT.withCredentialBroker(json);
                for (String namespace : new String[] {"default", "tenant"}) {
                    RequestOptions.call(options.withNamespace(namespace).withParent(null), () -> {
                        byte[] scoped = Frame.envelope(REQUEST, "worker");
                        assertBytes(bytes, property(scoped, 6), "scoped descriptor for " + id);
                        assertBytes(namespace.equals("default") ? null : namespace.getBytes(StandardCharsets.US_ASCII),
                                property(scoped, 5), "namespace for " + id);
                        return null;
                    });
                }
            } catch (IllegalArgumentException error) {
                if (expected) throw new AssertionError("rejected valid corpus case " + id);
                assertRedacted(error, id);
                try {
                    RequestOptions.ROOT.withCredentialBroker(json);
                    throw new AssertionError("options accepted invalid corpus case " + id);
                } catch (IllegalArgumentException optionError) {
                    assertRedacted(optionError, id);
                }
            }
        }
        if (ids.isEmpty() || edges == 0) throw new AssertionError("empty corpus or missing codec edges");
        System.out.println("request option validation passed: " + ids.size() + " shared vectors, " + edges + " labeled codec edges; raw UTF-16 rejected");
    }

    private static byte[] property(byte[] frame, int key) {
        if (frame[2] == 1) return null;
        int end = 18 + (ByteBuffer.wrap(frame).getShort(16) & 65535);
        for (int offset = 18; offset < end;) {
            int length = ByteBuffer.wrap(frame).getShort(offset + 1) & 65535;
            if ((frame[offset] & 255) == key) return Arrays.copyOfRange(frame, offset + 3, offset + 3 + length);
            offset += 3 + length;
        }
        return null;
    }

    private static void assertRedacted(IllegalArgumentException error, String id) {
        if (error.toString().contains(PRIVATE_MARKER) || error.getCause() != null) {
            throw new AssertionError("validation diagnostic leaked for " + id);
        }
    }

    private static void assertBytes(byte[] expected, byte[] actual, String label) {
        if (!Arrays.equals(expected, actual)) throw new AssertionError(label + " mismatch");
    }
}
