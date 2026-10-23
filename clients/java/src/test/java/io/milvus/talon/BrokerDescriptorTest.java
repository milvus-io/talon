package io.milvus.talon;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;

/** Shared descriptor contract and protocol ownership checks, with only the JDK. */
public final class BrokerDescriptorTest {
    private static final String PRIVATE_MARKER = "PRIVATE_FIXTURE_MARKER";
    private static final byte[] REQUEST = {
        0x54, 0x4c, 1, 1, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 1, 42
    };

    public static void main(String[] args) throws Exception {
        descriptorCopiesValidatedBytes();
        frameOwnsTraceNamespaceAndBroker();
        brokerMetadataBounds();
        nestedScopesRestoreAfterFailure();
        brokerTimeoutBudgets();
        System.out.println("Broker descriptor: immutable bytes, frame metadata and nested scope checks passed");
    }

    private static void descriptorCopiesValidatedBytes() {
        String json = " { \"request\": {\"token\":\"PRIVATE_FIXTURE_MARKER\"}, \"url\": \"https://broker\", \"origin\": {\"endpoint\":\"storage.example\"} } ";
        BrokerDescriptor descriptor = BrokerDescriptor.parse(json);
        byte[] original = json.getBytes(StandardCharsets.UTF_8);
        byte[] destination = new byte[descriptor.encodedLength() + 2];
        descriptor.writeTo(destination, 1);
        assertBytes(original, Arrays.copyOfRange(destination, 1, destination.length - 1), "original descriptor bytes");
        Arrays.fill(destination, (byte) 0);
        descriptor.writeTo(destination, 1);
        assertBytes(original, Arrays.copyOfRange(destination, 1, destination.length - 1), "descriptor remains immutable");
        if (descriptor.toString().contains(PRIVATE_MARKER)) throw new AssertionError("descriptor diagnostic leaked");
    }

    private static void frameOwnsTraceNamespaceAndBroker() {
        TraceContext parent = TraceContext.fromW3c("00-11111111111111111111111111111111-2222222222222222-00", "vendor=value");
        byte[] readId = new byte[16];
        Arrays.fill(readId, (byte) 3);
        String json = "{\"url\":\"https://broker\",\"request\":{},\"origin\":{\"endpoint\":\"storage.example\"}}";
        byte[] encoded = Frame.envelope(REQUEST, parent, readId, "tenant", BrokerDescriptor.parse(json));
        if (encoded[2] != 4) throw new AssertionError("Broker must require v4");
        assertBytes(parent.traceparent.getBytes(StandardCharsets.US_ASCII), property(encoded, 1), "traceparent");
        assertBytes("vendor=value".getBytes(StandardCharsets.US_ASCII), property(encoded, 2), "tracestate");
        assertBytes(readId, property(encoded, 3), "read id");
        assertBytes("tenant".getBytes(StandardCharsets.US_ASCII), property(encoded, 5), "namespace");
        assertBytes(json.getBytes(StandardCharsets.UTF_8), property(encoded, 6), "Broker");
        int business = 18 + (ByteBuffer.wrap(encoded).getShort(16) & 65535);
        assertBytes(new byte[] {42}, Arrays.copyOfRange(encoded, business, encoded.length), "business payload");
        if (Frame.decode(encoded).requestId() != 7 || Frame.decode(encoded).length() != encoded.length - 16) {
            throw new AssertionError("envelope header changed");
        }
        assertBytes(REQUEST, Frame.envelope(REQUEST, null, null, "default", null), "bare request");
        if (Frame.envelope(REQUEST, parent, null)[2] != 2
                || Frame.envelope(REQUEST, null, null, "tenant", null)[2] != 3) {
            throw new AssertionError("trace and namespace versions changed");
        }
    }

    private static void brokerMetadataBounds() {
        BrokerDescriptor broker = BrokerDescriptor.parse("{\"url\":\"https://broker\",\"request\":{},\"origin\":{\"endpoint\":\"storage.example\"}}");
        // Existing v2 metadata and business bytes must survive the v4 addition.
        byte[] existing = new byte[18 + 1024 + 1];
        System.arraycopy(REQUEST, 0, existing, 0, 16);
        existing[2] = 2;
        ByteBuffer.wrap(existing).putInt(12, existing.length - 16).putShort(16, (short) 1024);
        existing[18] = 99;
        ByteBuffer.wrap(existing).putShort(19, (short) 1021);
        Arrays.fill(existing, 21, existing.length - 1, (byte) 8);
        existing[existing.length - 1] = 42;
        byte[] encoded = Frame.envelope(existing, null, null, "default", broker);
        assertBytes(Arrays.copyOfRange(existing, 18, 1042), Arrays.copyOfRange(encoded, 18, 1042),
                "existing metadata at its size limit");
        assertBytes(new byte[] {42}, Arrays.copyOfRange(encoded, encoded.length - 1, encoded.length),
                "business after existing metadata");
        for (byte[] invalid : new byte[][] {encoded, Arrays.copyOf(existing, 16), Arrays.copyOf(existing, 17),
                Arrays.copyOf(existing, 100)}) {
            try {
                Frame.envelope(invalid, null, null, "default", broker);
                throw new AssertionError("accepted already enveloped or truncated request metadata");
            } catch (ProtocolException expected) { }
        }
        ByteBuffer.wrap(existing).putShort(16, (short) 1025);
        try {
            Frame.envelope(existing, null, null, "default", broker);
            throw new AssertionError("accepted non-Broker metadata over its size limit");
        } catch (ProtocolException expected) { }
    }

    private static void nestedScopesRestoreAfterFailure() throws IOException {
        Telemetry.configure(Set.of(), null);
        String json = "{\"url\":\"https://broker\",\"request\":{},\"origin\":{\"endpoint\":\"storage.example\"}}";
        RequestOptions options = RequestOptions.ROOT.withCredentialBroker(json).withNamespace("outer");
        RequestOptions.call(options, () -> {
            byte[] before = Frame.envelope(REQUEST, "worker");
            if (!RequestOptions.hasBroker() || RequestOptions.requestTimeoutMillis(5) != 12_705) {
                throw new AssertionError("cold Broker timeout scope lost");
            }
            for (RequestOptions nested : new RequestOptions[] {RequestOptions.INHERIT, options.withCredentialBroker(null)}) {
                try {
                    RequestOptions.call(nested, () -> {
                        if (RequestOptions.hasBroker() || property(Frame.envelope(REQUEST, "worker"), 6) != null) {
                            throw new AssertionError("Broker inherited after clear");
                        }
                        throw new IOException("fixture failure");
                    });
                } catch (IOException expected) {
                    assertBytes(before, Frame.envelope(REQUEST, "worker"), "outer scope restored after exception");
                }
            }
            return null;
        });
        if (RequestOptions.hasBroker() || RequestOptions.requestTimeoutMillis(5) != 5) {
            throw new AssertionError("Broker leaked outside scope");
        }
        assertBytes(REQUEST, Frame.envelope(REQUEST, "worker"), "scope cleared");
    }

    private static void brokerTimeoutBudgets() throws IOException {
        String[] timeouts = {"", ",\"request_timeout_ms\":null", ",\"request_timeout_ms\":123", ",\"request_timeout_ms\":18446744073709551615"};
        int[] expected = {42_700, 42_700, 31_192, Integer.MAX_VALUE};
        for (int i = 0; i < timeouts.length; i++) {
            RequestOptions options = RequestOptions.ROOT.withCredentialBroker("{\"url\":\"https://broker\",\"request\":{},\"origin\":{\"endpoint\":\"storage.example\"" + timeouts[i] + "}}");
            int actual = RequestOptions.call(options, () -> RequestOptions.requestTimeoutMillis(30_000));
            if (actual != expected[i]) throw new AssertionError("Broker budget: " + actual + " != " + expected[i]);
            int saturated = RequestOptions.call(options, () -> RequestOptions.requestTimeoutMillis(Integer.MAX_VALUE));
            if (saturated != Integer.MAX_VALUE) throw new AssertionError("Broker budget overflow");
        }
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

    private static void assertBytes(byte[] expected, byte[] actual, String label) {
        if (!Arrays.equals(expected, actual)) throw new AssertionError(label + " mismatch");
    }
}
