package io.milvus.talon;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * The 16-byte frame header that prefixes every message on both planes.
 *
 * <p><b>Big-endian</b>, unlike the bincode body that may follow it. Mixing the
 * two up is the first thing to check when a decoder produces nonsense.
 *
 * <pre>
 * offset  size  field
 *      0     2  magic 0x544C ("TL")
 *      2     1  protocol version
 *      3     1  message type
 *      4     2  flags
 *      6     2  reserved (zero on send, ignored on receive)
 *      8     4  request id
 *     12     4  payload length
 * </pre>
 */
public final class Frame {

    /** Header size in bytes. */
    public static final int HEADER_LEN = 16;
    /** Magic prefix, ASCII "TL". */
    public static final int MAGIC = 0x544C;
    /** Default protocol version; tracing uses v2, named namespaces v3 and credential properties v4. */
    public static final int PROTOCOL_VERSION = 1;

    /** Set when the body is an error message rather than a payload. */
    public static final int FLAG_ERROR = 0b10;
    /** Set on the final frame of a multi-frame response. */
    public static final int FLAG_END_OF_STREAM = 0b01;

    /** Message types. Values are wire-visible and must not be renumbered. */
    public enum MsgType {
        CONTROL(0),
        GET(1),
        GET_RANGE(2),
        PUT(3),
        PING(4),
        DELETE(5),
        GET_VERSIONED_RANGE(10),
        GET_VERSIONED_RANGE_NAMESPACE(11);

        final int value;

        MsgType(int value) {
            this.value = value;
        }

        static MsgType from(int value) {
            for (MsgType t : values()) {
                if (t.value == value) {
                    return t;
                }
            }
            throw new ProtocolException("unknown message type: " + value);
        }
    }

    private final int version;
    private final MsgType type;
    private final int flags;
    private final int requestId;
    private final int length;

    public Frame(MsgType type, int flags, int requestId, int length) {
        this(1, type, flags, requestId, length);
    }

    private Frame(int version, MsgType type, int flags, int requestId, int length) {
        this.version = version;
        this.type = type;
        this.flags = flags;
        this.requestId = requestId;
        this.length = length;
    }

    public MsgType type() {
        return type;
    }

    public int requestId() {
        return requestId;
    }

    /** Payload byte count. May legally be zero; that is not end-of-stream. */
    public int length() {
        return length;
    }

    public boolean isError() {
        return (flags & FLAG_ERROR) != 0;
    }

    public byte[] encode() {
        ByteBuffer b = ByteBuffer.allocate(HEADER_LEN).order(ByteOrder.BIG_ENDIAN);
        b.putShort((short) MAGIC);
        b.put((byte) version);
        b.put((byte) type.value);
        b.putShort((short) flags);
        b.putShort((short) 0); // reserved
        b.putInt(requestId);
        b.putInt(length);
        return b.array();
    }

    /**
     * Decode a header, validating magic and protocol version.
     *
     * <p>A wrong magic usually means the stream is desynchronised — for example
     * after a truncated payload — rather than that the peer sent a bad header.
     */
    public static Frame decode(byte[] bytes) {
        if (bytes.length < HEADER_LEN) {
            throw new ProtocolException(
                    "frame header truncated: need " + HEADER_LEN + " bytes, got " + bytes.length);
        }
        ByteBuffer b = ByteBuffer.wrap(bytes, 0, HEADER_LEN).order(ByteOrder.BIG_ENDIAN);
        int magic = b.getShort() & 0xFFFF;
        if (magic != MAGIC) {
            throw new ProtocolException(
                    String.format("bad frame magic 0x%04X (expected 0x%04X); stream desynchronised?",
                            magic, MAGIC));
        }
        int version = b.get() & 0xFF;
        if (version != 1 && version != 2 && version != 3 && version != 4) {
            throw new ProtocolException(
                    "unsupported protocol version " + version);
        }
        MsgType type = MsgType.from(b.get() & 0xFF);
        int flags = b.getShort() & 0xFFFF;
        b.getShort(); // reserved
        int requestId = b.getInt();
        int length = b.getInt();
        if (length < 0) {
            throw new ProtocolException("payload length exceeds 2^31: " + (length & 0xFFFFFFFFL));
        }
        return new Frame(version, type, flags, requestId, length);
    }

    /** Assemble request context in the protocol layer; credentials never depend on trace capability. */
    static byte[] envelope(byte[] frame, String endpoint) {
        Telemetry.CallContext current = Telemetry.currentContext();
        boolean trace = Telemetry.canSend(endpoint);
        return envelope(frame, trace ? current.parent : null, trace ? current.readId : null,
                current == null ? "default" : current.namespace, RequestOptions.currentBroker());
    }

    static byte[] envelope(byte[] frame, TraceContext parent, byte[] readId) {
        return traceEnvelope(frame, parent, readId, "default");
    }

    static byte[] envelope(byte[] frame, TraceContext parent, byte[] readId,
                           String namespace, BrokerDescriptor broker) {
        byte[] traced = traceEnvelope(frame, parent, readId, namespace);
        if (broker == null) return traced;
        Frame.decode(traced);
        int version = traced[2] & 255;
        if (version != 1 && version != 2 && version != 3) throw new ProtocolException("request already enveloped");
        // v4 keeps the existing trace and namespace TLVs and adds the broker property.
        if (version != 1 && traced.length < 18) throw new ProtocolException("request metadata truncated");
        int oldMetadata = version == 1 ? 0 : ByteBuffer.wrap(traced).getShort(16) & 65535;
        int business = version == 1 ? Frame.HEADER_LEN : 18 + oldMetadata;
        int metadata = oldMetadata + 3 + broker.encodedLength();
        if (business > traced.length || oldMetadata > 1024 || metadata > 1024 + 3 + BrokerDescriptor.MAX_BYTES) {
            throw new ProtocolException("request metadata exceeds size limit");
        }
        byte[] result = new byte[18 + metadata + traced.length - business];
        System.arraycopy(traced, 0, result, 0, Frame.HEADER_LEN);
        if (oldMetadata != 0) System.arraycopy(traced, 18, result, 18, oldMetadata);
        int offset = 18 + oldMetadata;
        result[offset] = 6;
        ByteBuffer.wrap(result).putShort(offset + 1, (short) broker.encodedLength());
        broker.writeTo(result, offset + 3);
        System.arraycopy(traced, business, result, 18 + metadata, traced.length - business);
        result[2] = 4;
        ByteBuffer.wrap(result).putInt(12, result.length - Frame.HEADER_LEN).putShort(16, (short) metadata);
        return result;
    }

    private static byte[] traceEnvelope(byte[] frame, TraceContext parent, byte[] readId, String namespace) {
        if (parent == null && namespace.equals("default")) return frame;
        ByteArrayOutputStream metadata = new ByteArrayOutputStream();
        if (parent != null) {
            tlv(metadata, 1, parent.traceparent.getBytes(StandardCharsets.US_ASCII));
            if (!parent.tracestate.isEmpty()) tlv(metadata, 2, parent.tracestate.getBytes(StandardCharsets.US_ASCII));
            if (readId != null) tlv(metadata, 3, readId);
        }
        boolean named = !namespace.equals("default");
        // v3 makes namespace routing mandatory: older peers must reject it, never ignore it.
        if (named) tlv(metadata, 5, namespace.getBytes(StandardCharsets.US_ASCII));
        byte[] values = metadata.toByteArray();
        byte[] result = new byte[frame.length + 2 + values.length];
        System.arraycopy(frame, 0, result, 0, Frame.HEADER_LEN);
        result[2] = (byte) (named ? 3 : 2);
        ByteBuffer.wrap(result).putInt(12, result.length - Frame.HEADER_LEN).putShort(16, (short) values.length);
        System.arraycopy(values, 0, result, 18, values.length);
        System.arraycopy(frame, 16, result, 18 + values.length, frame.length - 16);
        return result;
    }

    private static void tlv(ByteArrayOutputStream out, int key, byte[] value) {
        out.write(key); out.write(value.length >>> 8); out.write(value.length & 255); out.writeBytes(value);
    }
}
