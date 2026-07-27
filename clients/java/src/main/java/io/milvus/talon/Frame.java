package io.milvus.talon;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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
    /** Protocol version this client speaks. */
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
        DELETE(5);

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

    private final MsgType type;
    private final int flags;
    private final int requestId;
    private final int length;

    public Frame(MsgType type, int flags, int requestId, int length) {
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
        b.put((byte) PROTOCOL_VERSION);
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
        if (version != PROTOCOL_VERSION) {
            throw new ProtocolException(
                    "unsupported protocol version " + version + "; this client speaks "
                            + PROTOCOL_VERSION);
        }
        MsgType type = MsgType.from(b.get() & 0xFF);
        int flags = b.getShort() & 0xFFFF;
        b.getShort(); // reserved
        int requestId = b.getInt();
        int length = b.getInt();
        if (length < 0) {
            throw new ProtocolException("payload length exceeds 2^31: " + (length & 0xFFFFFFFFL));
        }
        return new Frame(type, flags, requestId, length);
    }
}
