package io.milvus.talon;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Reader and writer for the subset of bincode 1.3 the control plane uses.
 *
 * <p>This is not a general bincode library. It implements exactly the rules the
 * wire protocol reference specifies, and nothing else:
 *
 * <ul>
 *   <li>integers are fixed-width and <b>little-endian</b> — note this differs
 *       from the frame header, which is big-endian;
 *   <li>enum variants are a {@code u32} tag in declaration order starting at 0;
 *   <li>strings and sequences carry a {@code u64} length prefix, counting
 *       <b>bytes</b> for strings rather than characters;
 *   <li>an empty string or sequence is a {@code u64} zero followed by nothing —
 *       not an absent field and not a null.
 * </ul>
 *
 * <p>Lengths arrive as {@code u64} but Java arrays are indexed by {@code int}.
 * Rather than silently truncating, {@link #readLength} rejects anything that
 * would not fit, which also bounds allocation from a malformed or hostile peer.
 */
final class Bincode {

    private Bincode() {}

    /** Sequential reader over a bincode-encoded buffer. */
    static final class Reader {
        private final ByteBuffer buf;

        Reader(byte[] bytes) {
            this(bytes, 0, bytes.length);
        }

        Reader(byte[] bytes, int offset, int length) {
            this.buf = ByteBuffer.wrap(bytes, offset, length).order(ByteOrder.LITTLE_ENDIAN);
        }

        byte u8() {
            require(1);
            return buf.get();
        }

        int u16() {
            require(2);
            return buf.getShort() & 0xFFFF;
        }

        /** A {@code u32}, widened to {@code long} so values above 2^31 survive. */
        long u32() {
            require(4);
            return buf.getInt() & 0xFFFF_FFFFL;
        }

        /**
         * A {@code u64}. Returned as a signed {@code long}: values above 2^63
         * would wrap, but no field in this protocol legitimately reaches that
         * range, and treating one as negative is more visible than truncating.
         */
        long u64() {
            require(8);
            return buf.getLong();
        }

        boolean bool() {
            byte b = u8();
            if (b != 0 && b != 1) {
                throw new ProtocolException("bincode bool must be 0 or 1, got " + b);
            }
            return b == 1;
        }

        /** An enum variant tag. */
        int variant() {
            long tag = u32();
            if (tag > Integer.MAX_VALUE) {
                throw new ProtocolException("enum variant tag out of range: " + tag);
            }
            return (int) tag;
        }

        String string() {
            int len = readLength("string");
            require(len);
            byte[] bytes = new byte[len];
            buf.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }

        /** The element count of a sequence, validated as an array length. */
        int seqLen() {
            return readLength("sequence");
        }

        int remaining() {
            return buf.remaining();
        }

        /**
         * Read a {@code u64} length and validate it fits an {@code int} and the
         * remaining buffer, so a corrupt length cannot drive a huge allocation.
         */
        private int readLength(String what) {
            long len = u64();
            if (len < 0 || len > Integer.MAX_VALUE) {
                throw new ProtocolException(what + " length out of range: " + len);
            }
            if (len > buf.remaining()) {
                throw new ProtocolException(
                        what + " length " + len + " exceeds " + buf.remaining() + " remaining bytes");
            }
            return (int) len;
        }

        private void require(int n) {
            if (buf.remaining() < n) {
                throw new ProtocolException(
                        "truncated message: need " + n + " bytes, have " + buf.remaining());
            }
        }
    }

    /** Sequential writer producing a bincode-encoded buffer. */
    static final class Writer {
        private ByteBuffer buf = ByteBuffer.allocate(256).order(ByteOrder.LITTLE_ENDIAN);

        Writer u8(int v) {
            ensure(1);
            buf.put((byte) v);
            return this;
        }

        Writer u16(int v) {
            ensure(2);
            buf.putShort((short) v);
            return this;
        }

        Writer u32(long v) {
            ensure(4);
            buf.putInt((int) v);
            return this;
        }

        Writer u64(long v) {
            ensure(8);
            buf.putLong(v);
            return this;
        }

        /** An enum variant tag. */
        Writer variant(int tag) {
            return u32(tag);
        }

        Writer string(String s) {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            // The prefix counts bytes, not characters: a multi-byte key encodes
            // its UTF-8 length.
            u64(bytes.length);
            ensure(bytes.length);
            buf.put(bytes);
            return this;
        }

        Writer seqLen(int n) {
            return u64(n);
        }

        byte[] toBytes() {
            byte[] out = new byte[buf.position()];
            buf.duplicate().flip().get(out);
            return out;
        }

        private void ensure(int n) {
            if (buf.remaining() >= n) {
                return;
            }
            ByteBuffer bigger =
                    ByteBuffer.allocate(Math.max(buf.capacity() * 2, buf.position() + n))
                            .order(ByteOrder.LITTLE_ENDIAN);
            buf.flip();
            bigger.put(buf);
            buf = bigger;
        }
    }
}
