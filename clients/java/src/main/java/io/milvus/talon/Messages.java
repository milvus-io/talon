package io.milvus.talon;

import java.util.ArrayList;
import java.util.List;

/**
 * Control-plane message encoding and decoding.
 *
 * <p>Only the read path is implemented. Variant tags are the Rust enum's
 * declaration order and are wire-visible: inserting a variant renumbers
 * everything after it, which is a breaking change requiring a schema bump.
 * The values here are asserted against the conformance vectors, so a Rust-side
 * reordering fails a test rather than silently misrouting messages.
 */
final class Messages {

    /** Newest control schema this client understands. */
    static final int CONTROL_SCHEMA_VERSION = 2;
    /** Oldest schema the protocol defines. */
    static final int MIN_CONTROL_SCHEMA_VERSION = 1;

    // Variant tags, in Rust declaration order.
    static final int TAG_PLACEMENT_LOOKUP = 2;
    static final int TAG_PLACEMENT_RESPONSE = 3;
    static final int TAG_MEMBERSHIP_QUERY = 6;
    static final int TAG_MEMBERSHIP_LIST = 7;
    static final int TAG_ACK = 8;
    static final int TAG_STAT_OBJECT = 10;
    static final int TAG_OBJECT_STAT = 11;
    static final int TAG_LIST_OBJECTS = 12;
    static final int TAG_OBJECT_LIST = 13;

    // Backend enum tags.
    static final int BACKEND_S3 = 0;
    static final int BACKEND_GCS = 1;
    static final int BACKEND_AZURE = 2;

    // NodeRole enum tags.
    static final int ROLE_COORDINATOR = 0;
    static final int ROLE_WORKER = 1;

    private Messages() {}

    /**
     * Write the {@code Envelope { schema, message }} prefix.
     *
     * <p>The schema field is the <b>minimum</b> version that can represent this
     * message, not the newest the client speaks. Sending the newest would make a
     * peer running an older schema reject requests it could actually have
     * served — the field exists so a receiver can decide whether it understands
     * the message, so it must describe the message rather than the sender.
     */
    private static Bincode.Writer envelope(int tag) {
        return new Bincode.Writer().u16(minimumSchema(tag)).variant(tag);
    }

    /**
     * The oldest schema that can represent a message.
     *
     * <p>{@code StatObject}, {@code ObjectStat}, {@code ListObjects}, and
     * {@code ObjectList} were added in schema 2; everything else on the read
     * path predates it.
     */
    static int minimumSchema(int tag) {
        switch (tag) {
            case TAG_STAT_OBJECT:
            case TAG_OBJECT_STAT:
            case TAG_LIST_OBJECTS:
            case TAG_OBJECT_LIST:
                return 2;
            default:
                return MIN_CONTROL_SCHEMA_VERSION;
        }
    }

    /** Wrap a bincode body in a Control frame. */
    private static byte[] framed(int requestId, byte[] body) {
        byte[] header =
                new Frame(Frame.MsgType.CONTROL, 0, requestId, body.length).encode();
        byte[] out = new byte[header.length + body.length];
        System.arraycopy(header, 0, out, 0, header.length);
        System.arraycopy(body, 0, out, header.length, body.length);
        return out;
    }

    static void writeObjectId(Bincode.Writer w, ObjectId object) {
        w.variant(backendTag(object.backend()));
        w.string(object.bucket());
        w.string(object.key());
    }

    static int backendTag(ObjectId.Backend backend) {
        switch (backend) {
            case S3:
                return BACKEND_S3;
            case GCS:
                return BACKEND_GCS;
            case AZURE:
                return BACKEND_AZURE;
            default:
                throw new ProtocolException("unhandled backend: " + backend);
        }
    }

    static ObjectId.Backend backendFrom(int tag) {
        switch (tag) {
            case BACKEND_S3:
                return ObjectId.Backend.S3;
            case BACKEND_GCS:
                return ObjectId.Backend.GCS;
            case BACKEND_AZURE:
                return ObjectId.Backend.AZURE;
            default:
                throw new ProtocolException("unknown backend tag: " + tag);
        }
    }

    /** {@code PlacementLookup { block, k }} */
    static byte[] placementLookup(int requestId, BlockId block, int k) {
        Bincode.Writer w = envelope(TAG_PLACEMENT_LOOKUP);
        writeObjectId(w, block.object());
        w.u64(block.offset());
        w.u32(block.blockSize());
        w.string(block.version());
        w.u8(k);
        return framed(requestId, w.toBytes());
    }

    /** {@code MembershipQuery {}} — a unit variant: the tag and nothing else. */
    static byte[] membershipQuery(int requestId) {
        return framed(requestId, envelope(TAG_MEMBERSHIP_QUERY).toBytes());
    }

    /** {@code StatObject { object }} */
    static byte[] statObject(int requestId, ObjectId object) {
        Bincode.Writer w = envelope(TAG_STAT_OBJECT);
        writeObjectId(w, object);
        return framed(requestId, w.toBytes());
    }

    /** {@code ListObjects { prefix }} */
    static byte[] listObjects(int requestId, String prefix) {
        Bincode.Writer w = envelope(TAG_LIST_OBJECTS);
        w.string(prefix);
        return framed(requestId, w.toBytes());
    }

    /** A decoded control response. */
    static final class Response {
        final int tag;
        final Bincode.Reader body;

        Response(int tag, Bincode.Reader body) {
            this.tag = tag;
            this.body = body;
        }
    }

    /**
     * Decode a control response body, validating the schema before trusting it.
     *
     * <p>A schema the client cannot decode is rejected rather than
     * misinterpreted — reading a newer layout with older rules yields plausible
     * garbage, which is worse than a clear failure.
     */
    static Response decodeBody(byte[] payload) {
        Bincode.Reader r = new Bincode.Reader(payload);
        int schema = r.u16();
        if (schema > CONTROL_SCHEMA_VERSION) {
            throw new ProtocolException(
                    "server speaks control schema " + schema + "; this client understands at most "
                            + CONTROL_SCHEMA_VERSION + " — upgrade the client");
        }
        return new Response(r.variant(), r);
    }

    /** {@code PlacementResponse { owners, epoch }} */
    static Placement readPlacementResponse(Bincode.Reader r) {
        int n = r.seqLen();
        List<String> owners = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            owners.add(r.string());
        }
        long epoch = r.u64();
        return new Placement(owners, epoch);
    }

    /** {@code MembershipList { nodes }} */
    static List<NodeInfo> readMembershipList(Bincode.Reader r) {
        int n = r.seqLen();
        List<NodeInfo> nodes = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            String id = r.string();
            String address = r.string();
            int role = r.variant();
            nodes.add(new NodeInfo(id, address, role == ROLE_WORKER));
        }
        return nodes;
    }

    /** {@code Ack { ok, detail }} — {@code detail} is an {@code Option<String>}. */
    static String readAckDetail(Bincode.Reader r) {
        boolean ok = r.bool();
        boolean hasDetail = r.bool();
        String detail = hasDetail ? r.string() : null;
        return ok ? null : (detail == null ? "request rejected" : detail);
    }
}
