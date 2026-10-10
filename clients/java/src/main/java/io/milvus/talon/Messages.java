package io.milvus.talon;

import java.util.ArrayList;
import java.util.List;

/**
 * Bincode message encoding and decoding for the control plane and versioned
 * data-plane reads.
 *
 * <p>Read and prewarm requests are implemented. Variant tags are the Rust enum's
 * declaration order and are wire-visible: inserting a variant renumbers
 * everything after it, which is a breaking change requiring a schema bump.
 * The values here are asserted against the conformance vectors, so a Rust-side
 * reordering fails a test rather than silently misrouting messages.
 */
final class Messages {

    /** The single control schema supported before the first deployment. */
    static final int CONTROL_SCHEMA_VERSION = 6;
    static final int MIN_CONTROL_SCHEMA_VERSION = CONTROL_SCHEMA_VERSION;

    // Variant tags, in Rust declaration order.
    static final int TAG_PLACEMENT_LOOKUP = 0;
    static final int TAG_PLACEMENT_RESPONSE = 1;
    static final int TAG_MEMBERSHIP_QUERY = 4;
    static final int TAG_MEMBERSHIP_LIST = 5;
    static final int TAG_ACK = 6;
    static final int TAG_STAT_OBJECT = 8;
    static final int TAG_OBJECT_STAT = 9;
    static final int TAG_LIST_OBJECTS = 10;
    static final int TAG_OBJECT_LIST = 11;

    static final int TAG_CONTROL_FAILURE = 18;
    static final int TAG_LOAD_BLOCK = 19;
    static final int TAG_BATCH_LOAD = 20;
    static final int TAG_BATCH_LOAD_RESULT = 21;
    static final int MAX_BATCH_LOAD_BLOCKS = 1024;
    static final int MAX_LOAD_BODY_BYTES = (1 << 20) - 1026;
    static final int BATCH_LOAD_OVERHEAD = 14;

    record LoadBlock(BlockId block, long length) {}

    record LoadBlockFailure(int index, String error) {}

    static List<LoadBlockFailure> loadFailures(Response response, int count) {
        int length = response.body.seqLen();
        if (length > count) throw new ProtocolException("too many batch failures");
        List<LoadBlockFailure> failures = new ArrayList<>(length);
        long previous = -1;
        for (int i = 0; i < length; i++) {
            long index = response.body.u32();
            String error = response.body.string();
            if (index <= previous || index >= count || error.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 256) {
                throw new ProtocolException("invalid batch failure entry");
            }
            failures.add(new LoadBlockFailure((int) index, error));
            previous = index;
        }
        if (response.body.remaining() != 0) throw new ProtocolException("trailing batch failure bytes");
        return failures;
    }

    static long loadBlockSize(LoadBlock request) {
        BlockId b = request.block();
        // Backend tag, three length-prefixed strings, offset, block size and length.
        return 48L + b.object().bucket().getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                + b.object().key().getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                + b.version().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
    }

    private static void writeLoadBlock(Bincode.Writer w, LoadBlock request) {
        BlockId b = request.block();
        writeObjectId(w, b.object());
        w.u64(b.offset()).u32(b.blockSize()).string(b.version()).u64(request.length());
    }

    static byte[] loadBlock(int requestId, LoadBlock request) {
        Bincode.Writer w = envelope(TAG_LOAD_BLOCK);
        writeLoadBlock(w, request);
        byte[] body = w.toBytes();
        if (body.length > MAX_LOAD_BODY_BYTES) throw new IllegalArgumentException("load frame too large");
        return framed(requestId, body);
    }

    static byte[] batchLoad(int requestId, List<LoadBlock> requests) {
        if (requests.isEmpty() || requests.size() > MAX_BATCH_LOAD_BLOCKS) {
            throw new IllegalArgumentException("batch load requires 1..1024 blocks");
        }
        long size = BATCH_LOAD_OVERHEAD;
        for (LoadBlock request : requests) size += loadBlockSize(request);
        if (size > MAX_LOAD_BODY_BYTES) throw new IllegalArgumentException("batch load frame too large");
        Bincode.Writer w = envelope(TAG_BATCH_LOAD).u64(requests.size());
        for (LoadBlock request : requests) writeLoadBlock(w, request);
        return framed(requestId, w.toBytes());
    }

    // Backend enum tags.
    static final int BACKEND_S3 = 0;
    static final int BACKEND_GCS = 1;
    static final int BACKEND_AZURE = 2;

    // NodeRole enum tags.
    static final int ROLE_COORDINATOR = 0;
    static final int ROLE_WORKER = 1;

    private Messages() {}

    private static Bincode.Writer envelope(int tag) {
        return new Bincode.Writer().u16(CONTROL_SCHEMA_VERSION).variant(tag);
    }

    record DiscoveredWorker(String id, String zone, int state, String instance, String address) {}
    record Discovery(long topology, long state, long validForMs, List<DiscoveredWorker> workers) {}
    static Discovery readDiscovery(Bincode.Reader r) {
        long topology = r.u64();
        long state = r.u64();
        long validity = r.u64();
        if (validity < 0) throw new ProtocolException("invalid discovery validity");
        int count = r.seqLen();
        List<DiscoveredWorker> workers = new ArrayList<>(count);
        java.util.Set<String> ids = new java.util.HashSet<>();
        for (int i = 0; i < count; i++) {
            String id = r.string();
            String zone = r.bool() ? r.string() : null;
            boolean retired = r.bool();
            int status = r.variant();
            if (status > 2 || retired || !ids.add(id)) throw new ProtocolException("invalid discovered member");
            String instance = status == 2 ? r.string() : null;
            String address = status == 2 ? r.string() : null;
            workers.add(new DiscoveredWorker(id, zone, status, instance, address));
        }
        if (r.remaining() != 0) throw new ProtocolException("trailing discovery bytes");
        return new Discovery(topology, state, Math.min(validity, 500), List.copyOf(workers));
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

    /** {@code VersionedRangeRequest { request: RangeRequest, version }}. */
    static byte[] versionedRange(
            int requestId, ObjectId object, long offset, long length, String version) {
        Bincode.Writer w = new Bincode.Writer();
        writeObjectId(w, object);
        w.u64(offset);
        w.u64(length);
        w.string(version);
        byte[] body = w.toBytes();
        byte[] header =
                new Frame(
                                Frame.MsgType.GET_VERSIONED_RANGE,
                                0,
                                requestId,
                                body.length)
                        .encode();
        byte[] out = new byte[header.length + body.length];
        System.arraycopy(header, 0, out, 0, header.length);
        System.arraycopy(body, 0, out, header.length, body.length);
        return out;
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
        if (schema != CONTROL_SCHEMA_VERSION) {
            throw new ProtocolException(
                    "server speaks control schema " + schema + "; this client requires "
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

    /** Project persistent membership to nodes while retaining unavailable owners. */
    static List<NodeInfo> readMembershipList(Bincode.Reader r) {
        r.u64(); // topology token
        r.u64(); // instance-state token
        r.u64(); // observation validity in milliseconds
        int n = r.seqLen();
        List<NodeInfo> nodes = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            String id = r.string();
            if (r.bool()) r.string(); // optional zone
            boolean retired = r.bool();
            int state = r.variant();
            String address = "";
            if (state == 2) {
                r.string(); // instance id
                address = r.string();
            } else if (state != 0 && state != 1) {
                throw new ProtocolException("unknown worker instance state: " + state);
            }
            if (!retired) nodes.add(new NodeInfo(id, address, true));
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
