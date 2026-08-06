package io.milvus.talon;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A client for reading objects through a Talon cache cluster.
 *
 * <p>Pure JVM: no native library, no JNI. The trade is that the wire protocol is
 * implemented twice — here and in Rust — so this client is validated against the
 * <a href="https://milvus-io.github.io/talon/reference/wire-protocol.html">
 * conformance vectors</a>, which are generated from the Rust implementation. A
 * change that alters the wire fails a test rather than silently breaking this.
 *
 * <p>Read-only in this release.
 *
 * <h2>Thread safety</h2>
 *
 * Instances are safe for concurrent use. Each call opens its own connection
 * rather than sharing one, so a slow read cannot block an unrelated one; the
 * placement cache is shared and synchronised.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * try (TalonClient client = TalonClient.connect("coordinator:7000", 8 << 20)) {
 *     byte[] data = client.read("az://container/dataset.parquet",
 *                               "0x8DABCDEF", 0, 1 << 20);
 * }
 * }</pre>
 */
public final class TalonClient implements AutoCloseable {

    /** Membership and placement freshness window. Matches the Rust default. */
    private static final long PLACEMENT_TTL_MS = 30_000;
    /** Workers retained from the local ranking. RF=1 in v1. */
    private static final int REPLICAS_K = 1;
    private static final int CONNECT_TIMEOUT_MS = 10_000;
    private static final int READ_TIMEOUT_MS = 30_000;

    private final String coordinator;
    private final int blockSize;
    private final AtomicInteger requestIds = new AtomicInteger(1);
    private final Map<BlockId, CachedPlacement> placementCache = new HashMap<>();
    private final Object membershipLock = new Object();
    private CachedMembership membership;

    private TalonClient(String coordinator, int blockSize) {
        this.coordinator = coordinator;
        this.blockSize = blockSize;
    }

    /**
     * Connect to a coordinator.
     *
     * @param blockSize must match the workers' configured block size; placement
     *     is per block, so a mismatch addresses blocks that do not exist
     */
    public static TalonClient connect(String coordinator, int blockSize) {
        if (blockSize <= 0) {
            throw new IllegalArgumentException("blockSize must be positive, got " + blockSize);
        }
        return new TalonClient(coordinator, blockSize);
    }

    /** Connect using the worker default block size of 256 MiB. */
    public static TalonClient connect(String coordinator) {
        return connect(coordinator, 256 << 20);
    }

    public String coordinator() {
        return coordinator;
    }

    /**
     * Read {@code length} bytes of {@code uri} starting at {@code offset}.
     *
     * <p>Ranges spanning block boundaries are split into per-block fetches and
     * reassembled in order, each benefiting independently from the placement
     * cache.
     *
     * <p>Resolves the object's version with a {@code stat} first. Use
     * {@link #read(String, String, long, long)} to supply a known version and
     * skip that round trip.
     */
    public byte[] read(String uri, long offset, long length) throws IOException {
        ObjectId object = ObjectId.parse(uri);
        ObjectStat stat = stat(object);
        return read(object, stat.version(), offset, Math.min(length, Math.max(0, stat.size() - offset)));
    }

    /**
     * Read with a known version, skipping the {@code stat} round trip.
     *
     * <p>Worth using when reading many ranges of one object: the version is
     * stable for an object generation, so re-resolving it per read is wasted
     * work.
     */
    public byte[] read(String uri, String version, long offset, long length) throws IOException {
        return read(ObjectId.parse(uri), version, offset, length);
    }

    /** As {@link #read(String, String, long, long)}, with a parsed object id. */
    public byte[] read(ObjectId object, String version, long offset, long length)
            throws IOException {
        if (offset < 0 || length < 0) {
            throw new IllegalArgumentException(
                    "offset and length must be non-negative, got offset=" + offset
                            + " length=" + length);
        }
        if (length == 0) {
            return new byte[0];
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream((int) Math.min(length, 1 << 20));
        for (Segment seg : planRead(object, version, offset, length)) {
            out.write(readBlock(seg));
        }
        return out.toByteArray();
    }

    /** Return an object's size and version. */
    public ObjectStat stat(String uri) throws IOException {
        return stat(ObjectId.parse(uri));
    }

    /** As {@link #stat(String)}, with a parsed object id. */
    public ObjectStat stat(ObjectId object) throws IOException {
        int id = requestIds.getAndIncrement();
        Messages.Response resp = controlRoundTrip(Messages.statObject(id, object));
        if (resp.tag == Messages.TAG_OBJECT_STAT) {
            long size = resp.body.u64();
            return new ObjectStat(size, resp.body.string());
        }
        throw unexpected("StatObject", resp);
    }

    /**
     * List objects beneath a mount-relative prefix.
     *
     * <p>The prefix names a backend and bucket ({@code az/container}),
     * optionally followed by a key prefix. Returned paths are mount-relative;
     * convert, for example, {@code az/container/key} to
     * {@code az://container/key} before passing it to {@link #read}.
     *
     * <p>The control protocol carries one bounded response. If a prefix exceeds
     * the server's object, page, or payload limit, the call fails explicitly
     * instead of returning an incomplete list; use a narrower prefix.
     */
    public List<ObjectEntry> list(String prefix) throws IOException {
        int id = requestIds.getAndIncrement();
        Messages.Response resp = controlRoundTrip(Messages.listObjects(id, prefix));
        if (resp.tag == Messages.TAG_OBJECT_LIST) {
            int n = resp.body.seqLen();
            List<ObjectEntry> entries = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                entries.add(new ObjectEntry(resp.body.string(), resp.body.u64()));
            }
            return entries;
        }
        throw unexpected("ListObjects", resp);
    }

    @Override
    public void close() {
        synchronized (placementCache) {
            placementCache.clear();
        }
    }

    // --- read planning -----------------------------------------------------

    /** One block-aligned piece of a read. */
    private static final class Segment {
        final BlockId block;
        final long offsetInBlock;
        final int length;

        Segment(BlockId block, long offsetInBlock, int length) {
            this.block = block;
            this.offsetInBlock = offsetInBlock;
            this.length = length;
        }
    }

    /**
     * Split a byte range into per-block segments.
     *
     * <p>The boundary arithmetic is where a client quietly corrupts data: an
     * off-by-one here produces a plausible-looking buffer with the wrong bytes
     * in the middle, so it is covered directly by tests.
     */
    private List<Segment> planRead(ObjectId object, String version, long offset, long length) {
        List<Segment> segments = new ArrayList<>();
        long remaining = length;
        long pos = offset;
        while (remaining > 0) {
            long blockStart = (pos / blockSize) * (long) blockSize;
            long offsetInBlock = pos - blockStart;
            long available = blockSize - offsetInBlock;
            int take = (int) Math.min(available, remaining);
            segments.add(
                    new Segment(
                            new BlockId(object, blockStart, blockSize, version),
                            offsetInBlock,
                            take));
            pos += take;
            remaining -= take;
        }
        return segments;
    }

    // --- placement ---------------------------------------------------------

    private static final class CachedPlacement {
        final List<String> addresses;
        final long expiresAtMs;

        CachedPlacement(List<String> addresses, long expiresAtMs) {
            this.addresses = addresses;
            this.expiresAtMs = expiresAtMs;
        }
    }

    private static final class CachedMembership {
        final Placement.Table placement;
        final String identity;
        final long expiresAtMs;

        CachedMembership(List<NodeInfo> nodes, String identity, long expiresAtMs) {
            this(new Placement.Table(nodes), identity, expiresAtMs);
        }

        CachedMembership(Placement.Table placement, String identity, long expiresAtMs) {
            this.placement = placement;
            this.identity = identity;
            this.expiresAtMs = expiresAtMs;
        }
    }

    /**
     * Fetch one segment, walking replicas on failure.
     *
     * <p>If every cached replica fails the entry is invalidated and refreshed
     * once before giving up, so a stale placement costs one retry rather than a
     * permanent error.
     */
    private byte[] readBlock(Segment seg) throws IOException {
        List<String> addresses = resolve(seg.block, false);
        IOException last = null;
        for (String address : addresses) {
            try {
                return fetchRange(address, seg);
            } catch (IOException e) {
                last = e;
            }
        }
        // Every replica failed: the placement may be stale rather than the
        // workers being down.
        addresses = resolve(seg.block, true);
        for (String address : addresses) {
            try {
                return fetchRange(address, seg);
            } catch (IOException e) {
                last = e;
            }
        }
        throw new IOException(
                "no replica served block " + seg.block + " after a placement refresh",
                last);
    }

    private List<String> resolve(BlockId block, boolean forceRefresh) throws IOException {
        long now = System.currentTimeMillis();
        if (!forceRefresh) {
            synchronized (placementCache) {
                CachedPlacement cached = placementCache.get(block);
                if (cached != null && cached.expiresAtMs > now) {
                    return cached.addresses;
                }
            }
        }

        CachedMembership members = membership(forceRefresh, now);
        List<NodeInfo> owners;
        if (REPLICAS_K == 1) {
            NodeInfo primary = members.placement.primary(block);
            owners = primary == null
                    ? java.util.Collections.emptyList()
                    : java.util.Collections.singletonList(primary);
        } else {
            owners = members.placement.rank(block, REPLICAS_K);
        }
        List<String> addresses = new ArrayList<>(owners.size());
        for (NodeInfo owner : owners) {
            addresses.add(owner.address());
        }
        if (addresses.isEmpty()) {
            throw new IOException("membership contains no worker for " + block);
        }

        synchronized (placementCache) {
            placementCache.put(
                    block,
                    new CachedPlacement(addresses, now + PLACEMENT_TTL_MS));
        }
        return addresses;
    }

    private CachedMembership membership(boolean forceRefresh, long now) throws IOException {
        synchronized (membershipLock) {
            if (!forceRefresh && membership != null && membership.expiresAtMs > now) {
                return membership;
            }

            List<NodeInfo> nodes;
            try {
                int id = requestIds.getAndIncrement();
                Messages.Response resp = controlRoundTrip(Messages.membershipQuery(id));
                if (resp.tag != Messages.TAG_MEMBERSHIP_LIST) {
                    throw unexpected("MembershipQuery", resp);
                }
                nodes = Messages.readMembershipList(resp.body);
            } catch (IOException refreshFailure) {
                if (membership != null) {
                    return membership;
                }
                throw refreshFailure;
            }

            String identity = membershipIdentity(nodes);
            boolean changed = membership != null && !membership.identity.equals(identity);
            if (membership != null && !changed) {
                membership =
                        new CachedMembership(
                                membership.placement, identity, now + PLACEMENT_TTL_MS);
                return membership;
            }
            if (changed) {
                synchronized (placementCache) {
                    placementCache.clear();
                }
            }
            membership =
                    new CachedMembership(List.copyOf(nodes), identity, now + PLACEMENT_TTL_MS);
            return membership;
        }
    }

    private static String membershipIdentity(List<NodeInfo> nodes) {
        List<String> fields = new ArrayList<>();
        for (NodeInfo node : nodes) {
            if (node.isWorker()) {
                fields.add(node.id().length() + ":" + node.id() + node.address().length() + ":"
                        + node.address());
            }
        }
        fields.sort(Comparator.naturalOrder());
        return String.join("|", fields);
    }

    // --- transport ---------------------------------------------------------

    private Messages.Response controlRoundTrip(byte[] request) throws IOException {
        try (Socket socket = dial(coordinator)) {
            OutputStream out = socket.getOutputStream();
            out.write(request);
            out.flush();

            Frame header = readHeader(socket.getInputStream());
            byte[] payload = readExactly(socket.getInputStream(), header.length());
            if (header.isError()) {
                throw new IOException(
                        "coordinator returned an error: " + new String(payload, java.nio.charset.StandardCharsets.UTF_8));
            }
            return Messages.decodeBody(payload);
        }
    }

    private byte[] fetchRange(String workerAddress, Segment seg) throws IOException {
        int id = requestIds.getAndIncrement();
        Bincode.Writer w = new Bincode.Writer();
        Messages.writeObjectId(w, seg.block.object());
        // The data-plane RangeRequest offset is absolute within the object.
        w.u64(seg.block.offset() + seg.offsetInBlock);
        w.u64(seg.length);
        byte[] body = w.toBytes();
        byte[] header = new Frame(Frame.MsgType.GET_RANGE, 0, id, body.length).encode();

        try (Socket socket = dial(workerAddress)) {
            OutputStream out = socket.getOutputStream();
            out.write(header);
            out.write(body);
            out.flush();

            InputStream in = socket.getInputStream();
            Frame response = readHeader(in);
            byte[] payload = readExactly(in, response.length());
            if (response.isError()) {
                throw new IOException(
                        "worker " + workerAddress + " returned: "
                                + new String(payload, java.nio.charset.StandardCharsets.UTF_8));
            }
            return payload;
        }
    }

    private Socket dial(String hostPort) throws IOException {
        int colon = hostPort.lastIndexOf(':');
        if (colon < 0) {
            throw new IOException("address is missing a port: " + hostPort);
        }
        String host = hostPort.substring(0, colon);
        int port = Integer.parseInt(hostPort.substring(colon + 1));
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
        socket.setSoTimeout(READ_TIMEOUT_MS);
        socket.setTcpNoDelay(true);
        return socket;
    }

    private static Frame readHeader(InputStream in) throws IOException {
        return Frame.decode(readExactly(in, Frame.HEADER_LEN));
    }

    /**
     * Read exactly {@code n} bytes.
     *
     * <p>A short read means the peer went away mid-frame. The connection is then
     * desynchronised — a response header promising N bytes cannot be retracted —
     * so this fails rather than attempting to resynchronise.
     */
    private static byte[] readExactly(InputStream in, int n) throws IOException {
        byte[] buf = new byte[n];
        if (n > 0) {
            new DataInputStream(in).readFully(buf);
        }
        return buf;
    }

    private IOException unexpected(String request, Messages.Response resp) {
        if (resp.tag == Messages.TAG_ACK) {
            String detail = Messages.readAckDetail(resp.body);
            if (detail != null) {
                return new IOException(request + " rejected: " + detail);
            }
        }
        return new IOException("unexpected reply to " + request + ": variant tag " + resp.tag);
    }
}
