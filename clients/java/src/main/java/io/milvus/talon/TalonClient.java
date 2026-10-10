package io.milvus.talon;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

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
 * Instances are safe for concurrent use. Each call exclusively checks out a
 * connection, so a slow read cannot block an unrelated one. Idle connections
 * are reused; their limit does not constrain concurrent requests.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * try (TalonClient client = TalonClient.connect("coordinator:7000", 8 << 20)) {
 *     byte[] data = client.read("az://container/dataset.parquet",
 *                               "0x8DABCDEF", 64L << 20, 0, 1 << 20);
 * }
 * }</pre>
 */
public final class TalonClient implements AutoCloseable {

    private static final int CONNECT_TIMEOUT_MS = 10_000;
    private static final int READ_TIMEOUT_MS = 30_000;

    private final String coordinator;
    private final int blockSize;
    private final ConnectionPool coordinatorPool;
    private final AtomicInteger requestIds = new AtomicInteger(1);
    private final ReentrantLock membershipLock = new ReentrantLock();
    private volatile CachedMembership membership;
    private Messages.Discovery discovery;
    private long discoveryExpires;
    private long nextDiscoveryAttempt;
    private final Map<String, ConnectionPool> instancePools = new HashMap<>();
    private final int maxIdlePerAddr;
    private final java.util.concurrent.Semaphore loadSlots = new java.util.concurrent.Semaphore(8);

    private TalonClient(String coordinator, int blockSize, int maxIdlePerAddr) {
        this.maxIdlePerAddr = maxIdlePerAddr;
        this.coordinator = coordinator;
        this.blockSize = blockSize;
        this.coordinatorPool = new ConnectionPool(maxIdlePerAddr);
    }

    /**
     * Connect to a coordinator with a configurable idle connection limit.
     *
     * @param blockSize must match the workers' configured block size
     * @param maxIdlePerAddr positive maximum idle connections per address in each
     *     coordinator/worker pool; does not limit concurrent requests
     */
    public static TalonClient connect(String coordinator, int blockSize, int maxIdlePerAddr) {
        if (coordinator == null || coordinator.isBlank()) {
            throw new IllegalArgumentException("coordinator is required");
        }
        if (blockSize <= 0) {
            throw new IllegalArgumentException("blockSize must be positive, got " + blockSize);
        }
        if (maxIdlePerAddr <= 0) {
            throw new IllegalArgumentException("maxIdlePerAddr must be positive, got " + maxIdlePerAddr);
        }
        return new TalonClient(coordinator, blockSize, maxIdlePerAddr);
    }

    /**
     * Connect to a coordinator, retaining up to 8 idle connections per address.
     *
     * @param blockSize must match the workers' configured block size; placement
     *     is per block, so a mismatch addresses blocks that do not exist
     */
    public static TalonClient connect(String coordinator, int blockSize) {
        return connect(coordinator, blockSize, 8);
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
     * {@link #read(String, String, long, long, long)} to supply a known version and size and
     * skip that round trip.
     */
    public byte[] read(String uri, long offset, long length) throws IOException { return read(uri, offset, length, RequestOptions.INHERIT); }

    /** Explicit carrier; captured once on the caller thread before any network I/O. */
    public byte[] read(String uri, long offset, long length, RequestOptions options) throws IOException {
        return Telemetry.call(options == null ? RequestOptions.ROOT : options, () -> readInternal(uri, offset, length));
    }

    private byte[] readInternal(String uri, long offset, long length) throws IOException {
        ObjectId object = ObjectId.parse(uri);
        ObjectStat stat = stat(object);
        return readInternal(object, stat.version(), stat.size(), offset, length);
    }

    /** Read with a known source version and total size, without metadata HEAD. */
    public byte[] read(String uri, String version, long size, long offset, long length) throws IOException {
        return read(uri, version, size, offset, length, RequestOptions.INHERIT);
    }

    /** Explicit carrier for a read with known size and version. */
    public byte[] read(String uri, String version, long size, long offset, long length, RequestOptions options) throws IOException {
        return read(ObjectId.parse(uri), version, size, offset, length, options);
    }

    /** As {@link #read(String, String, long, long, long)}, with a parsed object id. */
    public byte[] read(ObjectId object, String version, long size, long offset, long length) throws IOException {
        return read(object, version, size, offset, length, RequestOptions.INHERIT);
    }

    /** Explicit carrier for a read with known size and version. */
    public byte[] read(ObjectId object, String version, long size, long offset, long length, RequestOptions options) throws IOException {
        return Telemetry.call(options == null ? RequestOptions.ROOT : options,
                () -> readInternal(object, version, size, offset, length));
    }

    private byte[] readInternal(ObjectId object, String version, long objectSize, long offset, long length) throws IOException {
        if (offset < 0 || length < 0) {
            throw new IllegalArgumentException(
                    "offset and length must be non-negative, got offset=" + offset
                            + " length=" + length);
        }
        if (objectSize < 0) throw new IllegalArgumentException("object size must be non-negative");
        length = Math.min(length, Math.max(0, objectSize - offset));
        if (length == 0) {
            return new byte[0];
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream((int) Math.min(length, 1 << 20));
        for (Segment seg : planRead(object, version, offset, length)) {
            out.write(readBlock(seg, objectSize));
        }
        return out.toByteArray();
    }

    /** Prewarm an exact source version without HEAD; completed fills survive failure. */
    public LoadResult load(String uri, String version, long size) throws IOException {
        return load(new LoadRequest(uri, version, size), RequestOptions.INHERIT);
    }

    /** Prewarm with a parsed request and inherited tracing. */
    public LoadResult load(LoadRequest request) throws IOException {
        return load(request, RequestOptions.INHERIT);
    }

    /** Prewarm a parsed object with explicit request tracing. */
    public LoadResult load(LoadRequest request, RequestOptions options) throws IOException {
        return Telemetry.call(options == null ? RequestOptions.ROOT : options,
                () -> loadFiles(List.of(request), false).get(0));
    }

    /** Batch by worker and frame size, with up to 1024 block instructions per RPC.
     * Results follow input order, including empty files and duplicates.
     * The operation is not atomic: failure may leave completed fills cached. */
    public List<LoadResult> batchLoad(List<LoadRequest> requests) throws IOException {
        return batchLoad(requests, RequestOptions.INHERIT);
    }

    /** Batch prewarm with explicit request tracing. */
    public List<LoadResult> batchLoad(List<LoadRequest> requests, RequestOptions options) throws IOException {
        List<LoadRequest> copy = List.copyOf(requests);
        return Telemetry.call(options == null ? RequestOptions.ROOT : options, () -> loadFiles(copy, true));
    }

    private record IndexedLoad(int index, Messages.LoadBlock block) {}

    private static final class LoadProgress {
        final LoadFailure[] failures;
        final long[] remaining;
        Throwable firstError;
        LoadProgress(List<LoadResult> results) {
            failures = new LoadFailure[results.size()];
            remaining = results.stream().mapToLong(LoadResult::blocks).toArray();
        }
        void fail(int index, boolean uncertain, String message) {
            if (failures[index] == null || (failures[index].uncertain() && !uncertain)) {
                failures[index] = new LoadFailure(index, uncertain, message);
            }
        }
        BatchLoadException exception() {
            return new BatchLoadException(java.util.Arrays.stream(failures).filter(java.util.Objects::nonNull).toList(), firstError);
        }
    }

    private void sendBatch(Placement.Table topology, List<IndexedLoad> assignments, long deadline, LoadProgress progress) {
        try {
            List<Messages.LoadBlockFailure> failures = sendLoad(topology, assignments.stream().map(IndexedLoad::block).toList(), true, deadline);
            for (Messages.LoadBlockFailure failure : failures) {
                progress.fail(assignments.get(failure.index()).index(), false, failure.error());
                if (progress.firstError == null) progress.firstError = new IOException(failure.error());
            }
        } catch (IOException | ProtocolException failure) {
            for (IndexedLoad assignment : assignments) progress.fail(assignment.index(), true, failure.toString());
            if (progress.firstError == null) progress.firstError = failure;
        }
        for (IndexedLoad assignment : assignments) progress.remaining[assignment.index()]--;
    }

    private List<LoadResult> loadFiles(List<LoadRequest> files, boolean batch) throws IOException {
        return loadFiles(files, batch, System.nanoTime() + TimeUnit.MINUTES.toNanos(30));
    }

    private List<LoadResult> loadFiles(List<LoadRequest> files, boolean batch, long deadline) throws IOException {
        List<LoadResult> results = new ArrayList<>(files.size());
        boolean nonempty = false;
        for (LoadRequest file : files) {
            long blocks = file.size() == 0 ? 0 : (file.size() - 1) / blockSize + 1;
            if (blocks > Long.MAX_VALUE / blockSize) throw new IllegalArgumentException("load extent overflows block addressing");
            if (blocks > 0 && Messages.BATCH_LOAD_OVERHEAD + Messages.loadBlockSize(
                    new Messages.LoadBlock(new BlockId(file.object(), 0, blockSize, file.version()), 1)) > Messages.MAX_LOAD_BODY_BYTES) {
                throw new IllegalArgumentException("load identity exceeds frame limit");
            }
            results.add(new LoadResult(file.size(), blocks));
            nonempty |= blocks > 0;
        }
        if (!nonempty) return List.copyOf(results);
        LoadProgress progress = new LoadProgress(results);
        Placement.Table topology = null;
        try {
            topology = membership(deadline).placement;
            // Bound pending planning across all workers independently of file sizes.
            Map<String, List<IndexedLoad>> groups = new java.util.LinkedHashMap<>();
            Map<String, Long> sizes = new HashMap<>();
            int pending = 0;
            long pendingBytes = 0;
            for (int fileIndex = 0; fileIndex < files.size(); fileIndex++) {
                LoadRequest file = files.get(fileIndex);
                for (long offset = 0; offset < file.size();) {
                    if (System.nanoTime() >= deadline) throw new TalonException(TalonException.Code.TIMEOUT, "load deadline exceeded");
                    int length = (int) Math.min(blockSize, file.size() - offset);
                    Messages.LoadBlock request = new Messages.LoadBlock(
                            new BlockId(file.object(), offset, blockSize, file.version()), length);
                    if (!batch) {
                        sendLoad(topology, List.of(request), false, deadline);
                    } else {
                        NodeInfo owner = topology.primary(request.block());
                        if (owner == null) throw new TalonException(TalonException.Code.UNAVAILABLE, "empty logical membership");
                        String key = owner.id();
                        List<IndexedLoad> group = groups.computeIfAbsent(key, ignored -> new ArrayList<>());
                        long bytes = sizes.getOrDefault(key, (long) Messages.BATCH_LOAD_OVERHEAD);
                        long size = Messages.loadBlockSize(request);
                        if (group.size() == Messages.MAX_BATCH_LOAD_BLOCKS || bytes + size > Messages.MAX_LOAD_BODY_BYTES) {
                            sendBatch(topology, group, deadline, progress);
                            pending -= group.size();
                            pendingBytes -= bytes - Messages.BATCH_LOAD_OVERHEAD;
                            group.clear();
                            bytes = Messages.BATCH_LOAD_OVERHEAD;
                        }
                        group.add(new IndexedLoad(fileIndex, request));
                        sizes.put(key, bytes + size);
                        pending++;
                        pendingBytes += size;
                        if (pending >= 8192 || pendingBytes >= 8L * Messages.MAX_LOAD_BODY_BYTES) {
                            for (List<IndexedLoad> window : groups.values()) sendBatch(topology, window, deadline, progress);
                            groups.clear(); sizes.clear(); pending = 0; pendingBytes = 0;
                        }
                    }
                    offset += length;
                }
            }
            for (List<IndexedLoad> group : groups.values()) sendBatch(topology, group, deadline, progress);
        } catch (IOException | ProtocolException failure) {
            if (!batch) throw failure;
            for (int i = 0; i < progress.remaining.length; i++) {
                if (progress.remaining[i] != 0) progress.fail(i, true, failure.toString());
            }
            if (progress.firstError == null) progress.firstError = failure;
        }
        // The immutable snapshot's volatile reference avoids waiting on another refresh
        // after a deadline interrupted dispatch. Check topology even on that failure path.
        if (topology != null && membership.placement != topology) {
            TalonException failure = new TalonException(TalonException.Code.UNAVAILABLE, "membership changed during load");
            if (!batch) throw failure;
            for (int i = 0; i < results.size(); i++) {
                if (results.get(i).blocks() != 0) progress.fail(i, true, failure.toString());
            }
            if (progress.firstError == null) progress.firstError = failure;
        }
        if (progress.firstError != null) throw progress.exception();
        return List.copyOf(results);
    }

    private List<Messages.LoadBlockFailure> sendLoad(Placement.Table topology, List<Messages.LoadBlock> blocks,
            boolean batch, long deadline) throws IOException {
        long remaining = deadline - System.nanoTime();
        try {
            if (remaining <= 0 || !loadSlots.tryAcquire(remaining, java.util.concurrent.TimeUnit.NANOSECONDS)) {
                throw new TalonException(TalonException.Code.TIMEOUT, "load deadline exceeded");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("load interrupted", interrupted);
        }
        try {
            membership(deadline);
            ConnectionPool pool;
            String address;
            long instanceDeadline;
            lockMembership(deadline);
            try {
                if (membership.placement != topology) throw new TalonException(TalonException.Code.UNAVAILABLE, "membership changed during load");
                NodeInfo owner = topology.primary(blocks.get(0).block());
                if (owner == null) throw new TalonException(TalonException.Code.UNAVAILABLE, "empty logical membership");
                Messages.DiscoveredWorker target = discovery.workers().stream()
                        .filter(w -> w.id().equals(owner.id())).findFirst().orElseThrow();
                if (target.state() != 2) throw new TalonException(TalonException.Code.UNAVAILABLE, "load owner offline or conflicting");
                address = target.address(); pool = instancePools.get(instanceKey(target)); instanceDeadline = discoveryExpires;
            } finally { membershipLock.unlock(); }
            int id = requestIds.getAndIncrement();
            byte[] request = batch ? Messages.batchLoad(id, blocks) : Messages.loadBlock(id, blocks.get(0));
            long rpcDeadline = Math.min(deadline, System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(batch ? 1800 : 120));
            ConnectionPool.checkRetryDeadline(instanceDeadline);
            return pool.exchange(address, instanceDeadline, rpcDeadline, socket -> {
                ConnectionPool.checkRetryDeadline(instanceDeadline);
                long left = rpcDeadline - System.nanoTime();
                if (left <= 0) throw new TalonException(TalonException.Code.TIMEOUT, "load deadline exceeded");
                socket.setSoTimeout((int) Math.max(1, java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(left)));
                try {
                    socket.getOutputStream().write(Telemetry.envelope(request, address));
                    socket.getOutputStream().flush();
                    Frame header = readHeader(socket.getInputStream());
                    if (header.isError() || header.type() != Frame.MsgType.CONTROL || header.requestId() != id || header.length() > (1 << 20)) {
                        throw new ProtocolException("invalid LOAD reply frame");
                    }
                    Messages.Response response = Messages.decodeBody(readExactly(socket.getInputStream(), header.length()));
                    if (batch && response.tag == Messages.TAG_BATCH_LOAD_RESULT) return Messages.loadFailures(response, blocks.size());
                    if (response.tag != Messages.TAG_ACK) throw unexpected("LOAD", response);
                    boolean ok = response.body.bool();
                    String detail = response.body.bool() ? response.body.string() : null;
                    if (response.body.remaining() != 0) throw new ProtocolException("trailing LOAD acknowledgement bytes");
                    if (!ok) throw new TalonException(TalonException.Code.UNKNOWN, "load rejected: " + detail);
                    return List.of();
                } finally { socket.setSoTimeout(READ_TIMEOUT_MS); }
            });
        } catch (IOException failure) {
            throw TalonException.transport(failure);
        } finally { loadSlots.release(); }
    }

    /** Return an object's size and version. */
    public ObjectStat stat(String uri) throws IOException { return stat(uri, RequestOptions.INHERIT); }

    /** Explicit carrier; captured once on the caller thread before any network I/O. */
    public ObjectStat stat(String uri, RequestOptions options) throws IOException {
        return Telemetry.call(options == null ? RequestOptions.ROOT : options, () -> statInternal(uri));
    }

    private ObjectStat statInternal(String uri) throws IOException {
        return stat(ObjectId.parse(uri));
    }

    /** As {@link #stat(String)}, with a parsed object id. */
    public ObjectStat stat(ObjectId object) throws IOException { return stat(object, RequestOptions.INHERIT); }

    /** Explicit carrier; captured once on the caller thread before any network I/O. */
    public ObjectStat stat(ObjectId object, RequestOptions options) throws IOException {
        return Telemetry.call(options == null ? RequestOptions.ROOT : options, () -> statInternal(object));
    }

    private ObjectStat statInternal(ObjectId object) throws IOException {
        int id = requestIds.getAndIncrement();
        return controlRoundTrip(Messages.statObject(id, object), resp -> {
            if (resp.tag == Messages.TAG_OBJECT_STAT) {
                long size = resp.body.u64();
                return new ObjectStat(size, resp.body.string());
            }
            throw unexpected("StatObject", resp);
        });
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
        return controlRoundTrip(Messages.listObjects(id, prefix), resp -> {
            if (resp.tag == Messages.TAG_OBJECT_LIST) {
                int n = resp.body.seqLen();
                List<ObjectEntry> entries = new ArrayList<>(n);
                for (int i = 0; i < n; i++) {
                    entries.add(new ObjectEntry(resp.body.string(), resp.body.u64()));
                }
                return entries;
            }
            throw unexpected("ListObjects", resp);
        });
    }

    @Override
    public void close() {
        membershipLock.lock();
        try {
            for (ConnectionPool pool : instancePools.values()) pool.close();
            instancePools.clear();
        } finally { membershipLock.unlock(); }
        coordinatorPool.close();
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

    private record CachedMembership(Placement.Table placement) {}

    /** Fetch from one logical owner, allowing one bounded stale-connection retry. */
    private byte[] readBlock(Segment seg, long objectSize) throws IOException {
        membership();
        ConnectionPool pool;
        String address;
        long retryDeadline;
        membershipLock.lock();
        try {
            NodeInfo owner = membership.placement.primary(seg.block);
            if (owner == null) throw new TalonException(TalonException.Code.UNAVAILABLE, "empty logical membership");
            if (System.nanoTime() >= discoveryExpires) throw new TalonException(TalonException.Code.UNAVAILABLE, "expired instance discovery");
            Messages.DiscoveredWorker target = discovery.workers().stream().filter(w -> w.id().equals(owner.id())).findFirst().orElseThrow();
            if (target.state() != 2) throw new TalonException(TalonException.Code.UNAVAILABLE, "worker " + owner.id() + " offline or conflicting");
            address = target.address();
            pool = instancePools.get(instanceKey(target));
            retryDeadline = discoveryExpires;
        } finally { membershipLock.unlock(); }
        try { return fetchRange(address, seg, pool, retryDeadline, objectSize); }
        catch (IOException failure) { throw TalonException.transport(failure); }
    }

    private CachedMembership membership() throws IOException {
        return membership(null);
    }

    private void lockMembership(Long deadline) throws IOException {
        if (deadline == null) {
            membershipLock.lock();
            return;
        }
        try {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0 || !membershipLock.tryLock(remaining, TimeUnit.NANOSECONDS)) {
                throw new TalonException(TalonException.Code.TIMEOUT, "load deadline exceeded waiting for discovery");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("load interrupted waiting for discovery", interrupted);
        }
    }

    private CachedMembership membership(Long deadline) throws IOException {
        lockMembership(deadline);
        try {
            if (discovery != null && System.nanoTime() < discoveryExpires) return membership;
            if (System.nanoTime() < nextDiscoveryAttempt) throw new TalonException(TalonException.Code.UNAVAILABLE, "discovery refresh cooling down");
            return refreshDiscovery(deadline);
        } finally { membershipLock.unlock(); }
    }

    private static String instanceKey(Messages.DiscoveredWorker worker) {
        return worker.id().length() + ":" + worker.id() + worker.instance().length() + ":" + worker.instance() + worker.address();
    }
    private CachedMembership refreshDiscovery(Long deadline) throws IOException {
        long started = System.nanoTime();
        nextDiscoveryAttempt = started + 100_000_000L;
        Messages.Discovery view;
        try {
            view = controlRoundTrip(Messages.membershipQuery(requestIds.getAndIncrement()), deadline, response -> {
                if (response.tag != Messages.TAG_MEMBERSHIP_LIST) throw unexpected("MembershipQuery", response);
                return Messages.readDiscovery(response.body);
            });
        } catch (IOException failure) { throw TalonException.transport(failure); }
        List<NodeInfo> logical = new ArrayList<>();
        java.util.Set<String> keys = new java.util.HashSet<>();
        for (Messages.DiscoveredWorker worker : view.workers()) {
            logical.add(new NodeInfo(worker.id(), "", true));
            if (worker.state() == 2) {
                String key = instanceKey(worker);
                keys.add(key);
                instancePools.computeIfAbsent(key, ignored -> new ConnectionPool(maxIdlePerAddr));
            }
        }
        instancePools.entrySet().removeIf(entry -> {
            if (keys.contains(entry.getKey())) return false;
            entry.getValue().close(); return true;
        });
        Placement.Table table = discovery != null && discovery.topology() == view.topology()
                ? membership.placement : new Placement.Table(logical);
        discovery = view;
        discoveryExpires = started + view.validForMs() * 1_000_000L;
        membership = new CachedMembership(table);
        return membership;
    }

    private <T> T controlRoundTrip(byte[] request, IoFunction<Messages.Response, T> decode)
            throws IOException {
        return controlRoundTrip(request, null, decode);
    }

    private <T> T controlRoundTrip(byte[] request, Long deadline, IoFunction<Messages.Response, T> decode)
            throws IOException {
        try { return coordinatorPool.exchange(coordinator, null, deadline, socket -> {
            OutputStream out = socket.getOutputStream();
            out.write(Telemetry.envelope(request, coordinator));
            out.flush();

            Frame header = readHeader(socket.getInputStream());
            byte[] payload = readExactly(socket.getInputStream(), header.length());
            if (header.isError()) {
                throw new IOException(
                        "coordinator returned an error: " + new String(payload, java.nio.charset.StandardCharsets.UTF_8));
            }
            Messages.Response response = Messages.decodeBody(payload);
            if (response.tag == Messages.TAG_CONTROL_FAILURE) throw TalonException.decode(response.body);
            return decode.apply(response);
        });
        } catch (IOException failure) { throw TalonException.transport(failure); }
    }

    private byte[] fetchRange(String workerAddress, Segment seg, ConnectionPool pool, long retryDeadline, long objectSize) throws IOException {
        int id = requestIds.getAndIncrement();
        byte[] request =
                Messages.versionedRange(
                        id,
                        seg.block.object(),
                        seg.block.offset() + seg.offsetInBlock,
                        seg.length,
                        seg.block.version(),
                        objectSize);

        return pool.exchange(workerAddress, retryDeadline, socket -> {
            OutputStream out = socket.getOutputStream();
            out.write(Telemetry.envelope(request, workerAddress));
            out.flush();

            InputStream in = socket.getInputStream();
            Frame response = readHeader(in);
            byte[] payload = readExactly(in, response.length());
            if (response.isError()) {
                if (payload.length >= 4 && payload[0] == 'T' && payload[1] == 'L' && payload[2] == 'E' && payload[3] == '1') {
                    throw TalonException.decode(new Bincode.Reader(java.util.Arrays.copyOfRange(payload, 4, payload.length)));
                }
                throw new TalonException(TalonException.Code.UNKNOWN, "worker " + workerAddress + ": " + new String(payload, java.nio.charset.StandardCharsets.UTF_8));
            }
            if (payload.length != seg.length) throw new ProtocolException("incomplete block response");
            return payload;
        });
    }

    @FunctionalInterface
    private interface IoFunction<T, R> {
        R apply(T input) throws IOException;
    }

    /** Closes an exclusively owned socket at an absolute deadline, including during writes. */
    private static final class SocketDeadline implements AutoCloseable {
        private static final ScheduledThreadPoolExecutor TIMER = timer();
        private final long deadline;
        private final ScheduledFuture<?> alarm;
        private boolean armed = true;

        private static ScheduledThreadPoolExecutor timer() {
            ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1, task -> {
                Thread thread = new Thread(task, "talon-load-deadlines");
                thread.setDaemon(true);
                return thread;
            });
            timer.setRemoveOnCancelPolicy(true);
            return timer;
        }

        SocketDeadline(Socket socket, long deadline) throws SocketTimeoutException {
            this.deadline = deadline;
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) throw timeout();
            alarm = TIMER.schedule(() -> {
                synchronized (this) {
                    if (armed) closeSocket(socket);
                }
            }, remaining, TimeUnit.NANOSECONDS);
        }

        static SocketTimeoutException timeout() {
            return new SocketTimeoutException("load deadline exceeded");
        }

        synchronized void finish() throws SocketTimeoutException {
            close();
            // Also reject a late response if the timer thread has not run yet.
            if (deadline - System.nanoTime() <= 0) throw timeout();
        }

        @Override
        public synchronized void close() {
            // Synchronize with the callback before returning this socket to its pool.
            armed = false;
            alarm.cancel(false);
        }
    }

    /** Exclusive checkout; only successfully decoded exchanges return to the pool. */
    private static final class ConnectionPool {
        private static final long IDLE_TTL_NANOS = 30_000_000_000L;
        private record Idle(Socket socket, long returnedAt) {}

        private final Map<String, List<Idle>> idle = new HashMap<>();
        private final int maxIdlePerAddr;
        private boolean closed;

        ConnectionPool(int maxIdlePerAddr) {
            this.maxIdlePerAddr = maxIdlePerAddr;
        }

        <T> T exchange(String address, Long retryDeadline, IoFunction<Socket, T> request) throws IOException {
            return exchange(address, retryDeadline, null, request);
        }
        <T> T exchange(String address, Long retryDeadline, Long deadline, IoFunction<Socket, T> request) throws IOException {
            Socket socket = takeIdle(address);
            boolean reused = socket != null;
            boolean retrying = false;
            for (;;) {
                if (socket == null) {
                    if (retrying) checkRetryDeadline(retryDeadline);
                    ensureOpen();
                    socket = new Socket();
                }
                boolean completed = false;
                try (SocketDeadline guard = deadline == null ? null : new SocketDeadline(socket, deadline)) {
                    // The same deadline covers connect, write, header, body, and any retry.
                    if (!socket.isConnected()) connect(socket, address);
                    // Dialing can consume the remaining discovery lifetime.
                    if (retrying) checkRetryDeadline(retryDeadline);
                    T result = request.apply(socket);
                    if (guard != null) guard.finish();
                    release(address, socket);
                    completed = true;
                    return result;
                } catch (IOException failure) {
                    if (deadline != null && deadline - System.nanoTime() <= 0) {
                        throw new TalonException(TalonException.Code.TIMEOUT, "load deadline exceeded", failure);
                    }
                    // Data reads retry transport I/O only, never a typed Worker refusal.
                    // Preserve the narrower disconnect-only policy for control requests.
                    if (!reused || failure instanceof TalonException
                            || (retryDeadline == null
                                && !(failure instanceof EOFException || failure instanceof SocketException))) {
                        throw failure;
                    }
                    // The peer may close an idle socket. Retry once, bypassing the pool.
                    reused = false;
                    retrying = true;
                } finally {
                    if (!completed) {
                        closeSocket(socket);
                    }
                }
                socket = null;
            }
        }

        private static void checkRetryDeadline(Long deadline) throws TalonException {
            if (deadline != null && System.nanoTime() >= deadline) {
                throw new TalonException(TalonException.Code.UNAVAILABLE,
                        "instance discovery expired before read retry");
            }
        }

        private synchronized void ensureOpen() throws IOException {
            if (closed) {
                throw new IOException("Talon client is closed");
            }
        }

        private synchronized Socket takeIdle(String address) throws IOException {
            ensureOpen();
            List<Idle> bucket = idle.get(address);
            while (bucket != null && !bucket.isEmpty()) {
                Idle entry = bucket.remove(bucket.size() - 1);
                if (bucket.isEmpty()) {
                    idle.remove(address);
                }
                if (System.nanoTime() - entry.returnedAt() < IDLE_TTL_NANOS) {
                    return entry.socket();
                }
                closeSocket(entry.socket());
            }
            return null;
        }

        private synchronized void release(String address, Socket socket) {
            if (!closed) {
                List<Idle> bucket = idle.computeIfAbsent(address, ignored -> new ArrayList<>());
                if (bucket.size() < maxIdlePerAddr) {
                    bucket.add(new Idle(socket, System.nanoTime()));
                    return;
                }
            }
            closeSocket(socket);
        }

        synchronized void close() {
            closed = true;
            for (List<Idle> bucket : idle.values()) {
                for (Idle entry : bucket) {
                    closeSocket(entry.socket());
                }
            }
            idle.clear();
        }
    }

    private static void closeSocket(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // Cleanup must not hide the original exchange failure.
        }
    }

    private static void connect(Socket socket, String hostPort) throws IOException {
        int colon = hostPort.lastIndexOf(':');
        if (colon < 0) {
            throw new IOException("address is missing a port: " + hostPort);
        }
        String host = hostPort.substring(0, colon);
        int port = Integer.parseInt(hostPort.substring(colon + 1));
        try {
            socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
            socket.setSoTimeout(READ_TIMEOUT_MS);
            socket.setTcpNoDelay(true);
        } catch (IOException | RuntimeException failure) {
            closeSocket(socket);
            throw failure;
        }
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
                return new TalonException(TalonException.Code.UNKNOWN, request + " rejected: " + detail);
            }
        }
        throw new ProtocolException("unexpected reply to " + request + ": variant tag " + resp.tag);
    }
}
