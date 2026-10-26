package io.milvus.talon;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Socket-level namespace routing, compatibility, tracing and retry checks. */
public final class NamespaceTest {
    private static final String PARENT = "00-11111111111111111111111111111111-2222222222222222-01";
    private static final String BROKER = "{\"url\":\"http://localhost/broker\",\"request\":{\"csp\":\"aws\"},\"origin\":{\"endpoint\":\"storage.example\",\"region\":\"us-west-2\"}}";
    private static final TraceContext TRACE = TraceContext.fromW3c(PARENT, "vendor=value");

    public static void main(String[] args) throws Exception {
        fluentOptions();
        nestedScopes(false);
        nestedScopes(true);
        requestNamespace();
        validateNames();
        for (String namespace : new String[] {null, "default", "team.A_1-", "a", "z".repeat(255),
                "worker_identity_2", ".worker.lock1"}) {
            workflow(namespace, false);
        }
        workflow("team.A_1-", true);
        workflow("default", true);
        for (boolean tracing : new boolean[] {false, true}) {
            workflow("team-a", tracing, true);
            workflow("default", tracing, true);
            combinedFluentOptions(tracing);
            concurrentNamespaces(tracing, true);
        }
        retryAndRefusal(false);
        retryAndRefusal(true);
        concurrentNamespaces(false);
        concurrentNamespaces(true);
        try (Peer peer = new Peer(); TalonClient client = TalonClient.connect(peer.address())) {
            client.stat("s3://bucket/key", RequestOptions.INHERIT);
            Request request = peer.requests.stream().filter(r -> r.tag() == Messages.TAG_STAT_OBJECT)
                    .findFirst().orElseThrow();
            assertEquals("default", request.namespace(), "default options namespace");
            assertEquals(1, request.version(), "default options namespace keeps v1");
            peer.assertHealthy();
        }
        System.out.println("namespaces: per-call validation, concurrent isolation, default/v3/v4 frames, broker properties, metadata, reads, LOAD, tracing and retries passed");
    }

    private static void fluentOptions() throws Exception {
        TraceContext other = TraceContext.fromW3c("00-33333333333333333333333333333333-4444444444444444-01", null);
        Telemetry.configure(Set.of("*"), () -> TRACE);
        try (Peer peer = new Peer(); TalonClient client = connect(peer.address())) {
            RequestOptions base = RequestOptions.INHERIT.withNamespace("team-a");
            RequestOptions branch = base.withParent(other).withNamespace("team-b");
            assertStatCarrier(client, peer, branch, "team-b", other);
            assertStatCarrier(client, peer, RequestOptions.INHERIT.withParent(other).withNamespace("team-b"), "team-b", other);
            assertStatCarrier(client, peer, RequestOptions.INHERIT.withNamespace("team-b").withParent(other), "team-b", other);
            assertStatCarrier(client, peer, RequestOptions.explicit(other).withNamespace("team-b"), "team-b", other);
            assertStatCarrier(client, peer, base, "team-a", TRACE);
            assertStatCarrier(client, peer, base.withParent(null), "team-a", null);
            assertStatCarrier(client, peer, base.withNamespace(null), "default", TRACE);
            assertStatCarrier(client, peer, base.withNamespace("default"), "default", TRACE);
            assertStatCarrier(client, peer, RequestOptions.INHERIT, "default", TRACE);
            assertStatCarrier(client, peer, RequestOptions.ROOT.withNamespace("team-b"), "team-b", null);
            assertStatCarrier(client, peer, RequestOptions.ROOT, "default", null);
            assertStatCarrier(client, peer, base, "team-a", TRACE);
            peer.assertHealthy();
        } finally { Telemetry.configure(Set.of(), null); }
    }

    private static void nestedScopes(boolean tracing) throws Exception {
        TraceContext ambient = TraceContext.fromW3c("00-33333333333333333333333333333333-4444444444444444-01", null);
        Telemetry.configure(tracing ? Set.of("*") : Set.of(), () -> ambient);
        try (Peer peer = new Peer(); TalonClient client = connect(peer.address())) {
            RequestOptions outer = RequestOptions.ROOT.withNamespace("team-a").withParent(TRACE);
            Telemetry.call(outer, () -> {
                assertStatCarrier(client, peer, RequestOptions.INHERIT, "team-a", tracing ? TRACE : null);
                assertStatCarrier(client, peer, RequestOptions.INHERIT.withNamespace("team-b"), "team-b", tracing ? TRACE : null);
                assertStatCarrier(client, peer, RequestOptions.INHERIT.withNamespace(null), "default", tracing ? TRACE : null);
                assertStatCarrier(client, peer, RequestOptions.INHERIT.withNamespace("default"), "default", tracing ? TRACE : null);
                assertStatCarrier(client, peer, RequestOptions.ROOT.withNamespace("team-b"), "team-b", null);
                assertStatCarrier(client, peer, RequestOptions.INHERIT.withNamespace("team-b").withParent(ambient), "team-b", tracing ? ambient : null);
                peer.rejectStat.set(true);
                try {
                    client.stat("s3://bucket/key", RequestOptions.INHERIT.withNamespace("team-b"));
                    throw new AssertionError("nested refusal hidden");
                } catch (TalonException expected) {
                    assertEquals(TalonException.Code.INVALID_REQUEST, expected.code(), "nested refusal code");
                }
                assertStatCarrier(client, peer, RequestOptions.INHERIT, "team-a", tracing ? TRACE : null);
                return null;
            });
            Telemetry.call(RequestOptions.ROOT.withNamespace("team-a"), () -> {
                assertStatCarrier(client, peer, RequestOptions.INHERIT.withNamespace("team-b"), "team-b", null);
                return null;
            });
            assertStatCarrier(client, peer, RequestOptions.INHERIT, "default", tracing ? ambient : null);
            peer.assertHealthy();
        } finally { Telemetry.configure(Set.of(), null); }
    }

    private static Request assertStatCarrier(TalonClient client, Peer peer, RequestOptions options,
                                             String namespace, TraceContext trace) throws IOException {
        client.stat("s3://bucket/key", options);
        Request request = peer.requests.stream().filter(r -> r.tag() == Messages.TAG_STAT_OBJECT)
                .reduce((first, last) -> last).orElseThrow();
        assertEquals(namespace, request.namespace(), "selected stat namespace");
        if (trace == null) {
            if (request.metadata().containsKey(1)) throw new AssertionError("unexpected trace parent");
        } else assertEquals(trace.traceparent(), request.text(1), "selected stat trace parent");
        // Background discovery may append a MembershipQuery after this STAT was selected.
        return request;
    }

    private static void combinedFluentOptions(boolean tracing) throws Exception {
        Telemetry.configure(tracing ? Set.of("*") : Set.of(), () -> TRACE);
        try (Peer peer = new Peer(); TalonClient client = connect(peer.address())) {
            for (String broker : new String[] {BROKER,
                    "{\"url\":\"http://localhost/broker\",\"request\":{\"csp\":\"aws\"},"
                    + "\"origin\":{\"endpoint\":\"localhost:9000\",\"use_ssl\":false,\"region\":\"us-east-1\"}}"}) {
                for (RequestOptions options : new RequestOptions[] {
                        RequestOptions.ROOT.withNamespace("team-a").withParent(TRACE).withCredentialBroker(broker),
                        RequestOptions.ROOT.withCredentialBroker(broker).withNamespace("team-a").withParent(TRACE),
                        RequestOptions.ROOT.withParent(TRACE).withCredentialBroker(broker).withNamespace("team-a")}) {
                    Request stat = assertStatCarrier(client, peer, options, "team-a", tracing ? TRACE : null);
                    assertEquals(broker, stat.text(6), "fluent broker preservation");
                    stat = assertStatCarrier(client, peer, options.withNamespace(null), "default", tracing ? TRACE : null);
                    assertEquals(broker, stat.text(6), "default reset keeps broker");
                    stat = assertStatCarrier(client, peer, options.withParent(null), "team-a", null);
                    assertEquals(broker, stat.text(6), "root reset keeps broker");
                    stat = assertStatCarrier(client, peer, options.withCredentialBroker(null), "team-a", tracing ? TRACE : null);
                    if (stat.metadata().containsKey(6)) {
                        throw new AssertionError("cleared broker retained");
                    }
                    stat = assertStatCarrier(client, peer, options, "team-a", tracing ? TRACE : null);
                    assertEquals(broker, stat.text(6), "fluent copies leave source unchanged");
                }
            }
            peer.assertHealthy();
        } finally { Telemetry.configure(Set.of(), null); }
    }

    private static void requestNamespace() throws Exception {
        RequestOptions options = RequestOptions.INHERIT.withNamespace("team-a");
        try (Peer peer = new Peer(); TalonClient client = TalonClient.connect(peer.address(), 64)) {
            byte[] named = new byte[8]; Arrays.fill(named, (byte) 2);
            byte[] defaults = new byte[8]; Arrays.fill(defaults, (byte) 1);
            assertBytes(named, client.read("s3://bucket/key", "v", 8, 0, 8, options));
            assertBytes(defaults, client.read("s3://bucket/key", "v", 8, 0, 8, RequestOptions.INHERIT));
            for (RequestOptions defaultsOption : new RequestOptions[] {null, RequestOptions.ROOT, RequestOptions.INHERIT}) {
                assertBytes(named, client.read("s3://bucket/key", "v", 8, 0, 8, options));
                assertBytes(defaults, client.read("s3://bucket/key", "v", 8, 0, 8, defaultsOption));
            }
            peer.assertHealthy();
        }
    }

    private static TalonClient connect(String address) {
        return TalonClient.connect(address, 64, 8, new ZoneOptions(null, false));
    }

    private static void validateNames() throws Exception {
        for (String name : new String[] {"", ".", "..", "worker_identity", ".worker.lock", "../a",
                "a/b", "a\\b", "a b", "a\n", "é", "中", "a\u0000", "x".repeat(256)}) {
            try {
                RequestOptions.INHERIT.withNamespace(name);
                throw new AssertionError("invalid namespace accepted: " + name);
            } catch (IllegalArgumentException expected) { }
        }
    }

    private static void workflow(String namespace, boolean tracing) throws Exception {
        workflow(namespace, tracing, false);
    }

    private static void workflow(String namespace, boolean tracing, boolean broker) throws Exception {
        Telemetry.configure(tracing ? Set.of("*") : Set.of(), () -> TRACE);
        RequestOptions options = RequestOptions.explicit(tracing ? TRACE : null).withNamespace(namespace);
        if (broker) options = options.withCredentialBroker(BROKER);
        try (Peer peer = new Peer(); TalonClient client = connect(peer.address())) {
            String expectedNamespace = namespace == null ? "default" : namespace;
            assertEquals(8L, client.stat("s3://bucket/key", options).size(), "stat size");
            List<ObjectEntry> entries = client.list("s3/bucket", options);
            assertEquals(1, entries.size(), "list count");
            assertEquals("s3/bucket/key", entries.get(0).path(), "list path");
            assertEquals(8L, entries.get(0).size(), "list size");
            byte[] expected = new byte[8];
            Arrays.fill(expected, (byte) (expectedNamespace.equals("default") ? 1 : 2));
            assertBytes(expected, client.read("s3://bucket/key", "v", 8, 0, 8, options));
            assertBytes(expected, client.read("s3://bucket/key", 0, 8, options));
            assertEquals(new LoadResult(8, 1), client.load(new LoadRequest("s3://bucket/key", "v", 8), options), "load result");
            assertEquals(List.of(new LoadResult(8, 1), new LoadResult(8, 1)), client.batchLoad(List.of(
                    new LoadRequest("s3://bucket/key", "v", 8), new LoadRequest("s3://bucket/other", "v", 8)), options),
                    "batch result");
            int requests = peer.requests.size();
            assertBytes(new byte[0], client.read("s3://bucket/key", "v", 8, 8, 1, options));
            assertEquals(new LoadResult(0, 0), client.load(new LoadRequest("s3://bucket/key", "v", 0), options), "empty load");
            assertEquals(List.of(), client.batchLoad(List.of(), options), "empty batch");
            assertEquals(requests, peer.requests.size(), "no-op requests stay local");
            Set<Integer> seen = new java.util.HashSet<>();
            for (Request request : peer.requests) {
                seen.add(request.tag());
                boolean discovery = request.tag() == Messages.TAG_MEMBERSHIP_QUERY;
                String selected = discovery ? "default" : expectedNamespace;
                assertEquals(selected, request.namespace(), "request namespace");
                boolean carriesTrace = tracing && !discovery;
                boolean carriesBroker = broker && !discovery;
                int version = carriesBroker ? 4 : selected.equals("default") ? (carriesTrace ? 2 : 1) : 3;
                if (carriesBroker) assertEquals(BROKER, request.text(6), "request broker");
                else if (request.metadata().containsKey(6)) throw new AssertionError("unexpected broker carrier");
                assertEquals(version, request.version(), "request version");
                if (carriesTrace) {
                    assertEquals(PARENT, request.text(1), "traceparent");
                    assertEquals("vendor=value", request.text(2), "tracestate");
                    assertEquals(16, request.metadata().get(3).length, "sampled read id");
                } else if (request.metadata().containsKey(1)) throw new AssertionError("unexpected trace carrier");
                if (selected.equals("default") && request.metadata().containsKey(5)) {
                    throw new AssertionError("default namespace changed legacy wire format");
                }
            }
            if (!seen.containsAll(Set.of(-1, Messages.TAG_MEMBERSHIP_QUERY, Messages.TAG_STAT_OBJECT,
                    Messages.TAG_LIST_OBJECTS, Messages.TAG_LOAD_BLOCK, Messages.TAG_BATCH_LOAD))) {
                throw new AssertionError("missing request coverage: " + seen);
            }
            peer.assertHealthy();
        } finally { Telemetry.configure(Set.of(), null); }
    }

    private static void retryAndRefusal(boolean broker) throws Exception {
        Telemetry.configure(Set.of("*"), () -> TRACE);
        RequestOptions options = RequestOptions.explicit(TRACE).withNamespace("team-a");
        if (broker) options = options.withCredentialBroker(BROKER);
        try (Peer peer = new Peer(); TalonClient client = connect(peer.address())) {
            client.read("s3://bucket/key", "v", 8, 0, 8, options);
            peer.dropRead.set(true);
            client.read("s3://bucket/key", "v", 8, 0, 8, options);
            assertEquals(3, peer.reads.get(), "stale connection retried once");
            List<Request> reads = peer.requests.stream().filter(r -> r.tag() == -1).toList();
            for (Request request : reads) {
                assertEquals("team-a", request.namespace(), "retry namespace");
                assertEquals(broker ? 4 : 3, request.version(), "retry protocol version");
                if (broker) assertEquals(BROKER, request.text(6), "retry broker");
                assertEquals(PARENT, request.text(1), "retry traceparent");
            }
            assertBytes(reads.get(1).body(), reads.get(2).body());
            assertBytes(reads.get(1).metadata().get(3), reads.get(2).metadata().get(3));
            peer.rejectRead.set(true);
            try {
                client.read("s3://bucket/key", "v", 8, 0, 8, options);
                throw new AssertionError("worker refusal hidden");
            } catch (TalonException expected) {
                assertEquals(TalonException.Code.INVALID_REQUEST, expected.code(), "worker refusal code");
                assertEquals("namespace rejected", expected.getMessage(), "worker refusal detail");
            }
            assertEquals(4, peer.reads.get(), "worker refusal must not retry or fall back");
            peer.rejectStat.set(true);
            try {
                client.stat("s3://bucket/key", options);
                throw new AssertionError("coordinator refusal hidden");
            } catch (TalonException expected) {
                assertEquals(TalonException.Code.INVALID_REQUEST, expected.code(), "coordinator refusal code");
            }
            assertEquals(1L, peer.requests.stream().filter(r -> r.tag() == Messages.TAG_STAT_OBJECT).count(),
                    "coordinator refusal must not fall back");
            byte[] defaults = new byte[8]; Arrays.fill(defaults, (byte) 1);
            assertBytes(defaults, client.read("s3://bucket/key", "v", 8, 0, 8, RequestOptions.INHERIT));
            client.stat("s3://bucket/key", RequestOptions.INHERIT);
            for (Request request : peer.requests.subList(peer.requests.size() - 2, peer.requests.size())) {
                assertEquals("default", request.namespace(), "failed call must not leak namespace");
                assertEquals(2, request.version(), "default keeps inherited tracing after failure");
            }
            peer.assertHealthy();
        } finally { Telemetry.configure(Set.of(), null); }
    }

    private static void concurrentNamespaces(boolean tracing) throws Exception {
        concurrentNamespaces(tracing, false);
    }

    private static void concurrentNamespaces(boolean tracing, boolean broker) throws Exception {
        String otherParent = "00-33333333333333333333333333333333-4444444444444444-01";
        TraceContext otherTrace = TraceContext.fromW3c(otherParent, "vendor=other");
        Telemetry.configure(tracing ? Set.of("*") : Set.of(), () -> TRACE);
        ExecutorService callers = Executors.newFixedThreadPool(3);
        try (Peer peer = new Peer(); TalonClient client = connect(peer.address())) {
            CountDownLatch ready = new CountDownLatch(3);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> results = new java.util.ArrayList<>();
            for (String namespace : List.of("team-a", "team-b", "default")) {
                results.add(callers.submit(() -> {
                    RequestOptions options = namespace.equals("default") ? RequestOptions.INHERIT
                            : RequestOptions.explicit(tracing ? (namespace.equals("team-b") ? otherTrace : TRACE) : null).withNamespace(namespace);
                    if (broker && !namespace.equals("default")) {
                        options = options.withCredentialBroker(
                                BROKER.replace("aws", namespace));
                    }
                    String uri = "s3://bucket/" + namespace + "/key";
                    byte[] expected = new byte[8];
                    Arrays.fill(expected, (byte) (namespace.equals("default") ? 1 : namespace.equals("team-b") ? 3 : 2));
                    ready.countDown();
                    if (!start.await(5, TimeUnit.SECONDS)) throw new AssertionError("concurrent calls did not start");
                    for (int i = 0; i < 6; i++) {
                        assertBytes(expected, client.read(uri, "v", 8, 0, 8, options));
                        assertBytes(expected, client.read(uri, 0, 8, options));
                        assertEquals(8L, client.stat(uri, options).size(), "concurrent stat");
                        assertEquals(1, client.list("s3/bucket/" + namespace, options).size(), "concurrent list");
                        assertEquals(new LoadResult(8, 1), client.load(new LoadRequest(uri, "v", 8), options), "concurrent load");
                        assertEquals(List.of(new LoadResult(8, 1)), client.batchLoad(List.of(new LoadRequest(uri, "v", 8)), options),
                                "concurrent batch");
                    }
                    return null;
                }));
            }
            if (!ready.await(5, TimeUnit.SECONDS)) throw new AssertionError("callers did not become ready");
            start.countDown();
            for (Future<?> result : results) result.get(10, TimeUnit.SECONDS);
            Map<String, Set<Integer>> seen = new HashMap<>();
            for (Request request : peer.requests) {
                int tag = request.tag();
                if (tag == Messages.TAG_MEMBERSHIP_QUERY) {
                    assertEquals("default", request.namespace(), "discovery has no per-call namespace");
                    assertEquals(1, request.version(), "discovery remains v1");
                    continue;
                }
                Bincode.Reader body = request.type() == Frame.MsgType.CONTROL
                        ? Messages.decodeBody(request.body()).body : new Bincode.Reader(request.body());
                String expectedNamespace;
                if (tag == Messages.TAG_LIST_OBJECTS) {
                    expectedNamespace = body.string().substring("s3/bucket/".length());
                } else {
                    if (tag == Messages.TAG_BATCH_LOAD) assertEquals(1, body.seqLen(), "concurrent batch size");
                    body.variant();
                    body.string();
                    expectedNamespace = body.string().split("/")[0];
                }
                assertEquals(expectedNamespace, request.namespace(), "concurrent request namespace matches caller");
                seen.computeIfAbsent(expectedNamespace, ignored -> new java.util.HashSet<>()).add(tag);
                boolean carriesBroker = broker && !expectedNamespace.equals("default");
                assertEquals(carriesBroker ? 4 : expectedNamespace.equals("default") ? (tracing ? 2 : 1) : 3,
                        request.version(), "concurrent request version");
                if (carriesBroker) assertEquals(BROKER.replace("aws", expectedNamespace), request.text(6), "concurrent broker");
                else if (request.metadata().containsKey(6)) throw new AssertionError("broker leaked across callers");
                if (tracing) {
                    assertEquals(expectedNamespace.equals("team-b") ? otherParent : PARENT, request.text(1), "concurrent trace context");
                } else if (request.metadata().containsKey(1)) throw new AssertionError("trace leaked with telemetry disabled");
            }
            for (String namespace : List.of("team-a", "team-b", "default")) {
                assertEquals(Set.of(-1, Messages.TAG_STAT_OBJECT, Messages.TAG_LIST_OBJECTS,
                        Messages.TAG_LOAD_BLOCK, Messages.TAG_BATCH_LOAD), seen.get(namespace), "concurrent operation coverage");
            }
            peer.assertHealthy();
        } finally {
            callers.shutdownNow();
            Telemetry.configure(Set.of(), null);
        }
    }

    private record Request(int version, Frame.MsgType type, int id, byte[] body, Map<Integer, byte[]> metadata) {
        String text(int key) {
            byte[] value = metadata.get(key);
            return value == null ? null : new String(value, StandardCharsets.US_ASCII);
        }
        String namespace() { return text(5) == null ? "default" : text(5); }
        int tag() { return type == Frame.MsgType.CONTROL ? Messages.decodeBody(body).tag : -1; }
    }

    private static final class Peer implements AutoCloseable {
        final ServerSocket server = new ServerSocket(0);
        final ExecutorService tasks = Executors.newCachedThreadPool();
        final List<Socket> sockets = new CopyOnWriteArrayList<>();
        final List<Request> requests = new CopyOnWriteArrayList<>();
        final List<Throwable> failures = new CopyOnWriteArrayList<>();
        final AtomicInteger reads = new AtomicInteger();
        final AtomicBoolean dropRead = new AtomicBoolean();
        final AtomicBoolean rejectRead = new AtomicBoolean();
        final AtomicBoolean rejectStat = new AtomicBoolean();

        Peer() throws IOException {
            tasks.submit(() -> {
                try {
                    while (!server.isClosed()) {
                        Socket socket = server.accept();
                        sockets.add(socket);
                        tasks.submit(() -> respond(socket));
                    }
                } catch (IOException failure) { if (!server.isClosed()) failures.add(failure); }
            });
        }

        String address() { return "127.0.0.1:" + server.getLocalPort(); }

        void respond(Socket socket) {
            try (socket) {
                socket.setSoTimeout(5000);
                DataInputStream in = new DataInputStream(socket.getInputStream());
                for (;;) {
                    byte[] header = in.readNBytes(16);
                    if (header.length == 0) return;
                    if (header.length != 16) throw new AssertionError("truncated request header");
                    ByteBuffer h = ByteBuffer.wrap(header);
                    if (h.getShort(0) != 0x544c) throw new AssertionError("invalid frame magic");
                    int version = header[2] & 255;
                    Frame.MsgType type = Frame.MsgType.from(header[3] & 255);
                    byte[] payload = in.readNBytes(h.getInt(12));
                    if (payload.length != h.getInt(12)) throw new AssertionError("truncated request payload");
                    Map<Integer, byte[]> metadata = new HashMap<>();
                    int start = 0;
                    if (version != 1) {
                        if (version != 2 && version != 3 && version != 4) throw new AssertionError("invalid request version");
                        start = 2 + (ByteBuffer.wrap(payload).getShort() & 65535);
                        if (start > payload.length || start > (version == 4 ? 1026 + 3 + 16384 : 1026)) throw new AssertionError("invalid metadata length");
                        for (int i = 2; i < start;) {
                            int key = payload[i++] & 255;
                            int length = ByteBuffer.wrap(payload).getShort(i) & 65535;
                            i += 2;
                            if (i + length > start || metadata.put(key, Arrays.copyOfRange(payload, i, i + length)) != null) {
                                throw new AssertionError("invalid metadata TLV");
                            }
                            i += length;
                        }
                    }
                    Request request = new Request(version, type, h.getInt(8), Arrays.copyOfRange(payload, start, payload.length), metadata);
                    requests.add(request);
                    byte[] response;
                    int flags = 0;
                    if (type == Frame.MsgType.GET_VERSIONED_RANGE) {
                        reads.incrementAndGet();
                        if (dropRead.getAndSet(false)) return;
                        if (rejectRead.getAndSet(false)) {
                            ByteArrayOutputStream error = new ByteArrayOutputStream();
                            error.writeBytes(new byte[] {'T', 'L', 'E', '1'});
                            error.writeBytes(new Bincode.Writer().variant(1).string("namespace rejected").toBytes());
                            response = error.toByteArray();
                            flags = Frame.FLAG_ERROR;
                        } else {
                            response = new byte[8];
                            Arrays.fill(response, (byte) (request.namespace().equals("default") ? 1
                                    : request.namespace().equals("team-b") ? 3 : 2));
                        }
                    } else {
                        Bincode.Writer body = new Bincode.Writer().u16(6);
                        switch (request.tag()) {
                            case Messages.TAG_MEMBERSHIP_QUERY -> body.variant(Messages.TAG_MEMBERSHIP_LIST)
                                    .u64(1).u64(1).u64(30000).u64(1).string("worker").u8(0).u8(0)
                                    .variant(2).string("instance").string(address());
                            case Messages.TAG_STAT_OBJECT -> {
                                if (rejectStat.getAndSet(false)) body.variant(Messages.TAG_CONTROL_FAILURE)
                                        .variant(1).string("namespace rejected");
                                else body.variant(Messages.TAG_OBJECT_STAT).u64(8).string("v");
                            }
                            case Messages.TAG_LIST_OBJECTS -> body.variant(Messages.TAG_OBJECT_LIST).u64(1)
                                    .string("s3/bucket/key").u64(8);
                            case Messages.TAG_LOAD_BLOCK -> body.variant(Messages.TAG_ACK).u8(1).u8(0);
                            case Messages.TAG_BATCH_LOAD -> body.variant(Messages.TAG_BATCH_LOAD_RESULT).u64(0);
                            default -> throw new AssertionError("unexpected request: " + request.tag());
                        }
                        response = body.toBytes();
                    }
                    byte[] reply = new Frame(type == Frame.MsgType.CONTROL ? type : Frame.MsgType.GET_RANGE,
                            flags, request.id(), response.length).encode();
                    reply[2] = (byte) version;
                    socket.getOutputStream().write(reply);
                    socket.getOutputStream().write(response);
                    socket.getOutputStream().flush();
                }
            } catch (Throwable failure) { if (!server.isClosed()) failures.add(failure); }
        }

        void assertHealthy() { if (!failures.isEmpty()) throw new AssertionError("peer failed", failures.get(0)); }

        @Override public void close() throws IOException {
            server.close();
            for (Socket socket : sockets) socket.close();
            tasks.shutdownNow();
            try {
                if (!tasks.awaitTermination(5, TimeUnit.SECONDS)) throw new AssertionError("peer did not stop");
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while stopping peer", interrupted);
            }
            assertHealthy();
        }
    }

    private static void assertBytes(byte[] expected, byte[] actual) {
        if (!Arrays.equals(expected, actual)) throw new AssertionError("bytes differ");
    }
    private static void assertEquals(Object expected, Object actual, String message) {
        if (!expected.equals(actual)) throw new AssertionError(message + ": expected " + expected + ", got " + actual);
    }
}
