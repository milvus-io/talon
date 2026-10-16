package io.milvus.talon;

import java.io.*;
import java.net.*;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Real TCP reads prove that default affinity cannot escape its zone. */
public final class ZoneAffinityTest {
    public static void main(String[] args) throws Exception {
        ZoneOptions defaults = ZoneOptions.fromEnvironment(k -> null);
        if (defaults.affinity() || defaults.fallback()) throw new AssertionError("unsafe defaults");
        try { ZoneOptions.fromEnvironment(k -> "typo"); throw new AssertionError("invalid boolean accepted"); }
        catch (IllegalArgumentException expected) { }
        for (int state : new int[] {0, 1, 2}) {
            for (boolean fallback : new boolean[] {false, true}) {
                try (Cluster cluster = new Cluster(state, null);
                     TalonClient client = cluster.client("a", fallback)) {
                    if (state == 2 || fallback) {
                        assertBytes(client, state == 2 ? 1 : 2);
                    } else expectUnavailable(client);
                    if (cluster.remoteHits.get() != (state == 2 || !fallback ? 0 : 1)) {
                        throw new AssertionError("unexpected remote request");
                    }
                    if (state != 2) {
                        int before = cluster.remoteHits.get();
                        try { client.load("s3://bucket/key", "v", 8); throw new AssertionError("LOAD remapped"); }
                        catch (IOException expected) { }
                        if (cluster.remoteHits.get() != before) throw new AssertionError("remote LOAD");
                    }
                }
            }
        }
        for (String zone : new String[] {null, "absent"}) {
            try (Cluster cluster = new Cluster(2, null); TalonClient client = cluster.client(zone, false)) {
                expectUnavailable(client);
                if (cluster.localHits.get() + cluster.remoteHits.get() != 0) throw new AssertionError("unknown zone dialed");
            }
        }
        for (TalonException.Code code : new TalonException.Code[] {
                TalonException.Code.VERSION_MISMATCH, TalonException.Code.RATE_LIMITED,
                TalonException.Code.ORIGIN, TalonException.Code.NOT_FOUND}) {
            try (Cluster cluster = new Cluster(2, code); TalonClient client = cluster.client("a", true)) {
                try { read(client); throw new AssertionError("terminal error hidden"); }
                catch (TalonException expected) { if (expected.code() != code) throw expected; }
                if (cluster.remoteHits.get() != 0) throw new AssertionError("terminal failure retried");
            }
        }
        try (Cluster cluster = new Cluster(2, TalonException.Code.UNAVAILABLE);
             TalonClient client = cluster.client("a", true)) { assertBytes(client, 2); }
        try (Cluster cluster = new Cluster(0, null); TalonClient client = cluster.client("a", true)) {
            assertBytes(client, 2);
            cluster.localState.set(2);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (read(client)[0] != 1) {
                if (System.nanoTime() > deadline) throw new AssertionError("local recovery failed");
                Thread.sleep(25);
            }
        }
        System.out.println("zone affinity: defaults, offline/conflict, recovery, LOAD and terminal errors passed");
    }

    static byte[] read(TalonClient client) throws IOException {
        return client.read("s3://bucket/key", "v", 8, 0, 8);
    }
    static void assertBytes(TalonClient client, int value) throws IOException {
        byte[] expected = new byte[8]; Arrays.fill(expected, (byte) value);
        if (!Arrays.equals(read(client), expected)) throw new AssertionError("wrong zone bytes");
    }
    static void expectUnavailable(TalonClient client) throws IOException {
        try { read(client); throw new AssertionError("unavailable zone succeeded"); }
        catch (TalonException expected) { if (expected.code() != TalonException.Code.UNAVAILABLE) throw expected; }
    }

    static final class Cluster implements AutoCloseable {
        final ServerSocket coordinator = new ServerSocket(0);
        final ServerSocket local = new ServerSocket(0);
        final ServerSocket remote = new ServerSocket(0);
        final ExecutorService tasks = Executors.newCachedThreadPool();
        final AtomicInteger localState;
        final AtomicInteger localHits = new AtomicInteger();
        final AtomicInteger remoteHits = new AtomicInteger();
        final TalonException.Code localError;
        Cluster(int state, TalonException.Code error) throws IOException {
            localState = new AtomicInteger(state); localError = error;
            serve(coordinator, 0); serve(local, 1); serve(remote, 2);
        }
        TalonClient client(String zone, boolean fallback) {
            return TalonClient.connect(address(coordinator), 64, 8, new ZoneOptions(zone, true, fallback));
        }
        static String address(ServerSocket server) { return "127.0.0.1:" + server.getLocalPort(); }
        void serve(ServerSocket server, int kind) {
            tasks.submit(() -> {
                try {
                    while (!server.isClosed()) {
                        Socket socket = server.accept();
                        socket.setSoTimeout(1000);
                        tasks.submit(() -> respond(socket, kind));
                    }
                } catch (IOException ignored) { }
            });
        }
        void respond(Socket socket, int kind) {
            try (socket) {
                DataInputStream in = new DataInputStream(socket.getInputStream());
                while (true) {
                    byte[] header = in.readNBytes(Frame.HEADER_LEN);
                    if (header.length == 0) return;
                    Frame request = Frame.decode(header);
                    in.readNBytes(request.length());
                    byte[] response;
                    int flags = 0;
                    if (kind == 0) {
                        Bincode.Writer b = new Bincode.Writer().u16(6).variant(Messages.TAG_MEMBERSHIP_LIST)
                                .u64(1).u64(2).u64(100).u64(2);
                        int state = localState.get();
                        b.string("local").u8(1).string("a").u8(0).variant(state);
                        if (state == 2) b.string("local-instance").string(address(local));
                        b.string("remote").u8(1).string("b").u8(0).variant(2)
                                .string("remote-instance").string(address(remote));
                        response = b.toBytes();
                    } else {
                        (kind == 1 ? localHits : remoteHits).incrementAndGet();
                        if (kind == 1 && localError != null) {
                            ByteArrayOutputStream error = new ByteArrayOutputStream();
                            error.write(new byte[] {'T', 'L', 'E', '1'});
                            error.write(new Bincode.Writer().variant(localError.ordinal()).string("local failure").toBytes());
                            response = error.toByteArray();
                            flags = Frame.FLAG_ERROR;
                        } else { response = new byte[8]; Arrays.fill(response, (byte) kind); }
                    }
                    socket.getOutputStream().write(new Frame(kind == 0 ? Frame.MsgType.CONTROL : Frame.MsgType.GET_RANGE,
                            flags, request.requestId(), response.length).encode());
                    socket.getOutputStream().write(response);
                }
            } catch (IOException ignored) { }
        }
        public void close() throws IOException {
            coordinator.close(); local.close(); remote.close(); tasks.shutdownNow();
        }
    }
}
