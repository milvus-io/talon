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
        if (defaults.affinity()) throw new AssertionError("unexpected default affinity");
        try { ZoneOptions.fromEnvironment(k -> "typo"); throw new AssertionError("invalid boolean accepted"); }
        catch (IllegalArgumentException expected) { }
        for (int state : new int[] {0, 1, 2}) {
            try (Cluster cluster = new Cluster(state, null);
                 TalonClient client = cluster.client("a")) {
                if (state == 2) assertBytes(client, 1);
                else expectUnavailable(client);
                if (cluster.remoteHits.get() != 0) throw new AssertionError("remote READ");
                if (state != 2) {
                    try { client.load("s3://bucket/key", "v", 8, RequestOptions.INHERIT); throw new AssertionError("LOAD remapped"); }
                    catch (IOException expected) { }
                    if (cluster.remoteHits.get() != 0) throw new AssertionError("remote LOAD");
                }
            }
        }
        for (String zone : new String[] {null, "absent"}) {
            try (Cluster cluster = new Cluster(2, null); TalonClient client = cluster.client(zone)) {
                expectUnavailable(client);
                if (cluster.localHits.get() + cluster.remoteHits.get() != 0) throw new AssertionError("unknown zone dialed");
            }
        }
        for (TalonException.Code code : TalonException.Code.values()) {
            try (Cluster cluster = new Cluster(2, code); TalonClient client = cluster.client("a")) {
                try { read(client); throw new AssertionError("local error hidden"); }
                catch (TalonException expected) { if (expected.code() != code) throw expected; }
                if (cluster.remoteHits.get() != 0) throw new AssertionError("local failure retried remotely");
            }
        }
        try (Cluster cluster = new Cluster(0, null); TalonClient client = cluster.client("a")) {
            expectUnavailable(client);
            cluster.localState.set(2);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (true) {
                try { assertBytes(client, 1); break; }
                catch (TalonException unavailable) {
                    if (unavailable.code() != TalonException.Code.UNAVAILABLE) throw unavailable;
                    if (System.nanoTime() > deadline) throw new AssertionError("local recovery failed");
                    Thread.sleep(25);
                }
            }
            if (cluster.remoteHits.get() != 0) throw new AssertionError("recovery escaped local zone");
        }
        System.out.println("zone affinity: defaults, offline/conflict, recovery, LOAD and terminal errors passed");
    }

    static byte[] read(TalonClient client) throws IOException {
        return client.read("s3://bucket/key", "v", 8, 0, 8, RequestOptions.INHERIT);
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
        TalonClient client(String zone) {
            return TalonClient.connect(address(coordinator), 64, 8, new ZoneOptions(zone, true));
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
