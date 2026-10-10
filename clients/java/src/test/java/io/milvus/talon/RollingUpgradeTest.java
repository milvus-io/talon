package io.milvus.talon;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Socket-level persistent membership acceptance, without an external cluster. */
public final class RollingUpgradeTest {
    public static void main(String[] args) throws Exception {
        replacementAfterFailedRead();
        reusedConnectionFailure("retry-fails");
        reusedConnectionFailure("expired");
        reusedConnectionFailure("protocol");
        for (TalonException.Code code : TalonException.Code.values()) {
            reusedConnectionFailure(code.name());
        }
        try (ServerSocket coordinator = new ServerSocket(0); ServerSocket worker = new ServerSocket(0)) {
            ExecutorService tasks = Executors.newCachedThreadPool();
            AtomicInteger accepts = new AtomicInteger();
            AtomicReference<String> incarnation = new AtomicReference<>("old");
            AtomicBoolean offline = new AtomicBoolean();
            String address = "127.0.0.1:" + worker.getLocalPort();
            tasks.submit(() -> {
                try {
                    while (!worker.isClosed()) {
                        Socket socket = worker.accept();
                        accepts.incrementAndGet();
                        tasks.submit(() -> {
                            try (socket) {
                                DataInputStream in = new DataInputStream(socket.getInputStream());
                                Frame request = Frame.decode(in.readNBytes(Frame.HEADER_LEN));
                                in.readNBytes(request.length());
                                socket.getOutputStream().write(new Frame(Frame.MsgType.GET_RANGE, 0, request.requestId(), 8).encode());
                                socket.getOutputStream().write(new byte[8]);
                            } catch (IOException ignored) { }
                        });
                    }
                } catch (IOException ignored) { }
            });
            tasks.submit(() -> {
                try {
                    while (!coordinator.isClosed()) {
                        Socket socket = coordinator.accept();
                        tasks.submit(() -> {
                            try (socket) {
                                DataInputStream in = new DataInputStream(socket.getInputStream());
                                for (;;) {
                                    byte[] header = in.readNBytes(Frame.HEADER_LEN);
                                    if (header.length == 0) break;
                                    Frame request = Frame.decode(header);
                                    Messages.Response message = Messages.decodeBody(in.readNBytes(request.length()));
                                    Bincode.Writer body = new Bincode.Writer().u16(6);
                                    if (message.tag == Messages.TAG_MEMBERSHIP_QUERY) {
                                        body.variant(Messages.TAG_MEMBERSHIP_LIST).u64(7).u64(9).u64(500).u64(1)
                                            .string("worker").u8(0).u8(0).variant(offline.get() ? 0 : 2);
                                        if (!offline.get()) body.string(incarnation.get()).string(address);
                                    } else throw new AssertionError("unexpected control request");
                                    byte[] bytes = body.toBytes();
                                    socket.getOutputStream().write(new Frame(Frame.MsgType.CONTROL, 0, request.requestId(), bytes.length).encode());
                                    socket.getOutputStream().write(bytes);
                                }
                            } catch (IOException ignored) { }
                        });
                    }
                } catch (IOException ignored) { }
            });
            try (TalonClient client = TalonClient.connect("127.0.0.1:" + coordinator.getLocalPort(), 64)) {
                if (client.read("s3://bucket/key", "v", 8, 0, 8).length != 8) throw new AssertionError("initial read");
                if (client.read("s3://bucket/key", "v", 8, 0, 8).length != 8) throw new AssertionError("stale socket recovery");
                if (accepts.get() != 2) throw new AssertionError("stale socket was not redialed once");
                incarnation.set("new");
                Thread.sleep(550);
                if (client.read("s3://bucket/key", "v", 8, 0, 8).length != 8) throw new AssertionError("recovery");
                offline.set(true);
                Thread.sleep(550);
                try { client.read("s3://bucket/key", "v", 8, 0, 8); throw new AssertionError("offline read succeeded"); }
                catch (TalonException expected) { if (expected.code() != TalonException.Code.UNAVAILABLE) throw expected; }
                if (accepts.get() != 3) throw new AssertionError("offline owner was dialed");
            } finally {
                coordinator.close(); worker.close(); tasks.shutdownNow();
            }
        }
        System.out.println("rolling upgrade: bounded stale-connection retry, terminal failures, incarnation recovery and offline owner passed");
    }

    /** Exercise failures after a successful read has returned a live socket to the pool. */
    private static void reusedConnectionFailure(String mode) throws Exception {
        try (ServerSocket coordinator = new ServerSocket(0); ServerSocket worker = new ServerSocket(0)) {
            ExecutorService tasks = Executors.newFixedThreadPool(2);
            AtomicInteger accepts = new AtomicInteger();
            AtomicInteger reads = new AtomicInteger();
            AtomicInteger discoveries = new AtomicInteger();
            String address = "127.0.0.1:" + worker.getLocalPort();
            Future<?> workerTask = tasks.submit(() -> {
                try {
                    while (!worker.isClosed()) {
                        try (Socket socket = worker.accept()) {
                            accepts.incrementAndGet();
                            DataInputStream in = new DataInputStream(socket.getInputStream());
                            for (;;) {
                                byte[] header = in.readNBytes(Frame.HEADER_LEN);
                                if (header.length == 0) break;
                                Frame request = Frame.decode(header);
                                in.readNBytes(request.length());
                                int call = reads.incrementAndGet();
                                if (call > 1 && mode.equals("retry-fails")) break;
                                if (call == 2 && mode.equals("expired")) {
                                    // Expire the original observation while the reused request is in flight.
                                    Thread.sleep(550);
                                    break;
                                }
                                byte[] payload = new byte[8];
                                int flags = 0;
                                if (call == 2 && mode.equals("protocol")) {
                                    payload = new byte[7]; // Complete frame, wrong range length.
                                } else if (call == 2) {
                                    TalonException.Code code = TalonException.Code.valueOf(mode);
                                    ByteArrayOutputStream error = new ByteArrayOutputStream();
                                    error.write(new byte[]{'T', 'L', 'E', '1'});
                                    error.write(new Bincode.Writer().variant(code.ordinal()).string("worker refusal").toBytes());
                                    payload = error.toByteArray();
                                    flags = Frame.FLAG_ERROR;
                                }
                                socket.getOutputStream().write(new Frame(Frame.MsgType.GET_RANGE, flags, request.requestId(), payload.length).encode());
                                socket.getOutputStream().write(payload);
                            }
                        }
                    }
                } catch (SocketException e) {
                    if (!worker.isClosed()) throw new RuntimeException(e);
                } catch (Exception e) { throw new RuntimeException(e); }
            });
            Future<?> coordinatorTask = tasks.submit(() -> {
                try (Socket socket = coordinator.accept()) {
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    for (;;) {
                        byte[] header = in.readNBytes(Frame.HEADER_LEN);
                        if (header.length == 0) break;
                        Frame request = Frame.decode(header);
                        Messages.Response message = Messages.decodeBody(in.readNBytes(request.length()));
                        if (message.tag != Messages.TAG_MEMBERSHIP_QUERY) throw new AssertionError("unexpected control request");
                        discoveries.incrementAndGet();
                        byte[] payload = new Bincode.Writer().u16(6).variant(Messages.TAG_MEMBERSHIP_LIST)
                            .u64(7).u64(9).u64(500).u64(1).string("worker").u8(0).u8(0)
                            .variant(2).string("instance").string(address).toBytes();
                        socket.getOutputStream().write(new Frame(Frame.MsgType.CONTROL, 0, request.requestId(), payload.length).encode());
                        socket.getOutputStream().write(payload);
                    }
                } catch (Exception e) { throw new RuntimeException(e); }
            });
            try (TalonClient client = TalonClient.connect("127.0.0.1:" + coordinator.getLocalPort(), 64)) {
                client.read("s3://bucket/key", "v", 8, 0, 8);
                try {
                    client.read("s3://bucket/key", "v", 8, 0, 8);
                    throw new AssertionError("failure hidden: " + mode);
                } catch (TalonException expected) {
                    TalonException.Code code = switch (mode) {
                        case "retry-fails", "expired" -> TalonException.Code.UNAVAILABLE;
                        default -> TalonException.Code.valueOf(mode);
                    };
                    if (expected.code() != code) throw expected;
                    if (mode.equals("expired") && !expected.getMessage().contains("expired before read retry")) throw expected;
                } catch (ProtocolException expected) {
                    if (!mode.equals("protocol")) throw expected;
                }
                int expectedAccepts = mode.equals("retry-fails") ? 2 : 1;
                int expectedReads = mode.equals("retry-fails") ? 3 : 2;
                if (accepts.get() != expectedAccepts || reads.get() != expectedReads)
                    throw new AssertionError("unexpected retry count: " + mode);
                if (discoveries.get() != 1) throw new AssertionError("read retry refreshed discovery: " + mode);
            } finally {
                coordinator.close(); worker.close(); tasks.shutdownNow();
            }
            workerTask.get(5, TimeUnit.SECONDS);
            coordinatorTask.get(5, TimeUnit.SECONDS);
        }
    }
    private static void replacementAfterFailedRead() throws Exception {
        try (ServerSocket coordinator = new ServerSocket(0); ServerSocket oldWorker = new ServerSocket(0); ServerSocket newWorker = new ServerSocket(0)) {
            ExecutorService tasks = Executors.newCachedThreadPool();
            AtomicBoolean activated = new AtomicBoolean();
            AtomicInteger oldCalls = new AtomicInteger();
            AtomicInteger newCalls = new AtomicInteger();
            String oldAddress = "127.0.0.1:" + oldWorker.getLocalPort();
            String newAddress = "127.0.0.1:" + newWorker.getLocalPort();
            tasks.submit(() -> {
                try (Socket socket = oldWorker.accept()) {
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    Frame frame = Frame.decode(in.readNBytes(Frame.HEADER_LEN));
                    in.readNBytes(frame.length());
                    oldCalls.incrementAndGet();
                    activated.set(true);
                    // Close without a response: the caller must see EOF.
                } catch (IOException ignored) { }
            });
            tasks.submit(() -> {
                try (Socket socket = newWorker.accept()) {
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    Frame frame = Frame.decode(in.readNBytes(Frame.HEADER_LEN));
                    in.readNBytes(frame.length());
                    newCalls.incrementAndGet();
                    socket.getOutputStream().write(new Frame(Frame.MsgType.GET_RANGE, 0, frame.requestId(), 8).encode());
                    socket.getOutputStream().write(new byte[8]);
                } catch (IOException ignored) { }
            });
            tasks.submit(() -> {
                try {
                    while (!coordinator.isClosed()) {
                        try (Socket socket = coordinator.accept()) {
                            DataInputStream in = new DataInputStream(socket.getInputStream());
                            for (;;) {
                                byte[] header = in.readNBytes(Frame.HEADER_LEN);
                                if (header.length == 0) break;
                                Frame request = Frame.decode(header);
                                Messages.Response message = Messages.decodeBody(in.readNBytes(request.length()));
                                Bincode.Writer body = new Bincode.Writer().u16(6);
                                if (message.tag == Messages.TAG_MEMBERSHIP_QUERY) {
                                    body.variant(Messages.TAG_MEMBERSHIP_LIST).u64(7).u64(9).u64(500).u64(1)
                                        .string("worker").u8(0).u8(0).variant(2)
                                        .string(activated.get() ? "new" : "old")
                                        .string(activated.get() ? newAddress : oldAddress);
                                } else throw new AssertionError("unexpected control request");
                                byte[] bytes = body.toBytes();
                                socket.getOutputStream().write(new Frame(Frame.MsgType.CONTROL, 0, request.requestId(), bytes.length).encode());
                                socket.getOutputStream().write(bytes);
                            }
                        }
                    }
                } catch (IOException ignored) { }
            });
            try (TalonClient client = TalonClient.connect("127.0.0.1:" + coordinator.getLocalPort(), 64)) {
                try { client.read("s3://bucket/key", "v", 8, 0, 8); throw new AssertionError("failure hidden after instance replacement"); }
                catch (TalonException expected) {
                    if (expected.code() != TalonException.Code.UNAVAILABLE || !(expected.getCause() instanceof EOFException)) throw expected;
                }
                if (oldCalls.get() != 1 || newCalls.get() != 0) throw new AssertionError("failed request was resent");
                Thread.sleep(550);
                if (client.read("s3://bucket/key", "v", 8, 0, 8).length != 8 || newCalls.get() != 1) throw new AssertionError("next request failed to recover");
            } finally {
                coordinator.close(); oldWorker.close(); newWorker.close(); tasks.shutdownNow();
            }
        }
    }

}
