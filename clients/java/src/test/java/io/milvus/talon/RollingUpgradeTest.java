package io.milvus.talon;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Socket-level persistent membership acceptance, without an external cluster. */
public final class RollingUpgradeTest {
    public static void main(String[] args) throws Exception {
        replacementAfterFailedRead();
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
                if (client.read("s3://bucket/key", "v", 0, 8).length != 8) throw new AssertionError("initial read");
                try { client.read("s3://bucket/key", "v", 0, 8); throw new AssertionError("hidden resend"); }
                catch (TalonException expected) { if (expected.code() != TalonException.Code.UNAVAILABLE) throw expected; }
                if (accepts.get() != 1) throw new AssertionError("redial after failed reused connection");
                incarnation.set("new");
                Thread.sleep(550);
                if (client.read("s3://bucket/key", "v", 0, 8).length != 8) throw new AssertionError("recovery");
                offline.set(true);
                Thread.sleep(550);
                try { client.read("s3://bucket/key", "v", 0, 8); throw new AssertionError("offline read succeeded"); }
                catch (TalonException expected) { if (expected.code() != TalonException.Code.UNAVAILABLE) throw expected; }
                if (accepts.get() != 2) throw new AssertionError("offline owner was dialed");
            } finally {
                coordinator.close(); worker.close(); tasks.shutdownNow();
            }
        }
        System.out.println("rolling upgrade: stale connection fails once, same-address incarnation recovers, offline owner is retained");
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
                try { client.read("s3://bucket/key", "v", 0, 8); throw new AssertionError("failure hidden after instance replacement"); }
                catch (TalonException expected) {
                    if (expected.code() != TalonException.Code.UNAVAILABLE || !(expected.getCause() instanceof EOFException)) throw expected;
                }
                if (oldCalls.get() != 1 || newCalls.get() != 0) throw new AssertionError("failed request was resent");
                Thread.sleep(550);
                if (client.read("s3://bucket/key", "v", 0, 8).length != 8 || newCalls.get() != 1) throw new AssertionError("next request failed to recover");
            } finally {
                coordinator.close(); oldWorker.close(); newWorker.close(); tasks.shutdownNow();
            }
        }
    }

}
