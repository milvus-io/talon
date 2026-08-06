package io.milvus.talon;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Where a block lives: owners in preference order, and the epoch they were
 * computed at.
 *
 * <p>The epoch matters. Observing a different one means the cached placement may
 * be stale, and continuing to use it is the quiet failure mode — reads keep
 * succeeding, from the wrong worker, until something else forces a refresh.
 */
public final class Placement {

    private static final byte[] BLOCK_DOMAIN =
            "talon-cache-maglev-block-v1\0".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] WORKER_DOMAIN =
            "talon-cache-maglev-worker-v1\0".getBytes(StandardCharsets.US_ASCII);
    private static final int MIN_TABLE_SIZE = 4_096;
    private static final int SLOTS_PER_WORKER = 64;

    private final List<String> owners;
    private final long epoch;

    public Placement(List<String> owners, long epoch) {
        this.owners = Collections.unmodifiableList(owners);
        this.epoch = epoch;
    }

    /** Owner node ids, primary first. Resolve to addresses via membership. */
    public List<String> owners() {
        return owners;
    }

    public long epoch() {
        return epoch;
    }

    /** Cold-path convenience wrapper; hot clients cache {@link Table}. */
    public static List<NodeInfo> rank(BlockId block, List<NodeInfo> nodes, int k) {
        return new Table(nodes).rank(block, k);
    }

    /** Deterministic Maglev index rebuilt only when worker membership changes. */
    public static final class Table {
        private final List<NodeInfo> workers;
        private final int[] slots;
        private final int mask;

        public Table(List<NodeInfo> nodes) {
            List<NodeInfo> sorted = new ArrayList<>();
            for (NodeInfo node : nodes) {
                if (node.isWorker()) {
                    sorted.add(node);
                }
            }
            sorted.sort(Comparator.comparing(NodeInfo::id));
            List<NodeInfo> unique = new ArrayList<>(sorted.size());
            for (NodeInfo node : sorted) {
                if (unique.isEmpty()
                        || !unique.get(unique.size() - 1).id().equals(node.id())) {
                    unique.add(node);
                }
            }
            workers = Collections.unmodifiableList(unique);
            if (workers.isEmpty()) {
                slots = new int[0];
                mask = 0;
                return;
            }

            long wanted = Math.max(MIN_TABLE_SIZE, (long) workers.size() * SLOTS_PER_WORKER);
            int tableSize = 1;
            while (tableSize < wanted) {
                if (tableSize >= (1 << 30)) {
                    throw new IllegalArgumentException("cache placement table is too large");
                }
                tableSize <<= 1;
            }
            mask = tableSize - 1;
            slots = new int[tableSize];
            java.util.Arrays.fill(slots, -1);
            int[] next = new int[workers.size()];
            int[] offsets = new int[workers.size()];
            int[] skips = new int[workers.size()];
            for (int i = 0; i < workers.size(); i++) {
                byte[] digest = workerHash(workers.get(i).id());
                offsets[i] = (int) littleEndianLong(digest, 0) & mask;
                skips[i] = ((int) littleEndianLong(digest, 8) | 1) & mask;
            }

            int filled = 0;
            while (filled < tableSize) {
                for (int worker = 0; worker < workers.size() && filled < tableSize; worker++) {
                    int slot;
                    do {
                        slot = (int) (offsets[worker] + (long) next[worker] * skips[worker]) & mask;
                        next[worker]++;
                    } while (slots[slot] != -1);
                    slots[slot] = worker;
                    filled++;
                }
            }
        }

        /** One hash plus one array access: O(1), independent of worker count. */
        public NodeInfo primary(BlockId block) {
            if (slots.length == 0) {
                return null;
            }
            byte[] digest = blockHash(block);
            return workers.get(slots[(int) littleEndianLong(digest, 0) & mask]);
        }

        public List<NodeInfo> rank(BlockId block, int k) {
            int target = Math.min(Math.max(k, 0), workers.size());
            if (target == 0) {
                return Collections.emptyList();
            }
            byte[] digest = blockHash(block);
            int start = (int) littleEndianLong(digest, 0) & mask;
            int step = ((int) littleEndianLong(digest, 8) | 1) & mask;
            List<Integer> selected = new ArrayList<>(target);
            List<NodeInfo> result = new ArrayList<>(target);
            for (int probe = 0; probe < slots.length && result.size() < target; probe++) {
                int worker = slots[(int) (start + (long) probe * step) & mask];
                if (!selected.contains(worker)) {
                    selected.add(worker);
                    result.add(workers.get(worker));
                }
            }
            return Collections.unmodifiableList(result);
        }
    }

    private static byte[] blockHash(BlockId block) {
        try {
            MessageDigest hash = MessageDigest.getInstance("SHA-256");
            ByteArrayOutputStream encoded = new ByteArrayOutputStream();
            encoded.writeBytes(BLOCK_DOMAIN);
            encoded.write(backendTag(block.object().backend()));
            field(encoded, block.object().bucket());
            field(encoded, block.object().key());
            encoded.writeBytes(littleEndianLong(block.offset()));
            encoded.writeBytes(littleEndianInt(block.blockSize()));
            field(encoded, block.version());
            return hash.digest(encoded.toByteArray());
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError("SHA-256 is required by every Java runtime", impossible);
        }
    }

    private static byte[] workerHash(String workerId) {
        try {
            MessageDigest hash = MessageDigest.getInstance("SHA-256");
            ByteArrayOutputStream encoded = new ByteArrayOutputStream();
            encoded.writeBytes(WORKER_DOMAIN);
            field(encoded, workerId);
            return hash.digest(encoded.toByteArray());
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError("SHA-256 is required by every Java runtime", impossible);
        }
    }

    private static void field(ByteArrayOutputStream out, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeBytes(littleEndianLong(bytes.length));
        out.writeBytes(bytes);
    }

    private static byte[] littleEndianLong(long value) {
        return ByteBuffer.allocate(Long.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putLong(value)
                .array();
    }

    private static long littleEndianLong(byte[] bytes, int offset) {
        return ByteBuffer.wrap(bytes, offset, Long.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN)
                .getLong();
    }

    private static byte[] littleEndianInt(int value) {
        return ByteBuffer.allocate(Integer.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(value)
                .array();
    }

    private static int backendTag(ObjectId.Backend backend) {
        switch (backend) {
            case S3:
                return 0;
            case GCS:
                return 1;
            case AZURE:
                return 2;
            default:
                throw new AssertionError("unhandled backend " + backend);
        }
    }

}
