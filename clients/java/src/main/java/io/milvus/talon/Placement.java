package io.milvus.talon;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
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

    private static final byte[] DOMAIN =
            "talon-cache-placement-v1\0".getBytes(StandardCharsets.US_ASCII);

    private final List<String> owners;
    private final long epoch;

    public Placement(List<String> owners, long epoch) {
        this.owners = Collections.unmodifiableList(owners);
        this.epoch = epoch;
    }

    /** Owner node ids, highest-weight first. Resolve to addresses via membership. */
    public List<String> owners() {
        return owners;
    }

    public long epoch() {
        return epoch;
    }

    /** Rank cache workers locally using the cross-language HRW v1 contract. */
    public static List<NodeInfo> rank(BlockId block, List<NodeInfo> nodes, int k) {
        if (k <= 0) {
            return Collections.emptyList();
        }
        List<ScoredNode> ranked = new ArrayList<>();
        for (NodeInfo node : nodes) {
            if (node.isWorker()) {
                ranked.add(new ScoredNode(score(block, node.id()), node));
            }
        }
        ranked.sort((a, b) -> {
            int scoreOrder = compareUnsigned(b.score, a.score);
            return scoreOrder != 0 ? scoreOrder : a.node.id().compareTo(b.node.id());
        });
        List<NodeInfo> result = new ArrayList<>(Math.min(k, ranked.size()));
        for (int i = 0; i < Math.min(k, ranked.size()); i++) {
            result.add(ranked.get(i).node);
        }
        return Collections.unmodifiableList(result);
    }

    private static byte[] score(BlockId block, String workerId) {
        try {
            MessageDigest hash = MessageDigest.getInstance("SHA-256");
            ByteArrayOutputStream encoded = new ByteArrayOutputStream();
            encoded.writeBytes(DOMAIN);
            encoded.write(backendTag(block.object().backend()));
            field(encoded, block.object().bucket());
            field(encoded, block.object().key());
            encoded.writeBytes(littleEndianLong(block.offset()));
            encoded.writeBytes(littleEndianInt(block.blockSize()));
            field(encoded, block.version());
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

    private static int compareUnsigned(byte[] left, byte[] right) {
        for (int i = 0; i < left.length; i++) {
            int order = Integer.compare(Byte.toUnsignedInt(left[i]), Byte.toUnsignedInt(right[i]));
            if (order != 0) {
                return order;
            }
        }
        return 0;
    }

    private static final class ScoredNode {
        final byte[] score;
        final NodeInfo node;

        ScoredNode(byte[] score, NodeInfo node) {
            this.score = score;
            this.node = node;
        }
    }
}
