package io.milvus.talon;

import java.util.Objects;

/**
 * A block's identity: an object, an offset, a block size, and a version.
 *
 * <p>Placement is computed per block, so the block size must match the workers'
 * configuration — a mismatch addresses blocks that do not exist. The version is
 * the object's source ETag, which is what makes an overwrite invalidate cached
 * blocks rather than serving stale bytes.
 */
public final class BlockId {

    private final ObjectId object;
    private final long offset;
    private final int blockSize;
    private final String version;

    public BlockId(ObjectId object, long offset, int blockSize, String version) {
        this.object = Objects.requireNonNull(object, "object");
        this.offset = offset;
        this.blockSize = blockSize;
        this.version = Objects.requireNonNull(version, "version");
    }

    public ObjectId object() {
        return object;
    }

    public long offset() {
        return offset;
    }

    public int blockSize() {
        return blockSize;
    }

    public String version() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BlockId)) {
            return false;
        }
        BlockId other = (BlockId) o;
        return offset == other.offset
                && blockSize == other.blockSize
                && object.equals(other.object)
                && version.equals(other.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(object, offset, blockSize, version);
    }

    @Override
    public String toString() {
        return object + "@" + offset + "/" + blockSize + "#" + version;
    }
}
