package io.milvus.talon;

/** An object's size and source version. */
public final class ObjectStat {

    private final long size;
    private final String version;

    public ObjectStat(long size, String version) {
        this.size = size;
        this.version = version;
    }

    public long size() {
        return size;
    }

    /** Source ETag; blocks are keyed by it. */
    public String version() {
        return version;
    }

    @Override
    public String toString() {
        return "ObjectStat{size=" + size + ", version=\"" + version + "\"}";
    }
}
