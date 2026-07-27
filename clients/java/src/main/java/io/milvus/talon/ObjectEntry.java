package io.milvus.talon;

/** One listing entry: a mount-relative path and its size. */
public final class ObjectEntry {

    private final String path;
    private final long size;

    public ObjectEntry(String path, long size) {
        this.path = path;
        this.size = size;
    }

    public String path() {
        return path;
    }

    public long size() {
        return size;
    }

    @Override
    public String toString() {
        return "ObjectEntry{path=\"" + path + "\", size=" + size + "}";
    }
}
