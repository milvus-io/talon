package io.milvus.talon;

/** A file to prewarm. Size belongs to the supplied source version; no HEAD is issued. */
public record LoadRequest(ObjectId object, String version, long size) {
    public LoadRequest {
        if (object == null || version == null || version.isBlank() || size < 0) {
            throw new IllegalArgumentException("load requires object, non-empty version and non-negative size");
        }
    }

    public LoadRequest(String uri, String version, long size) {
        this(ObjectId.parse(uri), version, size);
    }
}
