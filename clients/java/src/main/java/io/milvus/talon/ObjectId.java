package io.milvus.talon;

import java.util.Objects;

/** An object's address: which backend, which bucket, which key. */
public final class ObjectId {

    /** Backing stores, matching the namespaces the FUSE mount exposes. */
    public enum Backend {
        S3("s3"),
        GCS("gcs"),
        AZURE("az"),
        ALI("ali"),
        TC("tc");

        private final String scheme;

        Backend(String scheme) {
            this.scheme = scheme;
        }
    }

    private final Backend backend;
    private final String bucket;
    private final String key;

    public ObjectId(Backend backend, String bucket, String key) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.bucket = Objects.requireNonNull(bucket, "bucket");
        this.key = Objects.requireNonNull(key, "key");
    }

    /**
     * Parse a {@code scheme://bucket/key} URI.
     *
     * <p>Schemes match the FUSE mount's namespaces, so a path addresses the
     * same object through either client.
     */
    public static ObjectId parse(String uri) {
        int sep = uri.indexOf("://");
        if (sep < 0) {
            throw new IllegalArgumentException(
                    "expected a scheme://bucket/key URI, got \"" + uri
                            + "\" (schemes: s3, gcs, az, ali, tc)");
        }
        String scheme = uri.substring(0, sep);
        String rest = uri.substring(sep + 3);
        Backend backend;
        switch (scheme) {
            case "s3":
                backend = Backend.S3;
                break;
            case "gcs":
                backend = Backend.GCS;
                break;
            case "az":
                backend = Backend.AZURE;
                break;
            case "ali":
                backend = Backend.ALI;
                break;
            case "tc":
                backend = Backend.TC;
                break;
            default:
                throw new IllegalArgumentException(
                        "unknown backend scheme \"" + scheme + "\". Expected s3, gcs, az, ali, or tc");
        }
        int slash = rest.indexOf('/');
        if (slash < 0) {
            throw new IllegalArgumentException(
                    "URI is missing an object key: \"" + uri + "\" (expected " + scheme
                            + "://bucket/key)");
        }
        String bucket = rest.substring(0, slash);
        String key = rest.substring(slash + 1);
        if (bucket.isEmpty()) {
            throw new IllegalArgumentException("URI has an empty bucket: \"" + uri + "\"");
        }
        if (key.isEmpty()) {
            throw new IllegalArgumentException("URI has an empty object key: \"" + uri + "\"");
        }
        return new ObjectId(backend, bucket, key);
    }

    public Backend backend() {
        return backend;
    }

    public String bucket() {
        return bucket;
    }

    public String key() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ObjectId)) {
            return false;
        }
        ObjectId other = (ObjectId) o;
        return backend == other.backend
                && bucket.equals(other.bucket)
                && key.equals(other.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(backend, bucket, key);
    }

    @Override
    public String toString() {
        return backend.scheme + "://" + bucket + "/" + key;
    }
}
