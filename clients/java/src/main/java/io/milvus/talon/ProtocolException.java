package io.milvus.talon;

/**
 * A message could not be decoded, or violated the wire protocol.
 *
 * <p>Distinct from an I/O failure: this means the bytes on the wire were not
 * what the protocol specifies, which usually indicates a version mismatch
 * between this client and the cluster rather than a transient fault. Retrying
 * will not help.
 */
public class ProtocolException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ProtocolException(String message) {
        super(message);
    }

    public ProtocolException(String message, Throwable cause) {
        super(message, cause);
    }
}
