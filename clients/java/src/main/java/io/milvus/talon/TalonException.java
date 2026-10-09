package io.milvus.talon;

import java.io.IOException;

/** Structured failure. Fallback eligibility never performs origin access. */
public final class TalonException extends IOException {
    private static final long serialVersionUID = 1L;
    public enum Code { UNKNOWN, INVALID_REQUEST, NOT_FOUND, CACHE_MISS, UNAVAILABLE,
        TIMEOUT, VERSION_MISMATCH, ORIGIN, INTERNAL, RATE_LIMITED }
    private final Code code;
    public TalonException(Code code, String message) { super(message); this.code = code; }
    public TalonException(Code code, String message, Throwable cause) { super(message, cause); this.code = code; }
    public Code code() { return code; }
    public boolean fallbackEligible() { return code == Code.UNAVAILABLE || code == Code.TIMEOUT; }
    static TalonException decode(Bincode.Reader r) {
        int value = r.variant();
        if (value >= Code.values().length) throw new ProtocolException("unknown error code " + value);
        TalonException error = new TalonException(Code.values()[value], r.string());
        if (r.remaining() != 0) throw new ProtocolException("trailing error bytes");
        return error;
    }
    static TalonException transport(IOException cause) {
        if (cause instanceof TalonException typed) return typed;
        return new TalonException(cause instanceof java.net.SocketTimeoutException ? Code.TIMEOUT : Code.UNAVAILABLE, cause.getMessage(), cause);
    }
}
