package io.milvus.talon;

import java.io.IOException;
import java.util.List;

/** A submitted batch did not complete every file. Omitted input indices succeeded. */
public final class BatchLoadException extends IOException {
    private static final long serialVersionUID = 1L;
    private final List<LoadFailure> failedFiles;
    public BatchLoadException(List<LoadFailure> failedFiles, Throwable cause) {
        super("batch load incomplete for " + failedFiles.size() + " file(s)", cause);
        this.failedFiles = List.copyOf(failedFiles);
    }
    public List<LoadFailure> failedFiles() { return failedFiles; }
}
