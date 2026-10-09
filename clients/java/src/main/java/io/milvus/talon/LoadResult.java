package io.milvus.talon;

/** Completed prewarm; cached blocks remain subject to ordinary eviction. */
public record LoadResult(long size, long blocks) {}
