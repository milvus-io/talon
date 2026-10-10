package io.milvus.talon;

/** Failed/unconfirmed input file. Index is zero-based; duplicates remain distinct. */
public record LoadFailure(int index, boolean uncertain, String error) {}
