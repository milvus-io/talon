//! Compiles the WAL record schema.
//!
//! The `.proto` is the durable contract (ADR 0003 §9.4), so it is compiled
//! rather than hand-translated: a hand-written encoder can drift from the
//! schema silently, and this file cannot.

fn main() {
    println!("cargo:rerun-if-changed=proto/wal.proto");
    prost_build::compile_protos(&["proto/wal.proto"], &["proto"]).expect("compile proto/wal.proto");
}
