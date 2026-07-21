//! Reversible mapping between FUSE paths and Talon object/block keys.
//!
//! The mount exposes one top-level directory per backend namespace — `/s3`,
//! `/gcs`, `/az` — under which paths mirror the object hierarchy:
//!
//! ```text
//! /s3/<bucket>/<object key...>
//! /gcs/<bucket>/<object key...>
//! /az/<account-or-container>/<object key...>
//! ```
//!
//! [`path_to_object`] parses such a path into an [`ObjectId`]; [`object_to_path`]
//! is its inverse. For an open file, [`resolve_read`] maps a byte `offset` to
//! the [`BlockId`] that contains it plus the offset *within* that block, given
//! the file's block size and version (supplied by a coordinator `HEAD`).
//!
//! Parsing rejects ambiguous paths (empty components, `.`/`..`, absolute object
//! keys) so odd object names cannot escape their namespace.

use talon_core::{Backend, BlockId, Error, ObjectId, Result, Version};

/// Where a single byte offset within a file lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadTarget {
    /// The block containing the requested offset.
    pub block: BlockId,
    /// Offset of the requested byte within `block` (`0..block_size`).
    pub offset_in_block: u32,
}

/// Parse a mount-relative path into an [`ObjectId`].
///
/// The path must be `/<backend>/<bucket>/<object key...>` with at least one
/// non-empty object-key component. Returns [`Error::Other`] for malformed or
/// ambiguous paths.
pub fn path_to_object(path: &str) -> Result<ObjectId> {
    let trimmed = path.trim_start_matches('/');
    let mut parts = trimmed.split('/');

    let backend: Backend = parts
        .next()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::Other(format!("missing backend in path: {path:?}")))?
        .parse()?;

    let bucket = parts
        .next()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::Other(format!("missing bucket in path: {path:?}")))?;

    let rest: Vec<&str> = parts.collect();
    if rest.is_empty() || rest.iter().any(|c| c.is_empty() || *c == "." || *c == "..") {
        return Err(Error::Other(format!(
            "invalid or ambiguous object path: {path:?}"
        )));
    }
    let object_path = rest.join("/");

    Ok(ObjectId::new(backend, bucket, object_path))
}

/// Render an [`ObjectId`] back into its mount-relative path. Inverse of
/// [`path_to_object`].
pub fn object_to_path(obj: &ObjectId) -> String {
    obj.to_path()
}

/// Resolve a read at `offset` into the block that holds it.
///
/// `block_size` is the file's logical block size and `version` its current
/// source version/etag. The returned [`ReadTarget`] names the [`BlockId`] whose
/// range covers `offset` and the offset within that block.
pub fn resolve_read(
    obj: &ObjectId,
    offset: u64,
    block_size: u32,
    version: &Version,
) -> Result<ReadTarget> {
    if block_size == 0 {
        return Err(Error::Other("block_size must be > 0".into()));
    }
    let bs = block_size as u64;
    let block_start = (offset / bs) * bs;
    let offset_in_block = (offset - block_start) as u32;
    let block = BlockId::new(obj.clone(), block_start, block_size, version.clone());
    Ok(ReadTarget {
        block,
        offset_in_block,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_roundtrips_all_backends() {
        for (path, backend) in [
            ("/s3/bucket/data/checkpoint.bin", Backend::S3),
            ("/gcs/bucket/nested/dir/file.parquet", Backend::Gcs),
            ("/az/acct-container/logs/2026/07/21.log", Backend::Azure),
        ] {
            let obj = path_to_object(path).unwrap();
            assert_eq!(obj.backend, backend);
            assert_eq!(object_to_path(&obj), path);
        }
    }

    #[test]
    fn rejects_ambiguous_paths() {
        for bad in [
            "/s3/bucket",          // no object key
            "/s3/bucket/",         // trailing empty
            "/s3//key",            // empty bucket
            "/s3/bucket/a//b",     // empty middle component
            "/s3/bucket/../etc",   // parent escape
            "/s3/bucket/./x",      // current-dir component
            "/unknown/bucket/key", // bad backend
            "/",                   // empty
        ] {
            assert!(path_to_object(bad).is_err(), "should reject {bad:?}");
        }
    }

    #[test]
    fn odd_object_names_are_preserved() {
        // Spaces, unicode, and symbols in the key survive the round-trip.
        let path = "/s3/bucket/weird name (v2)/日本語.bin";
        let obj = path_to_object(path).unwrap();
        assert_eq!(obj.object_path, "weird name (v2)/日本語.bin");
        assert_eq!(object_to_path(&obj), path);
    }

    #[test]
    fn resolve_read_maps_offset_to_block() {
        let obj = ObjectId::new(Backend::S3, "b", "o");
        let bs: u32 = 256 << 20; // 256 MiB
        let v = Version::new("etag-1");

        // Offset in the first block.
        let t = resolve_read(&obj, 10, bs, &v).unwrap();
        assert_eq!(t.block.offset, 0);
        assert_eq!(t.offset_in_block, 10);

        // Offset in the third block.
        let off = bs as u64 * 2 + 4096;
        let t = resolve_read(&obj, off, bs, &v).unwrap();
        assert_eq!(t.block.offset, bs as u64 * 2);
        assert_eq!(t.block.block_index(), 2);
        assert_eq!(t.offset_in_block, 4096);

        // Exact block boundary belongs to the next block, offset 0.
        let t = resolve_read(&obj, bs as u64, bs, &v).unwrap();
        assert_eq!(t.block.offset, bs as u64);
        assert_eq!(t.offset_in_block, 0);
    }

    #[test]
    fn resolve_read_rejects_zero_block_size() {
        let obj = ObjectId::new(Backend::S3, "b", "o");
        assert!(resolve_read(&obj, 0, 0, &Version::new("v")).is_err());
    }
}
