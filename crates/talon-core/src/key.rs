//! Structured, reversible cache keys.
//!
//! The logical addressing unit is a fixed-size **block** (256MB in v1). A
//! [`BlockId`] identifies one block of a source object and is the unit used for
//! placement, the worker block index, and the cache key. Keys are reversible to
//! and from a hierarchical filesystem path (e.g. `/s3/<bucket>/<object>`), which
//! the FUSE client uses.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// A supported blob-storage backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Backend {
    /// Amazon S3 (and S3-compatible stores).
    S3,
    /// Google Cloud Storage.
    Gcs,
    /// Azure Blob Storage.
    Azure,
}

impl Backend {
    /// The path prefix used in the FUSE namespace (without slashes).
    pub fn prefix(&self) -> &'static str {
        match self {
            Backend::S3 => "s3",
            Backend::Gcs => "gcs",
            Backend::Azure => "az",
        }
    }
}

impl fmt::Display for Backend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.prefix())
    }
}

impl FromStr for Backend {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "s3" => Ok(Backend::S3),
            "gcs" => Ok(Backend::Gcs),
            "az" | "azure" => Ok(Backend::Azure),
            other => Err(Error::Other(format!("unknown backend: {other}"))),
        }
    }
}

/// An etag / version guard for a source object.
///
/// Included in a [`BlockId`] so that a source update never causes a stale block
/// to be served: a changed version yields a different key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Version(pub String);

impl Version {
    /// Create a new version token.
    pub fn new(v: impl Into<String>) -> Self {
        Self(v.into())
    }

    /// Return the version as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Identifies a source object in a backend.
///
/// `bucket` is the S3/GCS bucket or the Azure `account/container` scope. The
/// `object_path` is the remaining key within that scope.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectId {
    /// The backing store this object lives in.
    pub backend: Backend,
    /// Bucket (S3/GCS) or account/container scope (Azure).
    pub bucket: String,
    /// Object key/path within the bucket.
    pub object_path: String,
}

impl ObjectId {
    /// Create a new object id.
    pub fn new(backend: Backend, bucket: impl Into<String>, object_path: impl Into<String>) -> Self {
        Self {
            backend,
            bucket: bucket.into(),
            object_path: object_path.into(),
        }
    }

    /// Render as a hierarchical namespace path: `/<prefix>/<bucket>/<object_path>`.
    pub fn to_path(&self) -> String {
        format!(
            "/{}/{}/{}",
            self.backend.prefix(),
            self.bucket,
            self.object_path
        )
    }

    /// Parse an [`ObjectId`] from a namespace path produced by [`to_path`].
    ///
    /// [`to_path`]: ObjectId::to_path
    pub fn from_path(path: &str) -> Result<Self> {
        let trimmed = path.trim_start_matches('/');
        let mut parts = trimmed.splitn(3, '/');
        let prefix = parts
            .next()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Error::Other(format!("missing backend in path: {path}")))?;
        let bucket = parts
            .next()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Error::Other(format!("missing bucket in path: {path}")))?;
        let object_path = parts
            .next()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Error::Other(format!("missing object path in path: {path}")))?;
        Ok(ObjectId::new(prefix.parse::<Backend>()?, bucket, object_path))
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_path())
    }
}

/// A page index within a paged block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PageIndex(pub u32);

/// Identifies one fixed-size block of a source object.
///
/// This is the logical addressing unit for placement, the worker block index,
/// and the cache. Two blocks are equal iff they share object, offset, block
/// size, and version — so a version change produces a distinct key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlockId {
    /// The source object this block belongs to.
    pub object: ObjectId,
    /// Byte offset of the block within the object.
    pub offset: u64,
    /// Block size in bytes (256MB in v1).
    pub block_size: u32,
    /// Source version/etag guard.
    pub version: Version,
}

impl BlockId {
    /// Create a new block id.
    pub fn new(object: ObjectId, offset: u64, block_size: u32, version: Version) -> Self {
        Self {
            object,
            offset,
            block_size,
            version,
        }
    }

    /// Zero-based index of this block within its object.
    pub fn block_index(&self) -> u64 {
        if self.block_size == 0 {
            0
        } else {
            self.offset / self.block_size as u64
        }
    }

    /// Number of pages this block holds for the given page size.
    ///
    /// Returns 0 if `page_size` is 0. The last page may be shorter than
    /// `page_size`; it is still counted.
    pub fn page_count(&self, page_size: u32) -> u32 {
        if page_size == 0 {
            return 0;
        }
        self.block_size.div_ceil(page_size)
    }
}

impl fmt::Display for BlockId {
    /// Render as `<object-path>@<version>#<offset>+<block_size>`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}@{}#{}+{}",
            self.object.to_path(),
            self.version,
            self.offset,
            self.block_size
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_path_roundtrips_all_backends() {
        for (backend, path) in [
            (Backend::S3, "/s3/my-bucket/data/checkpoint.bin"),
            (Backend::Gcs, "/gcs/my-bucket/data/checkpoint.bin"),
            (Backend::Azure, "/az/acct-container/data/checkpoint.bin"),
        ] {
            let obj = ObjectId::from_path(path).unwrap();
            assert_eq!(obj.backend, backend);
            assert_eq!(obj.to_path(), path);
        }
    }

    #[test]
    fn from_path_rejects_incomplete() {
        assert!(ObjectId::from_path("/s3/only-bucket").is_err());
        assert!(ObjectId::from_path("/unknown/b/o").is_err());
        assert!(ObjectId::from_path("/").is_err());
    }

    #[test]
    fn page_count_and_block_index() {
        let obj = ObjectId::new(Backend::S3, "b", "o");
        let block_size = 256 * 1024 * 1024; // 256 MiB
        let id = BlockId::new(obj, block_size as u64 * 3, block_size, Version::new("v1"));
        assert_eq!(id.block_index(), 3);
        assert_eq!(id.page_count(256 * 1024), 1024);
        assert_eq!(id.page_count(4 * 1024 * 1024), 64);
        assert_eq!(id.page_count(0), 0);
    }
}
