//! Mapping an object to its write shard (ADR 0003 §9.1).
//!
//! A write shard is an ownership and recovery group, not a physical split of a
//! file. Every block and every generation of one file maps to the same shard,
//! and therefore to the same primary owner.
//!
//! # Why this cannot reuse read placement
//!
//! The read cache places blocks by `BlockId`, which includes the object version.
//! That is right for immutable read-cache blocks and wrong here, because **a
//! write produces a new version — so the very act a write lease exists to
//! arbitrate would move the range out from under its holder**.
//!
//! The ADR measured it: at 1000 objects differing only in version, 743/1000
//! change owner at 4 nodes, 875 at 8, 932 at 16 — matching `(N-1)/N`, ordinary
//! rehashing rather than an artifact of the test keys.
//!
//! So the write identity excludes version, ETag, block offset, block size, and
//! page index:
//!
//! ```text
//! (namespace_id, backend, bucket, object_path)
//! ```
//!
//! # Why the hash is named rather than derived
//!
//! > They must use an explicitly specified, cross-version-stable hash rather
//! > than Rust's `DefaultHasher`.
//!
//! `DefaultHasher`'s output is documented as unstable across Rust releases, so a
//! toolchain bump would silently reshard every namespace — every object would
//! route to a worker that does not hold its dirty data. `xxh3_64` is a specified
//! algorithm with a fixed output for a given input, which is what a durable
//! routing table needs.
//!
//! # Why the encoding is length-delimited
//!
//! Plain concatenation makes `("ab", "c")` and `("a", "bc")` hash identically.
//! Two unrelated objects would share a shard for no reason, and the mapping
//! would depend on where a separator happened to fall in a path. Length-prefixing
//! each field makes the encoding injective.

use core::fmt;

use crate::error::{MetadataError, MetadataResult};

/// Version of the shard-hash scheme itself.
///
/// Mixed into the hash so that changing the scheme changes every mapping
/// deliberately, rather than leaving old and new coordinators computing
/// different shards from the same inputs while both believe they are correct.
pub const SHARD_SCHEME_VERSION: u32 = 1;

/// Default write shards per namespace (ADR 0003 §9.1).
///
/// > the default provides 64 primary shards per worker at 64 workers before
/// > replication is considered.
pub const DEFAULT_SHARD_COUNT: u32 = 4096;

/// Smallest permitted shard count.
pub const MIN_SHARD_COUNT: u32 = 256;

/// Largest permitted shard count.
pub const MAX_SHARD_COUNT: u32 = 16_384;

/// The stable identity a write shard is computed from.
///
/// Deliberately does not carry a version: see the module docs.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WriteIdentity<'a> {
    /// Namespace owning the object.
    pub namespace: &'a str,
    /// Backend scheme, e.g. `s3`.
    pub backend: &'a str,
    /// Bucket or container.
    pub bucket: &'a str,
    /// Object path within the bucket.
    pub object_path: &'a str,
}

/// A write shard index within a namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WriteShard(u32);

impl WriteShard {
    /// The raw shard index.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for WriteShard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Durable configuration of a namespace's write-shard routing.
///
/// > The hash algorithm, encoding, salt, shard count, and scheme version form
/// > durable cluster configuration [...] Changing any of them is a resharding
/// > operation, not a normal configuration update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardConfig {
    shard_count: u32,
    salt: u64,
    scheme_version: u32,
}

impl ShardConfig {
    /// Configuration with the default shard count and a namespace salt.
    pub fn new(salt: u64) -> Self {
        Self {
            shard_count: DEFAULT_SHARD_COUNT,
            salt,
            scheme_version: SHARD_SCHEME_VERSION,
        }
    }

    /// Configuration with an explicit shard count.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataError::InvalidRecord`] unless `shard_count` is a power
    /// of two within [`MIN_SHARD_COUNT`]..=[`MAX_SHARD_COUNT`].
    ///
    /// The power-of-two requirement is not decoration: it keeps `mod
    /// shard_count` a mask, and it makes any future resharding a clean split or
    /// merge of existing shards rather than a full remap.
    pub fn with_shard_count(salt: u64, shard_count: u32) -> MetadataResult<Self> {
        if !(MIN_SHARD_COUNT..=MAX_SHARD_COUNT).contains(&shard_count) {
            return Err(MetadataError::InvalidRecord {
                detail: format!(
                    "shard count {shard_count} is outside {MIN_SHARD_COUNT}..={MAX_SHARD_COUNT}"
                ),
            });
        }
        if !shard_count.is_power_of_two() {
            return Err(MetadataError::InvalidRecord {
                detail: format!("shard count {shard_count} is not a power of two"),
            });
        }
        Ok(Self {
            shard_count,
            salt,
            scheme_version: SHARD_SCHEME_VERSION,
        })
    }

    /// Number of shards in this namespace.
    pub const fn shard_count(&self) -> u32 {
        self.shard_count
    }

    /// Namespace salt mixed into the hash.
    pub const fn salt(&self) -> u64 {
        self.salt
    }

    /// Scheme version this configuration was created under.
    pub const fn scheme_version(&self) -> u32 {
        self.scheme_version
    }

    /// The shard owning `identity`.
    ///
    /// Pure and deterministic: the same identity and configuration always yield
    /// the same shard, in any process and any build.
    pub fn shard_for(&self, identity: &WriteIdentity<'_>) -> WriteShard {
        let digest = xxhash_rust::xxh3::xxh3_64(&self.encode(identity));
        // shard_count is a power of two, so this is a mask rather than a
        // division, and the low bits of xxh3 are well distributed.
        WriteShard((digest % u64::from(self.shard_count)) as u32)
    }

    /// Length-delimited encoding of the hash input.
    ///
    /// Every variable-length field is prefixed with its length, so the encoding
    /// is injective: no two distinct identities can produce the same bytes.
    /// Plain concatenation would collide `("ab", "c")` with `("a", "bc")`.
    fn encode(&self, identity: &WriteIdentity<'_>) -> Vec<u8> {
        let mut buf = Vec::with_capacity(
            32 + identity.namespace.len()
                + identity.backend.len()
                + identity.bucket.len()
                + identity.object_path.len(),
        );
        buf.extend_from_slice(&self.scheme_version.to_le_bytes());
        buf.extend_from_slice(&self.salt.to_le_bytes());
        for field in [
            identity.namespace,
            identity.backend,
            identity.bucket,
            identity.object_path,
        ] {
            buf.extend_from_slice(&(field.len() as u64).to_le_bytes());
            buf.extend_from_slice(field.as_bytes());
        }
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity<'a>(bucket: &'a str, path: &'a str) -> WriteIdentity<'a> {
        WriteIdentity {
            namespace: "ns",
            backend: "s3",
            bucket,
            object_path: path,
        }
    }

    #[test]
    fn the_same_identity_always_lands_on_the_same_shard() {
        let config = ShardConfig::new(0x1234);
        let id = identity("data", "a/b/c.bin");
        let first = config.shard_for(&id);
        for _ in 0..100 {
            assert_eq!(config.shard_for(&id), first);
        }
    }

    #[test]
    fn the_shard_is_stable_across_versions_of_the_same_object() {
        // The property the whole section turns on. A write produces a new
        // version, so if the shard moved with the version, the lease would
        // relocate on the very operation it exists to arbitrate. The identity
        // has no version field at all, which is how this is guaranteed rather
        // than merely tested.
        let config = ShardConfig::new(7);
        let id = identity("data", "checkpoint.bin");
        let shard = config.shard_for(&id);

        // There is no way to express a version here -- that is the point. The
        // same path resolves identically no matter how many times it is
        // rewritten.
        assert_eq!(config.shard_for(&identity("data", "checkpoint.bin")), shard);
    }

    #[test]
    fn field_boundaries_are_unambiguous() {
        // Plain concatenation would make these identical, silently co-locating
        // two unrelated objects and making the mapping depend on where a
        // separator happens to fall.
        let config = ShardConfig::new(0);
        let ab_c = config.shard_for(&identity("ab", "c"));
        let a_bc = config.shard_for(&identity("a", "bc"));
        assert_ne!(
            ab_c, a_bc,
            "bucket/path boundary must be part of the encoding"
        );
    }

    #[test]
    fn the_namespace_participates_in_the_hash() {
        // §9.1 includes the namespace so two namespaces sharing a bucket do not
        // contend for one shard's ownership.
        let config = ShardConfig::new(0);
        let a = WriteIdentity {
            namespace: "alpha",
            backend: "s3",
            bucket: "data",
            object_path: "x.bin",
        };
        let b = WriteIdentity {
            namespace: "beta",
            ..a.clone()
        };
        assert_ne!(config.shard_for(&a), config.shard_for(&b));
    }

    #[test]
    fn the_salt_changes_the_mapping() {
        let id = identity("data", "x.bin");
        assert_ne!(
            ShardConfig::new(1).shard_for(&id),
            ShardConfig::new(2).shard_for(&id)
        );
    }

    #[test]
    fn the_scheme_version_is_mixed_in() {
        // Encoded so that a future scheme change relocates mappings
        // deliberately, instead of leaving two versions computing different
        // shards from identical inputs while both believe they are right.
        let config = ShardConfig::new(0);
        let encoded = config.encode(&identity("data", "x.bin"));
        assert_eq!(
            &encoded[..4],
            &SHARD_SCHEME_VERSION.to_le_bytes(),
            "the scheme version must lead the hash input"
        );
    }

    #[test]
    fn every_shard_is_within_the_configured_count() {
        let config = ShardConfig::with_shard_count(0, 256).expect("valid count");
        for i in 0..5_000 {
            let path = format!("obj-{i}.bin");
            let shard = config.shard_for(&identity("data", &path));
            assert!(
                shard.get() < 256,
                "shard {shard} outside the configured 256"
            );
        }
    }

    #[test]
    fn shard_counts_must_be_powers_of_two_within_range() {
        assert!(ShardConfig::with_shard_count(0, 4096).is_ok());
        assert!(ShardConfig::with_shard_count(0, 256).is_ok());
        assert!(ShardConfig::with_shard_count(0, 16_384).is_ok());

        // Out of range.
        assert!(ShardConfig::with_shard_count(0, 128).is_err());
        assert!(ShardConfig::with_shard_count(0, 32_768).is_err());
        // In range but not a power of two: mod stops being a mask and a future
        // reshard stops being a clean split.
        assert!(ShardConfig::with_shard_count(0, 1000).is_err());
        assert!(ShardConfig::with_shard_count(0, 4097).is_err());
    }

    #[test]
    fn the_default_count_is_four_thousand_and_ninety_six() {
        assert_eq!(DEFAULT_SHARD_COUNT, 4096);
        assert_eq!(ShardConfig::new(0).shard_count(), 4096);
    }

    #[test]
    fn shard_assignments_are_pinned_to_known_values() {
        // The property the in-process determinism tests cannot reach: these
        // literals were computed once and committed, so a change of hash
        // algorithm, field order, encoding, or salt handling fails here rather
        // than silently resharding every namespace on a toolchain bump.
        //
        // If this test fails, that is a resharding event, not a test to update:
        // every object would route to a worker that does not hold its dirty
        // data. Changing it requires the offline procedure in ADR 0003 §9.1 and
        // a bump of SHARD_SCHEME_VERSION.
        let config = ShardConfig::with_shard_count(0, 4096).expect("valid");
        let cases = [
            ("data", "checkpoint.bin"),
            ("data", "a/b/c.bin"),
            ("other-bucket", "checkpoint.bin"),
        ];
        let actual: Vec<u32> = cases
            .iter()
            .map(|(bucket, path)| config.shard_for(&identity(bucket, path)).get())
            .collect();
        assert_eq!(
            actual,
            vec![PINNED_SHARDS[0], PINNED_SHARDS[1], PINNED_SHARDS[2]],
            "write-shard mapping changed; see the comment above before touching these values"
        );
    }

    /// Committed shard assignments for the cases in the test above.
    const PINNED_SHARDS: [u32; 3] = [521, 3352, 2724];

    #[test]
    fn the_mapping_is_reasonably_uniform() {
        // Not a statistical proof -- a smoke test that the hash is not
        // degenerate. A mapping that piled objects into a few shards would
        // concentrate write ownership on a few workers and defeat the point of
        // sharding at all.
        let config = ShardConfig::with_shard_count(0, 256).expect("valid");
        let mut counts = vec![0u32; 256];
        for i in 0..25_600 {
            let path = format!("prefix/{i}/object.bin");
            counts[config.shard_for(&identity("data", &path)).get() as usize] += 1;
        }
        let empty = counts.iter().filter(|c| **c == 0).count();
        assert_eq!(
            empty, 0,
            "{empty} shards received nothing from 25600 objects"
        );
        let max = *counts.iter().max().expect("non-empty");
        assert!(
            max < 250,
            "worst shard took {max} of 25600 objects across 256 shards (expected ~100)"
        );
    }
}
