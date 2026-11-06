//! Compact resident identity. Public keys and journal JSON retain their schema.
use serde::{Deserialize, Serialize};
use std::{borrow::Borrow, hash::Hash, sync::Arc};
use talon_core::{Backend, BlockId, ObjectId, Version};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
pub(crate) struct ObjectRef<'a> {
    pub backend: Backend,
    pub bucket: &'a str,
    pub object_path: &'a str,
}

impl ObjectRef<'_> {
    pub fn materialize(self) -> ObjectId {
        ObjectId::new(self.backend, self.bucket, self.object_path)
    }
}

pub(crate) trait ObjectIdentity {
    fn object_view(&self) -> ObjectRef<'_>;
}
impl ObjectIdentity for ObjectId {
    fn object_view(&self) -> ObjectRef<'_> {
        ObjectRef {
            backend: self.backend,
            bucket: &self.bucket,
            object_path: &self.object_path,
        }
    }
}
impl ObjectIdentity for ObjectRef<'_> {
    fn object_view(&self) -> ObjectRef<'_> {
        *self
    }
}
impl Hash for dyn ObjectIdentity + '_ {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.object_view().hash(state);
    }
}
impl PartialEq for dyn ObjectIdentity + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.object_view() == other.object_view()
    }
}
impl Eq for dyn ObjectIdentity + '_ {}

/// Retains one metadata identity per checked object, until its last unit leaves.
#[derive(Clone)]
pub(crate) struct ObjectKey(pub BlockKey);

impl ObjectIdentity for ObjectKey {
    fn object_view(&self) -> ObjectRef<'_> {
        self.0.view().object
    }
}
impl<'a> Borrow<dyn ObjectIdentity + 'a> for ObjectKey {
    fn borrow(&self) -> &(dyn ObjectIdentity + 'a) {
        self
    }
}
impl Hash for ObjectKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.object_view().hash(state);
    }
}
impl PartialEq for ObjectKey {
    fn eq(&self, other: &Self) -> bool {
        self.object_view() == other.object_view()
    }
}
impl Eq for ObjectKey {}
impl std::fmt::Debug for ObjectKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.object_view().fmt(f)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
pub(crate) struct BlockRef<'a> {
    pub object: ObjectRef<'a>,
    pub offset: u64,
    pub block_size: u32,
    pub version: &'a str,
}

impl BlockRef<'_> {
    pub fn materialize(self) -> BlockId {
        BlockId::new(
            self.object.materialize(),
            self.offset,
            self.block_size,
            Version::new(self.version),
        )
    }

    pub fn page_count(self, page_size: u32) -> u32 {
        if page_size == 0 {
            0
        } else {
            self.block_size.div_ceil(page_size)
        }
    }
}

pub(crate) trait BlockIdentity: Send + Sync {
    fn view(&self) -> BlockRef<'_>;
}

impl BlockIdentity for BlockRef<'_> {
    fn view(&self) -> BlockRef<'_> {
        *self
    }
}

impl BlockIdentity for BlockId {
    fn view(&self) -> BlockRef<'_> {
        BlockRef {
            object: ObjectRef {
                backend: self.object.backend,
                bucket: &self.object.bucket,
                object_path: &self.object.object_path,
            },
            offset: self.offset,
            block_size: self.block_size,
            version: self.version.as_str(),
        }
    }
}

impl Hash for dyn BlockIdentity + '_ {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.view().hash(state);
    }
}
impl PartialEq for dyn BlockIdentity + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.view() == other.view()
    }
}
impl Eq for dyn BlockIdentity + '_ {}

struct Packed {
    text: Box<str>,
    bucket_end: usize,
    path_end: usize,
    offset: u64,
    block_size: u32,
    backend: Backend,
}

#[derive(Clone)]
pub(crate) struct BlockKey(Arc<Packed>);

impl BlockKey {
    pub fn new(id: &dyn BlockIdentity) -> Self {
        let id = id.view();
        let mut text = String::with_capacity(
            id.object.bucket.len() + id.object.object_path.len() + id.version.len(),
        );
        text.push_str(id.object.bucket);
        let bucket_end = text.len();
        text.push_str(id.object.object_path);
        let path_end = text.len();
        text.push_str(id.version);
        Self(Arc::new(Packed {
            text: text.into_boxed_str(),
            bucket_end,
            path_end,
            offset: id.offset,
            block_size: id.block_size,
            backend: id.object.backend,
        }))
    }

    pub fn materialize(&self) -> BlockId {
        self.view().materialize()
    }

    #[cfg(test)]
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    #[cfg(test)]
    pub fn downgrade(&self) -> impl Fn() -> bool + 'static {
        let weak = Arc::downgrade(&self.0);
        move || weak.upgrade().is_some()
    }
}

impl BlockIdentity for BlockKey {
    fn view(&self) -> BlockRef<'_> {
        BlockRef {
            object: ObjectRef {
                backend: self.0.backend,
                bucket: &self.0.text[..self.0.bucket_end],
                object_path: &self.0.text[self.0.bucket_end..self.0.path_end],
            },
            offset: self.0.offset,
            block_size: self.0.block_size,
            version: &self.0.text[self.0.path_end..],
        }
    }
}
impl<'a> Borrow<dyn BlockIdentity + 'a> for BlockKey {
    fn borrow(&self) -> &(dyn BlockIdentity + 'a) {
        self
    }
}
impl Hash for BlockKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.view().hash(state);
    }
}
impl PartialEq for BlockKey {
    fn eq(&self, other: &Self) -> bool {
        self.view() == other.view()
    }
}
impl Eq for BlockKey {}
impl std::fmt::Debug for BlockKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.view().fmt(f)
    }
}
impl Serialize for BlockKey {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.view().serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for BlockKey {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        BlockId::deserialize(deserializer).map(|id| Self::new(&id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn packed_keys_preserve_exact_identity_and_json() {
        let key = |bucket, path, version| {
            BlockId::new(
                ObjectId::new(Backend::Azure, bucket, path),
                17,
                123,
                Version::new(version),
            )
        };
        let ids = [
            key("a", "bc", "d"),
            key("ab", "c", "d"),
            key("a", "b", "cd"),
            key("桶", "路径/é", "版本"),
        ];
        let mut map = HashMap::new();
        for (i, id) in ids.iter().enumerate() {
            let packed = BlockKey::new(id);
            assert_eq!(packed.materialize(), *id);
            let json = serde_json::to_vec(&packed).unwrap();
            assert_eq!(json, serde_json::to_vec(id).unwrap());
            assert_eq!(serde_json::from_slice::<BlockKey>(&json).unwrap(), packed);
            map.insert(packed, i);
        }
        for (i, id) in ids.iter().enumerate() {
            assert_eq!(map.get(id as &dyn BlockIdentity), Some(&i));
        }
        assert_eq!(map.len(), ids.len());
    }
}
