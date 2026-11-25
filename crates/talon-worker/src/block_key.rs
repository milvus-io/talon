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

// The Arc owns both the header and the original UTF-8 bytes. The private
// header uses ASCII varints so all three fields can be borrowed as str slices.
const WIDE_BLOCK_SIZE: u8 = 32;

fn append_number(text: &mut String, mut value: u64) {
    while value >= 64 {
        text.push(char::from((value as u8 & 63) | 64));
        value >>= 6;
    }
    text.push(char::from(value as u8));
}

fn read_number(bytes: &mut &[u8]) -> u64 {
    let mut value = 0;
    for shift in (0..64).step_by(6) {
        let (byte, rest) = bytes.split_first().expect("packed block key header");
        *bytes = rest;
        value |= u64::from(byte & 63) << shift;
        if byte & 64 == 0 {
            return value;
        }
    }
    unreachable!("number exceeds block key header")
}

#[derive(Clone)]
pub(crate) struct BlockKey(Arc<str>);

impl BlockKey {
    pub fn new(id: &dyn BlockIdentity) -> Self {
        let id = id.view();
        let mut text = String::with_capacity(
            16 + id.object.bucket.len() + id.object.object_path.len() + id.version.len(),
        );
        text.push(match id.object.backend {
            Backend::S3 => 's',
            Backend::Gcs => 'g',
            Backend::Azure => 'a',
        });
        if id.block_size.is_power_of_two() {
            text.push(char::from(id.block_size.trailing_zeros() as u8));
        } else {
            text.push(char::from(WIDE_BLOCK_SIZE));
            append_number(&mut text, u64::from(id.block_size));
        }
        append_number(&mut text, id.offset);
        append_number(&mut text, id.object.bucket.len() as u64);
        append_number(&mut text, id.object.object_path.len() as u64);
        text.push_str(id.object.bucket);
        text.push_str(id.object.object_path);
        text.push_str(id.version);
        Self(Arc::from(text))
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
        let bytes = self.0.as_bytes();
        let backend = match bytes[0] {
            b's' => Backend::S3,
            b'g' => Backend::Gcs,
            b'a' => Backend::Azure,
            _ => unreachable!("unrecognized block key backend"),
        };
        let mut fields = &bytes[2..];
        let block_size = if bytes[1] == WIDE_BLOCK_SIZE {
            read_number(&mut fields) as u32
        } else {
            1 << bytes[1]
        };
        let offset = read_number(&mut fields);
        let bucket_len = read_number(&mut fields) as usize;
        let path_len = read_number(&mut fields) as usize;
        let text = &self.0[bytes.len() - fields.len()..];
        let (bucket, rest) = text.split_at(bucket_len);
        let (object_path, version) = rest.split_at(path_len);
        BlockRef {
            object: ObjectRef {
                backend,
                bucket,
                object_path,
            },
            offset,
            block_size,
            version,
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
            BlockId::new(
                ObjectId::new(Backend::Gcs, "桶".repeat(2048), "路径/é".repeat(1024)),
                u64::MAX,
                u32::MAX,
                Version::new("version"),
            ),
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
