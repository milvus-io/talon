//! One shared block identity per version; page membership stays in BlockIndex.
use super::BlockKey;
use crate::block_key::BlockIdentity;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};

#[derive(Default, PartialEq, Eq)]
pub(super) struct VersionIndex {
    first: HashMap<u64, BlockKey>,
    overflow: HashMap<u64, Vec<BlockKey>>,
}

fn digest(block: &dyn BlockIdentity) -> u64 {
    let block = block.view();
    let mut hash = DefaultHasher::new();
    block.object.hash(&mut hash);
    block.offset.hash(&mut hash);
    block.block_size.hash(&mut hash);
    hash.finish()
}

fn same_logical(a: &dyn BlockIdentity, b: &dyn BlockIdentity) -> bool {
    let (a, b) = (a.view(), b.view());
    a.object == b.object && a.offset == b.offset && a.block_size == b.block_size
}

impl VersionIndex {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            first: HashMap::with_capacity(capacity),
            overflow: HashMap::new(),
        }
    }
    pub fn len(&self) -> usize {
        self.first.len()
    }
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.first.is_empty()
    }
    #[cfg(test)]
    pub fn capacity(&self) -> usize {
        self.first.capacity()
    }
    pub fn insert(&mut self, block: BlockKey) {
        self.insert_at(digest(&block), block);
    }
    fn insert_at(&mut self, hash: u64, block: BlockKey) {
        use std::collections::hash_map::Entry;
        match self.first.entry(hash) {
            Entry::Vacant(entry) => {
                entry.insert(block);
            }
            Entry::Occupied(entry) if entry.get() != &block => {
                let rest = self.overflow.entry(hash).or_default();
                if !rest.contains(&block) {
                    rest.push(block);
                }
            }
            Entry::Occupied(_) => {}
        }
    }
    pub fn blocks<'a>(
        &'a self,
        block: &'a dyn BlockIdentity,
    ) -> impl Iterator<Item = &'a BlockKey> {
        self.blocks_at(digest(block), block)
    }
    fn blocks_at<'a>(
        &'a self,
        hash: u64,
        block: &'a dyn BlockIdentity,
    ) -> impl Iterator<Item = &'a BlockKey> {
        self.first
            .get(&hash)
            .into_iter()
            .chain(self.overflow.get(&hash).into_iter().flatten())
            .filter(move |candidate| same_logical(*candidate, block))
    }
    pub fn remove(&mut self, block: &dyn BlockIdentity) {
        self.remove_at(digest(block), block);
    }
    fn remove_at(&mut self, hash: u64, block: &dyn BlockIdentity) {
        if self
            .first
            .get(&hash)
            .is_some_and(|first| first.view() == block.view())
        {
            match self.overflow.get_mut(&hash).and_then(Vec::pop) {
                Some(next) => {
                    self.first.insert(hash, next);
                }
                None => {
                    self.first.remove(&hash);
                }
            }
        } else if let Some(rest) = self.overflow.get_mut(&hash) {
            if let Some(position) = rest.iter().position(|other| other.view() == block.view()) {
                rest.swap_remove(position);
            }
        }
        if self.overflow.get(&hash).is_some_and(Vec::is_empty) {
            self.overflow.remove(&hash);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use talon_core::{Backend, BlockId, ObjectId, Version};

    #[test]
    fn digest_collisions_do_not_cross_logical_blocks_or_lose_versions() {
        let block = |path, version| {
            BlockKey::new(&BlockId::new(
                ObjectId::new(Backend::S3, "bucket", path),
                0,
                256,
                Version::new(version),
            ))
        };
        let a = block("a", "v1");
        let a2 = block("a", "v2");
        let b = block("b", "v1");
        let mut index = VersionIndex::default();
        for key in [&a, &b, &a2, &a2] {
            index.insert_at(7, key.clone());
        }
        assert_eq!(
            index.blocks_at(7, &a).cloned().collect::<Vec<_>>(),
            [a.clone(), a2.clone()]
        );
        assert_eq!(
            index.blocks_at(7, &b).cloned().collect::<Vec<_>>(),
            [b.clone()]
        );
        index.remove_at(7, &a);
        assert_eq!(
            index.blocks_at(7, &a).cloned().collect::<Vec<_>>(),
            [a2.clone()]
        );
        index.remove_at(7, &b);
        assert_eq!(
            index.blocks_at(7, &a).cloned().collect::<Vec<_>>(),
            [a2.clone()]
        );
        index.remove_at(7, &a2);
        assert!(index.is_empty());
        assert!(index.overflow.is_empty());
    }
}
