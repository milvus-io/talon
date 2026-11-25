//! Resident pages keep a single entry inline and common multipage blocks in sorted arrays.
use crate::page_lifecycle::PageEntry;
use std::collections::BTreeMap;
use std::ops::{Index, RangeBounds};

// Keep the worst-case bytes moved during a nonsequential fill or delete
// bounded. Very large resident blocks use a tree, including in legacy stores.
const MAX_FLAT_PAGES: usize = 512;

#[derive(Clone, Debug)]
pub(crate) enum PageMap<V = PageEntry> {
    One(Option<(u32, V)>),
    Flat(Vec<(u32, V)>),
    Many(BTreeMap<u32, V>),
}
impl<V: PartialEq> PartialEq for PageMap<V> {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().eq(other.iter())
    }
}

impl<V: Eq> Eq for PageMap<V> {}

impl<V> Default for PageMap<V> {
    fn default() -> Self {
        Self::One(None)
    }
}
impl<V> PageMap<V> {
    pub fn insert(&mut self, key: u32, value: V) -> Option<V> {
        let mut value = Some(value);
        let slot = self.get_or_insert_with(key, || value.take().unwrap());
        value.map(|value| std::mem::replace(slot, value))
    }
    pub fn contains_key(&self, key: &u32) -> bool {
        self.get(key).is_some()
    }
    pub fn first_key_value(&self) -> Option<(&u32, &V)> {
        match self {
            Self::One(one) => one.as_ref().map(|(k, v)| (k, v)),
            Self::Flat(few) => few.first().map(|(k, v)| (k, v)),
            Self::Many(many) => many.first_key_value(),
        }
    }
    pub fn last_key_value(&self) -> Option<(&u32, &V)> {
        match self {
            Self::One(one) => one.as_ref().map(|(k, v)| (k, v)),
            Self::Flat(few) => few.last().map(|(k, v)| (k, v)),
            Self::Many(many) => many.last_key_value(),
        }
    }
    pub fn len(&self) -> usize {
        match self {
            Self::One(one) => usize::from(one.is_some()),
            Self::Flat(few) => few.len(),
            Self::Many(many) => many.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn get(&self, key: &u32) -> Option<&V> {
        match self {
            Self::One(one) => one.as_ref().filter(|(k, _)| k == key).map(|(_, v)| v),
            Self::Flat(few) => few
                .binary_search_by_key(key, |(k, _)| *k)
                .ok()
                .map(|i| &few[i].1),
            Self::Many(many) => many.get(key),
        }
    }
    pub fn get_mut(&mut self, key: &u32) -> Option<&mut V> {
        match self {
            Self::One(one) => one.as_mut().filter(|(k, _)| k == key).map(|(_, v)| v),
            Self::Flat(few) => few
                .binary_search_by_key(key, |(k, _)| *k)
                .ok()
                .map(|i| &mut few[i].1),
            Self::Many(many) => many.get_mut(key),
        }
    }
    pub fn get_or_insert_with(&mut self, key: u32, create: impl FnOnce() -> V) -> &mut V {
        if matches!(self, Self::One(Some((old, _))) if *old != key) {
            let mut few = Vec::with_capacity(2);
            let Self::One(Some(old)) = std::mem::take(self) else {
                unreachable!()
            };
            few.push(old);
            *self = Self::Flat(few);
        }
        if matches!(self, Self::Flat(few) if few.len() == MAX_FLAT_PAGES && few.binary_search_by_key(&key, |(k, _)| *k).is_err())
        {
            let Self::Flat(few) = std::mem::take(self) else {
                unreachable!()
            };
            *self = Self::Many(few.into_iter().collect());
        }
        match self {
            Self::One(one) => &mut one.get_or_insert_with(|| (key, create())).1,
            Self::Flat(few) => {
                let i = match few.binary_search_by_key(&key, |(k, _)| *k) {
                    Ok(i) => i,
                    Err(i) => {
                        let value = create();
                        few.insert(i, (key, value));
                        i
                    }
                };
                &mut few[i].1
            }
            Self::Many(many) => many.entry(key).or_insert_with(create),
        }
    }
    pub fn remove(&mut self, key: &u32) -> Option<V> {
        let removed = match self {
            Self::One(one) => {
                if one.as_ref().is_some_and(|(k, _)| k == key) {
                    one.take().map(|(_, v)| v)
                } else {
                    None
                }
            }
            Self::Flat(few) => few
                .binary_search_by_key(key, |(k, _)| *k)
                .ok()
                .map(|i| few.remove(i).1),
            Self::Many(many) => many.remove(key),
        };
        if matches!(self, Self::Many(many) if many.len() <= MAX_FLAT_PAGES / 2) {
            let Self::Many(many) = std::mem::take(self) else {
                unreachable!()
            };
            *self = Self::Flat(many.into_iter().collect());
        }
        if let Self::Flat(few) = self {
            if few.len() <= 1 {
                *self = Self::One(few.pop());
            } else if removed.is_some() && few.len() <= few.capacity() / 4 {
                few.shrink_to_fit();
            }
        }
        removed
    }
    pub fn iter(&self) -> impl Iterator<Item = (&u32, &V)> {
        let (one, few, many) = match self {
            Self::One(one) => (one.as_ref().map(|(k, v)| (k, v)), None, None),
            Self::Flat(few) => (None, Some(few.iter()), None),
            Self::Many(many) => (None, None, Some(many.iter())),
        };
        one.into_iter()
            .chain(few.into_iter().flatten().map(|(k, v)| (k, v)))
            .chain(many.into_iter().flatten())
    }
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&u32, &mut V)> {
        let (one, few, many) = match self {
            Self::One(one) => (one.as_mut().map(|(k, v)| (&*k, v)), None, None),
            Self::Flat(few) => (None, Some(few.iter_mut()), None),
            Self::Many(many) => (None, None, Some(many.iter_mut())),
        };
        one.into_iter()
            .chain(few.into_iter().flatten().map(|(k, v)| (&*k, v)))
            .chain(many.into_iter().flatten())
    }
    pub fn range(&self, range: impl RangeBounds<u32>) -> impl Iterator<Item = (&u32, &V)> {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        let bounds = (range.start_bound().cloned(), range.end_bound().cloned());
        let (one, few, many) = match self {
            Self::One(one) => (
                one.as_ref()
                    .filter(|(k, _)| range.contains(k))
                    .map(|(k, v)| (k, v)),
                None,
                None,
            ),
            Self::Flat(few) => {
                let start = match bounds.0 {
                    Included(start) => few.partition_point(|(page, _)| *page < start),
                    Excluded(start) => few.partition_point(|(page, _)| *page <= start),
                    Unbounded => 0,
                };
                let end = match bounds.1 {
                    Included(end) => few.partition_point(|(page, _)| *page <= end),
                    Excluded(end) => few.partition_point(|(page, _)| *page < end),
                    Unbounded => few.len(),
                };
                (None, Some(few[start..end.max(start)].iter()), None)
            }
            Self::Many(many) => (None, None, Some(many.range(bounds))),
        };
        one.into_iter()
            .chain(few.into_iter().flatten().map(|(k, v)| (k, v)))
            .chain(many.into_iter().flatten())
    }
    pub fn keys(&self) -> impl Iterator<Item = &u32> {
        self.iter().map(|(k, _)| k)
    }
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.iter().map(|(_, v)| v)
    }
}
impl<V> Index<&u32> for PageMap<V> {
    type Output = V;
    fn index(&self, key: &u32) -> &Self::Output {
        self.get(key).expect("registered page")
    }
}

impl<V> FromIterator<(u32, V)> for PageMap<V> {
    fn from_iter<T: IntoIterator<Item = (u32, V)>>(iter: T) -> Self {
        let mut map = Self::default();
        map.extend(iter);
        map
    }
}
impl<V, const N: usize> From<[(u32, V); N]> for PageMap<V> {
    fn from(value: [(u32, V); N]) -> Self {
        value.into_iter().collect()
    }
}
impl<V> Extend<(u32, V)> for PageMap<V> {
    fn extend<T: IntoIterator<Item = (u32, V)>>(&mut self, iter: T) {
        for (key, value) in iter {
            self.insert(key, value);
        }
    }
}
pub(crate) enum PageIntoIter<V> {
    One(std::option::IntoIter<(u32, V)>),
    Flat(std::vec::IntoIter<(u32, V)>),
    Many(std::collections::btree_map::IntoIter<u32, V>),
}
impl<V> Iterator for PageIntoIter<V> {
    type Item = (u32, V);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::One(one) => one.next(),
            Self::Flat(few) => few.next(),
            Self::Many(many) => many.next(),
        }
    }
}
impl<V> IntoIterator for PageMap<V> {
    type Item = (u32, V);
    type IntoIter = PageIntoIter<V>;
    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::One(one) => PageIntoIter::One(one.into_iter()),
            Self::Flat(few) => PageIntoIter::Flat(few.into_iter()),
            Self::Many(many) => PageIntoIter::Many(many.into_iter()),
        }
    }
}
impl<V: serde::Serialize> serde::Serialize for PageMap<V> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(self.iter())
    }
}
impl<'de, V: serde::Deserialize<'de>> serde::Deserialize<'de> for PageMap<V> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor<V>(std::marker::PhantomData<V>);
        impl<'de, V: serde::Deserialize<'de>> serde::de::Visitor<'de> for Visitor<V> {
            type Value = PageMap<V>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a page allocation map")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut pages = PageMap::default();
                while let Some((key, value)) = map.next_entry()? {
                    pages.insert(key, value);
                }
                Ok(pages)
            }
        }
        deserializer.deserialize_map(Visitor(std::marker::PhantomData))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sparse_transitions_preserve_order_ranges_mutation_and_encoding() {
        let mut pages = PageMap::default();
        let mut reference = BTreeMap::new();
        let keys = [u32::MAX, 4, 200, 0, 31, 8, 1000, 2];
        for _ in 0..3 {
            for key in keys {
                assert_eq!(
                    pages.insert(key, u64::from(key)),
                    reference.insert(key, u64::from(key))
                );
                assert_eq!(
                    *pages.get_or_insert_with(key, || panic!("existing key")),
                    reference[&key]
                );
                assert_eq!(pages.first_key_value(), reference.first_key_value());
                assert_eq!(pages.last_key_value(), reference.last_key_value());
                assert_eq!(
                    pages.range(4..=200).collect::<Vec<_>>(),
                    reference.range(4..=200).collect::<Vec<_>>()
                );
                assert_eq!(
                    serde_json::to_vec(&pages).unwrap(),
                    serde_json::to_vec(&reference).unwrap()
                );
            }
            for (_, value) in pages.iter_mut() {
                *value += 1;
            }
            for value in reference.values_mut() {
                *value += 1;
            }
            for key in keys {
                *pages.get_mut(&key).unwrap() += 1;
                *reference.get_mut(&key).unwrap() += 1;
                assert_eq!(pages.remove(&key), reference.remove(&key));
                assert_eq!(
                    pages.iter().collect::<Vec<_>>(),
                    reference.iter().collect::<Vec<_>>()
                );
                assert_eq!(
                    pages.clone().into_iter().collect::<Vec<_>>(),
                    reference.clone().into_iter().collect::<Vec<_>>()
                );
                let json = serde_json::to_vec(&pages).unwrap();
                assert_eq!(json, serde_json::to_vec(&reference).unwrap());
                assert_eq!(
                    serde_json::from_slice::<PageMap<u64>>(&json).unwrap(),
                    pages
                );
                if let PageMap::Flat(few) = &pages {
                    assert!(few.capacity() < few.len() * 4);
                }
            }
            assert!(pages.is_empty());
        }
    }
    #[test]
    fn sparse_flat_and_tree_transitions_keep_the_same_ordered_contents() {
        let mut pages = PageMap::default();
        let indexes = (0..=MAX_FLAT_PAGES as u32).map(|page| page * 1024);
        for key in indexes.clone().rev() {
            pages.insert(key, key);
        }
        assert!(matches!(pages, PageMap::Many(_)));
        pages.remove(&0);
        let roundtrip = serde_json::from_slice(&serde_json::to_vec(&pages).unwrap()).unwrap();
        assert!(matches!(roundtrip, PageMap::Flat(_)));
        assert_eq!(pages, roundtrip);
        for key in indexes.skip(1).take(MAX_FLAT_PAGES / 2) {
            pages.remove(&key);
        }
        assert!(matches!(pages, PageMap::Flat(_)));
        assert_eq!(pages.len(), MAX_FLAT_PAGES / 2);
        let last = MAX_FLAT_PAGES as u32 * 1024;
        assert_eq!(pages.last_key_value(), Some((&last, &last)));
    }

    #[test]
    fn catalog_encoding_matches_legacy_maps_across_sparse_transitions() {
        let mut pages = PageMap::<u64>::default();
        let mut legacy = BTreeMap::new();
        for (page, bytes) in [(255, 0), (7, 4096), (255, 8192)] {
            assert_eq!(pages.insert(page, bytes), legacy.insert(page, bytes));
            let bytes = serde_json::to_vec(&pages).unwrap();
            assert_eq!(bytes, serde_json::to_vec(&legacy).unwrap());
            assert_eq!(
                serde_json::from_slice::<PageMap<u64>>(&bytes).unwrap(),
                pages
            );
        }
        for page in [7, 255] {
            assert_eq!(pages.remove(&page), legacy.remove(&page));
            let bytes = serde_json::to_vec(&pages).unwrap();
            assert_eq!(bytes, serde_json::to_vec(&legacy).unwrap());
            assert_eq!(
                serde_json::from_slice::<PageMap<u64>>(&bytes).unwrap(),
                pages
            );
        }
    }
}
