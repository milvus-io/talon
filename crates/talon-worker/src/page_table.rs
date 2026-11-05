//! Shared resident-page storage. Single-page blocks need no tree allocation.
use crate::page_lifecycle::PageEntry;
use std::collections::BTreeMap;
use std::ops::{Index, RangeBounds};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PageMap<V = PageEntry> {
    One(Option<(u32, V)>),
    Many(BTreeMap<u32, V>),
}
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
            Self::Many(many) => many.first_key_value(),
        }
    }
    pub fn last_key_value(&self) -> Option<(&u32, &V)> {
        match self {
            Self::One(one) => one.as_ref().map(|(k, v)| (k, v)),
            Self::Many(many) => many.last_key_value(),
        }
    }
    pub fn len(&self) -> usize {
        match self {
            Self::One(one) => usize::from(one.is_some()),
            Self::Many(many) => many.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn clear(&mut self) {
        *self = Self::default();
    }
    pub fn get(&self, key: &u32) -> Option<&V> {
        match self {
            Self::One(one) => one.as_ref().filter(|(k, _)| k == key).map(|(_, v)| v),
            Self::Many(many) => many.get(key),
        }
    }
    pub fn get_mut(&mut self, key: &u32) -> Option<&mut V> {
        match self {
            Self::One(one) => one.as_mut().filter(|(k, _)| k == key).map(|(_, v)| v),
            Self::Many(many) => many.get_mut(key),
        }
    }
    pub fn get_or_insert_with(&mut self, key: u32, create: impl FnOnce() -> V) -> &mut V {
        if matches!(self, Self::One(Some((old, _))) if *old != key) {
            let Self::One(Some((old, value))) = std::mem::take(self) else {
                unreachable!()
            };
            *self = Self::Many(BTreeMap::from([(old, value)]));
        }
        match self {
            Self::One(one) => &mut one.get_or_insert_with(|| (key, create())).1,
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
            Self::Many(many) => many.remove(key),
        };
        if let Self::Many(many) = self {
            if many.len() <= 1 {
                *self = Self::One(many.pop_first());
            }
        }
        removed
    }
    pub fn iter(&self) -> impl Iterator<Item = (&u32, &V)> {
        let (one, many) = match self {
            Self::One(one) => (one.as_ref().map(|(k, v)| (k, v)), None),
            Self::Many(many) => (None, Some(many.iter())),
        };
        one.into_iter().chain(many.into_iter().flatten())
    }
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&u32, &mut V)> {
        let (one, many) = match self {
            Self::One(one) => (one.as_mut().map(|(k, v)| (&*k, v)), None),
            Self::Many(many) => (None, Some(many.iter_mut())),
        };
        one.into_iter().chain(many.into_iter().flatten())
    }
    pub fn range(&self, range: impl RangeBounds<u32>) -> impl Iterator<Item = (&u32, &V)> {
        let (one, many) = match self {
            Self::One(one) => (
                one.as_ref()
                    .filter(|(k, _)| range.contains(k))
                    .map(|(k, v)| (k, v)),
                None,
            ),
            Self::Many(many) => (None, Some(many.range(range))),
        };
        one.into_iter().chain(many.into_iter().flatten())
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
    Many(std::collections::btree_map::IntoIter<u32, V>),
}
impl<V> Iterator for PageIntoIter<V> {
    type Item = (u32, V);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::One(one) => one.next(),
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
