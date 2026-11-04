//! Shared resident-page storage. Single-page blocks need no tree allocation.
use crate::page_lifecycle::PageEntry;
use std::collections::BTreeMap;
use std::ops::{Index, RangeBounds};

pub(crate) enum PageMap {
    One(Option<(u32, PageEntry)>),
    Many(BTreeMap<u32, PageEntry>),
}
impl Default for PageMap {
    fn default() -> Self {
        Self::One(None)
    }
}
impl PageMap {
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
    pub fn get(&self, key: &u32) -> Option<&PageEntry> {
        match self {
            Self::One(one) => one.as_ref().filter(|(k, _)| k == key).map(|(_, v)| v),
            Self::Many(many) => many.get(key),
        }
    }
    pub fn get_mut(&mut self, key: &u32) -> Option<&mut PageEntry> {
        match self {
            Self::One(one) => one.as_mut().filter(|(k, _)| k == key).map(|(_, v)| v),
            Self::Many(many) => many.get_mut(key),
        }
    }
    pub fn get_or_insert_with(
        &mut self,
        key: u32,
        create: impl FnOnce() -> PageEntry,
    ) -> &mut PageEntry {
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
    pub fn remove(&mut self, key: &u32) -> Option<PageEntry> {
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
    pub fn iter(&self) -> impl Iterator<Item = (&u32, &PageEntry)> {
        let (one, many) = match self {
            Self::One(one) => (one.as_ref().map(|(k, v)| (k, v)), None),
            Self::Many(many) => (None, Some(many.iter())),
        };
        one.into_iter().chain(many.into_iter().flatten())
    }
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&u32, &mut PageEntry)> {
        let (one, many) = match self {
            Self::One(one) => (one.as_mut().map(|(k, v)| (&*k, v)), None),
            Self::Many(many) => (None, Some(many.iter_mut())),
        };
        one.into_iter().chain(many.into_iter().flatten())
    }
    pub fn range(&self, range: impl RangeBounds<u32>) -> impl Iterator<Item = (&u32, &PageEntry)> {
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
    pub fn values(&self) -> impl Iterator<Item = &PageEntry> {
        self.iter().map(|(_, v)| v)
    }
}
impl Index<&u32> for PageMap {
    type Output = PageEntry;
    fn index(&self, key: &u32) -> &Self::Output {
        self.get(key).expect("registered page")
    }
}
