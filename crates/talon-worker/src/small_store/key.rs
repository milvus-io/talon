use super::SmallStoreKey;
use crate::block_key::{BlockIdentity, BlockKey, BlockRef};
use crate::eviction::{CacheUnit, SharedUnit, UnitKey, UnitKind};
use std::borrow::Borrow;
use std::hash::Hash;
use talon_core::BlockId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct SmallRef<'a> {
    pub block: BlockRef<'a>,
    pub kind: UnitKind,
}

impl<'a> SmallRef<'a> {
    pub fn from_unit(unit: &'a impl UnitKey) -> Self {
        Self {
            block: unit.block().view(),
            kind: unit.kind(),
        }
    }

    pub fn valid_extent(self, len: u64) -> bool {
        match self.kind {
            UnitKind::Whole => self.offset == 0 && len > 0 && len < u64::from(self.block_size),
            UnitKind::ExactRange => {
                len > 0
                    && len <= talon_core::MAX_EXACT_READ_LEN
                    && len == u64::from(self.block_size)
                    && self.offset.checked_add(len).is_some()
            }
            UnitKind::Page(_) => false,
        }
    }

    pub fn materialize(self) -> BlockId {
        self.block.materialize()
    }

    pub fn cache_unit(self) -> CacheUnit {
        match self.kind {
            UnitKind::Whole => CacheUnit::Whole(self.materialize()),
            UnitKind::ExactRange => CacheUnit::ExactRange(self.materialize()),
            UnitKind::Page(page) => CacheUnit::Page(self.materialize(), page),
        }
    }
}

impl<'a> std::ops::Deref for SmallRef<'a> {
    type Target = BlockRef<'a>;
    fn deref(&self) -> &Self::Target {
        &self.block
    }
}

pub(crate) trait SmallIdentity: Sync {
    fn view(&self) -> SmallRef<'_>;
}

impl<T: SmallStoreKey + ?Sized> SmallIdentity for T {
    fn view(&self) -> SmallRef<'_> {
        SmallRef {
            block: BlockIdentity::view(self.block()),
            kind: if self.is_page() {
                UnitKind::Page(talon_core::PageIndex(0))
            } else if self.exact_range() {
                UnitKind::ExactRange
            } else {
                UnitKind::Whole
            },
        }
    }
}

impl SmallIdentity for SmallRef<'_> {
    fn view(&self) -> SmallRef<'_> {
        *self
    }
}

impl SmallIdentity for BlockRef<'_> {
    fn view(&self) -> SmallRef<'_> {
        SmallRef {
            block: *self,
            kind: UnitKind::Whole,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SmallKey(pub SharedUnit);

impl SmallKey {
    pub fn new(id: &dyn SmallIdentity) -> Self {
        let id = id.view();
        Self(SharedUnit {
            block: BlockKey::new(&id.block),
            kind: id.kind,
        })
    }

    pub fn materialize(&self) -> BlockId {
        self.0.block.materialize()
    }
}

impl SmallIdentity for SmallKey {
    fn view(&self) -> SmallRef<'_> {
        SmallRef::from_unit(&self.0)
    }
}

impl Hash for SmallKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.view().hash(state);
    }
}

impl<'a> Borrow<dyn SmallIdentity + 'a> for SmallKey {
    fn borrow(&self) -> &(dyn SmallIdentity + 'a) {
        self
    }
}

impl Hash for dyn SmallIdentity + '_ {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.view().hash(state);
    }
}

impl PartialEq for dyn SmallIdentity + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.view() == other.view()
    }
}
impl Eq for dyn SmallIdentity + '_ {}
