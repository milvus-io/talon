use super::catalog::Address;
use super::policy::SmallAccess;
use arc_swap::ArcSwapOption;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::sync::{Arc, Weak};

type Entries = Vec<(Address, Weak<SmallAccess>)>;

pub(super) struct Tickets {
    buckets: [ArcSwapOption<Entries>; 256],
    hash: RandomState,
}

impl Default for Tickets {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| ArcSwapOption::empty()),
            hash: RandomState::new(),
        }
    }
}

fn same(a: &Option<Arc<Entries>>, b: &Option<Arc<Entries>>) -> bool {
    a.as_ref().map(Arc::as_ptr) == b.as_ref().map(Arc::as_ptr)
}

impl Tickets {
    fn bucket(&self, address: Address) -> &ArcSwapOption<Entries> {
        &self.buckets[self.hash.hash_one(address) as usize % self.buckets.len()]
    }

    pub fn get_or_insert(
        &self,
        address: Address,
        create: impl FnOnce() -> SmallAccess,
    ) -> Arc<SmallAccess> {
        let bucket = self.bucket(address);
        let mut create = Some(create);
        let mut created = None;
        loop {
            let current = bucket.load_full();
            if let Some(ticket) = current
                .iter()
                .flat_map(|entries| entries.iter())
                .find(|(key, _)| *key == address)
                .and_then(|(_, ticket)| ticket.upgrade())
            {
                return ticket;
            }
            let ticket = created.get_or_insert_with(|| Arc::new(create.take().unwrap()()));
            let mut next: Entries = current
                .iter()
                .flat_map(|entries| entries.iter())
                .filter(|(key, ticket)| *key != address && ticket.strong_count() != 0)
                .cloned()
                .collect();
            next.push((address, Arc::downgrade(ticket)));
            let previous = bucket.compare_and_swap(&current, Some(Arc::new(next)));
            if same(&previous, &current) {
                return created.unwrap();
            }
        }
    }

    pub fn insert(&self, address: Address, ticket: Weak<SmallAccess>) {
        let bucket = self.bucket(address);
        bucket.rcu(|current| {
            let mut next: Entries = current
                .iter()
                .flat_map(|entries| entries.iter())
                .filter(|(key, entry)| *key != address && entry.strong_count() != 0)
                .cloned()
                .collect();
            next.push((address, ticket.clone()));
            Some(Arc::new(next))
        });
    }

    pub fn remove(&self, address: &Address) -> Option<Weak<SmallAccess>> {
        self.remove_if(*address, |_| true)
    }

    pub fn remove_if(
        &self,
        address: Address,
        matches: impl Fn(&Weak<SmallAccess>) -> bool,
    ) -> Option<Weak<SmallAccess>> {
        let bucket = self.bucket(address);
        loop {
            let current = bucket.load_full();
            let entries = current.as_ref()?;
            let position = entries
                .iter()
                .position(|(key, ticket)| *key == address && matches(ticket))?;
            let removed = entries[position].1.clone();
            let next: Entries = entries
                .iter()
                .enumerate()
                .filter(|(index, (_, ticket))| *index != position && ticket.strong_count() != 0)
                .map(|(_, entry)| entry.clone())
                .collect();
            let next = (!next.is_empty()).then(|| Arc::new(next));
            let previous = bucket.compare_and_swap(&current, next);
            if same(&previous, &current) {
                return Some(removed);
            }
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.buckets
            .iter()
            .map(|bucket| bucket.load().as_ref().map_or(0, |entries| entries.len()))
            .sum()
    }

    #[cfg(test)]
    pub fn capacity(&self) -> usize {
        self.buckets
            .iter()
            .map(|bucket| {
                bucket
                    .load()
                    .as_ref()
                    .map_or(0, |entries| entries.capacity())
            })
            .sum()
    }
}
