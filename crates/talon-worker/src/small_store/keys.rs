use std::io;

#[derive(Clone, Copy, Default)]
struct Span {
    offset: u32,
    len: u32,
}

impl Span {
    fn get(self, bytes: &[u8]) -> &[u8] {
        &bytes[self.offset as usize..self.offset as usize + self.len as usize]
    }

    fn append(bytes: &mut Vec<u8>, key: &[u8]) -> io::Result<Self> {
        let end = bytes
            .len()
            .checked_add(key.len())
            .filter(|&n| n <= u32::MAX as usize)
            .ok_or_else(|| io::Error::other("small-object key buffer overflow"))?;
        let span = Self {
            offset: bytes.len() as u32,
            len: key.len() as u32,
        };
        bytes.extend_from_slice(key);
        debug_assert_eq!(bytes.len(), end);
        Ok(span)
    }
}

#[derive(Clone, Default)]
pub(super) struct Keys {
    rows: Vec<Span>,
    offsets: Vec<u32>,
    suffixes: Vec<u8>,
    prefix_ids: Vec<u32>,
    prefixes: Vec<Span>,
    prefix_bytes: Vec<u8>,
    garbage: usize,
}

impl Keys {
    pub fn get(&self, row: usize) -> Option<(&[u8], &[u8])> {
        let span = if self.offsets.is_empty() {
            *self.rows.get(row)?
        } else {
            let &end = self.offsets.get(row.checked_add(1)?)?;
            let offset = self.offsets[row];
            Span {
                offset,
                len: end - offset,
            }
        };
        let suffix = span.get(&self.suffixes);
        let prefix = self
            .prefix_ids
            .get(row)
            .and_then(|&id| self.prefixes.get(id as usize));
        Some((prefix.map_or(&[], |p| p.get(&self.prefix_bytes)), suffix))
    }

    pub fn insert(&mut self, row: usize, key: &[u8], replace: bool) -> io::Result<()> {
        if !self.offsets.is_empty() {
            self.rows = self
                .offsets
                .windows(2)
                .map(|p| Span {
                    offset: p[0],
                    len: p[1] - p[0],
                })
                .collect();
            self.offsets = Vec::new();
        }
        if self.garbage > self.suffixes.len() / 2
            || self.suffixes.len().saturating_add(key.len()) > u32::MAX as usize
        {
            self.retain(|_| true);
        }
        let span = Span::append(&mut self.suffixes, key)?;
        if replace {
            self.garbage += self.rows[row].len as usize;
            self.rows[row] = span;
            if !self.prefix_ids.is_empty() {
                self.prefix_ids[row] = u32::MAX;
            }
        } else {
            self.rows.insert(row, span);
            if !self.prefix_ids.is_empty() {
                self.prefix_ids.insert(row, u32::MAX);
            }
        }
        Ok(())
    }

    pub fn remove(&mut self, row: usize) {
        if !self.offsets.is_empty() {
            return;
        }
        self.garbage += self.rows[row].len as usize;
        self.rows[row] = Span::default();
        if !self.prefix_ids.is_empty() {
            self.prefix_ids[row] = u32::MAX;
        }
    }

    pub fn retain(&mut self, mut keep: impl FnMut(usize) -> bool) {
        let sealed = !self.offsets.is_empty();
        let len = if sealed {
            self.offsets.len() - 1
        } else {
            self.rows.len()
        };
        let compressed = !self.prefix_ids.is_empty();
        let mut next = Self::default();
        let mut prefix_ids = vec![u32::MAX; self.prefixes.len()];
        for row in 0..len {
            if keep(row) {
                let (prefix, suffix) = self.get(row).unwrap();
                next.rows
                    .push(Span::append(&mut next.suffixes, suffix).unwrap());
                if compressed {
                    let old = self.prefix_ids[row];
                    let id = if old == u32::MAX {
                        u32::MAX
                    } else {
                        let id = &mut prefix_ids[old as usize];
                        if *id == u32::MAX {
                            *id = next.prefixes.len() as u32;
                            next.prefixes
                                .push(Span::append(&mut next.prefix_bytes, prefix).unwrap());
                        }
                        *id
                    };
                    next.prefix_ids.push(id);
                }
            }
        }
        if sealed {
            next.freeze();
        }
        next.shrink_to_fit();
        *self = next;
    }

    pub fn seal(&mut self) {
        if self.offsets.is_empty() {
            self.compress();
            self.freeze();
        }
        self.shrink_to_fit();
    }

    fn freeze(&mut self) {
        let mut offsets = Vec::with_capacity(self.rows.len() + 1);
        let mut suffixes = Vec::with_capacity(self.rows.iter().map(|r| r.len as usize).sum());
        for &row in &self.rows {
            offsets.push(suffixes.len() as u32);
            suffixes.extend_from_slice(row.get(&self.suffixes));
        }
        offsets.push(suffixes.len() as u32);
        self.offsets = offsets;
        self.rows = Vec::new();
        self.suffixes = suffixes;
        self.garbage = 0;
    }

    fn compress(&mut self) {
        if self.rows.is_empty() || !self.prefix_ids.is_empty() {
            return;
        }
        if self.garbage != 0 {
            self.retain(|_| true);
        }
        let mut order: Vec<_> = (0..self.rows.len()).collect();
        order.sort_unstable_by(|&a, &b| {
            self.rows[a]
                .get(&self.suffixes)
                .cmp(self.rows[b].get(&self.suffixes))
        });
        let groups = self.groups(&order);
        let prefix_len: usize = groups.iter().map(|g| g.prefix).sum();
        let prefix_count = groups.iter().filter(|g| g.prefix != 0).count();
        let suffix_len: usize = groups
            .iter()
            .map(|g| {
                order[g.start..g.end]
                    .iter()
                    .map(|&r| self.rows[r].len as usize - g.prefix)
                    .sum::<usize>()
            })
            .sum();
        if prefix_len + prefix_count * size_of::<Span>() + suffix_len + self.rows.len() * 4
            >= self.suffixes.len()
        {
            return;
        }
        let mut next = Self {
            rows: vec![Span::default(); self.rows.len()],
            offsets: Vec::new(),
            suffixes: Vec::with_capacity(suffix_len),
            prefix_ids: vec![u32::MAX; self.rows.len()],
            prefixes: Vec::with_capacity(prefix_count),
            prefix_bytes: Vec::with_capacity(prefix_len),
            garbage: 0,
        };
        for group in groups {
            let prefix_id = if group.prefix == 0 {
                u32::MAX
            } else {
                let id = next.prefixes.len() as u32;
                let key = self.rows[order[group.start]].get(&self.suffixes);
                next.prefixes
                    .push(Span::append(&mut next.prefix_bytes, &key[..group.prefix]).unwrap());
                id
            };
            for &row in &order[group.start..group.end] {
                let key = self.rows[row].get(&self.suffixes);
                next.rows[row] = Span::append(&mut next.suffixes, &key[group.prefix..]).unwrap();
                next.prefix_ids[row] = prefix_id;
            }
        }
        *self = next;
    }

    fn groups(&self, order: &[usize]) -> Vec<Group> {
        enum Work {
            Visit(usize, usize),
            Finish {
                group: Group,
                mark: usize,
                cost_mark: usize,
                whole: usize,
            },
        }
        let mut groups = Vec::new();
        let mut costs = Vec::new();
        let mut work = vec![Work::Visit(0, order.len())];
        while let Some(item) = work.pop() {
            match item {
                Work::Finish {
                    group,
                    mark,
                    cost_mark,
                    whole,
                } => {
                    let children: usize = costs.drain(cost_mark..).sum();
                    if whole <= children {
                        groups.truncate(mark);
                        groups.push(group);
                    }
                    costs.push(whole.min(children));
                }
                Work::Visit(start, end) => {
                    let first = self.rows[order[start]].get(&self.suffixes);
                    let last = self.rows[order[end - 1]].get(&self.suffixes);
                    let prefix = first.iter().zip(last).take_while(|(a, b)| a == b).count();
                    let total: usize = order[start..end]
                        .iter()
                        .map(|&r| self.rows[r].len as usize)
                        .sum();
                    let shared = prefix + size_of::<Span>() + total - (end - start) * prefix;
                    if end - start == 1 || first == last {
                        groups.push(Group {
                            start,
                            end,
                            prefix: if shared < total { prefix } else { 0 },
                        });
                        costs.push(shared.min(total));
                        continue;
                    }
                    work.push(Work::Finish {
                        group: Group { start, end, prefix },
                        mark: groups.len(),
                        cost_mark: costs.len(),
                        whole: if prefix == 0 { total } else { shared },
                    });
                    let mut i = start;
                    while i < end {
                        let symbol = self.rows[order[i]].get(&self.suffixes).get(prefix);
                        let mut j = i + 1;
                        while j < end
                            && self.rows[order[j]].get(&self.suffixes).get(prefix) == symbol
                        {
                            j += 1;
                        }
                        work.push(Work::Visit(i, j));
                        i = j;
                    }
                }
            }
        }
        groups
    }

    pub fn shrink_to_fit(&mut self) {
        self.rows.shrink_to_fit();
        self.offsets.shrink_to_fit();
        self.suffixes.shrink_to_fit();
        self.prefix_ids.shrink_to_fit();
        self.prefixes.shrink_to_fit();
        self.prefix_bytes.shrink_to_fit();
    }
}

struct Group {
    start: usize,
    end: usize,
    prefix: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(keys: &Keys, expected: &[Vec<u8>]) {
        for (row, key) in expected.iter().enumerate() {
            let (prefix, suffix) = keys.get(row).unwrap();
            assert_eq!([prefix, suffix].concat(), *key);
        }
        assert!(keys.get(expected.len()).is_none());
    }

    #[test]
    fn binary_keys_duplicates_and_frozen_rows_survive_repacking() {
        let mut expected = vec![vec![], vec![0], vec![0, 0], vec![0]];
        for byte in 0..=255 {
            let mut key = vec![42; 300];
            key.push(byte);
            key.extend_from_slice(&[0; 300]);
            expected.push(key);
        }
        let mut keys = Keys::default();
        for (row, key) in expected.iter().enumerate() {
            keys.insert(row, key, false).unwrap();
        }
        keys.seal();
        assert!(!keys.prefix_ids.is_empty());
        check(&keys, &expected);
        let frozen = keys.clone();
        let original = expected.clone();
        keys.remove(10);
        expected[10] = b"replacement".to_vec();
        keys.insert(10, &expected[10], true).unwrap();
        expected.insert(11, b"new row".to_vec());
        keys.insert(11, &expected[11], false).unwrap();
        keys.retain(|row| row % 3 != 0);
        expected = expected
            .into_iter()
            .enumerate()
            .filter_map(|(i, k)| (i % 3 != 0).then_some(k))
            .collect();
        check(&keys, &expected);
        check(&frozen, &original);
    }

    #[test]
    fn unshared_keys_stay_plain_and_replacements_reclaim_bytes() {
        let mut keys = Keys::default();
        for byte in 0..=255 {
            keys.insert(byte as usize, &[byte], false).unwrap();
        }
        keys.seal();
        assert!(keys.prefix_ids.is_empty());
        for _ in 0..1000 {
            keys.insert(0, &[0], true).unwrap();
        }
        assert!(keys.suffixes.len() <= 512);
        check(&keys, &(0..=255).map(|b| vec![b]).collect::<Vec<_>>());
    }

    #[test]
    fn deeply_nested_prefixes_do_not_use_the_call_stack() {
        let mut keys = Keys::default();
        for len in 0..4096 {
            keys.insert(len, &vec![b'a'; len], false).unwrap();
        }
        keys.seal();
        for len in 0..4096 {
            let (prefix, suffix) = keys.get(len).unwrap();
            assert_eq!(prefix.len() + suffix.len(), len);
            assert!(prefix.iter().chain(suffix).all(|&b| b == b'a'));
        }
    }
}
