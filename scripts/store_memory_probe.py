#!/usr/bin/env python3
"""Measure private Worker layouts without retaining changes to production source."""
import argparse
import pathlib
import subprocess


ROOT = pathlib.Path(__file__).resolve().parents[1]
SOURCE = ROOT / "crates/talon-worker/src"
OUTPUT = ROOT / "target/memory-analysis/current"
TYPES = {
    "block_key.rs": ["BlockKey", "Arc<str>"],
    "index.rs": ["DirectoryEntry", "LegacyPolicy", "Entry", "IndexedBlock", "ResidentMeta", "IndexedUnits", "UnitSlot", "PageUnit", "Option<PolicyState>", "Option<AccessHandle>", "RwLock<UnitSlot>"],
    "eviction.rs": ["SharedUnit", "AccessHandle", "Access", "PolicyState", "LegacyBlockActivity", "Inner"],
    "page_lifecycle.rs": ["PageWord", "PageAge", "PageEntry", "PageRows", "PageReadGuard", "PageMutationGate", "PageWork", "DirtyGroup", "CheckpointShard", "talon_core::BlockId", "talon_core::ObjectId"],
    "catalog/rows.rs": ["BlockRow", "Option<BlockRow>", "BlockRows", "Geometry", "Pages", "InlinePage", "CompactPage", "PageRows", "SharedPages", "ChunkedPages"],
    "index/directory.rs": ["Directory", "CatalogRevisions", "AtomicU64", "u32"],
    "fd_cache.rs": ["CachedFd", "Shard", "(PathBuf, CachedFd)", "OwnedFd"],
    "runtime.rs": ["CachedVersion", "(ObjectId, CachedVersion)", "crate::WholeBlockStore", "crate::PagedBlockStore"],
}
FIELDS = {
    "index.rs": {
        "DirectoryEntry": "resident legacy",
        "IndexedBlock": "meta units charged_bytes charged_pages",
        "ResidentMeta": "id len page_size page_count",
        "UnitSlot": "allocated access",
        "PageUnit": "allocated access",
    },
    "eviction.rs": {
        "PolicyState": "independent bytes revision pins",
        "LegacyBlockActivity": "pins revision access",
    },
    "page_lifecycle.rs": {"PageEntry": "unit state", "PageRows": "rows ages"},
}

# The probe accesses private counters from the defining modules.
COMPONENTS = {
    "catalog/rows.rs": """
#[cfg(test)]
impl BlockRows {
    pub(crate) fn memory_probe_components(&self) {
        let mut compact = (0, 0);
        let mut wide = (0, 0);
        let mut states = (0, 0);
        let mut ages = (0, 0);
        let mut shared = 0;
        let mut chunked = 0;
        for (_, row) in self.iter() {
            let mut record = |pages: &SharedPages| {
                shared += 1;
                let geometry = match &pages.rows {
                    PageRows::Compact(rows) => (&mut compact, rows.len(), rows.capacity()),
                    PageRows::Wide(rows) => (&mut wide, rows.len(), rows.capacity()),
                };
                geometry.0.0 += geometry.1;
                geometry.0.1 += geometry.2;
                states.0 += pages.states.len();
                states.1 += pages.states.capacity();
                ages.0 += pages.ages.len();
                ages.1 += pages.ages.capacity();
            };
            match &row.geometry.pages {
                Pages::Multiple(pages) => record(pages),
                Pages::Chunked(pages) => {
                    chunked += 1;
                    for chunk in pages.chunks.values() {
                        record(chunk);
                    }
                }
                _ => {}
            }
        }
        eprintln!("COMPONENT|slabs|{}|{}|{}", self.slabs.len(), self.slabs.capacity(), SLAB_ROWS);
        eprintln!("COMPONENT|lookup_slots|{}|{}", self.lookup.len(), self.lookup.capacity());
        eprintln!("COMPONENT|free_handles|{}|{}", self.free.len(), self.free.capacity());
        eprintln!("COMPONENT|whole_overlap|{}|{}", self.whole_overlap.len(), self.whole_overlap.capacity());
        eprintln!("COMPONENT|shared_page_headers|{shared}");
        eprintln!("COMPONENT|chunked_page_headers|{chunked}");
        for (name, (len, capacity)) in [
            ("compact_page_rows", compact),
            ("wide_page_rows", wide),
            ("page_states", states),
            ("page_ages", ages),
        ] {
            eprintln!("COMPONENT|{name}|{len}|{capacity}");
        }
    }
}
""",
    "index/directory.rs": """
#[cfg(test)]
impl Directory {
    pub(crate) fn memory_probe_components(&self) {
        self.rows.memory_probe_components();
        eprintln!("COMPONENT|legacy_runtime_rows|{}", self.runtime.len());
        eprintln!("COMPONENT|block_clocks|{}", self.clocks.as_ref().map_or(0, Vec::capacity));
        eprintln!("COMPONENT|inline_ages|{}", self.inline_ages.as_ref().map_or(0, Vec::capacity));
        let (len, capacity, width) = match &self.revisions {
            CatalogRevisions::Off => (0, 0, 0),
            CatalogRevisions::Compact(values) => (values.len(), values.capacity(), 4),
            CatalogRevisions::Wide(values) => (values.len(), values.capacity(), 8),
        };
        eprintln!("COMPONENT|block_revisions|{len}|{capacity}|{width}");
    }
}
""",
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=pathlib.Path, default=OUTPUT)
    output = parser.parse_args().output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    originals = {}
    installed = {}
    try:
        for name, types in TYPES.items():
            path = SOURCE / name
            original = path.read_text()
            if "fn memory_probe_layouts" in original:
                raise SystemExit(f"probe already installed: {path}")
            lines = ["\n#[cfg(test)]", "pub(crate) fn memory_probe_layouts() {"]
            for ty in types:
                lines.append(f'    eprintln!("LAYOUT|{name}|{ty}|{{}}|{{}}", std::mem::size_of::<{ty}>(), std::mem::align_of::<{ty}>());')
            for ty, fields in FIELDS.get(name, {}).items():
                for field in fields.split():
                    lines.append(f'    eprintln!("FIELD|{ty}|{field}|{{}}", std::mem::offset_of!({ty}, {field}));')
            if name == "index.rs":
                lines.append("    directory::memory_probe_layouts();")
            lines.append("}\n")
            originals[path] = original
            installed[path] = original + "\n".join(lines) + COMPONENTS.get(name, "")
            path.write_text(installed[path])
        path = SOURCE / "catalog/model.rs"
        originals[path] = path.read_text()
        installed[path] = originals[path] + '\n#[cfg(test)]\nmod memory_probe { include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../scripts/store_memory_probe.rs")); }\n'
        path.write_text(installed[path])
        command = [
            "cargo", "test", "--locked", "-p", "talon-worker", "--lib",
            "catalog::model::memory_probe::store_memory_breakdown", "--",
            "--ignored", "--exact", "--nocapture", "--test-threads=1",
        ]
        with (output / "probe.log").open("w") as log:
            result = subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        if result.returncode == 0:
            rows = [
                line.split("|")[1:]
                for line in (output / "probe.log").read_text().splitlines()
                if line.startswith("TOTAL|")
            ]
            keys = [
                line.split("|")[1:]
                for line in (output / "probe.log").read_text().splitlines()
                if line.startswith("KEY|")
            ]
            (output / "keys.tsv").write_text(
                "scenario\tblocks\ttext_bytes\trequested_bytes\tusable_bytes\n"
                + "".join("\t".join(row) + "\n" for row in keys)
            )
            (output / "totals.tsv").write_text(
                "scenario\tblocks\tpages_per_block\trequested_bytes\tusable_bytes\trequested_per_block\tusable_per_block\n"
                + "".join("\t".join(row) + "\n" for row in rows)
            )
        print(output / "probe.log", flush=True)
        raise SystemExit(result.returncode)
    finally:
        for path, original in originals.items():
            if path.read_text() == installed[path]:
                path.write_text(original)
            else:
                backup = output / (path.name + ".before-probe")
                backup.write_text(original)
                print(f"Concurrent edit retained at {path}; original saved to {backup}")


if __name__ == "__main__":
    main()
