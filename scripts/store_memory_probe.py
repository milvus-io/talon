#!/usr/bin/env python3
"""Measure private Worker layouts without retaining changes to production source."""
import argparse
import pathlib
import subprocess


ROOT = pathlib.Path(__file__).resolve().parents[1]
SOURCE = ROOT / "crates/talon-worker/src"
OUTPUT = ROOT / "target/memory-analysis/current"
TYPES = {
    "block_key.rs": ["Packed", "BlockKey", "ObjectKey"],
    "index.rs": ["DirectoryEntry", "PolicyUnits", "Entry", "IndexedBlock", "ResidentMeta", "IndexedUnits", "UnitSlot", "PageUnit", "Option<PolicyState>", "Option<AccessHandle>", "RwLock<UnitSlot>"],
    "eviction.rs": ["SharedUnit", "AccessHandle", "PageAccessSlot", "Access", "PolicyState", "BlockPolicy", "Inner"],
    "page_lifecycle.rs": ["PageState", "PageAccess", "PageEntry", "BlockState", "BlockPages", "PageMutationGate", "BlockWork", "ShardWork", "Registry", "crate::page_table::PageMap", "RwLock<crate::page_table::PageMap>", "talon_core::BlockId", "talon_core::ObjectId"],
    "fd_cache.rs": ["CachedFd", "Shard", "(PathBuf, CachedFd)", "OwnedFd"],
    "runtime.rs": ["CachedVersion", "(ObjectId, CachedVersion)", "crate::WholeBlockStore", "crate::PagedBlockStore"],
}
FIELDS = {
    "block_key.rs": {"Packed": "text bucket_end path_end offset block_size backend"},
    "index.rs": {
        "DirectoryEntry": "resident lifecycle policy activity",
        "IndexedBlock": "meta units charged_bytes charged_pages",
        "ResidentMeta": "id len page_size page_count",
        "UnitSlot": "resident allocated access policy",
        "PageUnit": "allocated access policy",
    },
    "eviction.rs": {
        "PolicyState": "independent bytes position revision pins",
        "BlockPolicy": "pins position revision ownership_position ownership_pending access",
    },
    "page_lifecycle.rs": {
        "PageState": "state recency access",
        "PageEntry": "unit handle selection",
        "PageAccess": "last_access dirty_since work",
    },
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
            lines.append("}\n")
            originals[path] = original
            installed[path] = original + "\n".join(lines)
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
