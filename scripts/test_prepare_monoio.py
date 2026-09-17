"""Regression tests for archive validation and patch preparation."""
import hashlib
import importlib.util
import io
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "prepare_monoio", Path(__file__).with_name("prepare_monoio.py")
)
prepare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prepare)


class PreparationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.archive = self.root / "release.crate"
        with tarfile.open(self.archive, "w:gz") as release:
            content = b"original\n"
            member = tarfile.TarInfo("monoio-0.2.4/src/lib.rs")
            member.size = len(content)
            release.addfile(member, io.BytesIO(content))
        self.patch_file = self.root / "patches/monoio-0.2.4.patch"
        self.patch_file.parent.mkdir()
        self.set_patch("patched")
        self.target = self.root / ".patched-deps/monoio/src/lib.rs"
        for name, value in [
            ("ROOT", self.root),
            ("SHA256", hashlib.sha256(self.archive.read_bytes()).hexdigest()),
        ]:
            mock = patch.object(prepare, name, value)
            mock.start()
            self.addCleanup(mock.stop)

    def set_patch(self, replacement):
        self.patch_file.write_text(
            "--- a/src/lib.rs\n+++ b/src/lib.rs\n"
            f"@@ -1 +1 @@\n-original\n+{replacement}\n"
        )

    def test_prepare_reuse_and_regenerate_from_original_archive(self):
        prepare.prepare(self.archive)
        self.assertEqual(self.target.read_text(), "patched\n")
        # No archive or network is needed while inputs remain unchanged.
        prepare.prepare(self.root / "missing.crate")
        self.set_patch("updated")
        prepare.prepare(self.archive)
        self.assertEqual(self.target.read_text(), "updated\n")

    def test_checksum_failure_preserves_previous_tree(self):
        prepare.prepare(self.archive)
        self.set_patch("updated")
        self.archive.write_bytes(b"invalid")
        with self.assertRaisesRegex(RuntimeError, "SHA-256 mismatch"):
            prepare.prepare(self.archive)
        self.assertEqual(self.target.read_text(), "patched\n")

    def test_inapplicable_patch_preserves_previous_tree(self):
        prepare.prepare(self.archive)
        self.patch_file.write_text(
            self.patch_file.read_text().replace("-original", "-absent")
        )
        with self.assertRaises(prepare.subprocess.CalledProcessError):
            prepare.prepare(self.archive)
        self.assertEqual(self.target.read_text(), "patched\n")

    def test_download_uses_same_checksum_and_patch_path(self):
        with patch.object(
            prepare.urllib.request, "urlopen",
            return_value=io.BytesIO(self.archive.read_bytes()),
        ) as download:
            prepare.prepare()
        download.assert_called_once_with(
            "https://static.crates.io/crates/monoio/monoio-0.2.4.crate", timeout=60
        )
        self.assertEqual(self.target.read_text(), "patched\n")


if __name__ == "__main__":
    unittest.main()
