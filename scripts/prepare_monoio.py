#!/usr/bin/env python3
"""Prepare Talon's patch-only Monoio and monoio-http dependencies. Requires Python 3 and Git."""

import argparse
import fcntl
import hashlib
import io
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import urllib.request

VERSION = "0.2.4"
SHA256 = "3bd0f8bcde87b1949f95338b547543fcab187bc7e7a5024247e359a5e828ba6a"
ROOT = Path(__file__).resolve().parents[1]


def prepare(archive=None, name="monoio", version=None, checksum=None):
    version = VERSION if version is None else version
    checksum = SHA256 if checksum is None else checksum
    patch = ROOT / "patches" / f"{name}-{version}.patch"
    fingerprint = hashlib.sha256(
        checksum.encode() + patch.read_bytes() + Path(__file__).read_bytes()
    ).hexdigest()
    generated = ROOT / ".patched-deps"
    generated.mkdir(exist_ok=True)
    target = generated / name
    stamp = target / ".talon-patch"
    # Serialize preparation across CI helpers or concurrent local invocations.
    with (generated / f".{name}.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if stamp.is_file() and stamp.read_text() == fingerprint:
            print(f"{name} {version} patch already prepared")
            return
        if target.exists() and not stamp.is_file():
            raise RuntimeError(f"Refusing to replace unmanaged directory: {target}")
        if archive is not None:
            data = Path(archive).read_bytes()
        else:
            url = f"https://static.crates.io/crates/{name}/{name}-{version}.crate"
            with urllib.request.urlopen(url, timeout=60) as response:
                data = response.read()
        if hashlib.sha256(data).hexdigest() != checksum:
            raise RuntimeError(f"{name} archive SHA-256 mismatch")
        # Complete extraction and patching before replacing a previous tree.
        with tempfile.TemporaryDirectory(prefix=f"{name}-", dir=generated) as tmp:
            stage = Path(tmp)
            with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as release:
                prefix = f"{name}-{version}/"
                for member in release:
                    if member.isdir():
                        continue
                    if not member.isfile() or not member.name.startswith(prefix):
                        raise RuntimeError(f"Unexpected archive entry: {member.name}")
                    relative = Path(member.name[len(prefix):])
                    if relative.is_absolute() or ".." in relative.parts:
                        raise RuntimeError(f"Unsafe archive entry: {member.name}")
                    destination = stage / relative
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    with release.extractfile(member) as source:
                        destination.write_bytes(source.read())
            subprocess.run(
                ["git", "apply", "--check", str(patch)], cwd=stage, check=True
            )
            subprocess.run(["git", "apply", str(patch)], cwd=stage, check=True)
            (stage / ".talon-patch").write_text(fingerprint)
            if target.exists():
                shutil.rmtree(target)
            stage.rename(target)
        print(f"Prepared {name} {version} with Talon's patch")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", help="Use a local .crate archive (offline)")
    parser.add_argument("--monoio-http-archive", help="Offline monoio-http 0.3.12 archive")
    parser.add_argument("--rustls-archive", help="Offline monoio-rustls 0.4.0 archive")
    args = parser.parse_args()
    prepare(args.archive)
    prepare(args.monoio_http_archive, "monoio-http", "0.3.12", "ff36c863e7bdb03358cda0d3df26a86382176a89d819d2ae652265695a55be96")
    prepare(args.rustls_archive, "monoio-rustls", "0.4.0", "6e31f422825bd7fb19957af6eaf89d7234ba143fcc0e515f5a2f526e332d1875")
