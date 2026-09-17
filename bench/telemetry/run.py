#!/usr/bin/env python3
"""Bounded local probe. Saves raw repetitions; does not certify production latency."""
import argparse
import json
import os
from pathlib import Path
import random
import shutil
import subprocess
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[2]
parser = argparse.ArgumentParser()
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--build-only', action='store_true')
parser.add_argument('--run-only', action='store_true')
parser.add_argument('--rounds', type=int, default=7)
args = parser.parse_args()
args.output.mkdir(parents=True, exist_ok=True)
state = args.output / 'build.json'
if (not args.run_only and state.exists()) or (not args.build_only and (args.output / 'raw.jsonl').exists()):
    raise SystemExit('Output already contains results; use a new directory to preserve earlier measurements.')
env = dict(os.environ, CARGO_BUILD_JOBS='2', NO_PROXY='127.0.0.1,localhost', no_proxy='127.0.0.1,localhost')
if not args.run_only:
    work = Path(tempfile.mkdtemp(prefix='talon-telemetry-bench-'))
    base = work / 'baseline'
    base.mkdir()
    archive = work / 'head.tar'
    with archive.open('wb') as f:
        subprocess.run(['git', 'archive', 'HEAD'], cwd=ROOT, stdout=f, check=True)
    with tarfile.open(archive) as f:
        f.extractall(base)  # Archive comes from the local pinned Git commit.
    head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    binaries = {}
    for name, source, features in [('baseline', base, ''), ('compile-off', ROOT, 'instrumentation'), ('recording', ROOT, 'recording')]:
        probe = work / name.replace('baseline', 'baseline-probe')
        probe.mkdir(exist_ok=True)
        (probe / 'src').mkdir()
        shutil.copyfile(ROOT / 'bench/telemetry/overhead.rs', probe / 'src/main.rs')
        deps = '\n'.join(f'{c} = {{ path = "{source}/crates/{c}" }}' for c in ['talon-cache-client', 'talon-core', 'talon-transport'])
        telemetry = ''
        if name != 'baseline':
            telemetry = f'''talon-telemetry = {{ path = "{ROOT}/crates/talon-telemetry", optional = true }}
opentelemetry = {{ version = "=0.28.0", optional = true }}
opentelemetry_sdk = {{ version = "=0.28.0", optional = true }}
tracing-opentelemetry = {{ version = "=0.29.0", optional = true }}
tracing = {{ version = "0.1", optional = true }}
tracing-subscriber = {{ version = "0.3", optional = true }}
[features]
instrumentation = ["dep:talon-telemetry"]
recording = ["instrumentation", "talon-telemetry/export", "dep:opentelemetry", "dep:opentelemetry_sdk", "dep:tracing", "dep:tracing-subscriber", "dep:tracing-opentelemetry"]
'''
        (probe / 'Cargo.toml').write_text(f'''[package]
name = "talon-telemetry-probe"
version = "0.1.0"
edition = "2021"
[workspace]
[dependencies]
{deps}
tokio = {{ version = "1", features = ["full"] }}
{telemetry}
''')
        shutil.copyfile(source / 'Cargo.lock', probe / 'Cargo.lock')
        cmd = ['cargo', 'build', '--release', '--offline', '--manifest-path', str(probe / 'Cargo.toml'), '--target-dir', str(work / 'target')]
        if features:
            cmd += ['--features', features]
        with (args.output / f'build-{name}.log').open('w') as log:
            subprocess.run(cmd, cwd=ROOT, env=env, stdout=log, stderr=log, check=True)
        executable = work / f'probe-{name}'
        shutil.copyfile(work / 'target/release/talon-telemetry-probe', executable)
        executable.chmod(0o755)
        binaries[name] = str(executable)
    state.write_text(json.dumps({'head': head, 'work': str(work), 'binaries': binaries, 'cpus': sorted(os.sched_getaffinity(0))[-2:]}, indent=2))
if args.build_only:
    raise SystemExit(0)
config = json.loads(state.read_text())
os.sched_setaffinity(0, config['cpus'])
cases = [('baseline', 'off'), ('compile-off', 'off'), ('recording', 'off'), ('recording', 'propagate'), ('recording', 'unsampled'), ('recording', 'one-percent'), ('recording', 'sampled')]
with (args.output / 'raw.jsonl').open('w') as output:
    for repeat in range(args.rounds):
        shuffled = cases.copy()
        random.Random(repeat).shuffle(shuffled)
        for build, mode in shuffled:
            for kind, size, iterations in [('scope', 64, 20000), ('rpc', 64, 5000), ('rpc', 4096, 5000)]:
                raw = subprocess.check_output([config['binaries'][build], kind, mode, str(iterations), str(size)], env=env, text=True, timeout=30)
                row = dict(json.loads(raw), build=build, repeat=repeat)
                output.write(json.dumps(row) + '\n')
                output.flush()
print(args.output / 'raw.jsonl')
