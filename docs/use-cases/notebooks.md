# Notebooks and data sharing

## The problem

Interactive analysis has a different profile from batch compute:

- **Exploration is repetitive.** The same file is opened repeatedly as a query
  is refined, and each open pays origin latency again.
- **Tools expect a filesystem.** Notebooks, pandas, and command-line utilities
  open paths; making them speak an object-store SDK means rewriting analysis
  code.
- **Teams read the same data.** Several people exploring one dataset each pull
  their own copy from the origin.
- **Credentials spread.** Every notebook that talks to the origin directly needs
  its own storage credentials.

## What Talon does

**Or skips the filesystem entirely.** The [Python client](../clients/python.md)
reads objects and byte ranges directly, which suits a notebook that already
thinks in those terms and avoids needing a privileged mount.

**Presents object storage as a POSIX filesystem.** The FUSE mount maps backend
namespaces to paths — `/s3/<bucket>/<key>`, `/gcs/...`, `/az/...` — so
`open()`, `read()`, and `ls` work against object storage with no client library
and no code change.

**Shares one cache across a team.** A colleague opening the same file reads from
NVMe, because someone already pulled it. The cache is a fleet resource, not a
per-user copy.

**Keeps the second open fast.** Iterative exploration re-reads the same bytes;
after the first read they are local.

**Centralises credentials.** Workers hold the storage credentials and clients
talk to workers, so notebooks need access to the cluster rather than to the
bucket. Credentials are read from the environment at use time and are kept out
of config structs and logs.

## Practical notes

- **Mount tuning is applied for large reads.** The kernel mount is configured
  for large reads and cached immutable data rather than general-purpose
  filesystem semantics.
- **This is a cache view, not a filesystem.** Directory listings are synthesised
  from the object namespace; POSIX metadata beyond size and type is
  approximated. It is a good way to *read* object storage, not a replacement for
  a shared filesystem.
- **Writes go through to the origin.** Creating or writing a file under the
  mount writes through to the backing store on release, and the object is cached
  on the way past.
