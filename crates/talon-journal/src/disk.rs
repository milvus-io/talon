use super::*;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::MetadataExt,
    rc::Rc,
};
use xxhash_rust::xxh3::xxh3_64;

const CURRENT_MAGIC: &[u8; 8] = b"TLNJCU01";
const MAX_GENERATIONS: usize = 64;
#[derive(Clone)]
struct Current {
    snapshot: u64,
    lsn: u64,
    bytes: u64,
    records: u64,
    logs: Vec<u64>,
    garbage: Vec<(bool, u64)>,
    pending: u64,
}
impl Current {
    fn encode(&self) -> io::Result<Bytes> {
        if self.logs.len() > MAX_GENERATIONS || self.garbage.len() > MAX_GENERATIONS * 2 {
            return Err(invalid("too many journal generations"));
        }
        let mut b = CURRENT_MAGIC.to_vec();
        for n in [
            self.snapshot,
            self.lsn,
            self.bytes,
            self.records,
            self.pending,
            self.logs.len() as u64,
            self.garbage.len() as u64,
        ] {
            b.extend_from_slice(&n.to_le_bytes());
        }
        for n in &self.logs {
            b.extend_from_slice(&n.to_le_bytes());
        }
        for (snapshot, n) in &self.garbage {
            b.extend_from_slice(&u64::from(*snapshot).to_le_bytes());
            b.extend_from_slice(&n.to_le_bytes());
        }
        b.extend_from_slice(&xxh3_64(&b).to_le_bytes());
        Ok(b.into())
    }
    fn decode(b: &[u8]) -> io::Result<Self> {
        if b.len() < 72
            || !b.starts_with(CURRENT_MAGIC)
            || xxh3_64(&b[..b.len() - 8])
                != u64::from_le_bytes(b[b.len() - 8..].try_into().unwrap())
        {
            return Err(invalid("journal CURRENT version/checksum"));
        }
        let mut input = &b[8..b.len() - 8];
        fn n(b: &mut &[u8]) -> io::Result<u64> {
            if b.len() < 8 {
                return Err(invalid("truncated CURRENT"));
            }
            let v = u64::from_le_bytes(b[..8].try_into().unwrap());
            *b = &b[8..];
            Ok(v)
        }
        let snapshot = n(&mut input)?;
        let lsn = n(&mut input)?;
        let bytes = n(&mut input)?;
        let records = n(&mut input)?;
        let pending = n(&mut input)?;
        let logs = n(&mut input)?;
        let garbage = n(&mut input)?;
        if logs == 0 || logs > MAX_GENERATIONS as u64 || garbage > 2 * MAX_GENERATIONS as u64 {
            return Err(invalid("CURRENT generation count"));
        }
        let logs = (0..logs)
            .map(|_| n(&mut input))
            .collect::<io::Result<Vec<_>>>()?;
        let garbage = (0..garbage)
            .map(|_| {
                let kind = n(&mut input)?;
                if kind > 1 {
                    return Err(invalid("CURRENT file kind"));
                }
                Ok((kind == 1, n(&mut input)?))
            })
            .collect::<io::Result<Vec<_>>>()?;
        if !input.is_empty()
            || snapshot == 0
            || logs[0] != snapshot
            || logs.windows(2).any(|p| p[1] != p[0] + 1)
            || (pending != 0 && pending != *logs.last().unwrap())
            || garbage
                .iter()
                .any(|(s, g)| (*s && *g == snapshot) || (!s && logs.contains(g)))
        {
            return Err(invalid("CURRENT recovery set"));
        }
        Ok(Self {
            snapshot,
            lsn,
            bytes,
            records,
            logs,
            garbage,
            pending,
        })
    }
}
fn path(root: &Path, snapshot: bool, generation: u64) -> PathBuf {
    root.join(format!(
        "{}.{generation:020}",
        if snapshot { "snapshot" } else { "journal" }
    ))
}
fn stat(path: &Path) -> io::Result<u64> {
    Ok(std::fs::metadata(path)?.blocks() * 512)
}
fn write_sync(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut f = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    f.write_all(bytes)?;
    f.sync_all()
}
fn publish_sync(root: &Path, current: &Current) -> io::Result<()> {
    write_sync(&root.join("CURRENT.tmp"), &current.encode()?)?;
    std::fs::rename(root.join("CURRENT.tmp"), root.join("CURRENT"))?;
    File::open(root)?.sync_all()
}
fn remove_sync(path: &Path) -> io::Result<()> {
    match std::fs::remove_file(path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        r => r,
    }
}

pub(crate) struct Accounting {
    files: Mutex<BTreeMap<PathBuf, u64>>,
    total: Arc<AtomicU64>,
}
impl Accounting {
    fn set(&self, path: PathBuf, n: u64) {
        let mut files = self.files.lock().unwrap();
        files.insert(path, n);
        self.total.store(files.values().sum(), Ordering::Relaxed);
    }
    fn remove(&self, path: &Path) {
        let mut files = self.files.lock().unwrap();
        files.remove(path);
        self.total.store(files.values().sum(), Ordering::Relaxed);
    }
}
pub(crate) struct Disk {
    #[cfg(test)]
    control: Arc<crate::tests::Control>,
    root: PathBuf,
    current: Current,
    pub file: Rc<monoio::fs::File>,
    pub offset: u64,
    pub lsn: u64,
    pub options: StreamOptions,
    accounting: Arc<Accounting>,
}
struct Recovered<M> {
    root: PathBuf,
    current: Current,
    offset: u64,
    lsn: u64,
    machine: M,
    files: BTreeMap<PathBuf, u64>,
}
fn recover<M: Machine>(root: PathBuf, mut machine: M, threads: usize) -> io::Result<Recovered<M>> {
    std::fs::create_dir_all(&root)?;
    let current_bytes = (|| {
        let f = File::open(root.join("CURRENT"))?;
        let mut bytes = Vec::new();
        f.take(65537).read_to_end(&mut bytes)?;
        if bytes.len() > 65536 {
            return Err(invalid("journal CURRENT too large"));
        }
        Ok(bytes)
    })();
    let mut current = match current_bytes {
        Ok(b) => Current::decode(&b)?,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let c = Current {
                snapshot: 1,
                lsn: 0,
                bytes: 0,
                records: 0,
                logs: vec![1],
                garbage: Vec::new(),
                pending: 0,
            };
            write_sync(&path(&root, true, 1), &[])?;
            write_sync(&path(&root, false, 1), &[])?;
            File::open(&root)?.sync_all()?;
            publish_sync(&root, &c)?;
            c
        }
        Err(e) => return Err(e),
    };
    let snapshot = path(&root, true, current.snapshot);
    if std::fs::metadata(&snapshot)?.len() != current.bytes {
        return Err(invalid("snapshot length mismatch"));
    }
    let mut ordinal = 0;
    replay::read(&snapshot, false, threads, M::decode_snapshot, |n, r| {
        ordinal += 1;
        if n != ordinal {
            return Err(invalid("snapshot ordinal gap"));
        }
        machine.restore(r)
    })?;
    if ordinal != current.records {
        return Err(invalid("snapshot record count mismatch"));
    }
    let mut lsn = current.lsn;
    let mut offset = 0;
    for (i, g) in current.logs.iter().enumerate() {
        offset = replay::read(
            &path(&root, false, *g),
            i + 1 == current.logs.len(),
            threads,
            M::decode,
            |n, r| {
                if Some(n) != lsn.checked_add(1) {
                    return Err(invalid("journal LSN gap"));
                }
                machine.apply(n, r)?;
                lsn = n;
                Ok(())
            },
        )?;
    }
    let last = path(&root, false, *current.logs.last().unwrap());
    let file = OpenOptions::new().write(true).open(&last)?;
    if file.metadata()?.len() != offset {
        file.set_len(offset)?;
        file.sync_all()?;
    }
    for (s, g) in &current.garbage {
        remove_sync(&path(&root, *s, *g))?;
    }
    if current.pending != 0 {
        remove_sync(&path(&root, true, current.pending))?;
    }
    for name in ["CURRENT.tmp", "journal.tmp", "snapshot.tmp"] {
        remove_sync(&root.join(name))?;
    }
    current.garbage.clear();
    current.pending = 0;
    File::open(&root)?.sync_all()?;
    publish_sync(&root, &current)?;
    let mut files = BTreeMap::new();
    for p in std::iter::once(snapshot)
        .chain(current.logs.iter().map(|g| path(&root, false, *g)))
        .chain(std::iter::once(root.join("CURRENT")))
    {
        files.insert(p.clone(), stat(&p)?);
    }
    Ok(Recovered {
        root,
        current,
        offset,
        lsn,
        machine,
        files,
    })
}
async fn create(path: &Path) -> io::Result<monoio::fs::File> {
    monoio::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .await
}
async fn remove(path: &Path) -> io::Result<()> {
    match monoio::fs::remove_file(path).await {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        r => r,
    }
}
async fn sync_dir(root: &Path) -> io::Result<()> {
    let f = monoio::fs::File::open(root).await?;
    f.sync_all().await?;
    f.close().await
}
impl Disk {
    #[cfg(test)]
    pub fn with_control(mut self, control: Arc<crate::tests::Control>) -> Self {
        self.control = control;
        self
    }
    pub async fn open<M: Machine>(
        root: PathBuf,
        machine: M,
        options: StreamOptions,
        threads: usize,
        total: Arc<AtomicU64>,
    ) -> io::Result<(Self, M)> {
        let (send, wait) = oneshot::channel();
        std::thread::Builder::new()
            .name("journal-recover".into())
            .spawn(move || {
                let _ = send.send(recover(root, machine, threads));
            })?;
        let r = wait.await.map_err(io::Error::other)??;
        let file = monoio::fs::OpenOptions::new()
            .write(true)
            .open(path(&r.root, false, *r.current.logs.last().unwrap()))
            .await?;
        total.store(r.files.values().sum(), Ordering::Relaxed);
        Ok((
            Self {
                #[cfg(test)]
                control: Default::default(),
                root: r.root,
                current: r.current,
                offset: r.offset,
                lsn: r.lsn,
                file: Rc::new(file),
                options,
                accounting: Arc::new(Accounting {
                    files: Mutex::new(r.files),
                    total,
                }),
            },
            r.machine,
        ))
    }
    pub fn write(&self, bytes: Bytes, offset: u64) -> LocalBoxFuture<'static, io::Result<()>> {
        let file = self.file.clone();
        let accounting = self.accounting.clone();
        let path = path(&self.root, false, *self.current.logs.last().unwrap());
        #[cfg(test)]
        let control = self.control.clone();
        Box::pin(async move {
            #[cfg(test)]
            {
                control.writes.fetch_add(1, Ordering::Relaxed);
                crate::tests::pause(&control.write_pause).await;
            }
            file.write_all_at(bytes, offset).await.0?;
            let n = file.metadata().await?.blocks() * 512;
            let old = accounting
                .files
                .lock()
                .unwrap()
                .get(&path)
                .copied()
                .unwrap_or(0);
            accounting.set(path, n.max(old));
            Ok(())
        })
    }
    pub fn sync(&self) -> LocalBoxFuture<'static, io::Result<()>> {
        let f = self.file.clone();
        #[cfg(test)]
        let control = self.control.clone();
        Box::pin(async move {
            #[cfg(test)]
            {
                control.syncs.fetch_add(1, Ordering::Relaxed);
                crate::tests::pause(&control.sync_pause).await;
                if control.fail_sync.load(Ordering::Relaxed) {
                    return Err(io::Error::other("injected sync failure"));
                }
            }
            f.sync_all().await
        })
    }
    async fn publish(&self) -> io::Result<()> {
        let temp = self.root.join("CURRENT.tmp");
        let f = create(&temp).await?;
        f.write_all_at(self.current.encode()?, 0).await.0?;
        f.sync_all().await?;
        let n = f.metadata().await?.blocks() * 512;
        f.close().await?;
        monoio::fs::rename(temp, self.root.join("CURRENT")).await?;
        sync_dir(&self.root).await?;
        self.accounting.set(self.root.join("CURRENT"), n);
        Ok(())
    }
    pub async fn rotate(&mut self) -> io::Result<u64> {
        self.sync().await?;
        let generation = self
            .current
            .logs
            .last()
            .unwrap()
            .checked_add(1)
            .ok_or_else(|| invalid("journal generation overflow"))?;
        let tmp = self.root.join("journal.tmp");
        let f = create(&tmp).await?;
        f.sync_all().await?;
        let p = path(&self.root, false, generation);
        monoio::fs::rename(tmp, &p).await?;
        sync_dir(&self.root).await?;
        self.accounting.set(p, f.metadata().await?.blocks() * 512);
        self.current.logs.push(generation);
        self.current.pending = generation;
        self.publish().await?;
        #[cfg(test)]
        crate::tests::crash("rotate");
        self.file = Rc::new(f);
        self.offset = 0;
        Ok(generation)
    }
    pub fn snapshot<M: Machine>(
        &self,
        generation: u64,
        lsn: u64,
        snapshot: M::Snapshot,
        policy: Option<Arc<dyn Maintenance>>,
        slots: Arc<Semaphore>,
    ) -> LocalBoxFuture<'static, io::Result<Snapshot>> {
        let root = self.root.clone();
        let accounting = self.accounting.clone();
        #[cfg(test)]
        let control = self.control.clone();
        Box::pin(async move {
            // One shared service bounds snapshot encoder threads/buffers even
            // if thousands of namespaces cross the rotation threshold together.
            let _slot = slots.acquire_owned().await.map_err(io::Error::other)?;
            // At most four chunks beyond the encoder's current frame. Blocking
            // sends happen only on the CPU encoder, never on the I/O runtime.
            let (send, mut receive) = mpsc::channel::<io::Result<Bytes>>(4);
            std::thread::Builder::new()
                .name("journal-snapshot".into())
                .spawn(move || {
                    let mut buffer = Vec::new();
                    let mut ordinal = 0;
                    let result = (|| {
                        for item in snapshot {
                            let bytes = item?;
                            ordinal += 1;
                            let encoded = frame::encode(ordinal, &bytes)?;
                            if !buffer.is_empty() && buffer.len() + encoded.len() > 256 << 10 {
                                send.blocking_send(Ok(std::mem::take(&mut buffer).into()))
                                    .map_err(|_| stopped())?;
                            }
                            buffer.extend_from_slice(&encoded);
                        }
                        if !buffer.is_empty() {
                            send.blocking_send(Ok(buffer.into()))
                                .map_err(|_| stopped())?;
                        }
                        Ok::<_, io::Error>(())
                    })();
                    match result {
                        Err(e) => {
                            let _ = send.blocking_send(Err(e));
                        }
                        Ok(()) => {
                            let _ = send.blocking_send(Ok(Bytes::new()));
                        }
                    }
                })?;
            let temp = root.join("snapshot.tmp");
            let f = {
                let _io = match &policy {
                    Some(p) => Some(p.io().await),
                    None => None,
                };
                create(&temp).await?
            };
            let mut offset = 0;
            let mut records = 0;
            let mut encoded_all = false;
            while let Some(bytes) = receive.recv().await {
                let bytes = bytes?;
                if bytes.is_empty() {
                    encoded_all = true;
                    break;
                }
                // Count validated local frames without decoding business state.
                let mut pos = 0;
                while pos < bytes.len() {
                    let n =
                        u32::from_le_bytes(bytes[pos + 8..pos + 12].try_into().unwrap()) as usize;
                    pos += frame::HEADER + n;
                    records += 1;
                }
                if let Some(p) = &policy {
                    p.bytes(bytes.len()).await;
                }
                let _io = match &policy {
                    Some(p) => Some(p.io().await),
                    None => None,
                };
                let n = bytes.len() as u64;
                f.write_all_at(bytes, offset).await.0?;
                offset += n;
                accounting.set(temp.clone(), f.metadata().await?.blocks() * 512);
            }
            if !encoded_all {
                return Err(io::Error::other("snapshot encoder stopped"));
            }
            #[cfg(test)]
            crate::tests::pause(&control.snapshot_finalize_pause).await;
            // Acquire outside the writer loop, which must keep serving callers
            // that can hold this permit. Retain it through CURRENT and cleanup.
            let io = match &policy {
                Some(p) => Some(p.io().await),
                None => None,
            };
            f.sync_all().await?;
            let allocated = f.metadata().await?.blocks() * 512;
            f.close().await?;
            let p = path(&root, true, generation);
            monoio::fs::rename(&temp, &p).await?;
            accounting.remove(&temp);
            accounting.set(p, allocated);
            sync_dir(&root).await?;
            #[cfg(test)]
            crate::tests::crash("snapshot_file");
            Ok(Snapshot {
                generation,
                lsn,
                bytes: offset,
                records,
                policy,
                _io: io,
            })
        })
    }
    pub async fn publish_snapshot(&mut self, snapshot: Snapshot) -> io::Result<()> {
        self.current.garbage.push((true, self.current.snapshot));
        for &g in &self.current.logs {
            if g < snapshot.generation {
                self.current.garbage.push((false, g));
            }
        }
        self.current.snapshot = snapshot.generation;
        self.current.lsn = snapshot.lsn;
        self.current.bytes = snapshot.bytes;
        self.current.records = snapshot.records;
        self.current.pending = 0;
        self.current.logs.retain(|g| *g >= snapshot.generation);
        self.publish().await?;
        #[cfg(test)]
        crate::tests::crash("snapshot_current");
        for (s, g) in &self.current.garbage {
            if let Some(p) = &snapshot.policy {
                p.delete().await;
            }
            let p = path(&self.root, *s, *g);
            remove(&p).await?;
            self.accounting.remove(&p);
        }
        sync_dir(&self.root).await?;
        self.current.garbage.clear();
        #[cfg(test)]
        crate::tests::crash("cleanup");
        self.publish().await
    }
}
pub(crate) struct Snapshot {
    generation: u64,
    lsn: u64,
    bytes: u64,
    records: u64,
    policy: Option<Arc<dyn Maintenance>>,
    _io: Option<Box<dyn Send>>,
}
