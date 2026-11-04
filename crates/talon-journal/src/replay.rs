//! Bounded read/decode pipeline. Framing stays sequential; checksum and record decoding
//! work runs on CPU threads. Results are consumed in file order even when a
//! later batch finishes first, preserving state-machine and LSN dependencies.
use super::{
    frame::{self, HEADER, MAGIC, MAX_RECORD},
    invalid,
};
use std::io;
use std::{
    collections::VecDeque,
    io::{BufReader, Read},
    path::Path,
    sync::mpsc,
};

const READ_BUFFER: usize = 1024 * 1024;
const BATCH_BYTES: usize = 512 * 1024;
const BATCH_RECORDS: usize = 1024;
const WINDOW_BYTES: usize = 64 * 1024 * 1024;

struct Batch {
    bytes: Vec<u8>,
    end: u64,
}

struct Frames {
    file: BufReader<std::fs::File>,
    length: u64,
    position: u64,
    header: Option<[u8; HEADER]>,
    tail_ok: bool,
    finished: bool,
}

impl Frames {
    fn open(path: &Path, tail_ok: bool) -> io::Result<Self> {
        let file = std::fs::File::open(path)?;
        let length = file.metadata()?.len();
        Ok(Self {
            file: BufReader::with_capacity(READ_BUFFER, file),
            length,
            position: 0,
            header: None,
            tail_ok,
            finished: false,
        })
    }

    fn next(&mut self) -> io::Result<Option<Batch>> {
        let mut bytes = Vec::new();
        let mut records = 0;
        while !self.finished && self.position < self.length && records < BATCH_RECORDS {
            if self.length - self.position < HEADER as u64 {
                if !self.tail_ok {
                    return Err(invalid("truncated journal header"));
                }
                self.finished = true;
                break;
            }
            let header = match self.header.take() {
                Some(header) => header,
                None => {
                    let mut header = [0; HEADER];
                    self.file.read_exact(&mut header)?;
                    header
                }
            };
            if &header[..8] != MAGIC {
                return Err(invalid("unsupported/corrupt journal format"));
            }
            let size = u32::from_le_bytes(header[8..12].try_into().unwrap());
            if !size != u32::from_le_bytes(header[12..16].try_into().unwrap()) {
                return Err(invalid("journal frame length checksum mismatch"));
            }
            let n = size as usize;
            if n > MAX_RECORD {
                return Err(invalid("invalid journal frame length"));
            }
            if self.length - self.position - (HEADER as u64) < n as u64 {
                if !self.tail_ok {
                    return Err(invalid("truncated journal body"));
                }
                self.finished = true;
                break;
            }
            // One oversized frame may exceed the batch target, but never the
            // record limit. Do not read its body until window space is available.
            if !bytes.is_empty() && bytes.len() + HEADER + n > BATCH_BYTES {
                self.header = Some(header);
                break;
            }
            let body = bytes.len() + HEADER;
            bytes.extend_from_slice(&header);
            bytes.resize(body + n, 0);
            self.file.read_exact(&mut bytes[body..])?;
            self.position += (HEADER + n) as u64;
            records += 1;
        }
        Ok((!bytes.is_empty()).then_some(Batch {
            bytes,
            end: self.position,
        }))
    }
}

/// The result is the end of the last complete physical record. Neither an
/// asynchronous decoder error nor a visitor error may turn into a torn tail.
pub(crate) fn read<T: Send>(
    path: &Path,
    tail_ok: bool,
    threads: usize,
    decode: fn(&[u8]) -> io::Result<T>,
    mut visit: impl FnMut(u64, T) -> io::Result<()>,
) -> io::Result<u64> {
    let mut frames = Frames::open(path, tail_ok)?;
    let threads = threads
        .max(1)
        .min(frames.length.div_ceil(BATCH_BYTES as u64).max(1) as usize);
    if threads == 1 {
        while let Some(batch) = frames.next()? {
            for (lsn, value) in frame::decode(batch.bytes, decode)? {
                visit(lsn, value)?;
            }
        }
        return Ok(frames.position);
    }
    // Count the framing/replay caller against the CPU budget too.
    let threads = threads - 1;
    std::thread::scope(|scope| {
        let mut senders = Vec::new();
        let mut tasks = Vec::new();
        for id in 0..threads {
            let (send, recv) =
                mpsc::sync_channel::<(Vec<u8>, mpsc::Sender<io::Result<Vec<(u64, T)>>>)>(1);
            senders.push(send);
            tasks.push(
                std::thread::Builder::new()
                    .name(format!("journal-decode-{id}"))
                    .spawn_scoped(scope, move || {
                        while let Ok((bytes, done)) = recv.recv() {
                            let _ = done.send(frame::decode(bytes, decode));
                        }
                    })?,
            );
        }
        let result = (|| {
            let mut pending = VecDeque::new();
            let mut queued = 0;
            let mut next_worker = 0;
            let mut eof = false;
            let mut offset = 0;
            // Leave room for one maximum-sized frame before reading ahead.
            // The count bound also limits decoded objects and reorder storage.
            while !eof || !pending.is_empty() {
                while !eof
                    && pending.len() < threads * 2
                    && queued <= WINDOW_BYTES - MAX_RECORD - HEADER
                {
                    match frames.next()? {
                        None => eof = true,
                        Some(batch) => {
                            let size = batch.bytes.len();
                            let (done, wait) = mpsc::channel();
                            senders[next_worker % threads]
                                .send((batch.bytes, done))
                                .map_err(|_| invalid("journal decoder stopped"))?;
                            next_worker += 1;
                            queued += size;
                            pending.push_back((size, batch.end, wait));
                        }
                    }
                }
                if let Some((size, end, wait)) = pending.pop_front() {
                    for (lsn, value) in wait.recv().map_err(io::Error::other)?? {
                        visit(lsn, value)?;
                    }
                    offset = end;
                    queued -= size;
                }
            }
            Ok(offset)
        })();
        // Close both directions before joining on all failure paths. Decoder
        // outputs are one-shot and never wait for a consumer that has failed.
        drop(senders);
        for task in tasks {
            task.join()
                .map_err(|_| invalid("journal decoder panicked"))?;
        }
        result
    })
}
