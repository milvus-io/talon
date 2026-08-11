//! Streaming decoder for the `aws-chunked` content coding used by
//! `STREAMING-UNSIGNED-PAYLOAD-TRAILER` uploads.
//!
//! Current AWS SDKs default to flexible checksums, which wraps every streaming
//! `PutObject`/`UploadPart` body in `aws-chunked` framing (a hex size line, the
//! payload, `CRLF`, a zero-length chunk, then trailer lines) and moves the real
//! length to `x-amz-decoded-content-length`. This decoder strips that framing
//! and yields only the payload, so the existing `UNSIGNED-PAYLOAD` forwarding
//! path can stream it to the origin.
//!
//! The framing carries a trailing checksum. It is verified at the gateway over
//! the decoded payload: the digest is computed incrementally as the payload is
//! decoded, and the trailer value is compared before the object is committed.
//! A declared trailer algorithm the gateway cannot compute is rejected rather
//! than ignored. Because the checksum value only arrives after the payload, the
//! adapter decodes into a bounded spool and verifies before dispatching to the
//! origin, so a checksum (or length) mismatch fails closed instead of storing a
//! corrupt object.
//!
//! The pure `Decoder::drive` state machine holds all parsing and verification
//! logic so it can be unit-tested without any I/O.

use std::pin::Pin;
use std::task::{Context, Poll};

use base64::Engine;
use bytes::{Buf, Bytes, BytesMut};
use futures::Stream;

/// Longest chunk header accepted, in bytes. A real one is ~90 bytes
/// (16 hex + `;chunk-signature=` + 64 hex + CRLF). The cap stops a peer that
/// never sends a newline from growing the buffer without bound.
const MAX_CHUNK_HEADER: usize = 1024;

/// Maximum trailer lines drained after the terminating zero chunk.
const MAX_TRAILERS: usize = 64;

/// A flexible-checksum algorithm carried in an `aws-chunked` trailer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumAlgorithm {
    Crc32,
    Crc32c,
    Crc64Nvme,
    Sha256,
}

impl ChecksumAlgorithm {
    /// Parse an `x-amz-trailer` value such as `x-amz-checksum-crc64nvme`.
    /// Returns `None` for an unrecognized algorithm so the caller can reject it
    /// before dispatch rather than silently skip verification.
    pub fn from_trailer_name(name: &str) -> Option<Self> {
        match name.trim().to_ascii_lowercase().as_str() {
            "x-amz-checksum-crc32" => Some(Self::Crc32),
            "x-amz-checksum-crc32c" => Some(Self::Crc32c),
            "x-amz-checksum-crc64nvme" => Some(Self::Crc64Nvme),
            "x-amz-checksum-sha256" => Some(Self::Sha256),
            _ => None,
        }
    }

    fn trailer_key(self) -> &'static str {
        match self {
            Self::Crc32 => "x-amz-checksum-crc32",
            Self::Crc32c => "x-amz-checksum-crc32c",
            Self::Crc64Nvme => "x-amz-checksum-crc64nvme",
            Self::Sha256 => "x-amz-checksum-sha256",
        }
    }
}

/// Running digest for the declared trailer algorithm.
enum Digest {
    Crc32(crc::Digest<'static, u32>),
    Crc32c(crc::Digest<'static, u32>),
    Crc64Nvme(crc::Digest<'static, u64>),
    Sha256(sha2::Sha256),
}

static CRC32: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);
static CRC32C: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
static CRC64NVME: crc::Crc<u64> = crc::Crc::<u64>::new(&crc::CRC_64_NVME);

impl Digest {
    fn new(algorithm: ChecksumAlgorithm) -> Self {
        use sha2::Digest as _;
        match algorithm {
            ChecksumAlgorithm::Crc32 => Self::Crc32(CRC32.digest()),
            ChecksumAlgorithm::Crc32c => Self::Crc32c(CRC32C.digest()),
            ChecksumAlgorithm::Crc64Nvme => Self::Crc64Nvme(CRC64NVME.digest()),
            ChecksumAlgorithm::Sha256 => Self::Sha256(sha2::Sha256::new()),
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        use sha2::Digest as _;
        match self {
            Self::Crc32(d) | Self::Crc32c(d) => d.update(bytes),
            Self::Crc64Nvme(d) => d.update(bytes),
            Self::Sha256(d) => d.update(bytes),
        }
    }

    /// The base64 checksum, matching the form S3 carries in the trailer.
    fn finalize_base64(self) -> String {
        use sha2::Digest as _;
        let engine = base64::engine::general_purpose::STANDARD;
        match self {
            Self::Crc32(d) | Self::Crc32c(d) => engine.encode(d.finalize().to_be_bytes()),
            Self::Crc64Nvme(d) => engine.encode(d.finalize().to_be_bytes()),
            Self::Sha256(d) => engine.encode(d.finalize()),
        }
    }
}

/// A fault in the `aws-chunked` framing. Diagnostic strings never include a
/// chunk signature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkDecodeError {
    /// The framing is structurally invalid.
    Malformed(&'static str),
    /// The decoded payload length disagreed with `x-amz-decoded-content-length`.
    /// Refusing here is what stops a truncated upload from being stored as an
    /// object with missing bytes.
    LengthMismatch { declared: u64, decoded: u64 },
    /// The trailer checksum did not match the decoded payload.
    ChecksumMismatch,
    /// The trailer the client promised in `x-amz-trailer` never arrived.
    MissingChecksum,
    /// The underlying request body stream failed.
    Source(String),
}

impl std::fmt::Display for ChunkDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Malformed(reason) => write!(f, "malformed aws-chunked framing: {reason}"),
            Self::LengthMismatch { declared, decoded } => write!(
                f,
                "aws-chunked body declared {declared} payload bytes but decoded {decoded}"
            ),
            Self::ChecksumMismatch => {
                write!(f, "aws-chunked trailer checksum did not match the payload")
            }
            Self::MissingChecksum => {
                write!(f, "aws-chunked body omitted its declared trailer checksum")
            }
            Self::Source(reason) => write!(f, "request body could not be read: {reason}"),
        }
    }
}

impl std::error::Error for ChunkDecodeError {}

#[derive(Debug, PartialEq, Eq)]
enum Phase {
    /// Reading a `<hex>[;ext]` size line.
    Header,
    /// Emitting the `remaining` payload bytes of the current chunk.
    Data { remaining: u64 },
    /// Consuming the `CRLF` that terminates a chunk's payload.
    AfterData,
    /// Draining trailer lines up to the blank line that ends the body.
    Trailer,
    /// The terminating chunk and trailers have been consumed.
    Done,
}

/// One step of the pure state machine.
#[derive(Debug, PartialEq, Eq)]
enum Drive {
    /// A run of decoded payload bytes is ready.
    Emit(Bytes),
    /// More source bytes are needed to make progress.
    NeedMore,
    /// The body is fully decoded.
    Done,
    /// The framing is invalid.
    Err(ChunkDecodeError),
}

/// Pure, I/O-free `aws-chunked` parser and checksum verifier. Feed it bytes
/// with [`Decoder::extend`] / [`Decoder::finish`], then pull payload with
/// [`Decoder::drive`].
struct Decoder {
    buf: BytesMut,
    phase: Phase,
    declared: u64,
    decoded: u64,
    trailer_lines: usize,
    source_done: bool,
    algorithm: Option<ChecksumAlgorithm>,
    digest: Option<Digest>,
    expected_checksum: Option<String>,
}

impl Decoder {
    fn new(declared: u64, algorithm: Option<ChecksumAlgorithm>) -> Self {
        Self {
            buf: BytesMut::new(),
            phase: Phase::Header,
            declared,
            decoded: 0,
            trailer_lines: 0,
            source_done: false,
            algorithm,
            digest: algorithm.map(Digest::new),
            expected_checksum: None,
        }
    }

    fn extend(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    fn finish(&mut self) {
        self.source_done = true;
    }

    /// Verify the decoded length and, when an algorithm was declared, the
    /// trailer checksum over the decoded payload.
    fn verify(&mut self) -> Result<(), ChunkDecodeError> {
        if self.decoded != self.declared {
            return Err(ChunkDecodeError::LengthMismatch {
                declared: self.declared,
                decoded: self.decoded,
            });
        }
        if self.algorithm.is_some() {
            let computed = self
                .digest
                .take()
                .expect("digest present when an algorithm was declared")
                .finalize_base64();
            match self.expected_checksum.take() {
                Some(expected) if expected == computed => {}
                Some(_) => return Err(ChunkDecodeError::ChecksumMismatch),
                None => return Err(ChunkDecodeError::MissingChecksum),
            }
        }
        Ok(())
    }

    /// Advance parsing using only the buffered bytes.
    fn drive(&mut self) -> Drive {
        loop {
            match self.phase {
                Phase::Done => return Drive::Done,
                Phase::Data { remaining } => {
                    if self.buf.is_empty() {
                        return self.need_more_or_truncated();
                    }
                    let take = remaining.min(self.buf.len() as u64) as usize;
                    let payload = self.buf.split_to(take).freeze();
                    self.decoded += take as u64;
                    if let Some(digest) = &mut self.digest {
                        digest.update(&payload);
                    }
                    let left = remaining - take as u64;
                    self.phase = if left == 0 {
                        Phase::AfterData
                    } else {
                        Phase::Data { remaining: left }
                    };
                    return Drive::Emit(payload);
                }
                Phase::AfterData => match self.take_crlf() {
                    Some(true) => self.phase = Phase::Header,
                    Some(false) => {
                        return Drive::Err(ChunkDecodeError::Malformed(
                            "chunk data not CRLF-terminated",
                        ))
                    }
                    None => return self.need_more_or_truncated(),
                },
                Phase::Header => match self.take_line() {
                    Some(Ok(line)) => match parse_chunk_size(&line) {
                        Ok(0) => self.phase = Phase::Trailer,
                        // No independent single-chunk cap: the length accounting
                        // already bounds every chunk by `declared`, which is in
                        // turn bounded by the runtime's `max_body_bytes` on the
                        // framed body. A separate constant here would either
                        // duplicate that or, worse, reject a legitimate large
                        // chunk an SDK sends for a body under the configured
                        // limit. `checked_add` keeps the guard correct even if a
                        // hostile `declared` and chunk size sum past u64.
                        Ok(size)
                            if self
                                .decoded
                                .checked_add(size)
                                .map_or(true, |total| total > self.declared) =>
                        {
                            return Drive::Err(ChunkDecodeError::LengthMismatch {
                                declared: self.declared,
                                decoded: self.decoded.saturating_add(size),
                            })
                        }
                        Ok(size) => self.phase = Phase::Data { remaining: size },
                        Err(error) => return Drive::Err(error),
                    },
                    Some(Err(error)) => return Drive::Err(error),
                    None => return self.need_more_or_truncated(),
                },
                Phase::Trailer => match self.take_line() {
                    Some(Ok(line)) if line.is_empty() => match self.verify() {
                        Ok(()) => {
                            self.phase = Phase::Done;
                            return Drive::Done;
                        }
                        Err(error) => return Drive::Err(error),
                    },
                    Some(Ok(line)) => {
                        self.trailer_lines += 1;
                        if self.trailer_lines > MAX_TRAILERS {
                            return Drive::Err(ChunkDecodeError::Malformed("too many trailers"));
                        }
                        self.capture_trailer(&line);
                    }
                    Some(Err(error)) => return Drive::Err(error),
                    None => {
                        // Some clients stop at the last trailer with a single
                        // CRLF and no blank line. Accept EOF only when the
                        // length balances and any declared checksum matched.
                        if self.source_done {
                            return match self.verify() {
                                Ok(()) => {
                                    self.phase = Phase::Done;
                                    Drive::Done
                                }
                                Err(error) => Drive::Err(error),
                            };
                        }
                        return self.need_more_or_truncated();
                    }
                },
            }
        }
    }

    /// Record a `x-amz-checksum-<algo>: <base64>` trailer line matching the
    /// declared algorithm. Other trailer lines are ignored.
    fn capture_trailer(&mut self, line: &[u8]) {
        let Some(algorithm) = self.algorithm else {
            return;
        };
        let Ok(text) = std::str::from_utf8(line) else {
            return;
        };
        let Some((name, value)) = text.split_once(':') else {
            return;
        };
        if name.trim().eq_ignore_ascii_case(algorithm.trailer_key()) {
            self.expected_checksum = Some(value.trim().to_string());
        }
    }

    fn need_more_or_truncated(&self) -> Drive {
        if self.source_done {
            Drive::Err(ChunkDecodeError::Malformed(
                "unexpected end of aws-chunked body",
            ))
        } else {
            Drive::NeedMore
        }
    }

    /// Take one CRLF-terminated line without its terminator, or `None` if a
    /// full line is not buffered yet.
    fn take_line(&mut self) -> Option<Result<Vec<u8>, ChunkDecodeError>> {
        match find_crlf(&self.buf) {
            Some(pos) => {
                let line = self.buf.split_to(pos + 2);
                Some(Ok(line[..pos].to_vec()))
            }
            None => {
                if self.buf.len() > MAX_CHUNK_HEADER {
                    return Some(Err(ChunkDecodeError::Malformed("chunk header too long")));
                }
                None
            }
        }
    }

    /// Consume a leading CRLF. `Some(true)` on match, `Some(false)` on mismatch,
    /// `None` if fewer than two bytes are buffered.
    fn take_crlf(&mut self) -> Option<bool> {
        if self.buf.len() < 2 {
            return None;
        }
        let ok = &self.buf[..2] == b"\r\n";
        self.buf.advance(2);
        Some(ok)
    }
}

/// `<hex>[;chunk-signature=<sig>]` -> the hex size. The signature is parsed off
/// and discarded: it chains from a seed signature the gateway does not have and
/// is meaningless once the framing is stripped.
fn parse_chunk_size(line: &[u8]) -> Result<u64, ChunkDecodeError> {
    let text = std::str::from_utf8(line)
        .map_err(|_| ChunkDecodeError::Malformed("non-utf8 chunk header"))?;
    let hex = text.split(';').next().unwrap_or("").trim();
    if hex.is_empty() {
        return Err(ChunkDecodeError::Malformed("empty chunk size"));
    }
    u64::from_str_radix(hex, 16).map_err(|_| ChunkDecodeError::Malformed("chunk size is not hex"))
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|pair| pair == b"\r\n")
}

/// Decode an `aws-chunked` body, yielding only the payload bytes.
///
/// `declared` is the client's `x-amz-decoded-content-length`. When `algorithm`
/// is set, the running digest is compared against the trailer checksum. The
/// stream fails closed on a length or checksum mismatch; the caller must spool
/// the whole payload so verification completes before the origin is dispatched.
pub fn decode_aws_chunked<S>(
    source: S,
    declared: u64,
    algorithm: Option<ChecksumAlgorithm>,
) -> impl Stream<Item = Result<Bytes, ChunkDecodeError>>
where
    S: Stream<Item = Result<Bytes, String>> + Unpin,
{
    ChunkedStream {
        source,
        decoder: Decoder::new(declared, algorithm),
        finished: false,
    }
}

struct ChunkedStream<S> {
    source: S,
    decoder: Decoder,
    /// Set once the stream yields an error or completes, so it is never polled
    /// back into the decoder — a second `drive()` after an error would call
    /// `verify()` on an already-taken digest and panic.
    finished: bool,
}

impl<S> Stream for ChunkedStream<S>
where
    S: Stream<Item = Result<Bytes, String>> + Unpin,
{
    type Item = Result<Bytes, ChunkDecodeError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(None);
        }
        loop {
            match this.decoder.drive() {
                Drive::Emit(bytes) => return Poll::Ready(Some(Ok(bytes))),
                Drive::Done => {
                    this.finished = true;
                    return Poll::Ready(None);
                }
                Drive::Err(error) => {
                    this.finished = true;
                    return Poll::Ready(Some(Err(error)));
                }
                Drive::NeedMore => match Pin::new(&mut this.source).poll_next(cx) {
                    Poll::Ready(Some(Ok(chunk))) => this.decoder.extend(&chunk),
                    Poll::Ready(Some(Err(error))) => {
                        this.finished = true;
                        return Poll::Ready(Some(Err(ChunkDecodeError::Source(error))));
                    }
                    Poll::Ready(None) => this.decoder.finish(),
                    Poll::Pending => return Poll::Pending,
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    fn framed(chunks: &[&str], trailers: &[&str]) -> Vec<u8> {
        let mut wire = Vec::new();
        for chunk in chunks {
            wire.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
            wire.extend_from_slice(chunk.as_bytes());
            wire.extend_from_slice(b"\r\n");
        }
        wire.extend_from_slice(b"0\r\n");
        for trailer in trailers {
            wire.extend_from_slice(trailer.as_bytes());
            wire.extend_from_slice(b"\r\n");
        }
        wire.extend_from_slice(b"\r\n");
        wire
    }

    fn signed_framed(chunks: &[&str]) -> Vec<u8> {
        // The AWS C++ SDK trailer mode carries no per-chunk signature, but the
        // parser must tolerate the `;chunk-signature=` extension either way.
        let mut wire = Vec::new();
        for chunk in chunks {
            wire.extend_from_slice(
                format!("{:x};chunk-signature={}\r\n", chunk.len(), "a".repeat(64)).as_bytes(),
            );
            wire.extend_from_slice(chunk.as_bytes());
            wire.extend_from_slice(b"\r\n");
        }
        wire.extend_from_slice(format!("0;chunk-signature={}\r\n\r\n", "b".repeat(64)).as_bytes());
        wire
    }

    async fn decode(wire: &[u8], declared: u64, split: usize) -> Result<Vec<u8>, ChunkDecodeError> {
        decode_with(wire, declared, split, None).await
    }

    async fn decode_with(
        wire: &[u8],
        declared: u64,
        split: usize,
        algorithm: Option<ChecksumAlgorithm>,
    ) -> Result<Vec<u8>, ChunkDecodeError> {
        let frames: Vec<Result<Bytes, String>> = wire
            .chunks(split.max(1))
            .map(|piece| Ok(Bytes::copy_from_slice(piece)))
            .collect();
        let source = futures::stream::iter(frames);
        let mut stream = Box::pin(decode_aws_chunked(source, declared, algorithm));
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.extend_from_slice(&item?);
        }
        Ok(out)
    }

    #[tokio::test]
    async fn strips_framing_across_arbitrary_source_splits() {
        let wire = framed(&["hello ", "world"], &["x-amz-checksum-crc32c:abcd"]);
        for split in [1, 2, 3, 7, 64, 4096] {
            let out = decode(&wire, 11, split).await.unwrap();
            assert_eq!(out, b"hello world", "split={split}");
        }
    }

    #[tokio::test]
    async fn tolerates_chunk_signature_extensions() {
        let wire = signed_framed(&["payload"]);
        assert_eq!(decode(&wire, 7, 3).await.unwrap(), b"payload");
    }

    #[tokio::test]
    async fn an_empty_body_is_just_the_final_chunk() {
        let wire = framed(&[], &[]);
        assert_eq!(decode(&wire, 0, 1).await.unwrap(), b"");
    }

    #[tokio::test]
    async fn a_short_stream_is_an_error_not_a_short_object() {
        // The declared length exceeds what the framing carries.
        let wire = framed(&["four"], &[]);
        let error = decode(&wire, 8, 1).await.unwrap_err();
        assert!(matches!(error, ChunkDecodeError::LengthMismatch { .. }));
    }

    #[tokio::test]
    async fn more_data_than_declared_is_refused() {
        let wire = framed(&["too much data"], &[]);
        let error = decode(&wire, 4, 1).await.unwrap_err();
        assert_eq!(
            error,
            ChunkDecodeError::LengthMismatch {
                declared: 4,
                decoded: 13,
            }
        );
    }

    #[tokio::test]
    async fn a_truncated_frame_is_refused() {
        // Well-formed prefix, but the source ends inside the payload.
        let wire = b"c\r\nhello".to_vec();
        let error = decode(&wire, 12, 1).await.unwrap_err();
        assert!(matches!(error, ChunkDecodeError::Malformed(_)));
    }

    #[tokio::test]
    async fn a_chunk_not_terminated_by_crlf_is_refused() {
        // Correct size and payload, but the two bytes after the data are not
        // CRLF — a desynchronized or hostile frame, not a valid boundary.
        let wire = b"5\r\nhelloXX0\r\n\r\n".to_vec();
        let error = decode(&wire, 5, 1).await.unwrap_err();
        assert_eq!(
            error,
            ChunkDecodeError::Malformed("chunk data not CRLF-terminated")
        );
    }

    #[tokio::test]
    async fn a_non_hex_size_is_refused() {
        let wire = b"zz\r\nhi\r\n0\r\n\r\n".to_vec();
        let error = decode(&wire, 2, 1).await.unwrap_err();
        assert_eq!(error, ChunkDecodeError::Malformed("chunk size is not hex"));
    }

    #[tokio::test]
    async fn an_oversized_chunk_header_is_refused() {
        // A size line with no CRLF that grows past the cap: a peer that never
        // terminates the header must not grow the buffer without bound.
        let wire = vec![b'a'; MAX_CHUNK_HEADER + 4];
        let error = decode(&wire, 16, 4096).await.unwrap_err();
        assert_eq!(error, ChunkDecodeError::Malformed("chunk header too long"));
    }

    #[tokio::test]
    async fn unbounded_trailers_are_refused() {
        let mut wire = b"3\r\nabc\r\n0\r\n".to_vec();
        for i in 0..(MAX_TRAILERS + 1) {
            wire.extend_from_slice(format!("x-trailer-{i}:v\r\n").as_bytes());
        }
        wire.extend_from_slice(b"\r\n");
        let error = decode(&wire, 3, 8).await.unwrap_err();
        assert_eq!(error, ChunkDecodeError::Malformed("too many trailers"));
    }

    fn checksum_base64(payload: &[u8], algorithm: ChecksumAlgorithm) -> String {
        let mut digest = Digest::new(algorithm);
        digest.update(payload);
        digest.finalize_base64()
    }

    #[test]
    fn checksums_match_the_canonical_reference_vectors() {
        // Known answers for the standard "123456789" CRC check string and the
        // sha256 of it, in S3's base64-of-big-endian form. These are external
        // ground truth: a wrong polynomial, byte order, or a future `crc` crate
        // change would fail here even though the self-referential trailer tests
        // (which digest with the same code) would still pass.
        for (algorithm, expected) in [
            (ChecksumAlgorithm::Crc32, "y/Q5Jg=="),
            (ChecksumAlgorithm::Crc32c, "4waSgw=="),
            (ChecksumAlgorithm::Crc64Nvme, "rosUhgp5mIg="),
            (
                ChecksumAlgorithm::Sha256,
                "FeKw08M4keuw8e9gnsQZQgwg4yDOlMZfvIwzEkSOsiU=",
            ),
        ] {
            assert_eq!(
                checksum_base64(b"123456789", algorithm),
                expected,
                "{algorithm:?} must match its canonical check vector"
            );
        }
    }

    #[tokio::test]
    async fn verifies_a_matching_trailer_checksum_for_every_algorithm() {
        let payload = b"hello world";
        for algorithm in [
            ChecksumAlgorithm::Crc32,
            ChecksumAlgorithm::Crc32c,
            ChecksumAlgorithm::Crc64Nvme,
            ChecksumAlgorithm::Sha256,
        ] {
            let trailer = format!(
                "{}:{}",
                algorithm.trailer_key(),
                checksum_base64(payload, algorithm)
            );
            let wire = framed(&["hello ", "world"], &[&trailer]);
            for split in [1, 3, 64] {
                let out = decode_with(&wire, 11, split, Some(algorithm))
                    .await
                    .unwrap();
                assert_eq!(out, payload, "{algorithm:?} split={split}");
            }
        }
    }

    #[tokio::test]
    async fn verifies_a_large_multi_chunk_body() {
        // A payload spanning many real chunks, not just source splits: this is
        // the shape an SDK sends for a multi-MB object and the shape the
        // adapter spools before dispatch. Kept in-memory so it stays a fast,
        // deterministic regression rather than a network test.
        let payload: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
        let algorithm = ChecksumAlgorithm::Crc64Nvme;
        let mut wire = Vec::new();
        for block in payload.chunks(64 * 1024) {
            wire.extend_from_slice(format!("{:x}\r\n", block.len()).as_bytes());
            wire.extend_from_slice(block);
            wire.extend_from_slice(b"\r\n");
        }
        wire.extend_from_slice(b"0\r\n");
        wire.extend_from_slice(
            format!(
                "{}:{}\r\n\r\n",
                algorithm.trailer_key(),
                checksum_base64(&payload, algorithm)
            )
            .as_bytes(),
        );
        for split in [1, 4096, 100_000] {
            let out = decode_with(&wire, payload.len() as u64, split, Some(algorithm))
                .await
                .unwrap();
            assert_eq!(out, payload, "split={split}");
        }
    }

    #[tokio::test]
    async fn an_empty_body_still_verifies_its_trailer_checksum() {
        // The zero-length payload has a well-defined checksum; the decoder must
        // compute and check it rather than skip verification for an empty body.
        let algorithm = ChecksumAlgorithm::Sha256;
        let good = framed(
            &[],
            &[&format!(
                "{}:{}",
                algorithm.trailer_key(),
                checksum_base64(&[], algorithm)
            )],
        );
        assert_eq!(
            decode_with(&good, 0, 1, Some(algorithm)).await.unwrap(),
            b""
        );
        let bad = framed(&[], &[&format!("{}:AAAA", algorithm.trailer_key())]);
        assert_eq!(
            decode_with(&bad, 0, 1, Some(algorithm)).await.unwrap_err(),
            ChunkDecodeError::ChecksumMismatch
        );
    }

    #[tokio::test]
    async fn accepts_a_single_chunk_spanning_the_whole_body() {
        // An SDK that frames an in-memory body as one chunk must not be
        // rejected: there is no independent single-chunk size cap.
        let payload: Vec<u8> = (0..512 * 1024).map(|i| (i % 251) as u8).collect();
        let algorithm = ChecksumAlgorithm::Crc32c;
        let mut wire = format!("{:x}\r\n", payload.len()).into_bytes();
        wire.extend_from_slice(&payload);
        wire.extend_from_slice(b"\r\n0\r\n");
        wire.extend_from_slice(
            format!(
                "{}:{}\r\n\r\n",
                algorithm.trailer_key(),
                checksum_base64(&payload, algorithm)
            )
            .as_bytes(),
        );
        let out = decode_with(&wire, payload.len() as u64, 4096, Some(algorithm))
            .await
            .unwrap();
        assert_eq!(out, payload);
    }

    #[tokio::test]
    async fn a_wrong_trailer_checksum_is_refused() {
        let wire = framed(&["hello world"], &["x-amz-checksum-crc32c:AAAAAA=="]);
        let error = decode_with(&wire, 11, 4, Some(ChecksumAlgorithm::Crc32c))
            .await
            .unwrap_err();
        assert_eq!(error, ChunkDecodeError::ChecksumMismatch);
    }

    #[tokio::test]
    async fn a_promised_trailer_that_never_arrives_is_refused() {
        // Declared crc32c but the body carries no matching trailer line.
        let wire = framed(&["hello world"], &["x-amz-checksum-crc32:AAAAAA=="]);
        let error = decode_with(&wire, 11, 4, Some(ChecksumAlgorithm::Crc32c))
            .await
            .unwrap_err();
        assert_eq!(error, ChunkDecodeError::MissingChecksum);
    }

    #[tokio::test]
    async fn polling_past_an_error_yields_none_not_a_panic() {
        // A wrong checksum makes the stream yield Err; a combinator that polls
        // once more must get None, never a second drive() that panics on the
        // already-taken digest.
        let wire = framed(&["hello world"], &["x-amz-checksum-crc32c:AAAAAA=="]);
        let source = futures::stream::iter(
            wire.chunks(4)
                .map(|p| Ok(Bytes::copy_from_slice(p)))
                .collect::<Vec<Result<Bytes, String>>>(),
        );
        let mut stream = Box::pin(decode_aws_chunked(
            source,
            11,
            Some(ChecksumAlgorithm::Crc32c),
        ));
        // Payload chunks stream before the trailer is checked (the spool holds
        // them, never the origin); the mismatch surfaces as the terminal item.
        let mut error = None;
        while let Some(item) = stream.next().await {
            if let Err(e) = item {
                error = Some(e);
                break;
            }
        }
        assert_eq!(error, Some(ChunkDecodeError::ChecksumMismatch));
        // Fused: polling past the error yields None instead of panicking on the
        // already-taken digest.
        assert_eq!(stream.next().await, None);
        assert_eq!(stream.next().await, None);
    }

    #[test]
    fn trailer_names_map_to_supported_algorithms_only() {
        assert_eq!(
            ChecksumAlgorithm::from_trailer_name("x-amz-checksum-crc64nvme"),
            Some(ChecksumAlgorithm::Crc64Nvme)
        );
        assert_eq!(
            ChecksumAlgorithm::from_trailer_name("X-Amz-Checksum-SHA256"),
            Some(ChecksumAlgorithm::Sha256)
        );
        assert_eq!(
            ChecksumAlgorithm::from_trailer_name("x-amz-checksum-crc32c "),
            Some(ChecksumAlgorithm::Crc32c)
        );
        assert_eq!(
            ChecksumAlgorithm::from_trailer_name("x-amz-checksum-md5"),
            None
        );
    }
}
