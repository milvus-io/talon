use super::*;
use xxhash_rust::xxh3::xxh3_64;
pub(crate) const MAGIC: &[u8; 8] = b"TLNJRN01";
pub(crate) const HEADER: usize = 32;
pub(crate) const MAX_RECORD: usize = 16 << 20;

pub(crate) fn encode(lsn: u64, body: &[u8]) -> io::Result<Bytes> {
    if body.len() > MAX_RECORD {
        return Err(invalid("journal frame too large"));
    }
    let mut bytes = Vec::with_capacity(HEADER + body.len());
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&(body.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&(!(body.len() as u32)).to_le_bytes());
    bytes.extend_from_slice(&lsn.to_le_bytes());
    bytes.extend_from_slice(&[0; 8]);
    bytes.extend_from_slice(body);
    let mut hash = xxhash_rust::xxh3::Xxh3::new();
    hash.update(&bytes[..24]);
    hash.update(body);
    bytes[24..32].copy_from_slice(&hash.digest().to_le_bytes());
    Ok(bytes.into())
}
pub(crate) fn decode<T>(
    mut bytes: Vec<u8>,
    decode: impl Fn(&[u8]) -> io::Result<T>,
) -> io::Result<Vec<(u64, T)>> {
    let mut records = Vec::new();
    let mut position = 0;
    while position < bytes.len() {
        if bytes.len() - position < HEADER {
            return Err(invalid("truncated journal header"));
        }
        let header = &bytes[position..position + HEADER];
        if &header[..8] != MAGIC {
            return Err(invalid("unsupported journal format"));
        }
        let n = u32::from_le_bytes(header[8..12].try_into().unwrap());
        if !n != u32::from_le_bytes(header[12..16].try_into().unwrap()) || n as usize > MAX_RECORD {
            return Err(invalid("journal frame length"));
        }
        let lsn = u64::from_le_bytes(header[16..24].try_into().unwrap());
        let expected = u64::from_le_bytes(header[24..32].try_into().unwrap());
        let end = position + HEADER + n as usize;
        if end > bytes.len() {
            return Err(invalid("truncated journal body"));
        }
        bytes.copy_within(position..position + 24, position + 8);
        if xxh3_64(&bytes[position + 8..end]) != expected {
            return Err(invalid("journal frame checksum"));
        }
        records.push((lsn, decode(&bytes[position + HEADER..end])?));
        position = end;
    }
    Ok(records)
}
