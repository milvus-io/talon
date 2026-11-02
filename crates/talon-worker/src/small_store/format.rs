//! Versioned cache metadata. Payloads are raw bytes in a separate segment file.
use std::io::{self, Read, Write};
use talon_core::{Backend, BlockId, ObjectId, Version};

pub(super) const MAGIC: &[u8; 8] = b"TLSM0001";
pub(super) const MAX_KEY: usize = 64 * 1024;

pub(super) fn key(id: &BlockId) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    out.push(match id.object.backend {
        Backend::S3 => 0,
        Backend::Gcs => 1,
        Backend::Azure => 2,
    });
    out.extend_from_slice(&id.offset.to_le_bytes());
    out.extend_from_slice(&id.block_size.to_le_bytes());
    for s in [&id.object.bucket, &id.object.object_path, &id.version.0] {
        if s.len() > MAX_KEY {
            return Err(invalid("small object key too long"));
        }
        out.extend_from_slice(&(s.len() as u32).to_le_bytes());
        out.extend_from_slice(s.as_bytes());
    }
    if out.len() > MAX_KEY {
        return Err(invalid("small object key too long"));
    }
    Ok(out)
}

pub(super) fn id(mut bytes: &[u8]) -> io::Result<BlockId> {
    let mut backend = [0];
    bytes.read_exact(&mut backend)?;
    let backend = match backend[0] {
        0 => Backend::S3,
        1 => Backend::Gcs,
        2 => Backend::Azure,
        _ => return Err(invalid("small object backend")),
    };
    let offset = u64(&mut bytes)?;
    let size = u32(&mut bytes)?;
    let mut string = || -> io::Result<String> {
        let len = u32(&mut bytes)? as usize;
        if len > bytes.len() {
            return Err(invalid("small object key length"));
        }
        let s = std::str::from_utf8(&bytes[..len])
            .map_err(|_| invalid("small object key UTF-8"))?
            .to_owned();
        bytes = &bytes[len..];
        Ok(s)
    };
    let bucket = string()?;
    let path = string()?;
    let version = string()?;
    if !bytes.is_empty() {
        return Err(invalid("small object key trailing bytes"));
    }
    Ok(BlockId::new(
        ObjectId::new(backend, bucket, path),
        offset,
        size,
        Version::new(version),
    ))
}

pub(super) fn frame(file: &mut impl Write, body: &[u8]) -> io::Result<()> {
    file.write_all(&(body.len() as u32).to_le_bytes())?;
    file.write_all(&xxhash_rust::xxh3::xxh3_64(body).to_le_bytes())?;
    file.write_all(body)
}

pub(super) fn frames(mut bytes: &[u8]) -> io::Result<Vec<&[u8]>> {
    let mut out = Vec::new();
    while !bytes.is_empty() {
        let len = u32(&mut bytes)? as usize;
        let checksum = u64(&mut bytes)?;
        if len > MAX_KEY + 32 || len > bytes.len() {
            return Err(invalid("small object metadata length"));
        }
        let (record, rest) = bytes.split_at(len);
        if xxhash_rust::xxh3::xxh3_64(record) != checksum {
            return Err(invalid("small object metadata checksum"));
        }
        out.push(record);
        bytes = rest;
    }
    Ok(out)
}

pub(super) fn u32(bytes: &mut &[u8]) -> io::Result<u32> {
    let mut value = [0; 4];
    bytes.read_exact(&mut value)?;
    Ok(u32::from_le_bytes(value))
}
pub(super) fn u64(bytes: &mut &[u8]) -> io::Result<u64> {
    let mut value = [0; 8];
    bytes.read_exact(&mut value)?;
    Ok(u64::from_le_bytes(value))
}
pub(super) fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
