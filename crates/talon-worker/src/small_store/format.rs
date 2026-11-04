//! Versioned cache metadata. Payloads are raw bytes in a separate segment file.
use crate::block_key::{BlockIdentity, BlockRef, ObjectRef};
use std::io::{self, Read, Write};
use talon_core::{Backend, BlockId};

pub(super) const MAX_KEY: usize = 64 * 1024;

pub(super) fn key_len(id: &dyn BlockIdentity) -> usize {
    let id = id.view();
    25 + id.object.bucket.len() + id.object.object_path.len() + id.version.len()
}

pub(super) fn validate_key(id: &dyn BlockIdentity) -> io::Result<()> {
    if key_len(id) > MAX_KEY {
        return Err(invalid("small object key too long"));
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn key(id: &dyn BlockIdentity) -> io::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(key_len(id));
    encode_key(&mut out, id)?;
    Ok(out)
}

pub(super) fn encode_key(out: &mut Vec<u8>, id: &dyn BlockIdentity) -> io::Result<()> {
    validate_key(id)?;
    let id = id.view();
    out.push(match id.object.backend {
        Backend::S3 => 0,
        Backend::Gcs => 1,
        Backend::Azure => 2,
    });
    out.extend_from_slice(&id.offset.to_le_bytes());
    out.extend_from_slice(&id.block_size.to_le_bytes());
    for s in [id.object.bucket, id.object.object_path, id.version] {
        out.extend_from_slice(&(s.len() as u32).to_le_bytes());
        out.extend_from_slice(s.as_bytes());
    }
    Ok(())
}

pub(super) fn id(bytes: &[u8]) -> io::Result<BlockId> {
    Ok(view(bytes)?.materialize())
}

pub(super) fn view(mut bytes: &[u8]) -> io::Result<BlockRef<'_>> {
    if bytes.len() > MAX_KEY {
        return Err(invalid("small object key too long"));
    }
    let mut backend = [0];
    bytes.read_exact(&mut backend)?;
    let exact = backend[0] & 0x80 != 0;
    let backend = match backend[0] & 0x7f {
        0 => Backend::S3,
        1 => Backend::Gcs,
        2 => Backend::Azure,
        _ => return Err(invalid("small object backend")),
    };
    let offset = u64(&mut bytes)?;
    let size = u32(&mut bytes)?;
    let mut string = || -> io::Result<&str> {
        let len = u32(&mut bytes)? as usize;
        if len > bytes.len() {
            return Err(invalid("small object key length"));
        }
        let s =
            std::str::from_utf8(&bytes[..len]).map_err(|_| invalid("small object key UTF-8"))?;
        bytes = &bytes[len..];
        Ok(s)
    };
    let bucket = string()?;
    let path = string()?;
    let version = string()?;
    if !bytes.is_empty() {
        return Err(invalid("small object key trailing bytes"));
    }
    Ok(BlockRef {
        object: ObjectRef {
            backend,
            bucket,
            object_path: path,
        },
        offset,
        block_size: size,
        version,
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_key::BlockKey;
    use talon_core::{ObjectId, Version};

    #[test]
    fn shared_keys_preserve_disk_bytes_and_reject_invalid_identity() {
        let block = BlockId::new(
            ObjectId::new(Backend::Azure, "桶", "bc"),
            17,
            1234,
            Version::new("v"),
        );
        let encoded = vec![
            2, 17, 0, 0, 0, 0, 0, 0, 0, 0xd2, 4, 0, 0, 3, 0, 0, 0, 0xe6, 0xa1, 0xb6, 2, 0, 0, 0,
            b'b', b'c', 1, 0, 0, 0, b'v',
        ];
        let shared = BlockKey::new(&view(&encoded).unwrap());
        assert_eq!(shared.materialize(), block);
        assert_eq!(key(&shared).unwrap(), encoded);
        assert_eq!(key_len(&shared), encoded.len());
        for len in 0..encoded.len() {
            assert!(view(&encoded[..len]).is_err());
        }
        let mut invalid_utf8 = encoded.clone();
        invalid_utf8[17] = 0xff;
        assert!(view(&invalid_utf8).is_err());
        let mut trailing = encoded;
        trailing.push(0);
        assert!(view(&trailing).is_err());
        let mut oversized = block;
        oversized.object.object_path = "x".repeat(MAX_KEY);
        assert!(key(&oversized).is_err());
    }
}
