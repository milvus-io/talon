use super::key::{SmallIdentity, SmallKey, SmallRef};
use crate::block_key::{BlockRef, ObjectRef};
use crate::eviction::UnitKind;
use talon_core::Backend;

fn field(out: &mut Vec<u8>, bytes: &[u8]) {
    for &byte in bytes {
        if byte == 0 {
            out.extend_from_slice(&[0, 255]);
        } else {
            out.push(byte);
        }
    }
    out.extend_from_slice(&[0, 0]);
}

fn unfield(input: &mut &[u8]) -> String {
    let mut out = Vec::new();
    object_field(input, &mut out).to_owned()
}

fn object_field<'a, 'b: 'a>(input: &mut &'b [u8], scratch: &'a mut Vec<u8>) -> &'a str {
    let end = input.iter().position(|&byte| byte == 0).unwrap();
    if input[end + 1] == 0 {
        let (field, rest) = input.split_at(end);
        *input = &rest[2..];
        return std::str::from_utf8(field).expect("encoded small-object UTF-8");
    }
    scratch.clear();
    while let Some((&b, tail)) = input.split_first() {
        *input = tail;
        if b == 0 {
            let tag = input[0];
            *input = &input[1..];
            if tag == 0 {
                break;
            }
        }
        scratch.push(b);
    }
    std::str::from_utf8(scratch).expect("encoded small-object UTF-8")
}

fn backend(tag: u8) -> Backend {
    Backend::from_tag(tag & 0x1f).expect("encoded small-object backend")
}

pub(super) fn object<'a>(
    mut input: &'a [u8],
    bucket: &'a mut Vec<u8>,
    path: &'a mut Vec<u8>,
) -> ObjectRef<'a> {
    let bucket = object_field(&mut input, bucket);
    let object_path = object_field(&mut input, path);
    ObjectRef {
        backend: backend(input[0]),
        bucket,
        object_path,
    }
}

pub(super) fn encode(id: &dyn SmallIdentity) -> Vec<u8> {
    let id = id.view();
    let mut out = Vec::with_capacity(super::format::key_len(&id));
    field(&mut out, id.object.bucket.as_bytes());
    field(&mut out, id.object.object_path.as_bytes());
    let version = id.version.as_bytes();
    let hex = version.len() % 2 == 0
        && version
            .iter()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b));
    let tag = id.object.backend as u8
        | if id.kind == UnitKind::ExactRange {
            0x80
        } else {
            0
        }
        | if id.offset != 0 { 0x40 } else { 0 }
        | if hex { 0x20 } else { 0 };
    out.push(tag);
    out.extend_from_slice(&id.block_size.to_le_bytes());
    if id.offset != 0 {
        out.extend_from_slice(&id.offset.to_le_bytes());
    }
    if hex {
        let digit = |b: u8| if b <= b'9' { b - b'0' } else { b - b'a' + 10 };
        for pair in version.chunks_exact(2) {
            out.push(digit(pair[0]) * 16 + digit(pair[1]));
        }
    } else {
        out.extend_from_slice(version);
    }
    out
}

pub(super) fn decode(mut input: &[u8]) -> SmallKey {
    let bucket = unfield(&mut input);
    let path = unfield(&mut input);
    let tag = input[0];
    let kind = if tag & 0x80 != 0 {
        UnitKind::ExactRange
    } else {
        UnitKind::Whole
    };
    let backend = backend(tag);
    let block_size = u32::from_le_bytes(input[1..5].try_into().unwrap());
    input = &input[5..];
    let offset = if tag & 0x40 != 0 {
        let offset = u64::from_le_bytes(input[..8].try_into().unwrap());
        input = &input[8..];
        offset
    } else {
        0
    };
    let version = if tag & 0x20 != 0 {
        const HEX: &[u8] = b"0123456789abcdef";
        input
            .iter()
            .flat_map(|b| [HEX[(b / 16) as usize], HEX[(b % 16) as usize]])
            .collect()
    } else {
        input.to_vec()
    };
    let version = std::str::from_utf8(&version).expect("encoded small-object version");
    SmallKey::new(&SmallRef {
        kind,
        block: BlockRef {
            object: ObjectRef {
                backend,
                bucket: &bucket,
                object_path: &path,
            },
            offset,
            block_size,
            version,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::small_store::format;

    #[test]
    fn round_trip_preserves_every_identity_field() {
        for backend in [
            Backend::S3,
            Backend::Gcs,
            Backend::Azure,
            Backend::Ali,
            Backend::Tc,
        ] {
            for path in [
                "",
                "a\0b",
                "路径/é/001234/9",
                "0000000000000000000000000000000000000000",
            ] {
                for version in ["", "00abcdef", "00ABcdef", "odd", "版本\0"] {
                    let key = BlockRef {
                        object: ObjectRef {
                            backend,
                            bucket: "桶\0",
                            object_path: path,
                        },
                        offset: u64::MAX,
                        block_size: u32::MAX,
                        version,
                    };
                    for kind in [UnitKind::Whole, UnitKind::ExactRange] {
                        let key = SmallRef { block: key, kind };
                        let encoded = encode(&key);
                        assert_eq!(decode(&encoded).view(), key);
                        let mut bucket = Vec::new();
                        let mut object_path = Vec::new();
                        assert_eq!(
                            object(&encoded, &mut bucket, &mut object_path),
                            key.block.object
                        );
                        let persisted = format::key(&key).unwrap();
                        assert_eq!(format::view(&persisted).unwrap(), key);
                    }
                }
            }
        }
    }
}
