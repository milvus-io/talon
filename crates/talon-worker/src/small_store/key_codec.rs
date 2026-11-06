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
    while let Some((&b, tail)) = input.split_first() {
        *input = tail;
        if b == 0 {
            let tag = input[0];
            *input = &input[1..];
            if tag == 0 {
                break;
            }
        }
        out.push(b);
    }
    String::from_utf8(out).expect("encoded small-object UTF-8")
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
    let backend = match id.object.backend {
        Backend::S3 => 0,
        Backend::Gcs => 1,
        Backend::Azure => 2,
    };
    let tag = backend
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
    let backend = match tag & 0x1f {
        0 => Backend::S3,
        1 => Backend::Gcs,
        2 => Backend::Azure,
        _ => unreachable!("encoded small-object backend"),
    };
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

    #[test]
    fn round_trip_preserves_every_identity_field() {
        for backend in [Backend::S3, Backend::Gcs, Backend::Azure] {
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
                        assert_eq!(decode(&encode(&key)).view(), key);
                    }
                }
            }
        }
    }
}
