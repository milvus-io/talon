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
    let tag = if id.kind == UnitKind::ExactRange {
        0x80
    } else {
        0
    };
    out.push(
        tag | match id.object.backend {
            Backend::S3 => 0,
            Backend::Gcs => 1,
            Backend::Azure => 2,
        },
    );
    out.extend_from_slice(&id.offset.to_le_bytes());
    out.extend_from_slice(&id.block_size.to_le_bytes());
    field(&mut out, id.object.bucket.as_bytes());
    let version = id.version.as_bytes();
    let hex = version.len() % 2 == 0
        && version
            .iter()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b));
    let mut code = ((version.len() / if hex { 2 } else { 1 }) << 1) | usize::from(hex);
    while code >= 128 {
        out.push(code as u8 | 128);
        code >>= 7;
    }
    out.push(code as u8);
    out.extend_from_slice(id.object.object_path.as_bytes());
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
    let kind = if input[0] & 0x80 != 0 {
        UnitKind::ExactRange
    } else {
        UnitKind::Whole
    };
    let backend = match input[0] & 0x7f {
        0 => Backend::S3,
        1 => Backend::Gcs,
        2 => Backend::Azure,
        _ => unreachable!("encoded small-object backend"),
    };
    let offset = u64::from_le_bytes(input[1..9].try_into().unwrap());
    let block_size = u32::from_le_bytes(input[9..13].try_into().unwrap());
    input = &input[13..];
    let bucket = unfield(&mut input);
    let (mut code, mut shift) = (0usize, 0);
    loop {
        let b = input[0];
        input = &input[1..];
        code |= usize::from(b & 127) << shift;
        shift += 7;
        if b < 128 {
            break;
        }
    }
    let (path, version) = input.split_at(input.len() - (code >> 1));
    let path = std::str::from_utf8(path).expect("encoded small-object path");
    let version = if code & 1 != 0 {
        const HEX: &[u8] = b"0123456789abcdef";
        version
            .iter()
            .flat_map(|b| [HEX[(b / 16) as usize], HEX[(b % 16) as usize]])
            .collect()
    } else {
        version.to_vec()
    };
    let version = std::str::from_utf8(&version).expect("encoded small-object version");
    SmallKey::new(&SmallRef {
        kind,
        block: BlockRef {
            object: ObjectRef {
                backend,
                bucket: &bucket,
                object_path: path,
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
    fn trie_preserves_all_256_branches_with_long_labels() {
        let mut trie = fast_radix_trie::RadixMap::new();
        let key = |branch| {
            let mut key = vec![b'p'; 300];
            key.push(branch);
            key.extend_from_slice(&[b's'; 300]);
            key
        };
        for branch in 0..=255_u8 {
            assert_eq!(trie.insert(key(branch), branch), None);
        }
        let snapshot = trie.clone();
        for branch in 0..=255_u8 {
            assert_eq!(trie.get(key(branch)), Some(&branch));
            assert_eq!(trie.remove(key(branch)), Some(branch));
            assert_eq!(snapshot.get(key(branch)), Some(&branch));
        }
        assert!(trie.is_empty());
        assert_eq!(snapshot.len(), 256);
    }

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
