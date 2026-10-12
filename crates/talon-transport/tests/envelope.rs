use talon_core::{Backend, ObjectId, TenantId, Version};
use talon_transport::{codec, data, envelope, FrameHeader, MsgType};

#[test]
fn carrier_reservation_covers_empty_and_maximum_tracestate_without_reallocation() {
    talon_telemetry::configure(talon_telemetry::Config {
        mode: talon_telemetry::Mode::Propagate,
        ..Default::default()
    })
    .unwrap();
    for state in [
        String::new(),
        format!("a={},b={}", "x".repeat(253), "y".repeat(254)),
    ] {
        let parent = talon_telemetry::TraceContext::from_w3c(
            "00-11111111111111111111111111111111-2222222222222222-00",
            Some(&state),
        )
        .unwrap();
        assert_eq!(parent.tracestate(), state);
        let operation = talon_telemetry::Operation::new(
            "read",
            "internal",
            talon_telemetry::TraceParent::Explicit(&parent),
        );
        operation.in_scope(|| {
            let mut frame = data::encode_request(
                1,
                &data::RangeRequest {
                    object: ObjectId::new(Backend::S3, "bucket", "key"),
                    offset: 0,
                    len: 64,
                },
            )
            .unwrap();
            let capacity = frame.capacity();
            let pointer = frame.as_ptr();
            envelope::encode(&mut frame, Some(&parent), Some([1; 16])).unwrap();
            assert_eq!(frame.capacity(), capacity);
            assert_eq!(frame.as_ptr(), pointer);
            let header = FrameHeader::decode(&frame).unwrap();
            assert_eq!(
                envelope::decode(&header, &frame[16..])
                    .unwrap()
                    .0
                    .context
                    .as_ref(),
                Some(&parent)
            );
        });
    }
}

#[test]
fn control_requests_reject_data_types_in_both_versions() {
    for version in [1, 2] {
        let mut frame = codec::encode(1, &codec::ControlMessage::MembershipQuery {}).unwrap();
        if version == 2 {
            envelope::encode(&mut frame, None, None).unwrap();
        }
        frame[3] = MsgType::GetRange as u8;
        assert!(matches!(
            codec::decode_request(&frame),
            Err(codec::CodecError::NotControl(MsgType::GetRange))
        ));
    }
}

#[test]
fn read_variants_roundtrip_and_responses_remain_raw() {
    let object = ObjectId::new(Backend::S3, "bucket", "key");
    let req = data::RangeRequest {
        object: object.clone(),
        offset: 7,
        len: 9,
    };
    let cached = data::CachedRangeRequest {
        object,
        offset: 7,
        len: 9,
        version: Version::new("v1"),
    };
    let parent = talon_telemetry::TraceContext::from_w3c(
        "00-11111111111111111111111111111111-2222222222222222-01",
        Some("vendor=value"),
    )
    .unwrap();
    let requests = [
        data::encode_request(7, &req).unwrap(),
        data::encode_tenant_request(
            7,
            &data::TenantScopedRange {
                tenant: TenantId::Unattributed,
                request: req.clone(),
            },
        )
        .unwrap(),
        data::encode_versioned_request(
            7,
            &data::VersionedRangeRequest {
                request: req.clone(),
                version: cached.version.clone(),
                object_len: 100_000,
            },
        )
        .unwrap(),
        data::encode_versioned_tenant_request(
            7,
            &data::TenantScopedVersionedRange {
                tenant: TenantId::named("acme"),
                request: data::VersionedRangeRequest {
                    request: req.clone(),
                    version: cached.version.clone(),
                    object_len: 100_000,
                },
            },
        )
        .unwrap(),
        data::encode_cached_request(7, &cached).unwrap(),
        data::encode_cached_tenant_request(
            7,
            &data::TenantScopedCachedRange {
                tenant: TenantId::Unattributed,
                request: cached.clone(),
            },
        )
        .unwrap(),
        codec::encode(
            7,
            &codec::ControlMessage::StatObject {
                object: req.object.clone(),
            },
        )
        .unwrap(),
    ];
    for mut frame in requests {
        let original = frame.clone();
        envelope::encode(&mut frame, Some(&parent), Some([8; 16])).unwrap();
        let header = FrameHeader::decode(&frame).unwrap();
        assert_eq!(header.version, 2);
        let (meta, business) = envelope::decode(&header, &frame[16..]).unwrap();
        assert_eq!(business, &original[16..]);
        assert_eq!(meta.context.as_ref(), Some(&parent));
        assert_eq!(meta.read_id, Some([8; 16]));
        match header.msg_type {
            MsgType::GetRange => {
                assert_eq!(data::decode_request(&frame).unwrap().1, req);
            }
            MsgType::GetRangeTenant => {
                assert_eq!(data::decode_tenant_request(&frame).unwrap().1.request, req);
            }
            MsgType::GetVersionedRange => {
                let decoded = data::decode_versioned_request(&frame).unwrap().1;
                assert_eq!(decoded.request, req);
                assert_eq!(decoded.version, cached.version);
                assert_eq!(decoded.object_len, 100_000);
            }
            MsgType::GetVersionedRangeTenant => {
                let decoded = data::decode_versioned_tenant_request(&frame).unwrap().1;
                assert_eq!(decoded.tenant, TenantId::named("acme"));
                assert_eq!(decoded.request.request, req);
                assert_eq!(decoded.request.object_len, 100_000);
                assert_eq!(decoded.request.version, cached.version);
            }
            MsgType::GetCachedRange => {
                assert_eq!(data::decode_cached_request(&frame).unwrap().1, cached);
            }
            MsgType::GetCachedRangeTenant => {
                assert_eq!(
                    data::decode_cached_tenant_request(&frame)
                        .unwrap()
                        .1
                        .request,
                    cached
                );
            }
            MsgType::Control => {
                codec::decode_request(&frame).unwrap();
            }
            _ => unreachable!(),
        }
        let response = envelope::response_version(data::response_header_ok(7, 4), 2);
        assert_eq!(FrameHeader::decode(&response).unwrap().length, 4);
    }
}

#[test]
fn malformed_metadata_is_bounded_but_bad_context_does_not_break_business() {
    let mut header = FrameHeader::new(MsgType::Control, 1, 0);
    header.version = 2;
    for body in [
        vec![],
        vec![0],
        vec![4, 1],
        vec![0, 2, 1, 0],
        vec![0, 3, 1, 0, 5],
    ] {
        header.length = body.len() as u32;
        assert!(envelope::decode(&header, &body).is_err());
    }
    // Duplicate traceparent discards metadata, while unknown TLVs are skippable.
    for meta in [vec![1, 0, 1, b'x', 1, 0, 0], vec![99, 0, 2, 1, 2]] {
        let mut body = (meta.len() as u16).to_be_bytes().to_vec();
        body.extend(meta);
        body.extend(b"business");
        header.length = body.len() as u32;
        let (meta, business) = envelope::decode(&header, &body).unwrap();
        assert!(meta.context.is_none());
        assert_eq!(business, b"business");
    }
    let mut put = FrameHeader::new(MsgType::Put, 1, 0).encode().to_vec();
    assert!(envelope::encode(&mut put, None, None).is_err());
    put[2] = 2;
    assert!(FrameHeader::decode(&put).is_err());
}

#[test]
fn tenant_v3_covers_control_and_raw_body_headers_and_preserves_trace() {
    use talon_transport::data::{CachedBlockPutRequest, DeleteRequest, PutRequest};
    let object = ObjectId::new(Backend::S3, "bucket", "key");
    let block = talon_core::BlockId::new(object.clone(), 0, 16, Version::new("v1"));
    let frames = vec![
        talon_transport::codec::encode(
            7,
            &talon_transport::ControlMessage::LoadBlock {
                block: block.clone(),
                len: 16,
            },
        )
        .unwrap(),
        talon_transport::data::encode_put_header(
            7,
            &PutRequest {
                object: object.clone(),
                body_len: 16,
            },
        )
        .unwrap(),
        talon_transport::data::encode_delete(
            7,
            &DeleteRequest {
                object: object.clone(),
            },
        )
        .unwrap(),
        talon_transport::data::encode_cached_block_put_header(
            7,
            &CachedBlockPutRequest {
                block,
                object_len: 16,
                body_len: 16,
            },
        )
        .unwrap(),
    ];
    for mut frame in frames {
        let before = frame[talon_transport::HEADER_LEN..].to_vec();
        talon_transport::envelope::encode_tenant(&mut frame, &TenantId::named("acme")).unwrap();
        let header = talon_transport::FrameHeader::decode(&frame).unwrap();
        assert_eq!(header.version, 3);
        let (metadata, business) =
            talon_transport::envelope::decode(&header, &frame[talon_transport::HEADER_LEN..])
                .unwrap();
        assert_eq!(metadata.tenant, Some(TenantId::named("acme")));
        assert_eq!(business, before);
        match header.msg_type {
            talon_transport::MsgType::Put => assert_eq!(
                talon_transport::data::decode_put_header(&frame)
                    .unwrap()
                    .1
                    .body_len,
                16
            ),
            talon_transport::MsgType::Delete => assert_eq!(
                talon_transport::data::decode_delete(&frame)
                    .unwrap()
                    .1
                    .object,
                object
            ),
            talon_transport::MsgType::AdmitCachedBlock => assert_eq!(
                talon_transport::data::decode_cached_block_put_header(&frame)
                    .unwrap()
                    .1
                    .body_len,
                16
            ),
            _ => {
                talon_transport::codec::decode_request(&frame).unwrap();
            }
        }
        talon_transport::envelope::encode_tenant(&mut frame, &TenantId::named("acme")).unwrap();
        assert!(talon_transport::envelope::encode_tenant(
            &mut frame,
            &TenantId::named("different")
        )
        .is_err());
    }
    let mut frame = talon_transport::data::encode_request(
        1,
        &talon_transport::data::RangeRequest {
            object,
            offset: 0,
            len: 4,
        },
    )
    .unwrap();
    let trace = talon_telemetry::TraceContext::from_w3c(
        "00-11111111111111111111111111111111-2222222222222222-01",
        None,
    )
    .unwrap();
    talon_transport::envelope::encode(&mut frame, Some(&trace), Some([9; 16])).unwrap();
    talon_transport::envelope::encode_tenant(&mut frame, &TenantId::named("acme")).unwrap();
    let header = talon_transport::FrameHeader::decode(&frame).unwrap();
    let (metadata, _) =
        talon_transport::envelope::decode(&header, &frame[talon_transport::HEADER_LEN..]).unwrap();
    assert_eq!(metadata.read_id, Some([9; 16]));
    assert_eq!(metadata.context.unwrap().traceparent(), trace.traceparent());
    // Corrupting trace context must not discard the routing identity.
    frame[talon_transport::HEADER_LEN + 5] = b'!';
    let (metadata, _) =
        talon_transport::envelope::decode(&header, &frame[talon_transport::HEADER_LEN..]).unwrap();
    assert!(metadata.context.is_none());
    assert_eq!(metadata.tenant, Some(TenantId::named("acme")));
}

#[test]
fn v3_cannot_silently_fall_back_when_tenant_metadata_is_invalid() {
    let mut frame = talon_transport::data::encode_request(
        1,
        &talon_transport::data::RangeRequest {
            object: ObjectId::new(Backend::S3, "bucket", "key"),
            offset: 0,
            len: 4,
        },
    )
    .unwrap();
    talon_transport::envelope::encode(&mut frame, None, None).unwrap();
    frame[2] = 3;
    let header = talon_transport::FrameHeader::decode(&frame).unwrap();
    assert!(
        talon_transport::envelope::decode(&header, &frame[talon_transport::HEADER_LEN..]).is_err()
    );
    frame[2] = 2;
    assert!(
        talon_transport::envelope::encode_tenant(&mut frame, &TenantId::named("../escape"))
            .is_err()
    );
}
