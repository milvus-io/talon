use talon_core::{Backend, NamespaceId, ObjectId};
use talon_telemetry::{Config, Mode, Operation, TraceContext, TraceParent};
use talon_transport::{codec, data, envelope, FrameHeader, HEADER_LEN};

#[test]
fn retry_refreshes_trace_context_without_changing_namespace_or_business_payload() {
    talon_telemetry::configure(Config {
        mode: Mode::Propagate,
        v2_endpoints: vec!["worker".into()],
        ..Default::default()
    })
    .unwrap();
    let first = TraceContext::from_w3c(
        "00-11111111111111111111111111111111-2222222222222222-01",
        Some("attempt=first"),
    )
    .unwrap();
    let retry = TraceContext::from_w3c(
        "00-11111111111111111111111111111111-3333333333333333-01",
        Some("attempt=retry"),
    )
    .unwrap();
    let object = ObjectId::new(Backend::S3, "bucket", "key");
    let frames = [
        data::encode_request(
            7,
            &data::RangeRequest {
                object: object.clone(),
                offset: 0,
                len: 4,
            },
        )
        .unwrap(),
        codec::encode(8, &codec::ControlMessage::StatObject { object }).unwrap(),
    ];
    for namespace in [NamespaceId::default(), NamespaceId::named("acme")] {
        for original in &frames {
            let mut frame = original.clone();
            for (context, read_id) in [(&first, [1; 16]), (&retry, [2; 16])] {
                Operation::server(Some(context), Some(read_id)).in_scope(|| {
                    envelope::outbound_namespace(&mut frame, "worker", &namespace).unwrap();
                });
                let header = FrameHeader::decode(&frame).unwrap();
                assert_eq!(header.version, if namespace.is_default() { 2 } else { 3 });
                let (metadata, business) = envelope::decode(&header, &frame[HEADER_LEN..]).unwrap();
                assert_eq!(metadata.context.as_ref(), Some(context));
                assert_eq!(metadata.read_id, Some(read_id));
                assert_eq!(metadata.namespace.unwrap_or_default(), namespace);
                assert_eq!(business, &original[HEADER_LEN..]);
            }
            // A fresh scope without a carrier must not retain the previous attempt's trace.
            Operation::new("retry", "client", TraceParent::Root).in_scope(|| {
                envelope::outbound_namespace(&mut frame, "worker", &namespace).unwrap();
            });
            let header = FrameHeader::decode(&frame).unwrap();
            let (metadata, business) = envelope::decode(&header, &frame[HEADER_LEN..]).unwrap();
            assert!(metadata.context.is_none());
            assert!(metadata.read_id.is_none());
            assert_eq!(metadata.namespace.unwrap_or_default(), namespace);
            assert_eq!(business, &original[HEADER_LEN..]);
        }
    }
}
