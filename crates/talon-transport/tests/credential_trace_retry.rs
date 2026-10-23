use talon_core::NamespaceId;
use talon_telemetry::{Config, Mode, Operation, TraceContext};
use talon_transport::{codec, envelope, ControlMessage, FrameHeader, RequestContext, HEADER_LEN};

#[tokio::test]
async fn credential_retry_refreshes_trace_without_changing_required_metadata() {
    talon_telemetry::configure(Config {
        mode: Mode::Propagate,
        v2_endpoints: vec!["worker".into()],
        ..Default::default()
    })
    .unwrap();
    let context = RequestContext::from_properties(&[(
        "credential_broker",
        r#"{"url":"http://broker","request":{"csp":"aws"},"origin":{"endpoint":"storage.example","region":"us-west-2"}}"#,
    )])
    .unwrap();
    context
        .scope(async {
            let mut frame = codec::encode(9, &ControlMessage::MembershipQuery {}).unwrap();
            let business = frame[HEADER_LEN..].to_vec();
            for parent in [
                "00-11111111111111111111111111111111-2222222222222222-01",
                "00-11111111111111111111111111111111-3333333333333333-01",
            ] {
                let trace = TraceContext::from_w3c(parent, Some("vendor=value")).unwrap();
                Operation::server(Some(&trace), Some([1; 16])).in_scope(|| {
                    envelope::outbound_namespace(&mut frame, "worker", &NamespaceId::named("team"))
                        .unwrap();
                });
                let header = FrameHeader::decode(&frame).unwrap();
                let (metadata, payload) = envelope::decode(&header, &frame[HEADER_LEN..]).unwrap();
                assert_eq!(header.version, 4);
                assert_eq!(metadata.context.as_ref(), Some(&trace));
                assert_eq!(metadata.namespace.unwrap().effective_name(), "team");
                assert_eq!(
                    metadata.credential_broker.unwrap().config().request["csp"],
                    "aws"
                );
                assert_eq!(payload, business);
            }
        })
        .await;
}

#[tokio::test]
async fn broker_wait_budget_includes_four_exchanges_backoff_and_origin_budget() {
    use std::time::Duration;
    for (timeout, acquisition_ms) in [
        (None, 12_700_u128),
        (Some(123_u64), 1_192),
        (Some(u64::MAX), 4 * u64::MAX as u128 + 700),
    ] {
        let mut descriptor = serde_json::json!({
            "url": "http://broker", "request": {},
            "origin": {"endpoint": "storage.example"}
        });
        if let Some(timeout) = timeout {
            descriptor["origin"]["request_timeout_ms"] = timeout.into();
        }
        let descriptor = descriptor.to_string();
        let context =
            RequestContext::from_properties(&[("credential_broker", &descriptor)]).unwrap();
        context
            .scope(async {
                assert_eq!(
                    talon_transport::request_timeout(Duration::from_secs(30)).as_millis(),
                    acquisition_ms + 30_000
                );
                assert_eq!(
                    talon_transport::request_timeout(Duration::MAX),
                    Duration::MAX
                );
            })
            .await;
    }
    assert_eq!(
        talon_transport::request_timeout(Duration::from_secs(30)),
        Duration::from_secs(30)
    );
}
