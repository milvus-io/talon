use talon_transport::{
    codec, current_credential_broker, envelope, ControlMessage, FrameHeader, RequestContext,
    HEADER_LEN,
};

const BROKER: &str = r#"{"url":"http://broker/v1/credentials/assume-role","request":{"csp":"aws","secret":"sentinel"},"origin":{"endpoint":"storage.example","region":"us-west-2"}}"#;

fn frame(metadata: &[u8], version: u8) -> Vec<u8> {
    let mut bytes = codec::encode(1, &ControlMessage::MembershipQuery {}).unwrap();
    let payload = bytes.split_off(HEADER_LEN);
    bytes[2] = version;
    bytes[12..16].copy_from_slice(&((payload.len() + metadata.len() + 2) as u32).to_be_bytes());
    bytes.extend_from_slice(&(metadata.len() as u16).to_be_bytes());
    bytes.extend_from_slice(metadata);
    bytes.extend_from_slice(&payload);
    bytes
}

fn broker_tlv() -> Vec<u8> {
    let mut metadata = vec![6];
    metadata.extend_from_slice(&(BROKER.len() as u16).to_be_bytes());
    metadata.extend_from_slice(BROKER.as_bytes());
    metadata
}

#[test]
fn credential_envelope_uses_a_required_protocol_version() {
    let bytes = frame(&broker_tlv(), 4);
    let header = FrameHeader::decode(&bytes).expect("credential-bearing v4 is supported");
    assert!(envelope::decode(&header, &bytes[HEADER_LEN..]).is_ok());
    assert!(codec::decode_request(&bytes).is_ok());
}

#[test]
fn v4_without_credentials_cannot_become_a_static_request() {
    let bytes = frame(&[], 4);
    let header = FrameHeader::decode(&bytes).expect("v4 header");
    assert!(envelope::decode(&header, &bytes[HEADER_LEN..]).is_err());
}

#[test]
fn request_properties_and_wire_frames_reject_missing_origin_settings() {
    for suffix in [
        "",
        r#", "origin":null"#,
        r#", "origin":{}"#,
        r#", "origin":{"request_timeout_ms":17}"#,
        r#", "origin":{"endpoint":null}"#,
        r#", "origin":{"endpoint":""}"#,
        r#", "origin":{"endpoint":" \t\n"}"#,
        r#", "origin":{"endpoint":"storage.example"}"#,
        r#", "origin":{"endpoint":"storage.example","region":null}"#,
        r#", "origin":{"endpoint":"storage.example","region":""}"#,
        r#", "origin":{"endpoint":"storage.example","region":" \t\n"}"#,
    ] {
        let json = format!(
            r#"{{"url":"http://broker/sentinel","request":{{"csp":"aws","region":"broker-sentinel","secret":"sentinel"}}{suffix}}}"#
        );
        let error = RequestContext::from_properties(&[("credential_broker", &json)]).unwrap_err();
        assert!(!error.contains("sentinel"));
        let mut metadata = vec![6];
        metadata.extend_from_slice(&(json.len() as u16).to_be_bytes());
        metadata.extend_from_slice(json.as_bytes());
        let bytes = frame(&metadata, 4);
        let header = FrameHeader::decode(&bytes).unwrap();
        assert!(envelope::decode(&header, &bytes[HEADER_LEN..]).is_err());
        assert!(codec::decode_request(&bytes).is_err());

        let raw = serde_json::from_str(&json).unwrap();
        let mut bytes = codec::encode(1, &ControlMessage::MembershipQuery {}).unwrap();
        let original = bytes.clone();
        assert!(envelope::encode_credential_broker(&mut bytes, &raw).is_err());
        assert_eq!(bytes, original);
    }
}

#[test]
fn credential_fields_are_strict_even_when_trace_fields_are_discarded() {
    let mut metadata = broker_tlv();
    metadata.extend_from_slice(&[1, 0, 1, 0xff]);
    let bytes = frame(&metadata, 4);
    let header = FrameHeader::decode(&bytes).unwrap();
    let (decoded, _) = envelope::decode(&header, &bytes[HEADER_LEN..]).unwrap();
    assert!(decoded.context.is_none());
    assert_eq!(
        decoded.credential_broker.unwrap().config().request["secret"],
        "sentinel"
    );

    for version in [2, 3] {
        let bytes = frame(&broker_tlv(), version);
        let header = FrameHeader::decode(&bytes).unwrap();
        assert!(envelope::decode(&header, &bytes[HEADER_LEN..]).is_err());
    }
    let mut duplicate = broker_tlv();
    duplicate.extend_from_slice(&broker_tlv());
    for metadata in [duplicate, vec![6, 0, 0], vec![6, 0, 2, b'{', b'}']] {
        let bytes = frame(&metadata, 4);
        let header = FrameHeader::decode(&bytes).unwrap();
        assert!(envelope::decode(&header, &bytes[HEADER_LEN..]).is_err());
    }
}

#[tokio::test]
async fn credentials_survive_retries_and_namespace_wrapping_with_tracing_off() {
    assert!(!talon_telemetry::enabled());
    let context = RequestContext::from_properties(&[("credential_broker", BROKER)]).unwrap();
    context
        .scope(async {
            let mut bytes = codec::encode(1, &ControlMessage::MembershipQuery {}).unwrap();
            let business = bytes[HEADER_LEN..].to_vec();
            for _ in 0..3 {
                envelope::outbound(&mut bytes, "unconfigured-peer").unwrap();
                envelope::encode_namespace(&mut bytes, &talon_core::NamespaceId::named("team"))
                    .unwrap();
                let header = FrameHeader::decode(&bytes).unwrap();
                assert_eq!(header.version, 4);
                let (decoded, payload) = envelope::decode(&header, &bytes[HEADER_LEN..]).unwrap();
                assert_eq!(payload, business);
                assert_eq!(decoded.namespace.unwrap().effective_name(), "team");
                assert_eq!(
                    decoded.credential_broker.unwrap().config().request["secret"],
                    "sentinel"
                );
                assert!(decoded.context.is_none());
            }
            RequestContext::default()
                .scope(async {
                    assert!(current_credential_broker().is_none());
                    let mut ordinary =
                        codec::encode(2, &ControlMessage::MembershipQuery {}).unwrap();
                    envelope::outbound(&mut ordinary, "unconfigured-peer").unwrap();
                    assert_eq!(FrameHeader::decode(&ordinary).unwrap().version, 1);
                })
                .await;
            tokio::task::yield_now().await;
            assert!(current_credential_broker().is_some());
        })
        .await;
    assert!(current_credential_broker().is_none());
}

#[test]
fn request_properties_reject_empty_duplicate_and_unknown_entries() {
    for properties in [
        vec![("credential_broker", "")],
        vec![("credential_broker", BROKER), ("credential_broker", BROKER)],
        vec![("secret-unknown-key", "sentinel")],
    ] {
        let error = RequestContext::from_properties(&properties).unwrap_err();
        assert!(!error.contains("sentinel"));
        assert!(!error.contains("secret-unknown-key"));
    }
}

#[test]
fn reencoding_credentials_rejects_a_different_origin() {
    let mut config = talon_core::BrokerConfig::from_json(BROKER).unwrap();
    config.origin = Some(talon_core::OriginOptions {
        endpoint: Some("first.example".into()),
        region: Some("us-west-2".into()),
        ..Default::default()
    });
    let mut bytes = codec::encode(1, &ControlMessage::MembershipQuery {}).unwrap();
    envelope::encode_credential_broker(&mut bytes, &config).unwrap();
    envelope::encode_credential_broker(&mut bytes, &config).unwrap();
    config.origin.as_mut().unwrap().endpoint = Some("second.example".into());
    assert!(envelope::encode_credential_broker(&mut bytes, &config).is_err());
}

#[test]
fn raw_descriptor_encoding_preserves_inbound_nesting_limit() {
    for (depth, accepted) in [(125, true), (126, false), (130, false)] {
        let mut nested = serde_json::Value::Null;
        for _ in 0..depth {
            nested = serde_json::Value::Array(vec![nested]);
        }
        let config = talon_core::BrokerConfig {
            url: "https://broker".into(),
            request: serde_json::Value::Object([("nested".into(), nested)].into_iter().collect()),
            origin: Some(talon_core::OriginOptions {
                endpoint: Some("storage.example".into()),
                ..Default::default()
            }),
        };
        let mut bytes = codec::encode(1, &ControlMessage::MembershipQuery {}).unwrap();
        let original = bytes.clone();
        assert_eq!(
            envelope::encode_credential_broker(&mut bytes, &config).is_ok(),
            accepted,
            "raw encoding at depth {depth}"
        );
        if accepted {
            let header = FrameHeader::decode(&bytes).unwrap();
            assert_eq!(header.version, 4);
            let (decoded, payload) = envelope::decode(&header, &bytes[HEADER_LEN..]).unwrap();
            assert_eq!(decoded.credential_broker.unwrap().config(), &config);
            assert_eq!(payload, &original[HEADER_LEN..]);
        } else {
            assert_eq!(bytes, original);
        }
    }
}

#[tokio::test]
async fn retries_preserve_origin_and_reject_a_different_origin() {
    let descriptor = |host| {
        let mut value: serde_json::Value = serde_json::from_str(BROKER).unwrap();
        value["origin"] = serde_json::json!({
            "endpoint": host, "region": "us-west-2", "tls_min_version": "1.3", "max_connections": 20
        });
        value.to_string()
    };
    let first = descriptor("first.example");
    let second = descriptor("second.example");
    let first = RequestContext::from_properties(&[("credential_broker", &first)]).unwrap();
    let second = RequestContext::from_properties(&[("credential_broker", &second)]).unwrap();
    let mut bytes = first
        .scope(async {
            let mut bytes = codec::encode(1, &ControlMessage::MembershipQuery {}).unwrap();
            envelope::encode_namespace(&mut bytes, &talon_core::NamespaceId::named("team"))
                .unwrap();
            for _ in 0..2 {
                envelope::outbound(&mut bytes, "unconfigured-peer").unwrap();
                let header = FrameHeader::decode(&bytes).unwrap();
                let (metadata, _) = envelope::decode(&header, &bytes[HEADER_LEN..]).unwrap();
                assert_eq!(metadata.namespace.unwrap().effective_name(), "team");
                let origin = metadata
                    .credential_broker
                    .unwrap()
                    .config()
                    .origin
                    .clone()
                    .unwrap();
                assert_eq!(origin.endpoint.as_deref(), Some("first.example"));
                assert_eq!(origin.tls_min_version.as_deref(), Some("1.3"));
                assert_eq!(origin.max_connections, Some(20));
            }
            bytes
        })
        .await;
    second
        .scope(async {
            assert!(envelope::outbound(&mut bytes, "unconfigured-peer").is_err());
        })
        .await;
}

#[tokio::test]
async fn v4_allows_large_bounded_descriptors_without_relaxing_legacy_limits() {
    let json = serde_json::json!({
        "url": "http://broker/v1/credentials/assume-role",
        "request": {"csp": "aws", "policy": "x".repeat(2048)},
        "origin": {"endpoint": "storage.example", "region": "us-west-2"}
    })
    .to_string();
    let context = RequestContext::from_properties(&[("credential_broker", &json)]).unwrap();
    let bytes = context
        .scope(async {
            let mut bytes = codec::encode(1, &ControlMessage::MembershipQuery {}).unwrap();
            envelope::outbound(&mut bytes, "worker").unwrap();
            bytes
        })
        .await;
    let (header, payload) =
        talon_transport::read_frame(&mut bytes.as_slice(), std::time::Duration::from_secs(1))
            .await
            .unwrap();
    assert!(envelope::decode(&header, &payload).is_ok());
    let mut legacy = header;
    legacy.version = 2;
    assert!(envelope::decode(&legacy, &payload).is_err());
    assert!(envelope::overhead(4) > envelope::overhead(2));
}

#[tokio::test]
async fn interleaved_requests_keep_their_own_credentials() {
    let second = BROKER.replace("sentinel", "second");
    let first = RequestContext::from_properties(&[("credential_broker", BROKER)]).unwrap();
    let second = RequestContext::from_properties(&[("credential_broker", &second)]).unwrap();
    let barrier = tokio::sync::Barrier::new(2);
    let barrier = &barrier;
    let verify = |expected| async move {
        barrier.wait().await;
        tokio::task::yield_now().await;
        assert_eq!(
            current_credential_broker().unwrap().config().request["secret"],
            expected
        );
    };
    tokio::join!(
        first.scope(verify("sentinel")),
        second.scope(verify("second"))
    );
    assert!(current_credential_broker().is_none());
}

#[tokio::test]
async fn external_noncanonical_descriptor_matches_validated_retry_context() {
    let json = r#"{ "request": { "secret": "sentinel", "csp": "aws" }, "origin": {"region": "us-west-2", "endpoint": "storage.example"}, "url": "http://broker/v1/credentials/assume-role" }"#;
    let mut metadata = vec![6];
    metadata.extend_from_slice(&(json.len() as u16).to_be_bytes());
    metadata.extend_from_slice(json.as_bytes());
    let mut bytes = frame(&metadata, 4);
    // A byte-only input can be wrapped before entering a request scope.
    envelope::encode_namespace(&mut bytes, &talon_core::NamespaceId::named("team")).unwrap();
    let context = RequestContext::from_properties(&[("credential_broker", BROKER)]).unwrap();
    let descriptor = context.credential_broker.as_ref().unwrap().clone();
    context
        .scope(async {
            for _ in 0..3 {
                envelope::outbound(&mut bytes, "unconfigured-peer").unwrap();
                envelope::encode_validated_credential_broker(&mut bytes, &descriptor).unwrap();
                assert!(std::sync::Arc::ptr_eq(
                    &descriptor,
                    &current_credential_broker().unwrap()
                ));
                let header = FrameHeader::decode(&bytes).unwrap();
                let (decoded, _) = envelope::decode(&header, &bytes[HEADER_LEN..]).unwrap();
                assert_eq!(decoded.credential_broker.as_ref(), Some(&descriptor));
                assert_eq!(decoded.namespace.unwrap().effective_name(), "team");
                assert!(bytes
                    .windows(descriptor.canonical_json().len())
                    .any(|window| window == descriptor.canonical_json()));
            }
        })
        .await;
}
