use std::sync::{Arc, Mutex};
use std::time::Duration;

use talon_core::{NodeId, NodeInfo, NodeRole};
use talon_rust_client::{parse_uri, ClientBuilder, LoadRequest, RequestOptions, Version};
use talon_transport::{codec, data, envelope, ControlMessage, MsgType, RequestContext, HEADER_LEN};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

#[path = "../../../crates/talon-transport/tests/support/membership.rs"]
mod membership;

const BROKER: &str = r#"{"url":"http://broker/v1/credentials/assume-role","request":{"csp":"aws","scope":"A"},"origin":{"endpoint":"storage.example","region":"us-west-2"}}"#;

#[tokio::test]
async fn namespace_and_properties_reach_every_operation_and_retry_with_tracing_off() {
    assert!(!talon_telemetry::enabled());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let events = observed.clone();
    let worker_address = address.clone();
    let server = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let events = events.clone();
            let worker_address = worker_address.clone();
            tokio::spawn(async move {
                // Closing after each response forces reused sockets to retry.
                if let Ok((header, payload)) =
                    talon_transport::read_frame(&mut stream, Duration::from_secs(5)).await
                {
                    let (metadata, _) = envelope::decode(&header, &payload).unwrap();
                    let mut namespace = metadata
                        .namespace
                        .unwrap_or_default()
                        .effective_name()
                        .to_owned();
                    let broker = metadata
                        .credential_broker
                        .map(|value| value.config().request["scope"].as_str().unwrap().to_owned());
                    let mut full = header.encode().to_vec();
                    full.extend_from_slice(&payload);
                    let (operation, reply) = if header.msg_type == MsgType::Control {
                        let (_, request) = codec::decode_request(&full).unwrap();
                        let (operation, reply) = match request {
                            ControlMessage::MembershipQuery {} => {
                                assert_eq!(namespace, "default");
                                assert!(
                                    broker.is_none(),
                                    "background discovery has no credentials"
                                );
                                (
                                    "membership",
                                    membership::plain(vec![NodeInfo {
                                        id: NodeId::new("worker"),
                                        address: worker_address.clone(),
                                        role: NodeRole::Worker,
                                    }]),
                                )
                            }
                            ControlMessage::StatObject { .. } => (
                                "stat",
                                ControlMessage::ObjectStat {
                                    size: 8,
                                    version: "v1".into(),
                                },
                            ),
                            ControlMessage::ListObjects { .. } => {
                                ("list", ControlMessage::ObjectList { entries: vec![] })
                            }
                            ControlMessage::LoadBlock { .. } => (
                                "load",
                                ControlMessage::Ack {
                                    ok: true,
                                    detail: None,
                                },
                            ),
                            ControlMessage::BatchLoad { blocks } => {
                                assert_eq!(blocks.len(), 2);
                                (
                                    "batch",
                                    ControlMessage::BatchLoadResult { failures: vec![] },
                                )
                            }
                            other => panic!("unexpected control request: {other:?}"),
                        };
                        (operation, codec::encode(header.request_id, &reply).unwrap())
                    } else {
                        let request = if header.msg_type == MsgType::GetVersionedRangeNamespace {
                            let (_, scoped) =
                                data::decode_versioned_namespace_request(&full).unwrap();
                            namespace = scoped.namespace.effective_name().to_owned();
                            scoped.request
                        } else {
                            data::decode_versioned_request(&full).unwrap().1
                        };
                        let mut reply =
                            data::response_header_ok(header.request_id, request.request.len as u32)
                                .to_vec();
                        reply.extend(std::iter::repeat(7).take(request.request.len as usize));
                        ("read", reply)
                    };
                    if operation != "membership" {
                        events
                            .lock()
                            .unwrap()
                            .push((operation, broker, header.version, namespace));
                    }
                    stream
                        .write_all(&envelope::response_version(reply, header.version))
                        .await
                        .unwrap();
                }
            });
        }
    });
    let client = ClientBuilder::default()
        .with_coordinator(address)
        .with_block_size(8)
        .build()
        .unwrap();
    let object = parse_uri("s3://bucket/key").unwrap();
    let options = RequestOptions::default()
        .with_namespace("analytics")
        .with_credential_broker(BROKER);
    let inherited_broker = BROKER.replace("\"A\"", "\"B\"");
    let inherited =
        RequestContext::from_properties(&[("credential_broker", &inherited_broker)]).unwrap();
    let stat = inherited
        .scope(client.stat(&object, &options))
        .await
        .unwrap();
    client.list("s3/bucket", &options).await.unwrap();
    assert_eq!(
        client
            .read(&object, 0, Some(4), None, &options)
            .await
            .unwrap(),
        vec![7; 4]
    );
    let mut output = [0; 4];
    client
        .read_into(&object, 0, &mut output, Some(&stat), &options)
        .await
        .unwrap();
    assert_eq!(output, [7; 4]);
    client
        .load(&object, &Version::new("v1"), 8, &options)
        .await
        .unwrap();
    client
        .batch_load(
            &[
                LoadRequest {
                    object: object.clone(),
                    version: Version::new("v1"),
                    size: 8,
                },
                LoadRequest {
                    object: object.clone(),
                    version: Version::new("v2"),
                    size: 8,
                },
            ],
            &options,
        )
        .await
        .unwrap();
    RequestContext::from_properties(&[("credential_broker", BROKER)])
        .unwrap()
        .scope(client.stat(&object, &Default::default()))
        .await
        .unwrap();
    let events = observed.lock().unwrap();
    assert_eq!(
        events
            .iter()
            .map(|(operation, _, _, _)| *operation)
            .collect::<Vec<_>>(),
        ["stat", "list", "stat", "read", "read", "load", "batch", "stat"]
    );
    for (_, broker, version, namespace) in &events[..events.len() - 1] {
        assert_eq!(broker.as_deref(), Some("A"));
        assert_eq!(*version, 4);
        assert_eq!(namespace, "analytics");
    }
    assert_eq!(events.last().unwrap().1, None);
    assert_eq!(events.last().unwrap().2, 1);
    assert_eq!(events.last().unwrap().3, "default");
    server.abort();
}

#[tokio::test]
async fn old_peer_rejection_never_resends_without_credentials() {
    use tokio::io::AsyncReadExt;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let versions = observed.clone();
    let server = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = [0; HEADER_LEN];
            stream.read_exact(&mut bytes).await.unwrap();
            let version = bytes[2];
            // An old decoder closes on an unsupported header before its body.
            if version == 4 {
                versions.lock().unwrap().push(version);
                continue;
            }
            // Background membership discovery is independently static.
            let header = talon_transport::FrameHeader::decode(&bytes).unwrap();
            let mut payload = vec![0; header.length as usize];
            stream.read_exact(&mut payload).await.unwrap();
            let mut full = bytes.to_vec();
            full.extend_from_slice(&payload);
            let (_, request) = codec::decode_request(&full).unwrap();
            assert!(matches!(request, ControlMessage::MembershipQuery {}));
            stream
                .write_all(&codec::encode(header.request_id, &membership::plain(vec![])).unwrap())
                .await
                .unwrap();
        }
    });
    let client = ClientBuilder::default()
        .with_coordinator(address)
        .build()
        .unwrap();
    let options = RequestOptions::default().with_credential_broker(BROKER);
    assert!(client
        .stat(&parse_uri("s3://bucket/key").unwrap(), &options)
        .await
        .is_err());
    assert_eq!(*observed.lock().unwrap(), [4]);
    server.abort();
}

#[tokio::test]
async fn invalid_credential_broker_fails_even_for_empty_reads_and_loads() {
    let client = ClientBuilder::default()
        .with_coordinator("127.0.0.1:1")
        .build()
        .unwrap();
    let object = parse_uri("s3://bucket/key").unwrap();
    for descriptor in [
        "",
        r#"{"url":"https://broker","request":{}}"#,
        r#"{"url":"https://broker","request":{},"origin":null}"#,
        r#"{"url":"https://broker","request":{},"origin":{}}"#,
        r#"{"url":"https://broker","request":{},"origin":{"request_timeout_ms":17}}"#,
        r#"{"url":"https://broker","request":{},"origin":{"endpoint":null}}"#,
        r#"{"url":"https://broker","request":{},"origin":{"endpoint":""}}"#,
        r#"{"url":"https://broker","request":{},"origin":{"endpoint":" \t\n"}}"#,
        r#"{"url":"https://broker/sentinel","request":{"csp":"aws","region":"broker-region"},"origin":{"endpoint":"storage.example"}}"#,
        r#"{"url":"https://broker/sentinel","request":{"csp":"ali","region":"broker-region"},"origin":{"endpoint":"storage.example","region":null}}"#,
        r#"{"url":"https://broker/sentinel","request":{"csp":"tc","region":"broker-region"},"origin":{"endpoint":"storage.example","region":""}}"#,
        r#"{"url":"https://broker/sentinel","request":{"csp":"hwc","region":"broker-region"},"origin":{"endpoint":"storage.example","region":" \t\n"}}"#,
    ] {
        let options = RequestOptions::default().with_credential_broker(descriptor);
        assert!(matches!(
            client.stat(&object, &options).await,
            Err(talon_rust_client::Error::InvalidArgument(error)) if !error.contains("sentinel")
        ));
        assert!(matches!(
            client.list("s3/bucket", &options).await,
            Err(talon_rust_client::Error::InvalidArgument(error)) if !error.contains("sentinel")
        ));
        assert!(matches!(
            client.read(&object, 0, Some(0), None, &options).await,
            Err(talon_rust_client::Error::InvalidArgument(error)) if !error.contains("sentinel")
        ));
        assert!(matches!(
            client.read_into(&object, 0, &mut [], None, &options).await,
            Err(talon_rust_client::Error::InvalidArgument(error)) if !error.contains("sentinel")
        ));
        assert!(matches!(
            client.load(&object, &Version::new("v1"), 0, &options).await,
            Err(talon_rust_client::LoadError::InvalidArgument(error)) if !error.contains("sentinel")
        ));
        assert!(matches!(
            client.batch_load(&[], &options).await,
            Err(talon_rust_client::LoadError::InvalidArgument(error)) if !error.contains("sentinel")
        ));
    }
}
