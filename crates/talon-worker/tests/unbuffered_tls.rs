//! Exercise the opt-in rustls patch without a socket or an async runtime.
use rustls::{client::UnbufferedClientConnection, unbuffered::ConnectionState};
use std::{io::Write, sync::Arc};

fn exercise(
    suite: rustls::SupportedCipherSuite,
    version: &'static rustls::SupportedProtocolVersion,
    corrupt: bool,
) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
    let mut provider = rustls::crypto::ring::default_provider();
    provider.cipher_suites = vec![suite];
    let provider = Arc::new(provider);
    let mut server_config = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[version])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert.cert.der().clone()], key.into())
        .unwrap();
    server_config.max_fragment_size = Some(256);
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let client_config = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[version])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let mut client =
        UnbufferedClientConnection::new(Arc::new(client_config), "localhost".try_into().unwrap())
            .unwrap();
    let mut server = rustls::ServerConnection::new(Arc::new(server_config)).unwrap();
    server.set_buffer_limit(None);
    let expected: Vec<u8> = (0..65539).map(|i| (i % 251) as u8).collect();
    let mut incoming = Vec::new();
    let mut outgoing = Vec::new();
    let mut received = Vec::new();
    let mut sent = false;
    let mut records = 0;
    for _ in 0..2000 {
        let input_start = incoming.as_ptr() as usize;
        let input_len = incoming.len();
        let status = client.process_tls_records_in_place(&mut incoming);
        let mut discard = status.discard;
        let state = match status.state {
            Ok(state) => state,
            Err(error) => {
                assert!(corrupt && sent, "unexpected error: {error}");
                assert!(matches!(error, rustls::Error::DecryptError));
                assert!(received.is_empty(), "unauthenticated plaintext escaped");
                return;
            }
        };
        match state {
            ConnectionState::EncodeTlsData(mut encode) => {
                let mut encoded = vec![0; 65536];
                let n = encode.encode(&mut encoded).unwrap();
                outgoing.extend_from_slice(&encoded[..n]);
            }
            ConnectionState::TransmitTlsData(transmit) => {
                server.read_tls(&mut outgoing.as_slice()).unwrap();
                server.process_new_packets().unwrap();
                outgoing.clear();
                transmit.done();
            }
            ConnectionState::WriteTraffic(_) if !sent => {
                assert!(!server.is_handshaking());
                if version.version == rustls::ProtocolVersion::TLSv1_3 && !corrupt {
                    // Post-handshake key changes still pass through the original
                    // protocol state handler before exposing plaintext.
                    server.refresh_traffic_keys().unwrap();
                }
                server.writer().write_all(&expected).unwrap();
                sent = true;
            }
            ConnectionState::ReadTraffic(mut traffic) => {
                assert!(!corrupt, "tampered first record must be rejected");
                while let Some(record) = traffic.next_record() {
                    let record = record.unwrap();
                    let start = (record.payload.as_ptr() as usize)
                        .checked_sub(input_start)
                        .unwrap();
                    assert!(
                        start + record.payload.len() <= input_len,
                        "payload was copied to an internal queue"
                    );
                    received.extend_from_slice(record.payload);
                    discard += record.discard;
                    records += 1;
                }
            }
            ConnectionState::BlockedHandshake | ConnectionState::WriteTraffic(_) => (),
            _ => panic!("unexpected TLS state"),
        }
        incoming.drain(..discard);
        while server.wants_write() {
            let start = incoming.len();
            server.write_tls(&mut incoming).unwrap();
            if sent && corrupt {
                // TLS 1.3 may put session tickets before application data. Server
                // output has been drained at every preceding handshake step, so
                // this is the first application record in this fixture.
                let record_length =
                    u16::from_be_bytes([incoming[start + 3], incoming[start + 4]]) as usize;
                incoming[start + 5 + record_length - 1] ^= 1;
            }
        }
        if received.len() == expected.len() {
            assert_eq!(received, expected);
            assert!(records > 200);
            return;
        }
    }
    panic!("TLS state machine failed to complete");
}

#[test]
fn authenticated_plaintext_borrows_input_for_tls12_tls13_and_all_ring_aeads() {
    use rustls::crypto::ring::cipher_suite::*;
    for (suite, version) in [
        (TLS13_AES_128_GCM_SHA256, &rustls::version::TLS13),
        (TLS13_AES_256_GCM_SHA384, &rustls::version::TLS13),
        (TLS13_CHACHA20_POLY1305_SHA256, &rustls::version::TLS13),
        (
            TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
            &rustls::version::TLS12,
        ),
        (
            TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
            &rustls::version::TLS12,
        ),
        (
            TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
            &rustls::version::TLS12,
        ),
    ] {
        exercise(suite, version, false);
        exercise(suite, version, true);
    }
}
