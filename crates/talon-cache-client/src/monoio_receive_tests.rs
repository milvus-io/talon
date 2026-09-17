//! Real-ring receive regressions. The probe counts actual native read submissions.
use super::*;
use monoio::buf::IoVecBufMut;
use std::io::Write;
use talon_transport::{response_header_ok, HEADER_LEN};

struct Probe {
    socket: monoio::net::TcpStream,
    reads: usize,
    interrupt_first: bool,
    readvs: usize,
    destination: Option<(usize, usize)>,
    destinations: Vec<(usize, usize)>,
    continue_send: Option<std::sync::mpsc::Sender<()>>,
}
impl AsyncReadRent for Probe {
    async fn read<T: IoBufMut>(&mut self, mut buffer: T) -> monoio::BufResult<usize, T> {
        self.reads += 1;
        self.destinations
            .push((buffer.write_ptr() as usize, buffer.bytes_total()));
        self.socket.read(buffer).await
    }
    async fn readv<T: IoVecBufMut>(&mut self, mut buffer: T) -> monoio::BufResult<usize, T> {
        if std::mem::take(&mut self.interrupt_first) {
            return (Err(io::Error::from(io::ErrorKind::Interrupted)), buffer);
        }
        self.readvs += 1;
        if let Some((ptr, len)) = self.destination {
            assert_eq!(buffer.write_iovec_len(), 2);
            // This is the actual descriptor passed to native readv.
            let body = unsafe { &*buffer.write_iovec_ptr().add(1) };
            assert_eq!((body.iov_base as usize, body.iov_len), (ptr, len));
        }
        let result = self.socket.readv(buffer).await;
        if let Some(next) = self.continue_send.take() {
            let _ = next.send(());
        }
        result
    }
}

fn response(bytes: &[u8]) -> Vec<u8> {
    let mut frame = response_header_ok(1, bytes.len() as u32).to_vec();
    frame.extend_from_slice(bytes);
    frame
}

// Queue the first fragment before reading. For split replies, the server waits
// until the first readv completes before sending the remainder; no timing sleeps.
fn run(
    frame: Vec<u8>,
    split: Option<usize>,
    expected: u64,
    check: impl FnOnce(Result<Reply, Error>, &Probe),
) {
    run_with_interrupt(frame, split, expected, false, check);
}

fn run_with_interrupt(
    frame: Vec<u8>,
    split: Option<usize>,
    expected: u64,
    interrupt_first: bool,
    check: impl FnOnce(Result<Reply, Error>, &Probe),
) {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let (ready, queued) = tokio::sync::oneshot::channel();
    let (next, received) = std::sync::mpsc::channel();
    let (done, finished) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut peer, _) = listener.accept().unwrap();
        peer.set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let prefix = split.unwrap_or(frame.len());
        peer.write_all(&frame[..prefix]).unwrap();
        ready.send(()).unwrap();
        if split.is_some() {
            received.recv_timeout(Duration::from_secs(5)).unwrap();
            peer.write_all(&frame[prefix..]).unwrap();
        }
        // Keep the peer open: short errors/invalid lengths must finish without EOF.
        finished.recv_timeout(Duration::from_secs(5)).unwrap();
    });
    monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .enable_timer()
        .build()
        .unwrap()
        .block_on(async {
            let socket = monoio::net::TcpStream::connect_addr(addr).await.unwrap();
            queued.await.unwrap();
            let prefix = expected.min(RECEIVE_PREFIX as u64) as usize;
            let mut probe = Probe {
                socket,
                reads: 0,
                destination: None,
                destinations: Vec::new(),
                interrupt_first,
                readvs: 0,
                continue_send: split.map(|_| next),
            };
            let result = monoio::time::timeout(
                Duration::from_secs(2),
                range_response(&mut probe, rpc::Expected::Range(expected), prefix),
            )
            .await
            .expect("receive must not wait for the successful length on errors");
            check(result, &probe);
        });
    done.send(()).unwrap();
    server.join().unwrap();
}

#[test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
fn queued_header_and_body_complete_in_one_readv() {
    let bytes: Vec<u8> = (0..4096).map(|n| n as u8).collect();
    run(
        response(&bytes),
        None,
        bytes.len() as u64,
        |result, probe| {
            assert!(matches!(result, Ok(Reply::Range(body)) if body == bytes));
            assert_eq!((probe.readvs, probe.reads), (1, 0));
        },
    );
}

#[test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
fn split_header_and_partial_body_preserve_every_byte() {
    for split in [5, HEADER_LEN, HEADER_LEN + 17, HEADER_LEN + RECEIVE_PREFIX] {
        let bytes: Vec<u8> = (0..(128 * 1024 + 17)).map(|n| n as u8).collect();
        run(
            response(&bytes),
            Some(split),
            bytes.len() as u64,
            |result, probe| {
                assert!(matches!(result, Ok(Reply::Range(body)) if body == bytes));
                assert_eq!(probe.readvs, 1);
                assert!(probe.reads > 0);
            },
        );
    }
}

#[test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
fn short_error_reply_does_not_wait_for_requested_body() {
    let frame =
        talon_transport::encode_typed_error(1, talon_transport::DataErrorCode::NotFound, "absent");
    run(frame, None, 1024 * 1024, |result, probe| {
        assert!(
            matches!(result, Err(Error::Worker(crate::WorkerError::Remote(e))) if e.code == talon_transport::DataErrorCode::NotFound)
        );
        assert_eq!((probe.readvs, probe.reads), (1, 0));
    });
}

#[test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
fn invalid_length_is_rejected_after_bounded_prefix() {
    run(
        response_header_ok(1, 1).to_vec(),
        None,
        1024 * 1024 * 1024,
        |result, probe| {
            assert!(matches!(
                result,
                Err(Error::Worker(
                    crate::WorkerError::RangeLengthMismatch { .. }
                ))
            ));
            assert_eq!((probe.readvs, probe.reads), (1, 0));
        },
    );
}

#[test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
fn unsolicited_bytes_after_error_are_not_discarded_into_pool() {
    let mut frame =
        talon_transport::encode_typed_error(1, talon_transport::DataErrorCode::NotFound, "absent");
    frame.extend_from_slice(b"unexpected trailing bytes");
    run(frame, None, 4096, |result, _| {
        assert!(matches!(
            result,
            Err(Error::Worker(crate::WorkerError::Encode(
                talon_transport::DataError::LengthMismatch { .. }
            )))
        ));
    });
}

#[test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
fn interrupted_vectored_receive_retries_before_parsing() {
    run_with_interrupt(response(b"payload"), None, 7, true, |result, probe| {
        assert!(matches!(result, Ok(Reply::Range(body)) if body == b"payload"));
        assert_eq!((probe.readvs, probe.reads), (1, 0));
    });
}

fn run_direct(
    frame: Vec<u8>,
    split: Option<usize>,
    length: usize,
    interrupt: bool,
    check: impl FnOnce(Result<Reply, Error>, &[u8], &Probe),
) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let (ready, queued) = tokio::sync::oneshot::channel();
    let (next, received) = std::sync::mpsc::channel();
    let (done, finished) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut peer, _) = listener.accept().unwrap();
        peer.set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let prefix = split.unwrap_or(frame.len());
        peer.write_all(&frame[..prefix]).unwrap();
        ready.send(()).unwrap();
        if split.is_some() {
            received.recv_timeout(Duration::from_secs(5)).unwrap();
            peer.write_all(&frame[prefix..]).unwrap();
        }
        finished.recv_timeout(Duration::from_secs(5)).unwrap();
    });
    monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .enable_timer()
        .build()
        .unwrap()
        .block_on(async {
            let socket = monoio::net::TcpStream::connect_addr(addr).await.unwrap();
            queued.await.unwrap();
            let bytes = vec![0xCD; length + 2];
            let ptr = bytes.as_ptr() as usize;
            let mut buffer = crate::read_buffer::ReadBuffer::new(bytes);
            let (guard, rest) = buffer.take_target().split_at(1);
            let (mut target, tail) = rest.split_at(length);
            let mut probe = Probe {
                socket,
                reads: 0,
                readvs: 0,
                interrupt_first: interrupt,
                destination: Some((ptr + 1, length)),
                destinations: Vec::new(),
                continue_send: split.map(|_| next),
            };
            let result = monoio::time::timeout(
                Duration::from_secs(2),
                range_response_into(&mut probe, rpc::Expected::Range(length as u64), &mut target),
            )
            .await
            .expect("direct receive stalled");
            drop((guard, target, tail));
            let bytes = buffer.finish().await;
            assert_eq!(
                bytes.as_ptr() as usize,
                ptr,
                "allocation must be returned unchanged"
            );
            assert_eq!(
                (bytes[0], bytes[length + 1]),
                (0xCD, 0xCD),
                "receive exceeded destination"
            );
            check(result, &bytes[1..length + 1], &probe);
        });
    done.send(()).unwrap();
    server.join().unwrap();
}

#[test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
fn direct_receive_uses_caller_address_for_initial_and_partial_body() {
    let bytes: Vec<u8> = (0..4096).map(|n| n as u8).collect();
    for split in [None, Some(5), Some(HEADER_LEN), Some(HEADER_LEN + 17)] {
        run_direct(
            response(&bytes),
            split,
            bytes.len(),
            true,
            |result, actual, probe| {
                assert!(matches!(result, Ok(Reply::Written(4096))));
                assert_eq!(actual, bytes);
                assert_eq!(probe.readvs, 1);
                let (ptr, len) = probe.destination.unwrap();
                if let Some(split) = split {
                    let prefix = split.saturating_sub(HEADER_LEN);
                    assert!(probe.destinations.contains(&(ptr + prefix, len - prefix)),
                    "body continuation must write into original destination at the received offset");
                } else {
                    assert_eq!(probe.reads, 0);
                }
            },
        );
    }
}

#[test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
fn direct_receive_errors_and_invalid_lengths_finish_without_body_wait() {
    for length in [1, 4096] {
        let error = talon_transport::encode_typed_error(
            1,
            talon_transport::DataErrorCode::NotFound,
            "absent",
        );
        run_direct(error, None, length, false, |result, _, _| {
            assert!(
                matches!(result, Err(Error::Worker(crate::WorkerError::Remote(e)))
                if e.code == talon_transport::DataErrorCode::NotFound)
            );
        });
    }
    run_direct(
        response_header_ok(1, 8).to_vec(),
        None,
        4096,
        false,
        |result, _, _| {
            assert!(matches!(
                result,
                Err(Error::Worker(
                    crate::WorkerError::RangeLengthMismatch { .. }
                ))
            ));
        },
    );
}

#[test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
fn native_shared_deadline_preserves_exact_deadline_and_fast_completion() {
    monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .enable_timer()
        .build()
        .unwrap()
        .block_on(async {
            let client = MonoioClient::new();
            assert_eq!(
                client.deadline(Duration::ZERO, async { 7 }).await.unwrap(),
                7
            );
            assert!(client.wakeup.borrow().is_none());
            let start = Instant::now();
            let result = client
                .deadline(Duration::from_millis(35), std::future::pending::<()>())
                .await;
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
            assert!(start.elapsed() >= Duration::from_millis(35));
            let short = client.deadline(Duration::from_millis(5), std::future::pending::<()>());
            assert_eq!(short.await.unwrap_err().kind(), io::ErrorKind::TimedOut);
            // Many pending operations share the initial wakeup but all progress.
            let operations = (0..64).map(|_| {
                client.deadline(Duration::from_secs(1), async {
                    monoio::time::sleep(Duration::from_millis(1)).await;
                    9
                })
            });
            for result in futures::future::join_all(operations).await {
                assert_eq!(result.unwrap(), 9);
            }
        });
}

#[test]
fn receive_metadata_is_reused_only_after_its_owner_retires() {
    futures::executor::block_on(async {
        let mut buffer = crate::read_buffer::ReadBuffer::new(vec![0; 32]);
        let mut target = buffer.take_target();
        let receive = Metadata::receive(target.lease().await);
        let address = receive.metadata.storage.as_ref().unwrap().as_ref() as *const _;
        drop(receive);
        let mut receive = Metadata::receive(target.lease().await);
        assert_eq!(
            address,
            receive.metadata.storage.as_ref().unwrap().as_ref() as *const _
        );
        let body = unsafe { &*monoio::buf::IoVecBufMut::write_iovec_ptr(&mut receive).add(1) };
        assert_eq!(body.iov_base, receive._lease.write_ptr().cast());
        drop((receive, target));
        assert_eq!(buffer.finish().await.len(), 32);
    });
}
