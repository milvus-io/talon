//! Idle connections remain owned by a ring-local monitor until checkout.
//! Only readiness waits can be cancelled while retaining a connection. If a
//! checkout interrupts a TLS read/write, discard the entire connection: an
//! owned io_uring buffer may still be in flight, or a TLS record partly read.
use super::Connection;
use monoio::net::TcpStream;
use std::{
    cell::RefCell,
    io,
    os::fd::AsRawFd,
    sync::{Arc, Weak},
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

const IDLE_TIMEOUT: Duration = Duration::from_secs(90);
const MAX_IDLE: usize = 64;

struct Idle {
    owner: Weak<rustls::ClientConfig>,
    authority: String,
    since: Instant,
    checkout: oneshot::Sender<oneshot::Sender<Connection>>,
}

thread_local! {
    // Sockets and monitors never leave their ring. TLS/trust configuration and
    // authority isolate clients, and the bound covers all authorities.
    static IDLE: RefCell<Vec<Idle>> = const { RefCell::new(Vec::new()) };
}

fn retain_live(pool: &mut Vec<Idle>) {
    pool.retain(|entry| {
        entry.owner.strong_count() > 0
            && entry.since.elapsed() < IDLE_TIMEOUT
            && !entry.checkout.is_closed()
    });
}

pub(super) async fn take(owner: &Arc<rustls::ClientConfig>, authority: &str) -> Option<Connection> {
    loop {
        let idle = IDLE.with(|pool| {
            let mut pool = pool.borrow_mut();
            retain_live(&mut pool);
            let index = pool.iter().rposition(|entry| {
                entry.authority == authority && entry.owner.ptr_eq(&Arc::downgrade(owner))
            })?;
            Some(pool.swap_remove(index))
        })?;
        let (return_connection, connection) = oneshot::channel();
        if idle.checkout.send(return_connection).is_ok() {
            if let Ok(connection) = connection.await {
                return Some(connection);
            }
        }
        // EOF, expiry, or an interrupted TLS operation: try another entry.
    }
}

pub(super) fn put(owner: &Arc<rustls::ClientConfig>, authority: String, connection: Connection) {
    if matches!(&connection, Connection::Tls(stream) if !stream.can_idle()) {
        return;
    }
    IDLE.with(|pool| {
        let mut pool = pool.borrow_mut();
        retain_live(&mut pool);
        if pool.len() >= MAX_IDLE {
            return;
        }
        let (checkout, request) = oneshot::channel();
        pool.push(Idle {
            owner: Arc::downgrade(owner),
            authority,
            since: Instant::now(),
            checkout,
        });
        // Disposable idle work must not delay LocalRing::drain. Dropping the
        // runtime drops the monitor; dropping a pool entry wakes it immediately.
        monoio::spawn(async move {
            let _ = monoio::time::timeout(IDLE_TIMEOUT, monitor(connection, request)).await;
        });
    });
}

impl Connection {
    fn socket(&self) -> &TcpStream {
        match self {
            Self::Plain(socket) => socket,
            Self::Tls(stream) => stream.socket(),
        }
    }

    async fn read_idle(&mut self) -> io::Result<()> {
        match self {
            // HTTP/1.1 has no unsolicited messages. Readability while idle is
            // EOF, an error, or unexpected data; none permits reuse.
            Self::Plain(_) => Err(io::ErrorKind::UnexpectedEof.into()),
            Self::Tls(stream) => stream.read_idle().await,
        }
    }
}

async fn monitor(
    mut connection: Connection,
    mut checkout: oneshot::Receiver<oneshot::Sender<Connection>>,
) -> io::Result<()> {
    loop {
        tokio::select! {
            biased;
            ready = connection.socket().readable(false) => ready?,
            request = &mut checkout => {
                if let Ok(request) = request {
                    // A FIN/data event may already be queued in the socket but
                    // its PollAdd completion not yet delivered. Do not hand off
                    // such a connection. TLS control records are normally
                    // handled below; a checkout racing them conservatively
                    // opens another connection rather than waiting on a peer.
                    if socket_quiet(connection.socket()) {
                        let _ = request.send(connection);
                    }
                }
                return Ok(());
            }
        }
        tokio::select! {
            biased;
            _ = &mut checkout => return Ok(()),
            result = connection.read_idle() => result?,
        }
    }
}

fn socket_quiet(socket: &TcpStream) -> bool {
    let mut byte = 0_u8;
    // Non-consuming, nonblocking check: never steal TLS ciphertext or leave a
    // kernel read running on a connection handed back to the request path.
    let result = unsafe {
        libc::recv(
            socket.as_raw_fd(),
            (&mut byte as *mut u8).cast(),
            1,
            libc::MSG_PEEK | libc::MSG_DONTWAIT,
        )
    };
    result < 0 && io::Error::last_os_error().kind() == io::ErrorKind::WouldBlock
}
