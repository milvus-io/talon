//! Client connection pool with separate control and data channels.
//!
//! DESIGN.md §1 keeps the control and data planes on **separate connections**
//! so bulk data transfers can't starve latency-sensitive control messages. This
//! pool is keyed by worker address and, within each worker, maintains two
//! independent sub-pools — one for [`Channel::Control`], one for
//! [`Channel::Data`] — each with its own reuse set and per-worker connection
//! cap.
//!
//! The pool is generic over a [`Connector`] so it is testable without real
//! sockets: [`Pool::checkout`] hands out an idle connection or, if under the
//! cap, opens a new one via the connector; [`Pool::checkin`] returns it for
//! reuse. Connect failures propagate so the caller can trigger a placement
//! refresh; nothing is leaked because every open connection is either idle in
//! the pool or checked out to exactly one caller.

use std::collections::HashMap;
use std::sync::Mutex;

/// Which plane a connection serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Channel {
    /// Latency-sensitive control-plane messages.
    Control,
    /// Bulk data-plane transfers (sendfile/splice).
    Data,
}

/// Opens new connections to a worker for a given [`Channel`].
///
/// Real implementations open a TCP socket; tests use a counting mock.
pub trait Connector {
    /// The connection handle type.
    type Conn;
    /// The error returned when a connection cannot be established.
    type Error;

    /// Open a new connection to `addr` for `channel`.
    fn connect(&self, addr: &str, channel: Channel) -> Result<Self::Conn, Self::Error>;
}

/// Per-(worker, channel) pool configuration.
#[derive(Debug, Clone, Copy)]
pub struct PoolConfig {
    /// Maximum concurrent connections per worker per channel.
    pub max_per_channel: usize,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self { max_per_channel: 8 }
    }
}

#[derive(Default)]
struct ChannelState<C> {
    idle: Vec<C>,
    // Total live (idle + checked out) connections for this (worker, channel).
    live: usize,
}

/// Internal map from (worker addr, channel) to that sub-pool's state.
type StateMap<C> = HashMap<(String, Channel), ChannelState<C>>;

/// A connection pool keyed by worker address and channel.
pub struct Pool<K: Connector> {
    connector: K,
    config: PoolConfig,
    state: Mutex<StateMap<K::Conn>>,
}

/// Error checking a connection out of the pool.
#[derive(Debug, PartialEq, Eq)]
pub enum CheckoutError<E> {
    /// The per-worker-per-channel connection cap was reached with none idle.
    Exhausted,
    /// The connector failed to establish a new connection.
    Connect(E),
}

impl<K: Connector> Pool<K> {
    /// Create a pool over `connector` with the given config.
    pub fn new(connector: K, config: PoolConfig) -> Self {
        Self {
            connector,
            config,
            state: Mutex::new(HashMap::new()),
        }
    }

    /// Check out a connection to `addr` on `channel`.
    ///
    /// Reuses an idle connection if available; otherwise opens a new one when
    /// under the per-channel cap. Returns [`CheckoutError::Exhausted`] if the
    /// cap is reached with none idle, or [`CheckoutError::Connect`] if the
    /// connector fails (the caller can then refresh placement).
    pub fn checkout(
        &self,
        addr: &str,
        channel: Channel,
    ) -> Result<K::Conn, CheckoutError<K::Error>> {
        let key = (addr.to_string(), channel);
        // First, try to reuse an idle connection under the lock.
        {
            let mut g = self.state.lock().unwrap();
            let st = g.entry(key.clone()).or_insert_with(|| ChannelState {
                idle: Vec::new(),
                live: 0,
            });
            if let Some(conn) = st.idle.pop() {
                return Ok(conn);
            }
            if st.live >= self.config.max_per_channel {
                return Err(CheckoutError::Exhausted);
            }
            // Reserve a slot before connecting so concurrent checkouts respect
            // the cap.
            st.live += 1;
        }
        // Connect outside the lock; on failure, release the reserved slot.
        match self.connector.connect(addr, channel) {
            Ok(conn) => Ok(conn),
            Err(e) => {
                let mut g = self.state.lock().unwrap();
                if let Some(st) = g.get_mut(&key) {
                    st.live -= 1;
                }
                Err(CheckoutError::Connect(e))
            }
        }
    }

    /// Return a connection to the pool for reuse on the same worker/channel.
    pub fn checkin(&self, addr: &str, channel: Channel, conn: K::Conn) {
        let key = (addr.to_string(), channel);
        let mut g = self.state.lock().unwrap();
        let st = g.entry(key).or_insert_with(|| ChannelState {
            idle: Vec::new(),
            live: 0,
        });
        st.idle.push(conn);
    }

    /// Drop a broken connection (do not reuse), freeing its live slot.
    ///
    /// Call this instead of [`checkin`](Self::checkin) when a connection errored,
    /// so the cap accounting stays correct and a replacement can be opened.
    pub fn discard(&self, addr: &str, channel: Channel) {
        let key = (addr.to_string(), channel);
        let mut g = self.state.lock().unwrap();
        if let Some(st) = g.get_mut(&key) {
            st.live = st.live.saturating_sub(1);
        }
    }

    /// Number of idle (reusable) connections for a worker/channel.
    pub fn idle_count(&self, addr: &str, channel: Channel) -> usize {
        let g = self.state.lock().unwrap();
        g.get(&(addr.to_string(), channel))
            .map_or(0, |s| s.idle.len())
    }

    /// Number of live (idle + checked-out) connections for a worker/channel.
    pub fn live_count(&self, addr: &str, channel: Channel) -> usize {
        let g = self.state.lock().unwrap();
        g.get(&(addr.to_string(), channel)).map_or(0, |s| s.live)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A connector that hands out sequential ids and can be scripted to fail.
    struct MockConnector {
        opened: AtomicUsize,
        fail: bool,
    }

    impl MockConnector {
        fn new() -> Self {
            Self {
                opened: AtomicUsize::new(0),
                fail: false,
            }
        }
    }

    impl Connector for MockConnector {
        type Conn = usize;
        type Error = &'static str;

        fn connect(&self, _addr: &str, _channel: Channel) -> Result<usize, &'static str> {
            if self.fail {
                return Err("connect refused");
            }
            Ok(self.opened.fetch_add(1, Ordering::SeqCst))
        }
    }

    fn pool(max: usize) -> Pool<MockConnector> {
        Pool::new(
            MockConnector::new(),
            PoolConfig {
                max_per_channel: max,
            },
        )
    }

    #[test]
    fn control_and_data_are_separate_sub_pools() {
        let p = pool(2);
        // Opening data connections doesn't consume control capacity.
        let _d0 = p.checkout("w1", Channel::Data).unwrap();
        let _d1 = p.checkout("w1", Channel::Data).unwrap();
        assert_eq!(p.live_count("w1", Channel::Data), 2);
        // Control is still fully available -> bulk data can't starve control.
        let c0 = p.checkout("w1", Channel::Control).unwrap();
        assert_eq!(p.live_count("w1", Channel::Control), 1);
        assert!(c0 == 2 || c0 == 3); // some fresh id
    }

    #[test]
    fn connections_are_reused_after_checkin() {
        let p = pool(4);
        let c = p.checkout("w1", Channel::Control).unwrap();
        assert_eq!(p.live_count("w1", Channel::Control), 1);
        p.checkin("w1", Channel::Control, c);
        assert_eq!(p.idle_count("w1", Channel::Control), 1);
        // Next checkout reuses the idle connection: same id, no new open.
        let c2 = p.checkout("w1", Channel::Control).unwrap();
        assert_eq!(c2, c);
        assert_eq!(p.idle_count("w1", Channel::Control), 0);
    }

    #[test]
    fn cap_is_enforced_per_worker_channel() {
        let p = pool(2);
        let _a = p.checkout("w1", Channel::Data).unwrap();
        let _b = p.checkout("w1", Channel::Data).unwrap();
        // Third exceeds the cap with none idle.
        assert_eq!(
            p.checkout("w1", Channel::Data),
            Err(CheckoutError::Exhausted)
        );
        // A different worker has its own budget.
        assert!(p.checkout("w2", Channel::Data).is_ok());
    }

    #[test]
    fn connect_failure_frees_the_reserved_slot() {
        let mut connector = MockConnector::new();
        connector.fail = true;
        let p = Pool::new(connector, PoolConfig { max_per_channel: 1 });
        assert_eq!(
            p.checkout("w1", Channel::Control),
            Err(CheckoutError::Connect("connect refused"))
        );
        // The failed attempt must not leak a live slot.
        assert_eq!(p.live_count("w1", Channel::Control), 0);
    }

    #[test]
    fn discard_frees_slot_for_a_replacement() {
        let p = pool(1);
        let _c = p.checkout("w1", Channel::Data).unwrap();
        assert_eq!(
            p.checkout("w1", Channel::Data),
            Err(CheckoutError::Exhausted)
        );
        // Discard the broken connection, then a replacement can be opened.
        p.discard("w1", Channel::Data);
        assert_eq!(p.live_count("w1", Channel::Data), 0);
        assert!(p.checkout("w1", Channel::Data).is_ok());
    }
}
