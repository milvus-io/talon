//! Runtime-independent discovery refresh, shared by all clones of a reader.
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use tokio::sync::{oneshot, watch};

use crate::lock::RwLockExt;
use crate::membership_cache::{MembershipCache, MembershipSnapshot};
use crate::{BlockReadError, ConnectionPool, CoordinatorClient, CoordinatorError, PlacementCache};

struct Config {
    membership: Arc<MembershipCache>,
    workers: Arc<ConnectionPool>,
    placement: Arc<PlacementCache>,
}

#[derive(Clone, Default)]
struct Status {
    initialized: bool,
    error: Option<Arc<CoordinatorError>>,
}

pub(crate) struct DiscoveryRefresh {
    config: Arc<RwLock<Config>>,
    status: watch::Receiver<Status>,
    reconfigured: watch::Sender<()>,
    stop: Option<oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl DiscoveryRefresh {
    pub(crate) fn new(
        coordinator: &CoordinatorClient,
        membership: Arc<MembershipCache>,
        workers: Arc<ConnectionPool>,
        placement: Arc<PlacementCache>,
    ) -> Self {
        // Discovery sockets must never be pooled with foreground sockets owned
        // by another Tokio runtime (including the C/Python runtime).
        let coordinator = coordinator.isolated();
        let config = Arc::new(RwLock::new(Config {
            membership,
            workers,
            placement,
        }));
        let background = config.clone();
        let (status_tx, status) = watch::channel(Status::default());
        let failure_tx = status_tx.clone();
        let (stop, mut stopped) = oneshot::channel();
        let (reconfigured, mut configuration_changed) = watch::channel(());
        let thread = std::thread::Builder::new()
            .name("talon-discovery".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        status_tx.send_replace(Status {
                            initialized: true,
                            error: Some(Arc::new(error.into())),
                        });
                        return;
                    }
                };
                runtime.block_on(async move {
                    loop {
                        let observed = Instant::now();
                        let result = tokio::select! {
                            biased;
                            _ = &mut stopped => return,
                            result = coordinator.discovery() => result,
                        };
                        let delay = match result {
                            Ok(view) => {
                                let config = background.read_recover();
                                let snapshot =
                                    config.membership.replace(view, observed, &config.workers);
                                config.placement.clear();
                                status_tx.send_replace(Status {
                                    initialized: true,
                                    error: None,
                                });
                                (snapshot
                                    .valid_until
                                    .saturating_duration_since(Instant::now())
                                    / 2)
                                .clamp(Duration::from_millis(100), Duration::from_secs(5))
                            }
                            Err(error) => {
                                status_tx.send_replace(Status {
                                    initialized: true,
                                    error: Some(Arc::new(error)),
                                });
                                Duration::from_secs(1)
                            }
                        };
                        tokio::select! {
                            biased;
                            _ = &mut stopped => return,
                            _ = configuration_changed.changed() => {}
                            _ = tokio::time::sleep(delay) => {}
                        }
                    }
                });
            });
        let thread = match thread {
            Ok(thread) => Some(thread),
            Err(error) => {
                failure_tx.send_replace(Status {
                    initialized: true,
                    error: Some(Arc::new(error.into())),
                });
                None
            }
        };
        Self {
            config,
            status,
            reconfigured,
            stop: Some(stop),
            thread,
        }
    }

    pub(crate) fn configure(&self, membership: Arc<MembershipCache>, workers: Arc<ConnectionPool>) {
        let mut config = self.config.write_recover();
        config.membership = membership;
        config.workers = workers;
        // Builder configuration is independent of data requests. An in-flight
        // query publishes into the new cache; an idle refresher wakes immediately.
        self.reconfigured.send_replace(());
    }

    pub(crate) async fn snapshot(&self) -> Result<MembershipSnapshot, BlockReadError> {
        let mut status = self.status.clone();
        loop {
            let current = status.borrow_and_update().clone();
            if let Some(snapshot) = self.config.read_recover().membership.last_good() {
                return Ok(snapshot);
            }
            if current.initialized {
                // A failed refresh never revokes the last published snapshot.
                // Requests do not wake or retry refresh.
                if let Some(error) = current.error {
                    return Err(BlockReadError::Discovery(error));
                }
            }
            if status.changed().await.is_err() {
                return Err(crate::WorkerError::Remote(talon_transport::DataPlaneError {
                    code: talon_transport::DataErrorCode::Unavailable,
                    message: "background discovery stopped".into(),
                })
                .into());
            }
        }
    }
}

impl Drop for DiscoveryRefresh {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::worker_membership::InstanceState;
    use talon_core::{Backend, BlockId, NodeId, NodeInfo, NodeRole, ObjectId, Version};
    use talon_transport::{
        frame::{FrameHeader, HEADER_LEN},
        ControlMessage,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    async fn request(listener: &TcpListener) -> (TcpStream, u32) {
        tokio::time::timeout(Duration::from_secs(3), async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut header = [0; HEADER_LEN];
            socket.read_exact(&mut header).await.unwrap();
            let header = FrameHeader::decode(&header).unwrap();
            let mut body = vec![0; header.length as usize];
            socket.read_exact(&mut body).await.unwrap();
            let (_, message) =
                talon_transport::decode(&[header.encode().as_slice(), &body].concat()).unwrap();
            assert_eq!(message, ControlMessage::MembershipQuery {});
            (socket, header.request_id)
        })
        .await
        .unwrap()
    }

    fn view(state: InstanceState) -> ControlMessage {
        let ControlMessage::MembershipList { mut view } =
            crate::membership_fixture::plain(vec![NodeInfo {
                id: NodeId::new("a"),
                address: "old:1".into(),
                role: NodeRole::Worker,
            }])
        else {
            unreachable!()
        };
        view.valid_for_ms = 100;
        view.workers[0].state = state;
        ControlMessage::MembershipList { view }
    }

    async fn reply(mut socket: TcpStream, id: u32, message: ControlMessage) {
        socket
            .write_all(&talon_transport::encode(id, &message).unwrap())
            .await
            .unwrap();
    }

    async fn updated(status: &mut watch::Receiver<Status>) {
        tokio::time::timeout(Duration::from_secs(2), status.changed())
            .await
            .unwrap()
            .unwrap();
        status.borrow_and_update();
    }

    #[tokio::test]
    async fn idle_refresh_publishes_states_and_never_blocks_foreground_or_leaks_on_drop() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cache = Arc::new(MembershipCache::new(30_000));
        let refresh = Arc::new(DiscoveryRefresh::new(
            &CoordinatorClient::new(listener.local_addr().unwrap().to_string()),
            cache.clone(),
            Arc::new(ConnectionPool::new()),
            Arc::new(PlacementCache::new(30_000)),
        ));
        let clone = refresh.clone();
        let mut status = refresh.status.clone();
        // No foreground call has been made: construction starts discovery.
        let (socket, id) = request(&listener).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(10), refresh.snapshot())
                .await
                .is_err()
        );
        reply(
            socket,
            id,
            view(InstanceState::Serving {
                instance_id: "old".into(),
                address: "old:1".into(),
            }),
        )
        .await;
        updated(&mut status).await;
        let first = refresh.snapshot().await.unwrap();
        let block = BlockId::new(
            ObjectId::new(Backend::S3, "bucket", "key"),
            0,
            8,
            Version::new("v"),
        );
        assert_eq!(first.owner(&block).unwrap().addr(), "old:1");

        // The timer starts a second query with no foreground traffic. Stall it
        // beyond the advertised age: reads still only consult the published view.
        let (socket, id) = request(&listener).await;
        assert!(cache.fresh().is_none());
        tokio::time::timeout(Duration::from_millis(50), async {
            for _ in 0..100 {
                assert_eq!(
                    clone
                        .snapshot()
                        .await
                        .unwrap()
                        .owner(&block)
                        .unwrap()
                        .addr(),
                    "old:1"
                );
            }
        })
        .await
        .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
        reply(
            socket,
            id,
            ControlMessage::ControlFailure {
                code: talon_transport::DataErrorCode::Unavailable,
                message: "store down".into(),
            },
        )
        .await;
        updated(&mut status).await;
        assert_eq!(
            refresh
                .snapshot()
                .await
                .unwrap()
                .owner(&block)
                .unwrap()
                .addr(),
            "old:1"
        );

        // Only an authoritative observation marks an owner offline.
        let (socket, id) = request(&listener).await;
        reply(socket, id, view(InstanceState::Offline)).await;
        updated(&mut status).await;
        assert!(refresh.snapshot().await.unwrap().owner(&block).is_err());
        let (socket, id) = request(&listener).await;
        reply(
            socket,
            id,
            view(InstanceState::Serving {
                instance_id: "new".into(),
                address: "new:2".into(),
            }),
        )
        .await;
        updated(&mut status).await;
        let latest = refresh.snapshot().await.unwrap();
        assert!(Arc::ptr_eq(&first.placement, &latest.placement));
        assert_eq!(latest.owner(&block).unwrap().addr(), "new:2");

        // Last-clone drop cancels a blocked network request, without waiting for
        // the RPC timeout. Dropping another clone must not stop the service.
        let (mut socket, _) = request(&listener).await;
        drop(refresh);
        let mut byte = [0];
        assert!(
            tokio::time::timeout(Duration::from_millis(20), socket.read(&mut byte))
                .await
                .is_err()
        );
        drop(clone);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), socket.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    }

    #[test]
    fn construction_and_shutdown_do_not_require_a_caller_runtime() {
        let refresh = DiscoveryRefresh::new(
            &CoordinatorClient::new("127.0.0.1:1"),
            Arc::new(MembershipCache::new(30_000)),
            Arc::new(ConnectionPool::new()),
            Arc::new(PlacementCache::new(30_000)),
        );
        drop(refresh);
    }
}
