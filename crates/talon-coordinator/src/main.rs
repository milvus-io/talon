//! Talon coordinator entry point.
//!
//! Runs the real control-plane serve loop: workers `Register` and `Heartbeat`,
//! clients issue `PlacementLookup` and `MembershipQuery`. Each accepted
//! connection carries one framed [`ControlMessage`] request and receives one
//! framed reply (see [`talon_transport::codec`]).

use std::sync::Arc;

use clap::Parser;
use talon_coordinator::{Membership, PlacementService, RendezvousPlacement};
use talon_transport::frame::HEADER_LEN;
use talon_transport::{codec, ControlMessage, FrameHeader};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Command-line arguments for the Talon coordinator.
#[derive(Debug, Parser)]
#[command(name = "talon-coordinator", version, about)]
struct Args {
    /// Address to bind the coordinator RPC service to.
    #[arg(long, default_value = "127.0.0.1:7000")]
    listen: String,
}

/// Shared coordinator state behind the serve loop.
struct Coordinator {
    service: PlacementService<RendezvousPlacement>,
}

impl Coordinator {
    fn new() -> Arc<Self> {
        let service = PlacementService::new(Membership::new(), RendezvousPlacement);
        Arc::new(Self { service })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    tracing::info!(listen = %args.listen, "starting talon-coordinator");

    let state = Coordinator::new();

    let listener = TcpListener::bind(&args.listen).await?;
    loop {
        let (stream, peer) = listener.accept().await?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(e) = handle_conn(stream, state).await {
                tracing::debug!(%peer, error = %e, "coordinator: connection ended");
            }
        });
    }
}

/// Read one framed control request, dispatch it, and write one framed reply.
async fn handle_conn(mut stream: TcpStream, state: Arc<Coordinator>) -> anyhow::Result<()> {
    loop {
        let Some((_header, msg)) = read_control(&mut stream).await? else {
            return Ok(()); // clean EOF
        };
        let reply = state.dispatch(msg);
        let buf = codec::encode(0, &reply)?;
        stream.write_all(&buf).await?;
        stream.flush().await?;
    }
}

impl Coordinator {
    /// Handle one decoded control message, returning the reply message.
    fn dispatch(&self, msg: ControlMessage) -> ControlMessage {
        match msg {
            ControlMessage::Register { node } => {
                tracing::info!(id = %node.id, address = %node.address, "worker registered");
                self.service.membership().register(node);
                ControlMessage::Ack {
                    ok: true,
                    detail: None,
                }
            }
            ControlMessage::Heartbeat { node, block_count } => {
                tracing::debug!(%node, block_count, "heartbeat");
                ControlMessage::Ack {
                    ok: true,
                    detail: None,
                }
            }
            lookup @ ControlMessage::PlacementLookup { .. } => self.service.handle(lookup),
            ControlMessage::MembershipQuery {} => ControlMessage::MembershipList {
                nodes: self.service.membership().snapshot(),
            },
            other => ControlMessage::Ack {
                ok: false,
                detail: Some(format!("unexpected control message: {other:?}")),
            },
        }
    }
}

/// Read a full framed control message (header + payload). `Ok(None)` on clean
/// EOF before any bytes of a new frame.
async fn read_control(
    stream: &mut TcpStream,
) -> anyhow::Result<Option<(FrameHeader, ControlMessage)>> {
    let mut header_buf = [0u8; HEADER_LEN];
    match stream.read_exact(&mut header_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let header = FrameHeader::decode(&header_buf)?;
    let mut payload = vec![0u8; header.length as usize];
    stream.read_exact(&mut payload).await?;
    let mut full = Vec::with_capacity(HEADER_LEN + payload.len());
    full.extend_from_slice(&header_buf);
    full.extend_from_slice(&payload);
    let (h, msg) = codec::decode(&full)?;
    Ok(Some((h, msg)))
}
