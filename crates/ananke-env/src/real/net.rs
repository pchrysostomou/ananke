//! TCP transport for [`RealNet`] (DECISIONS.md D-015).
//!
//! One outbound TCP connection per destination, created lazily by a background task
//! owned by the socket. The first frame on a connection is a hello carrying the
//! sender's bound address; every frame is a big-endian `u32` length followed by the
//! payload. Nothing is retransmitted: a frame handed to a connection that then breaks is
//! lost. This path is invisible to the simulator, so it is kept deliberately small.

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Handle;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;

use super::emit;
use crate::{DropReason, MAX_FRAME_LEN, Network, Socket, TraceEvent};

/// Frames queued per destination before the oldest is dropped.
pub const SEND_QUEUE_LEN: usize = 1024;
/// Received messages buffered per socket before readers apply TCP backpressure.
const INBOUND_QUEUE_LEN: usize = 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RECONNECT_MIN: Duration = Duration::from_millis(50);
const RECONNECT_MAX: Duration = Duration::from_secs(2);

/// Real sockets over TCP.
#[derive(Clone, Debug)]
pub struct RealNet {
    handle: Handle,
}

impl RealNet {
    pub(super) fn new(handle: Handle) -> Self {
        Self { handle }
    }
}

impl Network for RealNet {
    type Socket = RealSocket;

    fn bind(&self, addr: SocketAddr) -> impl Future<Output = io::Result<RealSocket>> + Send {
        let handle = self.handle.clone();
        async move {
            let listener = TcpListener::bind(addr).await?;
            let local = listener.local_addr()?;
            let (inbound_tx, inbound_rx) = mpsc::channel(INBOUND_QUEUE_LEN);
            let accept = handle.spawn(accept_loop(listener, local, inbound_tx, handle.clone()));
            Ok(RealSocket {
                inner: Arc::new(SocketInner {
                    handle,
                    local,
                    inbound: tokio::sync::Mutex::new(inbound_rx),
                    peers: Mutex::new(BTreeMap::new()),
                    accept,
                }),
            })
        }
    }
}

/// A bound TCP-backed socket. Dropping it closes the listener and stops every
/// connection task it owns.
#[derive(Clone, Debug)]
pub struct RealSocket {
    inner: Arc<SocketInner>,
}

struct SocketInner {
    handle: Handle,
    local: SocketAddr,
    inbound: tokio::sync::Mutex<mpsc::Receiver<(SocketAddr, Bytes)>>,
    peers: Mutex<BTreeMap<SocketAddr, Peer>>,
    accept: JoinHandle<()>,
}

impl std::fmt::Debug for SocketInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SocketInner")
            .field("local", &self.local)
            .finish_non_exhaustive()
    }
}

impl Drop for SocketInner {
    fn drop(&mut self) {
        self.accept.abort();
        for peer in self
            .peers
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
        {
            peer.task.abort();
        }
    }
}

struct Peer {
    queue: Arc<SendQueue>,
    task: JoinHandle<()>,
}

/// The bounded per-destination queue: drop-oldest on overflow (D-015).
#[derive(Default)]
struct SendQueue {
    frames: Mutex<VecDeque<Bytes>>,
    notify: Notify,
}

impl SendQueue {
    /// Appends `frame`; returns `true` if the oldest frame was dropped to make room.
    fn push(&self, frame: Bytes) -> bool {
        let mut frames = self.frames.lock().unwrap_or_else(PoisonError::into_inner);
        let dropped = frames.len() >= SEND_QUEUE_LEN && frames.pop_front().is_some();
        frames.push_back(frame);
        drop(frames);
        self.notify.notify_one();
        dropped
    }

    async fn pop(&self) -> Bytes {
        loop {
            let next = self
                .frames
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .pop_front();
            match next {
                Some(frame) => return frame,
                None => self.notify.notified().await,
            }
        }
    }
}

impl Socket for RealSocket {
    fn local_addr(&self) -> SocketAddr {
        self.inner.local
    }

    fn send(&self, to: SocketAddr, msg: Bytes) -> impl Future<Output = io::Result<()>> + Send {
        std::future::ready(self.inner.enqueue(to, msg))
    }

    // `async fn` here still satisfies the trait's `impl Future + Send`; the compiler
    // checks the future is `Send`.
    async fn recv(&self) -> io::Result<(SocketAddr, Bytes)> {
        let mut inbound = self.inner.inbound.lock().await;
        inbound
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "socket closed"))
    }
}

impl SocketInner {
    fn enqueue(&self, to: SocketAddr, msg: Bytes) -> io::Result<()> {
        if msg.len() > MAX_FRAME_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "message exceeds MAX_FRAME_LEN",
            ));
        }
        let len = msg.len();
        let queue = {
            let mut peers = self.peers.lock().unwrap_or_else(PoisonError::into_inner);
            let peer = peers.entry(to).or_insert_with(|| {
                let queue = Arc::new(SendQueue::default());
                let task = self.handle.spawn(peer_loop(self.local, to, queue.clone()));
                Peer { queue, task }
            });
            peer.queue.clone()
        };
        let dropped = queue.push(msg);
        emit(TraceEvent::MessageSent {
            from: self.local,
            to,
            len,
        });
        if dropped {
            emit(TraceEvent::MessageDropped {
                from: self.local,
                to,
                reason: DropReason::QueueFull,
            });
        }
        Ok(())
    }
}

/// Drains one destination's queue over a connection that is rebuilt whenever it breaks.
async fn peer_loop(local: SocketAddr, peer: SocketAddr, queue: Arc<SendQueue>) {
    let mut backoff = RECONNECT_MIN;
    loop {
        let mut stream = match connect(peer, local).await {
            Ok(stream) => stream,
            Err(_) => {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(RECONNECT_MAX);
                continue;
            }
        };
        backoff = RECONNECT_MIN;
        loop {
            let frame = queue.pop().await;
            // A failed write loses this frame; nothing is retried (D-015).
            if write_frame(&mut stream, &frame).await.is_err() {
                break;
            }
        }
    }
}

async fn connect(peer: SocketAddr, local: SocketAddr) -> io::Result<TcpStream> {
    let mut stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(peer))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connect timed out"))??;
    stream.set_nodelay(true)?;
    write_frame(&mut stream, local.to_string().as_bytes()).await?;
    Ok(stream)
}

/// Accepts inbound connections and starts a reader for each. Readers stop on their own
/// when the socket is dropped, because the inbound receiver goes away.
async fn accept_loop(
    listener: TcpListener,
    local: SocketAddr,
    inbound: mpsc::Sender<(SocketAddr, Bytes)>,
    handle: Handle,
) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                handle.spawn(reader_loop(stream, local, inbound.clone()));
            }
            // Transient failures such as running out of descriptors: back off briefly.
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
}

async fn reader_loop(
    mut stream: TcpStream,
    local: SocketAddr,
    inbound: mpsc::Sender<(SocketAddr, Bytes)>,
) {
    let hello = tokio::select! {
        frame = read_frame(&mut stream) => frame,
        () = inbound.closed() => return,
    };
    let from = hello
        .ok()
        .and_then(|h| std::str::from_utf8(&h).ok()?.parse::<SocketAddr>().ok());
    let Some(from) = from else { return };
    loop {
        let frame = tokio::select! {
            frame = read_frame(&mut stream) => match frame {
                Ok(frame) => frame,
                Err(_) => return,
            },
            () = inbound.closed() => return,
        };
        let len = frame.len();
        if inbound.send((from, frame)).await.is_err() {
            return;
        }
        emit(TraceEvent::MessageDelivered {
            from,
            to: local,
            len,
        });
    }
}

async fn read_frame(stream: &mut TcpStream) -> io::Result<Bytes> {
    let len = stream.read_u32().await? as usize;
    if len > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame exceeds MAX_FRAME_LEN",
        ));
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(Bytes::from(buf))
}

async fn write_frame(stream: &mut TcpStream, frame: &[u8]) -> io::Result<()> {
    let len = u32::try_from(frame.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame too long"))?;
    stream.write_u32(len).await?;
    stream.write_all(frame).await
}
