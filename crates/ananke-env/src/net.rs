//! The [`Network`] and [`Socket`] traits (SPEC.md §1.1, §1.4; DECISIONS.md D-015).
//!
//! Node-to-node transport is message-oriented. A [`Socket`] bound to an address sends
//! and receives datagrams of at most [`MAX_FRAME_LEN`] bytes: unreliable, unordered,
//! at-most-once. Reliability is the protocol's job.
//!
//! Sending never blocks. [`Socket::send`] enqueues and returns; it never awaits a
//! connect, a slow peer or a full buffer, so a dead or slow peer can never stall the
//! caller. That is a Raft liveness requirement, not an implementation detail. Each
//! destination has a bounded queue and on overflow the oldest frame is dropped with a
//! [`TraceEvent::MessageDropped`](crate::TraceEvent::MessageDropped) event.
//!
//! Peers are identified by `SocketAddr`. That is a Phase 0–5 simplification: under mTLS
//! in Phase 6 the authenticated identity comes from the certificate, and `recv` will
//! return a peer handle rather than a bare address.

use std::future::Future;
use std::io;
use std::net::SocketAddr;

use bytes::Bytes;

/// The largest frame a socket will send or accept, in bytes. Phase 2 snapshot chunking
/// reads this; it is the one place the cap is defined.
pub const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;

/// Creates sockets.
pub trait Network: Send + Sync + 'static {
    /// The socket type.
    type Socket: Socket;

    /// Binds a socket to `addr`. Port 0 asks for any free port; the address actually
    /// bound is [`Socket::local_addr`].
    fn bind(&self, addr: SocketAddr) -> impl Future<Output = io::Result<Self::Socket>> + Send;
}

/// A bound endpoint that sends and receives datagrams.
pub trait Socket: Send + Sync + 'static {
    /// The address this socket is bound to; what receivers see as `from`.
    fn local_addr(&self) -> SocketAddr;

    /// Enqueues `msg` for delivery to `to` and returns at once.
    ///
    /// Never waits on the network. Fails only if `msg` is longer than
    /// [`MAX_FRAME_LEN`]; a message that is later lost was still sent successfully.
    fn send(&self, to: SocketAddr, msg: Bytes) -> impl Future<Output = io::Result<()>> + Send;

    /// The next received message, with the bound address of the socket that sent it.
    fn recv(&self) -> impl Future<Output = io::Result<(SocketAddr, Bytes)>> + Send;
}
