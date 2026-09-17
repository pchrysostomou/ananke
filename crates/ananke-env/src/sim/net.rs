//! The in-memory network with the SPEC.md §1.4 fault model (D-015 semantics).
//!
//! `send` decides the message's fate immediately: dropped for a partition or by the
//! random drop probability, otherwise queued for delivery after a random delay in the
//! configured range. Different delays reorder messages. Partitions are checked again at
//! delivery, so a message in flight when a partition starts is lost; so are frame-length
//! limits, the path-MTU black hole a scenario can put on one direction of a link (D-049).
//!
//! Every frame that survives the send's checks joins its sending socket's queue to its
//! destination, as `RealEnv`'s does (D-015): one frame at a time is written, at the
//! link's drain rate, and the rest wait behind it. A frame's delay starts once its last
//! byte is written. When as many frames wait as the queue holds, the oldest waiting
//! frame is dropped, `MessageDropped` with [`DropReason::QueueFull`], and the frames
//! behind it move up (PROPOSED D-056).

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use bytes::Bytes;

use super::NetFaults;
use super::state::{Shared, State};
use crate::{DropReason, Instant, MAX_FRAME_LEN, MessageId, Network, NodeId, Socket, TraceEvent};

pub(super) struct SocketState {
    node: NodeId,
    id: u64,
    inbound: VecDeque<(SocketAddr, Bytes)>,
    wakers: Vec<Waker>,
}

pub(super) struct Delivery {
    id: MessageId,
    from: SocketAddr,
    from_node: NodeId,
    to: SocketAddr,
    msg: Bytes,
    /// A second delivery of a message the fault model duplicated.
    dup: bool,
}

/// One frame in a sending socket's queue to one destination: the one being written,
/// or one waiting behind it.
// PROPOSED(D-056): SimEnv's bounded, drop-oldest queue per (sending socket,
// destination), drained at a modelled per-link rate.
struct Queued {
    id: MessageId,
    len: usize,
    /// When `send` accepted it.
    enqueued: Instant,
    /// When its last byte is written: its delays start here.
    written: Instant,
    /// Each delivery the frame makes, the original and a duplicate if one was drawn:
    /// the delay drawn at the send and the delivery's tie-breaker, which together with
    /// `written` are its key in [`Fabric::deliveries`].
    deliveries: Vec<(Duration, u64)>,
}

/// A sending socket's queue to one destination, oldest first. Every frame in it has
/// its last byte written after now: the first is being written and the rest wait
/// (D-015).
// PROPOSED(D-056): SimEnv's bounded, drop-oldest queue per (sending socket,
// destination), drained at a modelled per-link rate.
#[derive(Default)]
struct SendQueue {
    frames: VecDeque<Queued>,
}

/// How long writing `len` bytes takes at `bytes_per_sec`, rounded up to the
/// nanosecond, so that every byte takes time.
// PROPOSED(D-056): SimEnv's bounded, drop-oldest queue per (sending socket,
// destination), drained at a modelled per-link rate.
fn write_time(len: usize, bytes_per_sec: u64) -> Duration {
    let nanos = (len as u128 * 1_000_000_000).div_ceil(u128::from(bytes_per_sec));
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

/// Every socket, every message in flight, and every blocked link.
#[derive(Default)]
pub(super) struct Fabric {
    sockets: BTreeMap<SocketAddr, SocketState>,
    deliveries: BTreeMap<(Instant, u64), Delivery>,
    /// Each sending socket's queue to each destination it has sent to, by socket id.
    // PROPOSED(D-056): SimEnv's bounded, drop-oldest queue per (sending socket,
    // destination), drained at a modelled per-link rate.
    queues: BTreeMap<(u64, SocketAddr), SendQueue>,
    blocked: BTreeSet<(NodeId, NodeId)>,
    /// The symmetric partition in force, if any, as recorded in the trace.
    pub(super) active_partition: Option<Vec<Vec<NodeId>>>,
    /// Link directions blocked individually, as recorded in the trace.
    pub(super) links: BTreeSet<(NodeId, NodeId)>,
    /// Link directions that lose every frame longer than a bound, a path-MTU black
    /// hole, until the next heal (D-049).
    pub(super) limited: BTreeMap<(NodeId, NodeId), usize>,
    /// Every address ever bound and the node that bound it, for the moirae export.
    pub(super) known: BTreeMap<SocketAddr, NodeId>,
    next_socket: u64,
    next_port: u16,
    next_message: u64,
}

impl Fabric {
    pub(super) fn next_delivery_time(&self) -> Option<Instant> {
        self.deliveries.keys().next().map(|(at, _)| *at)
    }

    pub(super) fn take_due(&mut self, now: Instant) -> Vec<Delivery> {
        let due: Vec<(Instant, u64)> = self
            .deliveries
            .range(..=(now, u64::MAX))
            .map(|(k, _)| *k)
            .collect();
        due.into_iter()
            .filter_map(|key| self.deliveries.remove(&key))
            .collect()
    }

    pub(super) fn is_blocked(&self, from: NodeId, to: NodeId) -> bool {
        self.blocked.contains(&(from, to))
    }

    pub(super) fn block(&mut self, from: NodeId, to: NodeId) {
        self.blocked.insert((from, to));
    }

    pub(super) fn heal(&mut self) {
        self.blocked.clear();
    }

    /// Whether a frame of `len` bytes from `from` to `to` is longer than the limit
    /// on that direction, if it carries one (D-049).
    pub(super) fn is_oversized(&self, from: NodeId, to: NodeId, len: usize) -> bool {
        self.limited.get(&(from, to)).is_some_and(|&max| len > max)
    }

    pub(super) fn remove_node_sockets(&mut self, node: NodeId) {
        let gone: BTreeSet<u64> = self
            .sockets
            .values()
            .filter(|socket| socket.node == node)
            .map(|socket| socket.id)
            .collect();
        self.sockets.retain(|_, socket| socket.node != node);
        self.forget_queues(&gone);
    }

    /// Forgets the queues of sockets that are gone. Their frames already in the
    /// queue keep the deliveries they were given, as a frame in flight always has;
    /// no send can add to a queue whose socket is gone, so nothing is dropped from it.
    // PROPOSED(D-056): SimEnv's bounded, drop-oldest queue per (sending socket,
    // destination), drained at a modelled per-link rate.
    fn forget_queues(&mut self, sockets: &BTreeSet<u64>) {
        if !sockets.is_empty() {
            self.queues
                .retain(|(socket, _), _| !sockets.contains(socket));
        }
    }

    /// Admits frame `id` of `len` bytes to `socket`'s queue to `to` at `now`. Frames
    /// already written leave the queue first; then, if as many frames as `net`'s
    /// [`send_queue_len`](NetFaults::send_queue_len) wait behind the one being written,
    /// the oldest waiting one is dropped, its deliveries cancelled and the frames behind
    /// it moved up. Returns the id of the frame dropped to make room, if one was.
    // PROPOSED(D-056): SimEnv's bounded, drop-oldest queue per (sending socket,
    // destination), drained at a modelled per-link rate.
    fn admit(
        &mut self,
        socket: u64,
        to: SocketAddr,
        (id, len): (MessageId, usize),
        now: Instant,
        net: &NetFaults,
    ) -> Option<MessageId> {
        let (capacity, bytes_per_sec) = (net.send_queue_len, net.link_bytes_per_sec);
        let Self {
            queues, deliveries, ..
        } = self;
        let queue = queues.entry((socket, to)).or_default();
        while queue.frames.front().is_some_and(|f| f.written <= now) {
            queue.frames.pop_front();
        }
        // Every frame left is unwritten, so the first is being written (it started
        // no later than now) and the rest wait.
        let mut dropped = None;
        if queue.frames.len().saturating_sub(1) >= capacity {
            let oldest = queue.frames.remove(1).expect("a waiting frame");
            for (delay, seq) in &oldest.deliveries {
                deliveries.remove(&(oldest.written + *delay, *seq));
            }
            dropped = Some(oldest.id);
            for i in 1..queue.frames.len() {
                let ahead = queue.frames[i - 1].written;
                let frame = &mut queue.frames[i];
                let written = frame.enqueued.max(ahead) + write_time(frame.len, bytes_per_sec);
                if written != frame.written {
                    for (delay, seq) in &frame.deliveries {
                        if let Some(delivery) = deliveries.remove(&(frame.written + *delay, *seq)) {
                            deliveries.insert((written + *delay, *seq), delivery);
                        }
                    }
                    frame.written = written;
                }
            }
        }
        let start = queue
            .frames
            .back()
            .map_or(now, |last| last.written.max(now));
        queue.frames.push_back(Queued {
            id,
            len,
            enqueued: now,
            written: start + write_time(len, bytes_per_sec),
            deliveries: Vec::new(),
        });
        dropped
    }

    /// Records a delivery of the newest frame in `socket`'s queue to `to`.
    // PROPOSED(D-056): SimEnv's bounded, drop-oldest queue per (sending socket,
    // destination), drained at a modelled per-link rate.
    fn schedule(&mut self, socket: u64, delay: Duration, seq: u64, delivery: Delivery) {
        let to = delivery.to;
        let frame = self
            .queues
            .get_mut(&(socket, to))
            .and_then(|queue| queue.frames.back_mut())
            .expect("the frame was just admitted");
        frame.deliveries.push((delay, seq));
        self.deliveries
            .insert((frame.written + delay, seq), delivery);
    }
}

impl State {
    fn net_bind(&mut self, node: NodeId, mut addr: SocketAddr) -> io::Result<(SocketAddr, u64)> {
        if addr.port() == 0 {
            loop {
                self.fabric.next_port = self
                    .fabric
                    .next_port
                    .checked_add(1)
                    .unwrap_or(10_000)
                    .max(10_000);
                addr.set_port(self.fabric.next_port);
                if !self.fabric.sockets.contains_key(&addr) {
                    break;
                }
            }
        }
        if self.fabric.sockets.contains_key(&addr) {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                "address already bound",
            ));
        }
        let id = self.fabric.next_socket;
        self.fabric.next_socket += 1;
        self.fabric.known.insert(addr, node);
        self.fabric.sockets.insert(
            addr,
            SocketState {
                node,
                id,
                inbound: VecDeque::new(),
                wakers: Vec::new(),
            },
        );
        Ok((addr, id))
    }

    fn net_send(
        &mut self,
        node: NodeId,
        socket: u64,
        from: SocketAddr,
        to: SocketAddr,
        msg: Bytes,
    ) -> io::Result<()> {
        if msg.len() > MAX_FRAME_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "message exceeds MAX_FRAME_LEN",
            ));
        }
        let id = MessageId::new(self.fabric.next_message);
        self.fabric.next_message += 1;
        self.record(
            Some(node),
            TraceEvent::MessageSent {
                id,
                from,
                to,
                payload: msg.clone(),
            },
        );
        if let Some(to_node) = self.fabric.sockets.get(&to).map(|s| s.node) {
            let reason = if self.fabric.is_blocked(node, to_node) {
                Some(DropReason::Partitioned)
            } else if self.fabric.is_oversized(node, to_node, msg.len()) {
                Some(DropReason::Oversized)
            } else {
                None
            };
            if let Some(reason) = reason {
                self.record(
                    Some(node),
                    TraceEvent::MessageDropped {
                        id,
                        from,
                        to,
                        reason,
                    },
                );
                return Ok(());
            }
        }
        let p_drop = self.config.net.p_drop;
        if self.net_stream.chance(p_drop) {
            self.record(
                Some(node),
                TraceEvent::MessageDropped {
                    id,
                    from,
                    to,
                    reason: DropReason::Injected,
                },
            );
            return Ok(());
        }
        let (min, max) = (self.config.net.delay_min, self.config.net.delay_max);
        let delay = super::rng::duration_between(&mut self.net_stream, min, max);
        // PROPOSED(D-056): the frame joins its socket's queue to `to`; the draws above
        // and below are the ones a send made before the queue, in the same order.
        let now = self.now;
        let dropped = self
            .fabric
            .admit(socket, to, (id, msg.len()), now, &self.config.net);
        if let Some(oldest) = dropped {
            self.record(
                Some(node),
                TraceEvent::MessageDropped {
                    id: oldest,
                    from,
                    to,
                    reason: DropReason::QueueFull,
                },
            );
        }
        let seq = self.next_seq();
        self.fabric.schedule(
            socket,
            delay,
            seq,
            Delivery {
                id,
                from,
                from_node: node,
                to,
                msg: msg.clone(),
                dup: false,
            },
        );
        // A duplicate is a second delivery with a delay of its own, so the copy can
        // overtake the original; the draws come from the network stream only when
        // duplication is on, so that a zero changes no trace.
        let p_duplicate = self.config.net.p_duplicate;
        if p_duplicate > 0.0 && self.net_stream.chance(p_duplicate) {
            let delay = super::rng::duration_between(&mut self.net_stream, min, max);
            let seq = self.next_seq();
            self.fabric.schedule(
                socket,
                delay,
                seq,
                Delivery {
                    id,
                    from,
                    from_node: node,
                    to,
                    msg,
                    dup: true,
                },
            );
        }
        Ok(())
    }

    pub(super) fn deliver(&mut self, delivery: Delivery, wakers: &mut Vec<Waker>) {
        let Delivery {
            id,
            from,
            from_node,
            to,
            msg,
            dup,
        } = delivery;
        let Some(to_node) = self.fabric.sockets.get(&to).map(|s| s.node) else {
            self.record(
                Some(from_node),
                TraceEvent::MessageDropped {
                    id,
                    from,
                    to,
                    reason: DropReason::Unreachable,
                },
            );
            return;
        };
        let reason = if self.fabric.is_blocked(from_node, to_node) {
            Some(DropReason::Partitioned)
        } else if self.fabric.is_oversized(from_node, to_node, msg.len()) {
            Some(DropReason::Oversized)
        } else {
            None
        };
        if let Some(reason) = reason {
            self.record(
                Some(from_node),
                TraceEvent::MessageDropped {
                    id,
                    from,
                    to,
                    reason,
                },
            );
            return;
        }
        let len = msg.len();
        if let Some(socket) = self.fabric.sockets.get_mut(&to) {
            socket.inbound.push_back((from, msg));
            wakers.append(&mut socket.wakers);
        }
        self.record(
            Some(to_node),
            TraceEvent::MessageDelivered {
                id,
                from,
                to,
                len,
                dup,
            },
        );
    }
}

/// One node's view of the simulated network.
#[derive(Clone)]
pub struct SimNet {
    pub(super) shared: Arc<Shared>,
    pub(super) node: NodeId,
}

impl std::fmt::Debug for SimNet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimNet")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}

impl Network for SimNet {
    type Socket = SimSocket;

    fn bind(&self, addr: SocketAddr) -> impl Future<Output = io::Result<SimSocket>> + Send {
        let result = self
            .shared
            .lock()
            .net_bind(self.node, addr)
            .map(|(addr, id)| SimSocket {
                shared: self.shared.clone(),
                node: self.node,
                addr,
                id,
            });
        std::future::ready(result)
    }
}

/// A bound simulated socket. Dropping it unbinds the address.
pub struct SimSocket {
    shared: Arc<Shared>,
    node: NodeId,
    addr: SocketAddr,
    id: u64,
}

impl std::fmt::Debug for SimSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimSocket")
            .field("node", &self.node)
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

impl Drop for SimSocket {
    fn drop(&mut self) {
        let mut st = self.shared.lock();
        if st
            .fabric
            .sockets
            .get(&self.addr)
            .is_some_and(|s| s.id == self.id)
        {
            st.fabric.sockets.remove(&self.addr);
            st.fabric.forget_queues(&BTreeSet::from([self.id]));
        }
    }
}

impl Socket for SimSocket {
    fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    fn send(&self, to: SocketAddr, msg: Bytes) -> impl Future<Output = io::Result<()>> + Send {
        std::future::ready(
            self.shared
                .lock()
                .net_send(self.node, self.id, self.addr, to, msg),
        )
    }

    fn recv(&self) -> impl Future<Output = io::Result<(SocketAddr, Bytes)>> + Send {
        SimRecv {
            shared: self.shared.clone(),
            addr: self.addr,
            id: self.id,
        }
    }
}

/// A pending receive on a simulated socket.
pub struct SimRecv {
    shared: Arc<Shared>,
    addr: SocketAddr,
    id: u64,
}

impl Future for SimRecv {
    type Output = io::Result<(SocketAddr, Bytes)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut st = self.shared.lock();
        match st.fabric.sockets.get_mut(&self.addr) {
            Some(socket) if socket.id == self.id => match socket.inbound.pop_front() {
                Some(message) => Poll::Ready(Ok(message)),
                None => {
                    if !socket.wakers.iter().any(|w| w.will_wake(cx.waker())) {
                        socket.wakers.push(cx.waker().clone());
                    }
                    Poll::Pending
                }
            },
            _ => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "socket closed",
            ))),
        }
    }
}
