//! The in-memory network with the SPEC.md §1.4 fault model (D-015 semantics).
//!
//! `send` decides the message's fate immediately: dropped for a partition or by the
//! random drop probability, otherwise queued for delivery after a random delay in the
//! configured range. Different delays reorder messages. Partitions are checked again at
//! delivery, so a message in flight when a partition starts is lost.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use bytes::Bytes;

use super::state::{Shared, State};
use crate::{DropReason, Instant, MAX_FRAME_LEN, Network, NodeId, Socket, TraceEvent};

pub(super) struct SocketState {
    node: NodeId,
    id: u64,
    inbound: VecDeque<(SocketAddr, Bytes)>,
    wakers: Vec<Waker>,
}

pub(super) struct Delivery {
    from: SocketAddr,
    from_node: NodeId,
    to: SocketAddr,
    msg: Bytes,
}

/// Every socket, every message in flight, and every blocked link.
#[derive(Default)]
pub(super) struct Fabric {
    sockets: BTreeMap<SocketAddr, SocketState>,
    deliveries: BTreeMap<(Instant, u64), Delivery>,
    blocked: BTreeSet<(NodeId, NodeId)>,
    next_socket: u64,
    next_port: u16,
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

    pub(super) fn remove_node_sockets(&mut self, node: NodeId) {
        self.sockets.retain(|_, socket| socket.node != node);
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
        self.record(
            Some(node),
            TraceEvent::MessageSent {
                from,
                to,
                len: msg.len(),
            },
        );
        if let Some(to_node) = self.fabric.sockets.get(&to).map(|s| s.node)
            && self.fabric.is_blocked(node, to_node)
        {
            self.record(
                Some(node),
                TraceEvent::MessageDropped {
                    from,
                    to,
                    reason: DropReason::Partitioned,
                },
            );
            return Ok(());
        }
        let p_drop = self.config.net.p_drop;
        if self.rng.chance(p_drop) {
            self.record(
                Some(node),
                TraceEvent::MessageDropped {
                    from,
                    to,
                    reason: DropReason::Injected,
                },
            );
            return Ok(());
        }
        let (min, max) = (self.config.net.delay_min, self.config.net.delay_max);
        let delay = self.rng.duration_between(min, max);
        let at = self.now + delay;
        let seq = self.next_seq();
        self.fabric.deliveries.insert(
            (at, seq),
            Delivery {
                from,
                from_node: node,
                to,
                msg,
            },
        );
        Ok(())
    }

    pub(super) fn deliver(&mut self, delivery: Delivery, wakers: &mut Vec<Waker>) {
        let Delivery {
            from,
            from_node,
            to,
            msg,
        } = delivery;
        let Some(to_node) = self.fabric.sockets.get(&to).map(|s| s.node) else {
            self.record(
                Some(from_node),
                TraceEvent::MessageDropped {
                    from,
                    to,
                    reason: DropReason::Unreachable,
                },
            );
            return;
        };
        if self.fabric.is_blocked(from_node, to_node) {
            self.record(
                Some(from_node),
                TraceEvent::MessageDropped {
                    from,
                    to,
                    reason: DropReason::Partitioned,
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
            TraceEvent::MessageDelivered { from, to, len },
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
        }
    }
}

impl Socket for SimSocket {
    fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    fn send(&self, to: SocketAddr, msg: Bytes) -> impl Future<Output = io::Result<()>> + Send {
        std::future::ready(self.shared.lock().net_send(self.node, self.addr, to, msg))
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
