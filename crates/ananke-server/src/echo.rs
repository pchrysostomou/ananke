//! The Phase 0 echo protocol (SPEC.md §1.6). Every node pings a random peer at a fixed
//! interval and answers every ping with a pong carrying the same sequence number.
//!
//! [`node`] is generic over [`Environment`]: `sim/echo.rs` runs it under the simulator
//! with faults, and `ananke-server echo` runs it on `RealEnv` across real processes.
//! The [`Stats`] a node keeps are the protocol-level invariants both harnesses check.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::pin::pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use ananke_env::{Clock, Either, Environment, Network, Rng, Socket, race};
use bytes::Bytes;

/// One node's configuration.
#[derive(Clone, Debug)]
pub struct Echo {
    /// The address to bind; peers see it as the sender.
    pub listen: SocketAddr,
    /// Who to ping. With no peers the node only answers.
    pub peers: Vec<SocketAddr>,
    /// How often to ping.
    pub interval: Duration,
    /// Distinguishes sequence numbers across restarts, so a late pong for a
    /// pre-restart ping is still recognised rather than counted as unknown.
    pub incarnation: u32,
}

/// What one node observed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    /// Pings sent and not yet answered, as (peer, sequence number).
    outstanding: BTreeSet<(SocketAddr, u64)>,
    /// Pings sent.
    pub pings_sent: u64,
    /// Pongs that answered an outstanding ping.
    pub pongs_received: u64,
    /// Pongs that matched no outstanding ping: fabricated or duplicated.
    pub unknown_pongs: u64,
    /// Messages that did not parse.
    pub garbage: u64,
}

impl Stats {
    /// Pings still waiting for an answer.
    #[must_use]
    pub fn outstanding(&self) -> usize {
        self.outstanding.len()
    }

    /// The protocol-level invariants, or a description of the first violation.
    ///
    /// # Errors
    ///
    /// A message naming the violated invariant.
    pub fn check(&self) -> Result<(), String> {
        if self.unknown_pongs > 0 {
            return Err(format!(
                "{} pongs matched no outstanding ping (fabricated or duplicated)",
                self.unknown_pongs
            ));
        }
        if self.garbage > 0 {
            return Err(format!("{} messages failed to parse", self.garbage));
        }
        if self.pongs_received == 0 || self.pongs_received > self.pings_sent {
            return Err(format!(
                "{} pongs for {} pings",
                self.pongs_received, self.pings_sent
            ));
        }
        Ok(())
    }
}

/// [`Stats`] shared between a running node and the harness observing it.
pub type SharedStats = Arc<Mutex<Stats>>;

/// Locks shared stats, ignoring poisoning.
pub fn lock(stats: &SharedStats) -> MutexGuard<'_, Stats> {
    stats.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The wire format: `ping <seq>` and `pong <seq>` as text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Message {
    /// A request, carrying the sender's sequence number.
    Ping(u64),
    /// The answer, carrying the ping's sequence number.
    Pong(u64),
}

impl Message {
    /// Encodes for the wire.
    #[must_use]
    pub fn encode(self) -> Bytes {
        match self {
            Message::Ping(seq) => Bytes::from(format!("ping {seq}")),
            Message::Pong(seq) => Bytes::from(format!("pong {seq}")),
        }
    }

    /// Decodes from the wire; `None` for anything that is not a well-formed message.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Message> {
        let text = std::str::from_utf8(bytes).ok()?;
        let (kind, seq) = text.split_once(' ')?;
        let seq = seq.parse().ok()?;
        match kind {
            "ping" => Some(Message::Ping(seq)),
            "pong" => Some(Message::Pong(seq)),
            _ => None,
        }
    }
}

/// Runs one echo node until its socket closes or the task is aborted.
pub async fn node<E: Environment>(env: E, echo: Echo, stats: SharedStats) {
    let Ok(sock) = env.net().bind(echo.listen).await else {
        return;
    };
    let mut seq = u64::from(echo.incarnation) << 32;
    let mut next_ping = env.clock().now();
    loop {
        let recv = pin!(sock.recv());
        let timer = pin!(env.clock().sleep_until(next_ping));
        match race(&env, recv, timer).await {
            Either::Left(Err(_)) => return,
            Either::Left(Ok((from, msg))) => match Message::decode(&msg) {
                Some(Message::Ping(n)) => {
                    let _ = sock.send(from, Message::Pong(n).encode()).await;
                }
                Some(Message::Pong(n)) => {
                    let mut stats = lock(&stats);
                    if stats.outstanding.remove(&(from, n)) {
                        stats.pongs_received += 1;
                    } else {
                        stats.unknown_pongs += 1;
                    }
                }
                None => lock(&stats).garbage += 1,
            },
            Either::Right(()) => {
                next_ping = env.clock().now() + echo.interval;
                if echo.peers.is_empty() {
                    continue;
                }
                // The peer comes from this node's own seeded stream.
                let index = usize::try_from(env.rng().below(echo.peers.len() as u64))
                    .expect("index fits usize");
                let peer = echo.peers[index];
                seq += 1;
                {
                    let mut stats = lock(&stats);
                    stats.outstanding.insert((peer, seq));
                    stats.pings_sent += 1;
                }
                let _ = sock.send(peer, Message::Ping(seq).encode()).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_round_trip_and_reject_garbage() {
        for message in [
            Message::Ping(0),
            Message::Pong(u64::MAX),
            Message::Ping(1 << 32 | 7),
        ] {
            assert_eq!(Message::decode(&message.encode()), Some(message));
        }
        for garbage in ["", "ping", "ping x", "pang 1", "ping 1 2"] {
            assert_eq!(Message::decode(garbage.as_bytes()), None, "{garbage:?}");
        }
    }

    #[test]
    fn stats_check_catches_each_violation() {
        let mut stats = Stats {
            pings_sent: 10,
            pongs_received: 5,
            ..Stats::default()
        };
        assert_eq!(stats.check(), Ok(()));
        stats.unknown_pongs = 1;
        assert!(
            stats
                .check()
                .unwrap_err()
                .contains("fabricated or duplicated")
        );
        stats.unknown_pongs = 0;
        stats.garbage = 1;
        assert!(stats.check().unwrap_err().contains("failed to parse"));
        stats.garbage = 0;
        stats.pongs_received = 0;
        assert!(stats.check().unwrap_err().contains("0 pongs"));
        stats.pongs_received = 11;
        assert!(stats.check().unwrap_err().contains("11 pongs for 10 pings"));
    }
}
