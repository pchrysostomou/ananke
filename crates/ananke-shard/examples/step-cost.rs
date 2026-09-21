//! A core step's cost (SHARD.md §12, Stage B's measurements): host time, release
//! build, outside the simulator, whose virtual time does not measure it.
//!
//! ```sh
//! cargo run --release -p ananke-shard --example step-cost -- [steps]
//! ```
//!
//! §4 leaves the tree with no measurement of a core step's cost. If a step costs `c`,
//! an idle tick costs 500 `c` at 1 000 ranges and 5 000 `c` at 10 000 (SHARD.md:504,
//! 512-517), so one `raft` task holding every core keeps its 10 ms ticker only while
//! `c` is under 20 µs at 1 000 ranges — a figure computed from those constants, not
//! measured. **The pass bound is an idle step below 20 µs.** A step at or above it
//! means one `raft` task per node, which Q41 fixes, cannot hold 1 000 ranges, and goes
//! to the owner before Stage C begins.
//!
//! Five shapes are measured, each on a correct core (`Variants::correct()`):
//!
//! - *idle tick*: a follower's `Tick` with a live leader. An idle step persists
//!   nothing (core.rs:1278-1298) and this is the one the pass bound is about.
//! - *leader tick*: a leader's `Tick`, which is a heartbeat on one phase of two. At
//!   1 000 ranges 300 of a node's 500 steps a tick are ticks (SHARD.md:504-506).
//!
//! The two tick shapes run on a core whose election timeout is set long, so that
//! *every* tick is the tick that does not time out and no check quorum falls due
//! inside the measurement. That is the idle tick: on a node with 300 replicas in a
//! steady cluster almost every tick of almost every replica is this one, and a tick
//! that times out is an election, which is a different thing and not what §4's 500
//! steps a tick are. Without it the follower times out after ten ticks and the
//! figure becomes an election storm's.
//! - *heartbeat in*: a follower stepping an `AppendEntries` with no entries — 10 000
//!   of a node's steps a second at 1 000 ranges.
//! - *response in*: a leader stepping an `AppendEntriesResponse` — another 10 000.
//! - *loaded*: the sweep's client load, a leader proposing 64-byte commands with its
//!   followers acknowledging, which is what `sim/raft.rs`'s clients drive.
//!
//! From the idle figure it then computes the replay burst after a slow persist
//! (SHARD.md §12): the ticks a core replays once its persist resolves, times the
//! cores held, times a step's cost, against the 10 ms tick at 1 000 ranges.
//!
//! The clock is [`ananke_env::RealEnv`]'s, the host's monotonic clock, as
//! `ananke-storage`'s bench reads it; nothing here touches `std::time` directly
//! (CLAUDE.md, D-013).

use std::collections::VecDeque;
use std::hint::black_box;

use ananke_env::{Clock, Environment, Instant, RealEnv};
use ananke_raft::core::{Raft, RaftConfig, Variants};
use ananke_raft::message::Message;
use ananke_raft::types::{Configuration, Entry, Index, Payload, ServerId};
use ananke_raft::{Input, Output};
use bytes::Bytes;

/// The pass bound, computed from §4's constants: 500 steps in a 10 ms tick.
const IDLE_BOUND_NANOS: u128 = 20_000;

/// What §4 counts in one tick of the one `raft` task at 1 000 ranges.
const STEPS_PER_TICK_AT_1000: u128 = 500;

/// The ticker the `raft` task keeps.
const TICK_NANOS: u128 = 10_000_000;

/// Replicas on a node at 1 000 ranges (SHARD.md:504).
const REPLICAS_AT_1000: u128 = 300;

const ME: ServerId = ServerId(1);
const PEER: ServerId = ServerId(2);
const THIRD: ServerId = ServerId(3);

fn config() -> RaftConfig {
    RaftConfig {
        variants: Variants::correct(),
        ..RaftConfig::default()
    }
}

/// A core whose timers do not fire inside a measurement: see the module
/// documentation.
fn steady() -> RaftConfig {
    RaftConfig {
        election_ticks: (1_000_000, 1_000_001),
        ..config()
    }
}

fn follower(config: RaftConfig) -> Raft {
    Raft::new(ME, Configuration::of(&[ME, PEER, THIRD]), config, 1)
}

/// A leader of the three: campaign, win both peers' pre-votes and votes, and be at
/// the head of its term.
fn leader(config: RaftConfig) -> Raft {
    let mut core = Raft::new(ME, Configuration::of(&[ME, PEER, THIRD]), config, 1);
    for _ in 0..2_000_002 {
        if core.role() == ananke_raft::Role::Leader {
            break;
        }
        let outputs = core.step(Input::Tick);
        answer(&mut core, outputs);
    }
    assert_eq!(
        core.role(),
        ananke_raft::Role::Leader,
        "the core takes office"
    );
    core
}

/// Answers every vote request among `outputs`, and among everything they lead to, in
/// the affirmative from both peers: a pre-vote's answers make the core a candidate,
/// whose own request has to be answered in its turn.
fn answer(core: &mut Raft, outputs: Vec<Output>) {
    let mut queue = VecDeque::from(outputs);
    while let Some(output) = queue.pop_front() {
        let Output::Send { message, .. } = output else {
            continue;
        };
        let reply = match message {
            Message::PreVote { term, .. } => Message::PreVoteResponse {
                term,
                granted: true,
            },
            Message::RequestVote { term, .. } => Message::RequestVoteResponse {
                term,
                granted: true,
            },
            _ => continue,
        };
        for from in [PEER, THIRD] {
            queue.extend(core.step(Input::Message {
                from,
                message: reply.clone(),
                now: 0,
            }));
        }
    }
}

fn heartbeat(term: u64, prev: Index, prev_term: u64) -> Message {
    Message::AppendEntries {
        term,
        prev_index: prev,
        prev_term,
        entries: Vec::new(),
        commit: prev,
        sent: 0,
    }
}

fn response(term: u64, prev: Index, matched: Index) -> Message {
    Message::AppendEntriesResponse {
        term,
        prev_index: prev,
        success: true,
        match_index: matched,
        hint: 0,
        echo: 0,
        local: 0,
        incarnation: 1,
    }
}

/// Runs `steps` steps of `next` and reports the nanoseconds each cost.
fn measure<F>(env: &RealEnv, steps: u64, mut next: F) -> u128
where
    F: FnMut(u64),
{
    // A warm-up run, so the figure is not the first allocation's.
    for i in 0..steps.min(1_000) {
        next(i);
    }
    let start = env.clock().now();
    for i in 0..steps {
        next(i);
    }
    let elapsed = env.clock().now();
    nanos(elapsed).saturating_sub(nanos(start)) / u128::from(steps.max(1))
}

fn nanos(at: Instant) -> u128 {
    u128::from(at.as_nanos())
}

fn report(name: &str, per_step: u128) {
    let tick = per_step * STEPS_PER_TICK_AT_1000;
    println!(
        "{name:<14} {per_step:>7} ns/step   500 steps = {:>8.3} ms of a 10 ms tick ({:.1}%)",
        tick as f64 / 1e6,
        tick as f64 * 100.0 / TICK_NANOS as f64,
    );
}

fn main() {
    let steps: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000_000);
    RealEnv::run(|env| async move {
        println!("a core step's cost, {steps} steps a shape, release, host time\n");

        // Idle: a follower with a live leader, ticking.
        let mut core = follower(steady());
        core.step(Input::Message {
            from: PEER,
            message: heartbeat(1, 0, 0),
            now: 0,
        });
        let idle = measure(&env, steps, |_| {
            black_box(core.step(Input::Tick));
        });

        // A leader's tick, which is a heartbeat on one phase of two.
        let mut lead = leader(steady());
        let leader_tick = measure(&env, steps, |_| {
            black_box(lead.step(Input::Tick));
        });

        // A heartbeat arriving at a follower, which re-arms its timer at every one:
        // the default election timeout is right here.
        let mut core = follower(config());
        core.step(Input::Message {
            from: PEER,
            message: heartbeat(1, 0, 0),
            now: 0,
        });
        let heartbeat_in = measure(&env, steps, |i| {
            black_box(core.step(Input::Message {
                from: PEER,
                message: heartbeat(1, 0, 0),
                now: i,
            }));
        });

        // A response arriving at a leader. No tick falls due inside the loop, so the
        // default configuration's check quorum never runs and the core stays leader.
        let mut lead = leader(config());
        let term = lead.term();
        let response_in = measure(&env, steps, |i| {
            black_box(lead.step(Input::Message {
                from: PEER,
                message: response(term, 0, 0),
                now: i,
            }));
        });

        // The sweep's client load: a leader proposing 64-byte commands, its followers
        // acknowledging each. Two steps a command, reported per step.
        let mut lead = leader(config());
        let term = lead.term();
        let command = Bytes::from(vec![b'c'; 64]);
        let mut index: Index = lead.last_index();
        let loaded = measure(&env, steps / 2, |_| {
            black_box(lead.step(Input::Propose(command.clone())));
            index += 1;
            for from in [PEER, THIRD] {
                black_box(lead.step(Input::Message {
                    from,
                    message: response(term, index - 1, index),
                    now: 0,
                }));
            }
        }) / 3;

        // The log the proposals grew, so the figure is not of an empty core.
        let entries = lead.log().len();

        report("idle tick", idle);
        report("leader tick", leader_tick);
        report("heartbeat in", heartbeat_in);
        report("response in", response_in);
        report("loaded", loaded);
        println!("\nthe leader's log at the end: {entries} entries");

        // The replay burst after a slow persist (SHARD.md §12): the ticks a core
        // replays once its persist resolves, times the cores held, times a step's
        // cost, against the 10 ms tick at 1 000 ranges. A core behind a sync of `s`
        // holds one tick per 10 ms of it, every one of them stepped (§4), and a node
        // at 1 000 ranges holds 300 replicas (SHARD.md:504).
        println!("\nthe replay burst at 1 000 ranges, 300 replicas, at {idle} ns a step:");
        println!("  sync      ticks replayed   burst        of a 10 ms tick");
        for sync_ms in [2u128, 20, 80, 200, 1_000] {
            let ticks = sync_ms * 1_000_000 / TICK_NANOS;
            let burst = ticks * REPLICAS_AT_1000 * idle;
            println!(
                "  {sync_ms:>5} ms {ticks:>10}    {:>8.3} ms   {:>6.2}%",
                burst as f64 / 1e6,
                burst as f64 * 100.0 / TICK_NANOS as f64,
            );
        }
        let breaks = TICK_NANOS / (REPLICAS_AT_1000 * idle.max(1));
        println!(
            "  the burst first fills a whole 10 ms tick at {breaks} ticks replayed, \
             a sync of {} ms",
            breaks * TICK_NANOS / 1_000_000,
        );
        println!(
            "\nthe pass bound (SHARD.md §12): an idle step below {} ns. \
             measured {idle} ns: {}",
            IDLE_BOUND_NANOS,
            if idle < IDLE_BOUND_NANOS {
                "PASS"
            } else {
                "FAIL — one `raft` task per node cannot hold 1 000 ranges; to the owner"
            }
        );
        // An entry that keeps the type from being unused when the shapes change.
        let _ = Entry {
            index: 0,
            term: 0,
            payload: Payload::Command(Bytes::new()),
        };
    });
}
