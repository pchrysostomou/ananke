# Phase 0: a runtime that can be replayed

_Draft. September 2026._

## What ananke is

ananke is a distributed SQL database written in Rust. That sentence is the least
interesting thing about it. The interesting thing is the constraint I put on it before
writing a line: every bug found in simulation has to be reproducible byte for byte, from a
seed, and steppable in a viewer. I wrote moirae, a deterministic simulation testing
framework in TypeScript, and I wanted a real system to point it at. ananke is that system.
The two projects justify each other: ananke is moirae's flagship consumer, moirae is
ananke's test harness.

Phase 0 is the runtime that makes the constraint possible. There is no database yet. There
is an `Environment` trait, two implementations of it, a fault model, a bridge to moirae's
trace format, and a toy echo protocol that runs identically under both implementations.
That is the whole deliverable, and it is tagged as v0.1.0.

## Why an `Environment` trait

Deterministic simulation only works if the code under test cannot bypass the simulator.
Every source of non-determinism has to go through one door: the clock, the disk, the
network, randomness, and task spawning. In ananke that door is the `Environment` trait.
Every crate is generic over it. `RealEnv` is tokio, the real disk with fsync honoured, TCP,
and OS entropy. `SimEnv` is a single-threaded executor with a virtual clock, seeded
streams, an in-memory network and an in-memory disk, both with faults.

The rule is enforced by clippy rather than by review. `clippy.toml` lists 147 paths as
disallowed methods and types, from `std::time::Instant::now` to `tokio::spawn` to
`std::collections::HashMap`, and the lint is denied in the workspace so a plain
`cargo clippy` fails. Only the real implementation module inside `ananke-env` may switch
it off, and a script fails CI if any other file does.

Two of those bans came from holes I only saw once the trait existed. `std::time::Instant`
has no public constructor, so a simulated clock cannot produce one, and `Instant::elapsed`
reads the real clock behind any abstraction you put in front of it. So ananke owns its
`Instant` and `WallTime` types, plain nanosecond counters. And `std::collections::HashMap`
seeds its hasher from OS entropy per map, so iterating one emits events in a different
order every run, which silently breaks the byte-identical promise. ananke's hash maps are
seeded from the environment's random stream instead, and the std types are banned outside
`ananke-env`.

## The simulator, briefly

The executor polls tasks on one thread. When several are runnable at the same instant, a
scheduling policy picks one and the choice is written to the trace. When none is, virtual
time jumps to the next timer or message delivery. Each node has its own clock with a skew
and a drift, so nothing in ananke can assume clocks agree. Every random draw comes from a
stream derived from the seed by name: one for the scheduler, one for network faults, one
for disk faults, one for clock faults, and two per node, one for the protocol and one for
the executor's own coin flips. A scheduling change cannot move a fault draw; a fault
change cannot move a protocol draw.

The network is message-oriented and unreliable by design. `send` enqueues and returns; it
never waits for a peer, because a Raft leader whose heartbeat loop stalls on a partitioned
follower is a liveness bug I do not want to write. The disk model keeps, for every file,
what the node sees and what would survive a crash. `fsync` persists with a configurable
probability and otherwise lies. At a crash a random prefix of the pending writes survives,
and the next one may survive torn. These are the faults FoundationDB and TigerBeetle test
against.

## The moirae bridge

moirae's trace format is JSONL: a header, then one event per line with a time and a
sequence number. The studio is a pure function of that file. The bridge maps ananke's
trace records onto it: sends, deliveries and drops with a message id and a decoded payload,
fault lines for crashes, restarts, partitions and heals, and namespaced log lines for
everything moirae has no vocabulary for, like task scheduling and filesystem faults.

Two things had to change in moirae for this to be honest rather than approximate. The
format got a version 2 with a `unit` field in the header, because ananke's clock is
nanoseconds and moirae's is milliseconds, and a float millisecond would have run out of
precision after eleven days of virtual time. And two Rust crates now live in the moirae
repo: `moirae-trace`, a writer whose output is byte-identical to the TypeScript engine's,
held to that by fixtures committed on both sides, and `moirae-sched`, the engine's PRNG
with the same seeding and reference vectors, plus the scheduling policies. Half the seeds
schedule uniformly at random, half use PCT, the priority-based scheduler from Burckhardt
and others, which finds deep interleaving bugs with a probability you can write down.

The Phase 0 exit criterion was that the echo trace opens in the studio. It does. The seed-42
trace is committed in the moirae repo as a fixture, and a studio test parses it, derives
the picture, and asserts the same hash ananke pins in its own CI. The two repositories can
only drift from each other loudly.

## The seed-88 episode

The echo scenario is three nodes pinging each other under ten percent message loss, one to
ten milliseconds of delay, clock skew, a symmetric partition, a one-way block, and a crash
with a restart. A report at the end states invariants: no pong answers a ping that was
never sent, nothing crosses the partition, healing restores traffic, and so on. CI runs the
scenario for a hundred consecutive seeds.

Late in the phase I changed how `race`, the combinator that waits for either a message or
a timer, decides which side to poll first. It had been left-biased, which means a node
under a steady stream of ready messages would never fire its timer. In Phase 2 that is a
Raft follower flooded with `AppendEntries` that never times out, a bug the simulator should
expose, not mask. So `race` now flips a coin from the node's scheduling stream on every
poll.

The hundred-seed run immediately failed at seed 88: "node 2 could not reach node 1 during a
block of the other direction". The invariant said that while the link from node 1 to node 2
is blocked, node 2 must still reach node 1, and it checked that by counting deliveries in
the window. Seed 88 had zero.

It was not a simulator bug. Node 2 picks its ping target at random from its own stream,
and in a 200 millisecond window it sends about ten pings. In seed 88 it picked node 3 every
time. The coin flips in `race` had consumed different draws than before, the stream
shifted, and a run that happened to pass under the old draws stopped passing under the
new ones. The invariant had been probabilistic all along; I had been lucky for a hundred
seeds.

The fix was to the claim, not the code. A one-way block has two deterministic properties:
nothing gets through the blocked direction, and nothing in the open direction is ever
dropped as partitioned. Whether node 2 happens to ping node 1 inside the window is its
business; that it reaches node 1 at all is asserted over the whole run. I swept eleven
hundred seeds after the change.

What I take from it: a seed reproduced the failure exactly, the trace showed what node 2
had chosen, and the diagnosis took minutes. That is the thesis working. And an invariant
about a random choice has to be stated so that the choice cannot falsify it, which is
harder than it sounds and is the kind of thing a simulator teaches you by failing.

## What is next

Before Phase 1 can start, the simulator needs the rest of the disk model that the storage
engine's crash tests depend on: bit rot after a crash, and directory entries that are lost
when a rename is not followed by a directory fsync. And it needs a poll budget, so a task
that keeps itself runnable fails a test instead of hanging a ten-thousand-seed nightly run.
Then the storage engine: a write-ahead log first, with crash injection from the first
commit.
