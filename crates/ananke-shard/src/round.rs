//! Q41's round: the order one `raft` task keeps over every core on the node
//! (SHARD.md §4).
//!
//! The node holds every range's core and drives them all from one ticker. *A round is
//! a tick's steps, or the messages drained since the last round.* In each round the
//! task
//!
//! - flushes, before the round's sync, every send RAFT.md §3 already lets leave early:
//!   the sends that precede a core's `Persist`, and all sends of a core that persisted
//!   nothing;
//! - submits the round's persists together, so the WAL writer's group commit syncs
//!   them once (wal.rs:16-20, D-018);
//! - flushes each persisting core's later outputs when that core's own persist
//!   resolves, and steps that core no further until then.
//!
//! The order covers every output, not only sends: a core's `Apply`, `ReadReady`,
//! `ReadDropped`, snapshot actions and the step's trace events all wait on that core's
//! own persist, as `execute` waits on `store.persist(..)` today
//! (ananke-raft node.rs:2083-2180). An `Apply` handed out early would let the `apply`
//! task make an applied index durable above the durable log; a trace event handed out
//! early would put a `RaftAppend` in the trace before it is durable, which D-026 keeps
//! from happening (DECISIONS.md:831-834).
//!
//! This module is the discipline and nothing else: no clock, no socket, no disk. It
//! takes a step's inputs and hands back a [`Round`] — what runs now, what is submitted
//! together, and what waits — so the order can be asserted without a simulation.
//! [`crate::node`] drives it.
//!
//! **What is held.** A message for a core whose persist is outstanding is still taken
//! from the inbox and held for that core; a tick that falls due meanwhile is held as
//! one tick. Once the persist resolves the held work is stepped in the order it
//! arrived or fell due, *every missed tick stepped, none collapsed*, as today's loop
//! steps each tick it missed while `execute` awaited a persist
//! (ananke-raft node.rs:718-728).

use std::collections::{BTreeMap, VecDeque};

use ananke_env::{Decision, Environment};
use ananke_raft::core::SnapshotAction;
use ananke_raft::{Index, Input, Output, Persist, Raft, ServerId};

use crate::RangeId;
use crate::variant::{NodeVariant, NodeVariants};

/// The stamps a step's outputs carry.
///
/// `decided` is the step's decision time (D-047): its trace events are recorded when
/// they become durable and carry this as the time the step took them. `received` is
/// when the peer's message the step took reached the node, which a term change
/// records and every other input leaves empty (D-050).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stamps {
    /// When the step was decided (D-047).
    pub decided: Decision,
    /// When the message the step took was received, if it took one (D-050).
    pub received: Option<Decision>,
}

/// One output of one core, with the range it belongs to and its step's stamps.
#[derive(Clone, Debug)]
pub struct Act {
    /// The range whose core produced it.
    pub range: RangeId,
    /// The step's stamps.
    pub stamps: Stamps,
    /// What to do.
    pub output: Output,
}

/// A persist the round submits.
#[derive(Clone, Debug)]
pub struct Submission {
    /// The range whose core asked for it.
    pub range: RangeId,
    /// The step's stamps: the events that follow this persist carry them, so they are
    /// kept beside it only for the debug view.
    pub stamps: Stamps,
    /// What must be durable.
    pub persist: Persist,
}

/// What one round asks of the node.
///
/// `early` is executed and flushed **before** the round's sync; `persists` is
/// submitted together after it, and each core's later outputs wait for that core's own
/// persist to resolve, which is [`Cores::resolved`].
#[derive(Clone, Debug, Default)]
pub struct Round {
    /// The outputs that leave before the round's sync, in the order they were
    /// produced: the outputs that precede a core's `Persist`, and all outputs of a
    /// core that persisted nothing.
    pub early: Vec<Act>,
    /// The persists this round submits together, in range order.
    pub persists: Vec<Submission>,
}

impl Round {
    /// Whether the round asks nothing at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.early.is_empty() && self.persists.is_empty()
    }

    /// Whether any of the round's early outputs is a send, and so whether flushing
    /// before the sync puts a frame on the wire.
    #[must_use]
    pub fn sends_early(&self) -> bool {
        self.early
            .iter()
            .any(|act| matches!(act.output, Output::Send { .. }))
    }
}

/// Work a core was handed while its own persist was outstanding.
#[derive(Clone, Debug)]
struct Held {
    input: Input,
    received: Option<Decision>,
    /// What the message cost the node's inbox, still charged against the node's byte
    /// bound while it is held (Q14); zero for a tick, which arrived on no socket.
    bytes: usize,
}

/// One core on the node, with what it is waiting for.
struct Slot {
    core: Raft,
    /// A persist of this core is outstanding: it steps nothing until it resolves.
    persisting: bool,
    /// The outputs that follow this core's `Persist`, in order, to be executed when
    /// that persist resolves.
    deferred: Vec<Act>,
    /// The messages and ticks handed to this core while its persist was outstanding,
    /// in the order they arrived or fell due.
    held: VecDeque<Held>,
}

impl Slot {
    fn new(core: Raft) -> Self {
        Self {
            core,
            persisting: false,
            deferred: Vec::new(),
            held: VecDeque::new(),
        }
    }
}

/// What the node measures about its own rounds: the figures Stage B owes
/// (SHARD.md §12).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Meters {
    /// The most cores persisting at one moment: the cores a slow sync holds.
    pub cores_held_most: usize,
    /// The most ticks one core replayed at one resolution: the replay burst's first
    /// factor (SHARD.md §12).
    pub ticks_replayed_most: u64,
    /// Every tick replayed, over the run.
    pub ticks_replayed: u64,
    /// The most work held for one core at one moment, messages and ticks together.
    pub held_most: usize,
    /// The bytes held for cores whose persists are outstanding, at their highest.
    pub held_bytes_most: usize,
}

/// Every core on the node, keyed by range, and the order they are stepped in
/// (SHARD.md §4).
pub struct Cores {
    slots: BTreeMap<RangeId, Slot>,
    variants: NodeVariants,
    held_bytes: usize,
    meters: Meters,
}

impl Cores {
    /// An empty node: no range yet.
    #[must_use]
    pub fn new(variants: NodeVariants) -> Self {
        Self {
            slots: BTreeMap::new(),
            variants,
            held_bytes: 0,
            meters: Meters::default(),
        }
    }

    /// Puts `core` on the node as `range`'s, replacing whatever was there.
    pub fn insert(&mut self, range: RangeId, core: Raft) {
        self.slots.insert(range, Slot::new(core));
    }

    /// The ranges on the node, in order.
    pub fn ranges(&self) -> impl Iterator<Item = RangeId> + '_ {
        self.slots.keys().copied()
    }

    /// How many cores the node holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Whether the node holds no core.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// `range`'s core, for what a step needs to read of it: the entries an `Apply`
    /// names, the index a proposal took.
    #[must_use]
    pub fn core(&self, range: RangeId) -> Option<&Raft> {
        self.slots.get(&range).map(|slot| &slot.core)
    }

    /// Whether a persist of `range`'s core is outstanding.
    #[must_use]
    pub fn persisting(&self, range: RangeId) -> bool {
        self.slots.get(&range).is_some_and(|slot| slot.persisting)
    }

    /// How many cores are waiting on a persist.
    #[must_use]
    pub fn held_cores(&self) -> usize {
        self.slots.values().filter(|slot| slot.persisting).count()
    }

    /// The bytes of held messages, still counted against the node's byte bound (Q14).
    #[must_use]
    pub fn held_bytes(&self) -> usize {
        self.held_bytes
    }

    /// What the node measured about its rounds.
    #[must_use]
    pub fn meters(&self) -> Meters {
        self.meters
    }

    /// The round of one tick: a `Tick` stepped into every core, in range order, and
    /// held for every core whose persist is outstanding.
    ///
    /// The ticker is the node's, not a core's: a tick falls due for every range at
    /// once, so a core behind a slow sync collects one held tick per tick it missed
    /// and steps every one of them when its persist resolves.
    pub fn tick<E: Environment>(&mut self, env: &E) -> Round {
        let mut round = Round::default();
        for range in self.slots.keys().copied().collect::<Vec<_>>() {
            self.drive(env, range, Input::Tick, None, 0, &mut round);
        }
        self.remember();
        round
    }

    /// The round of the messages drained since the last round: each stepped into its
    /// range's core, in the order it arrived.
    ///
    /// `bytes` is what the message cost the node's inbox; it is charged against the
    /// node's bound again if the message has to be held (Q14).
    pub fn messages<E, I>(&mut self, env: &E, inputs: I) -> Round
    where
        E: Environment,
        I: IntoIterator<Item = (RangeId, Input, Option<Decision>, usize)>,
    {
        let mut round = Round::default();
        for (range, input, received, bytes) in inputs {
            self.drive(env, range, input, received, bytes, &mut round);
        }
        self.remember();
        round
    }

    /// One input for one core: stepped now, or held because that core's persist is
    /// outstanding.
    fn drive<E: Environment>(
        &mut self,
        env: &E,
        range: RangeId,
        input: Input,
        received: Option<Decision>,
        bytes: usize,
        round: &mut Round,
    ) {
        let variants = self.variants;
        let hold = self.persisting(range) && !variants.contains(NodeVariant::StepWhilePersisting);
        if !hold {
            self.step(env, range, input, received, round);
            return;
        }
        let collapse =
            variants.contains(NodeVariant::CollapseHeldTicks) && matches!(input, Input::Tick);
        let Some(slot) = self.slots.get_mut(&range) else {
            return;
        };
        if collapse
            && slot
                .held
                .iter()
                .any(|held| matches!(held.input, Input::Tick))
        {
            // The variant: the ticks a core missed become one.
            return;
        }
        slot.held.push_back(Held {
            input,
            received,
            bytes,
        });
        if !variants.contains(NodeVariant::HeldNotCounted) {
            self.held_bytes += bytes;
        }
    }

    /// Steps `range`'s core and splits the outputs at its `Persist` (SHARD.md §4).
    fn step<E: Environment>(
        &mut self,
        env: &E,
        range: RangeId,
        input: Input,
        received: Option<Decision>,
        round: &mut Round,
    ) {
        // D-047: the step's decision time, taken at the step, whether the step is of
        // work that just arrived or of work that was held.
        let decided = env.decision();
        let stamps = Stamps { decided, received };
        let early = self.variants.contains(NodeVariant::DeferredFlushedEarly);
        let Some(slot) = self.slots.get_mut(&range) else {
            return;
        };
        let outputs = slot.core.step(input);
        let mut persisted = false;
        for output in outputs {
            match output {
                Output::Persist(persist) => {
                    debug_assert!(!persisted, "a step asks for one persist");
                    persisted = true;
                    round.persists.push(Submission {
                        range,
                        stamps,
                        persist,
                    });
                }
                output => {
                    let act = Act {
                        range,
                        stamps,
                        output,
                    };
                    if persisted && !early {
                        slot.deferred.push(act);
                    } else {
                        round.early.push(act);
                    }
                }
            }
        }
        if persisted {
            // Steps that core no further until its persist resolves.
            slot.persisting = true;
        }
    }

    /// `range`'s persist resolved: the outputs that waited on it, then the work held
    /// while it was outstanding, stepped in the order it arrived or fell due.
    ///
    /// The result is a round of its own: its early outputs are executed and flushed
    /// together with the deferred ones, and a persist the replay asks for is submitted
    /// as that round's. A replay stops at the first step that persists, because that
    /// core steps no further until *that* persist resolves.
    pub fn resolved<E: Environment>(&mut self, env: &E, range: RangeId) -> Round {
        let mut round = Round::default();
        let Some(slot) = self.slots.get_mut(&range) else {
            return round;
        };
        slot.persisting = false;
        round.early.append(&mut slot.deferred);
        let mut ticks = 0;
        while let Some(slot) = self.slots.get_mut(&range) {
            // The replay stops where the core does: at a step that persisted again,
            // or when there is nothing left it was handed.
            if slot.persisting {
                break;
            }
            let Some(held) = slot.held.pop_front() else {
                break;
            };
            self.held_bytes = self.held_bytes.saturating_sub(held.bytes);
            if matches!(held.input, Input::Tick) {
                ticks += 1;
            }
            self.step(env, range, held.input, held.received, &mut round);
        }
        self.meters.ticks_replayed += ticks;
        self.meters.ticks_replayed_most = self.meters.ticks_replayed_most.max(ticks);
        self.remember();
        round
    }

    /// Takes the highest water marks the measurements want.
    fn remember(&mut self) {
        self.meters.cores_held_most = self.meters.cores_held_most.max(self.held_cores());
        self.meters.held_bytes_most = self.meters.held_bytes_most.max(self.held_bytes);
        let held = self.slots.values().map(|slot| slot.held.len()).max();
        self.meters.held_most = self.meters.held_most.max(held.unwrap_or(0));
    }
}

/// Where one of a step's outputs goes, for a check that wants to name it.
#[must_use]
pub fn describe(output: &Output) -> &'static str {
    match output {
        Output::Persist(_) => "persist",
        Output::Send { .. } => "send",
        Output::Apply { .. } => "apply",
        Output::Rejected { .. } => "rejected",
        Output::ReadReady { .. } => "read-ready",
        Output::ReadDropped { .. } => "read-dropped",
        Output::Trace(_) => "trace",
        Output::Snapshot(SnapshotAction::Take) => "snapshot-take",
        Output::Snapshot(_) => "snapshot-stream",
    }
}

/// The entries an `Apply { through }` names, for the `apply` task: the core's log from
/// `after` to `through`.
#[must_use]
pub fn entries_to_apply(core: &Raft, after: Index, through: Index) -> Vec<ananke_raft::Entry> {
    if through <= after {
        return Vec::new();
    }
    (after + 1..=through)
        .filter_map(|index| core.entry(index).cloned())
        .collect()
}

/// The peer a send is for, for the outbox.
#[must_use]
pub fn send_to(output: &Output) -> Option<ServerId> {
    match output {
        Output::Send { to, .. } => Some(*to),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use ananke_env::sim::{Sim, SimConfig, SimEnv};
    use ananke_raft::message::Message;
    use ananke_raft::types::{Configuration, Entry, Payload};
    use ananke_raft::{Index, RaftConfig};
    use bytes::Bytes;

    use super::*;
    use crate::variant::NodeVariants;

    const R1: RangeId = RangeId(1);
    const R2: RangeId = RangeId(2);
    const ME: ServerId = ServerId(1);
    const LEADER: ServerId = ServerId(2);

    fn env() -> SimEnv {
        let mut sim = Sim::new(SimConfig::new(3));
        let node = sim.add_node();
        sim.env(node)
    }

    fn core(range: RangeId) -> Raft {
        let config = RaftConfig {
            range: range.get(),
            ..RaftConfig::default()
        };
        Raft::new(ME, Configuration::of(&[ME, LEADER, ServerId(3)]), config, 7)
    }

    fn append(index: Index, term: u64) -> Input {
        Input::Message {
            from: LEADER,
            message: Message::AppendEntries {
                term,
                prev_index: index - 1,
                prev_term: if index > 1 { term } else { 0 },
                entries: vec![Entry {
                    index,
                    term,
                    payload: Payload::Command(Bytes::from_static(b"x")),
                }],
                commit: 0,
                sent: 0,
            },
            now: 0,
        }
    }

    fn cores(variants: NodeVariants) -> Cores {
        let mut cores = Cores::new(variants);
        cores.insert(R1, core(R1));
        cores.insert(R2, core(R2));
        cores
    }

    /// A step's `Persist` is the first of its outputs (`finish`, core.rs:1277-1300),
    /// so everything the step produced after it waits for that core's own persist and
    /// the round carries none of it.
    #[test]
    fn the_outputs_after_a_cores_persist_are_not_the_rounds_early_ones() {
        let env = env();
        let mut cores = cores(NodeVariants::correct());
        let round = cores.messages(&env, [(R1, append(1, 1), None, 64)]);
        assert_eq!(round.persists.len(), 1, "a follower that appends persists");
        assert_eq!(round.persists[0].range, R1);
        assert!(
            round.early.is_empty(),
            "a step whose first output is its persist has no early output: {:?}",
            round.early
        );
        assert!(cores.persisting(R1));
        assert!(
            !cores.persisting(R2),
            "r2 stepped nothing and waits on nothing"
        );

        // The response was held for r1 and leaves when r1's persist resolves.
        let later = cores.resolved(&env, R1);
        assert!(
            later
                .early
                .iter()
                .any(|act| matches!(act.output, Output::Send { .. })),
            "the response follows the persist: {:?}",
            later.early
        );
        assert!(!cores.persisting(R1));
    }

    /// A core steps no further in the round it persisted in, and the work handed to
    /// it meanwhile is held, not dropped and not stepped.
    #[test]
    fn a_core_that_persisted_steps_no_further_until_its_persist_resolves() {
        let env = env();
        let mut cores = cores(NodeVariants::correct());
        let round = cores.messages(
            &env,
            [
                (R1, append(1, 1), None, 64),
                // The same range again, in the same round.
                (R1, append(2, 1), None, 64),
                // And another range, which persists on its own account.
                (R2, append(1, 1), None, 64),
            ],
        );
        assert_eq!(
            round.persists.len(),
            2,
            "one persist per core, not one per message: {:?}",
            round.persists
        );
        assert_eq!(cores.held_bytes(), 64, "r1's second append is held");
        assert_eq!(cores.meters().held_most, 1);

        // A tick falling due meanwhile is held for both cores and steps neither.
        let tick = cores.tick(&env);
        assert!(tick.is_empty(), "both cores are waiting: {tick:?}");
        assert_eq!(cores.meters().cores_held_most, 2);

        // r2's disk answers first: only r2 steps.
        let later = cores.resolved(&env, R2);
        assert!(!later.is_empty());
        assert!(
            later.early.iter().all(|act| act.range == R2),
            "a core's resolution stepped another core: {:?}",
            later.early
        );
        assert!(cores.persisting(R1), "r1 still waits on its own disk");
        assert_eq!(cores.meters().ticks_replayed_most, 1);
    }

    /// The replay stops at the step that persists again: that core steps no further
    /// until *that* persist resolves, and the rest of its held work waits.
    #[test]
    fn a_replay_stops_at_the_step_that_persists_again() {
        let env = env();
        let mut cores = cores(NodeVariants::correct());
        cores.messages(&env, [(R1, append(1, 1), None, 64)]);
        // Two more appends and a tick, all held behind the outstanding persist.
        cores.messages(
            &env,
            [(R1, append(2, 1), None, 64), (R1, append(3, 1), None, 64)],
        );
        cores.tick(&env);
        assert_eq!(cores.held_bytes(), 128, "the two appends, not the tick");

        let later = cores.resolved(&env, R1);
        assert_eq!(
            later.persists.len(),
            1,
            "the first replayed append persists: {:?}",
            later.persists
        );
        assert!(cores.persisting(R1));
        assert_eq!(
            cores.held_bytes(),
            64,
            "the append behind the new persist is still held"
        );
        assert_eq!(
            cores.meters().ticks_replayed,
            0,
            "the held tick is behind the appends and has not been reached"
        );
    }

    /// Every missed tick is stepped, none collapsed, and they are stepped in the
    /// order they fell due among the messages that arrived beside them.
    #[test]
    fn the_held_work_is_stepped_in_the_order_it_arrived_or_fell_due() {
        let env = env();
        let mut cores = cores(NodeVariants::correct());
        cores.messages(&env, [(R1, append(1, 1), None, 64)]);
        for _ in 0..4 {
            cores.tick(&env);
        }
        assert_eq!(cores.meters().held_most, 4, "four ticks held for r1");
        let later = cores.resolved(&env, R1);
        assert_eq!(
            cores.meters().ticks_replayed,
            4,
            "every one of them stepped"
        );
        assert!(
            later.persists.is_empty(),
            "a follower's ticks persist nothing"
        );
        assert_eq!(cores.held_bytes(), 0);
        assert!(!cores.persisting(R1));
    }

    /// The variant beside it: the ticks a core missed become one, which is a core
    /// whose election and heartbeat timers run slow by however long the sync took.
    #[test]
    fn the_variant_collapses_the_missed_ticks_into_one() {
        let env = env();
        let mut cores = cores(NodeVariants::correct().with(NodeVariant::CollapseHeldTicks));
        cores.messages(&env, [(R1, append(1, 1), None, 64)]);
        for _ in 0..4 {
            cores.tick(&env);
        }
        assert_eq!(cores.meters().held_most, 1, "the variant holds one tick");
        cores.resolved(&env, R1);
        assert_eq!(cores.meters().ticks_replayed, 1);
    }

    #[test]
    fn a_node_names_the_ranges_it_holds() {
        let cores = cores(NodeVariants::correct());
        assert_eq!(cores.ranges().collect::<Vec<_>>(), vec![R1, R2]);
        assert_eq!(cores.len(), 2);
        assert!(!cores.is_empty());
        assert!(cores.core(R1).is_some());
        assert!(cores.core(RangeId(9)).is_none());
        assert_eq!(cores.held_cores(), 0);
    }

    #[test]
    fn an_output_is_named_by_what_it_asks_for() {
        assert_eq!(describe(&Output::Apply { through: 1 }), "apply");
        assert_eq!(
            describe(&Output::Snapshot(SnapshotAction::Take)),
            "snapshot-take"
        );
        assert_eq!(send_to(&Output::Apply { through: 1 }), None);
        assert_eq!(
            send_to(&Output::Send {
                to: LEADER,
                message: Message::TimeoutNow { term: 1 },
            }),
            Some(LEADER)
        );
    }

    #[test]
    fn the_entries_an_apply_names_are_the_ones_not_handed_out_yet() {
        let env = env();
        let mut cores = cores(NodeVariants::correct());
        cores.messages(&env, [(R1, append(1, 1), None, 64)]);
        cores.resolved(&env, R1);
        let core = cores.core(R1).expect("r1");
        assert_eq!(entries_to_apply(core, 0, 1).len(), 1);
        assert!(entries_to_apply(core, 1, 1).is_empty());
    }
}
