# RAFT.md — ananke's Raft

_Status: approved 2026-09-05, with the lease section rewritten as approved. Each
implementation stage turns its part into a DECISIONS.md entry as it lands._

Sources of truth, in order: Ongaro and Ousterhout, *In Search of an Understandable
Consensus Algorithm* (USENIX ATC 2014), cited as the paper; Ongaro, *Consensus: Bridging
Theory and Practice* (PhD thesis, 2014), cited as the thesis, for everything the paper
defers; moirae's [docs/RAFT.md](https://github.com/pchrysostomou/moirae/blob/main/docs/RAFT.md),
whose ten rules that are almost always implemented incorrectly apply here word for word
and are not repeated. Where this document and those disagree, they win. SPEC §3 fixes
the variant; this document says how.

## 1. The variant

The base is Figure 2 of the paper with the thesis' extensions. Every rule below names
its source.

**Pre-vote (thesis §9.6).** A follower whose election timer fires does not increment
its term. It sends `PreVote { term: current + 1, last_log }` and becomes a candidate
only on a majority of pre-votes granted. A node grants a pre-vote only if it has not
heard from a leader within its own minimum election timeout and the candidate's log is
at least as up to date as its own (the §5.4.1 restriction, applied twice). Terms
therefore rise only when an election can succeed, and a node rejoining from a partition
cannot depose a working leader. The same test, "heard from a leader within the timeout",
is what a leader uses to step down when it loses contact with a majority (check quorum),
so a leader on the wrong side of a partition stops serving.

**Persistence discipline (paper Figure 2, thesis §3.8).** Current term, the vote and
the log are durable before any message that depends on them is sent. In ananke that is
one `WriteBatch` with `sync: true` against the engine, awaited, and only then the
send. The node task structure in §3 makes this the shape of the code, not a comment.
Crashes in the simulator land between two polls, so a crash between the persist and
the send is a run the sweep takes, and the variant that sends first is caught.

**Log replication with batching and pipelining (paper §5.3, thesis §10.2).** A leader
sends up to `max_batch` entries per `AppendEntries` and keeps up to `max_inflight`
requests outstanding per follower, advancing `next_index` optimistically. A follower's
response carries `match_index = prev_log_index + entries.len()`, moirae's deviation D1,
and the leader's `match_index` is monotone: a stale or duplicated response can only
propose a value already passed. On a rejection the leader resets `next_index` for that
follower to the rejection's hint (the follower's last index plus one, or the first index
of the conflicting term) and drops what was in flight past it. From then on it probes
with one message at a time until a success, and a rejection of anything but the
outstanding probe is stale and ignored: the pipeline's other messages are rejected too,
each with an older `prev_index` carried back in the response, and acting on each would
restart the probe as many times, which is a flood the sweep found (D-026). Conflicts
truncate only from the first genuinely conflicting entry (rule 3), and only a follower
truncates; a leader appends (rule 8).

**Commit (paper §5.4.2).** The commit index advances to the highest index replicated on
a majority whose term is the leader's current term, and to nothing older on its own.
A new leader appends a no-op entry for its term at once (thesis §6.4), so earlier
entries commit without waiting for a client.

**Reads (thesis §6.4).** Two paths, both linearizable by construction and both
checked by the linearizability checker, never assumed.

*Read-index* is the baseline. A leader records its commit index as the read index,
confirms it is still leader by receiving heartbeat acknowledgements from a majority
sent after the read arrived, waits until its applied index reaches the read index, and
serves the read from the engine at that state. If it has not yet committed an entry of
its term, it waits for the no-op.

*Lease reads* skip the heartbeat round while a lease holds. The lease is granted by
followers, not assumed from the clock alone: every `AppendEntries` response says "I
will not grant a vote or a pre-vote to anyone before my election timer next fires,
which is at least `election_timeout_min` after this message's request was sent as I
measure time". The leader computes `lease_end` from the time it sent the acknowledged
request, by its own clock, as `sent + election_timeout_min × (1 − drift_bound) −
heartbeat_margin`, over the earliest such time among a majority. A read arriving
before `lease_end` is served at the commit index without the round.

The lease assumes bounded clock rate drift between the leader and each follower over
one lease period, and the simulator will run with drift beyond `drift_bound` on some
seeds (SPEC §3). The leader guards against what it assumed: every response also
carries the follower's local time, and the leader keeps, per follower, the offset
between the follower's clock and its own as seen at each response. The guard is
conservative. Heartbeat responses carry network delay, and delay jitter is
indistinguishable from clock movement in either direction, so the leader revokes its
lease on any observed movement of a follower's offset beyond `drift_bound × elapsed`,
jitter included, and serves reads by read-index until a fresh majority of responses
shows the offsets steady again. A spurious revoke costs one read-index round trip; a
missed one costs a stale read. No argument here establishes that the guard is
sufficient. Lease safety is established by the sweep, as the sixth invariant in §2:
in every seed where the simulator's drift exceeds the configured bound, either the
guard revoked the lease before the lease read, or the linearizability checker reports
the stale read and the run fails. The sweep runs the drift violation with the guard on,
where the invariant must hold on every seed, and with the guard off,
`Variant::LeaseTrustsTheClock`, where the checker must catch a stale read on some.

**Membership changes by joint consensus (thesis §4.3).** A change from C_old to C_new
is two log entries: `C_old,new` and, once that is committed, `C_new`. While the joint
entry is in the log, uncommitted or committed, elections and commits need majorities
of both configurations. A server uses the latest configuration entry in its log,
committed or not. A leader that is not in C_new steps down once `C_new` is committed,
and does not count itself for the majorities that commit it. Servers being added first
catch up as non-voting learners (thesis §4.2.1): they receive entries and snapshots but
count for nothing until a round of replication to them takes less than an election
timeout, at which point the leader proposes the joint entry. One change is in flight at
a time. The exit criterion, 3 → 5 → 3 under partition with no availability loss beyond
one election timeout, is a scenario in `sim/raft.rs` with a leader on the minority side
during the change.

**Snapshots (thesis §5, SPEC §3).** A snapshot is an `Engine::checkpoint` of the state
machine's store at an applied index, with the index, term and configuration at that
point written into the checkpoint's reserved tenant before `CURRENT`. `InstallSnapshot`
streams the checkpoint's files in order, in chunks of `snapshot_chunk` bytes, each
chunk naming the file, its offset and the total; the receiver writes into a staging
directory and acknowledges the offset, so a resend after loss or a leader change
resumes from the last acknowledged offset of the last file rather than from zero, as
long as the snapshot's identity (last index and term, and the leader's term) matches.
The receiver verifies every table with the engine's own checks when it opens the
staging directory as a store, discards its log entries at or below the snapshot's index,
keeps those after it whose terms match, and switches its state machine to the new
store. A leader takes a snapshot when its log past the last snapshot exceeds
`snapshot_threshold` entries, and compacts the Raft log to the snapshot's index only once
every follower's `match_index` is past it or the follower is a learner being replaced by
the snapshot.

**Timing.** Election timeouts are drawn per node per election from the node's
protocol stream over `[election_timeout_min, 2 × election_timeout_min)`; heartbeats
every `election_timeout_min / 5`. The simulator's clocks skew and drift per node, so
nothing compares timestamps from two nodes except the lease guard above, which is
built to.

## 2. The five invariants and how the trace checks each

Raft emits a trace event for every state transition that the invariants read. Proposed
events, all carrying the node and the term:

| Event | When | Fields |
|---|---|---|
| `RaftTerm` | the current term changes | `term`, `role` |
| `RaftVote` | a vote or pre-vote is granted or refused | `term`, `candidate`, `granted`, `pre` |
| `RaftLeader` | a node becomes leader | `term`, `last_index` |
| `RaftAppend` | an entry is written to the log | `index`, `entry_term`, `hash` of the payload |
| `RaftTruncate` | a conflict removes entries | `from_index` |
| `RaftCommit` | the commit index advances | `index` |
| `RaftApply` | an entry is applied | `index`, `entry_term`, `hash` |
| `RaftConfig` | a configuration entry takes effect | `index`, `old`, `new`, `joint` |
| `RaftSnapshot` | a snapshot is taken or installed | `last_index`, `last_term`, `taken` |
| `RaftRead` | a read is served | `index`, `lease` |
| `RaftLeaseRevoked` | the guard revoked a lease | `follower`, `offset_moved` |
| `RaftRecovered` | a server starts on what its store held | `term`, `applied`, `last_index` |
| `RaftProposed` | a leader made a client's request an entry | `client`, `seq`, `index`, `term` |
| `RaftRefused` | a server's store lost state and it will not start | `reason` |
| `RaftServerFailed` | a server stopped on an I/O error | `reason` |
| `RaftInboxDropped` | a full inbox dropped a message | `kind` |
| `ClientInvoke` | a client operation starts | `client`, `seq`, `op` |
| `ClientReturn` | it returns | `client`, `seq`, `result` |

Each event carries the node's persistent term, so the studio's per-term filter (issue
#3) needs nothing more. The checks, all in `sim/raft.rs` over the trace of a run, after
every crash and at the end:

1. **Election safety.** Fold `RaftLeader` events: a map from term to the node that
   became leader in it; a second node for a term is a violation. Cheap, exact.
2. **Log matching.** Reconstruct each node's log from `RaftAppend`, `RaftTruncate` and
   `RaftSnapshot` events as a map from index to (term, hash). After every event, for
   the node it touches and every other node holding the same index with the same term,
   every index below must agree in term and hash. Incremental: the new entry's index is
   the only one that can newly violate it.
3. **Leader completeness.** From `RaftCommit` on a leader, the set of committed
   (index, term, hash). At every `RaftLeader` event, the new leader's reconstructed log
   must contain every entry committed in an earlier term. Also checked, as the property
   that makes rule 2 of moirae's list bite: an entry counted committed must have
   been appended on a majority of the configuration then in force, which the
   reconstructed logs and `RaftConfig` events show.
4. **State machine safety.** Fold `RaftApply`: a map from index to (term, hash); a
   second value for an index is a violation. Per node, applied indices must be the
   consecutive integers from one, so an entry applied twice or skipped shows here as
   well, which is where the applied index being written in the same batch as the
   entry's writes (§3) is proven. An apply durable at a crash but not yet traced
   shows at the restart as `RaftRecovered` carrying an applied index past the last
   traced apply; the entries between are what the server's log holds there, and
   are checked like any other. For the same reason a restarting server re-states
   its durable log, a truncation at its end and an append per entry, before
   `RaftRecovered`, so the trace's picture of every log is the disk's.
5. **Linearizability of the KV API.** The history is the `ClientInvoke` and
   `ClientReturn` pairs with their virtual times, checked by the checker in §4. An
   operation that never returned, because its client's node crashed or the run ended,
   is kept as pending and may take effect or not, as porcupine treats it.
6. **Lease safety under drift.** For every `RaftRead` with `lease` set, served while
   the simulator's clock drift between the leader and some voter exceeded the
   configured `drift_bound` over the lease period, either a `RaftLeaseRevoked` event on
   that leader precedes the read, or the read is a linearizability violation reported
   by check 5. Stated the other way: on every seed with the drift violation on, the
   run passes only if no lease read was served stale. The invariant is a fold over
   `RaftRead`, `RaftLeaseRevoked` and the simulator's clock configuration, and the
   checker in §4 is what decides staleness; the guard's sufficiency is never assumed.

Three more folds check the rules behind the properties directly, so a broken rule is
seen the first time it is exercised and not only when its consequence happens to
land: *commit by majority*, every entry a leader commits was durable, as `RaftAppend`
after the persist says, on a majority when it did; *commit by current term* (§5.4.2),
a leader's commit index only ever lands on an entry of its own term; *committed
entries stay*, no server truncates at or below its own commit index. Two checks are
about time and run only on seeds the simulator scheduled uniformly, where no task can
be starved (D-016): after the last fault heals, a client write completes within ten
maximum election timeouts; and a follower that heard from no leader of its term and
granted no vote for three maximum election timeouts has started an election (moirae
rule 5). One check is pre-vote's own property (thesis §9.6): a server the schedule
isolated has, at the heal, the term it had when the isolation began.

The pair rule holds for each: a buggy variant in §5 fails each check, and the correct
variant passes every seed. Every check is a function of the trace alone, so a failing
seed replays in the studio with the invariant's own events on screen.

## 3. The `ananke-raft` crate

```
crates/ananke-raft/
  src/lib.rs        the crate, Variant (§5), RaftConfig (timeouts, batch and pipeline
                    limits, snapshot thresholds, drift_bound)
  src/types.rs      Term, Index, ServerId, Entry { term, index, payload }, Payload
                    { Command(Bytes), Config(Configuration), Noop }, Configuration
                    { voters: old and, when joint, new; learners }
  src/message.rs    Message and its codec
  src/core.rs       the pure state machine of the protocol
  src/store.rs      persistent state in the engine
  src/node.rs       the tasks that run one server
  src/apply.rs      the state machine adapter: commands to engine batches
  src/snapshot.rs   checkpoints as snapshots, chunked both ways
  src/read.rs       read-index and lease reads
  tests/            the paper's scenarios against the core: Figure 8, moirae's ten
                    rules, the D1 replay, a joint change with a leader outside C_new
sim/raft.rs         the crash-and-partition sweep: the five checks, the workload
sim/lin.rs          the linearizability checker
```

**The pure core.** `core.rs` is a state machine with no I/O, in the shape of raft-rs's
`RawNode`: `Raft::step(&mut self, input) -> Outputs`, where an input is a message from
a peer, a tick, a proposal, a read request or a completed persist or apply, and the
outputs are a list of `Send(to, Message)`, `Persist(PersistBatch)`, `Apply(through:
Index)`, `ReadReady(request, index)` and `Snapshot(take | install)`. The core is where
Figure 2 lives and where the paper's scenarios are unit tests without a simulator. It
is generic over nothing: the node task does the I/O.

**The log in the engine.** SPEC §3 puts the Raft log in the storage engine under a
reserved tenant, tenant 0 in the §2.6 key encoding, table ids by purpose:

| Key | Value |
|---|---|
| `0 / 0 / hard` | current term, vote |
| `0 / 0 / applied` | the applied index |
| `0 / 1 / <index: u64 BE>` | the entry: term, payload |
| `0 / 2 / config` | the latest configuration entry's index and content |
| `0 / 3 / snapshot` | last snapshot's index, term, and its checkpoint directory |

Appending entries is a `WriteBatch` with `sync: true` of the entries and, when the
term or vote changed with them, `hard`; the batch's future resolving is the persist the
core waits for before sending. The hard state and the applied index are separate keys
because separate tasks write them, the `raft` task and the `apply` task, and neither
waits for the other. Truncation is deletes of the conflicting indices in the
same batch as the entries that replace them. Reading entries back for a follower behind
the leader is a `scan` over the index range, which is what the engine's scan exists for.
Applying entry `i` is one `WriteBatch` with the command's writes under the user's
tenant and the applied index in `applied`: the two are durable together, so a crash
between them cannot exist, and an entry is applied exactly once whatever the crash
schedule. Until read-index reads arrive (stage C), a get is a command like any other:
it goes through the log and reads its key at its place in the order. The Raft log is compacted by deleting indices at or below a snapshot's;
the engine's compaction reclaims the space in its own time.

The engine is opened with `allow_manifest_fallback` and `allow_head_gap` off, and the
store refuses a recovery that dropped an unreadable table, fell back to an older
manifest, discarded a log head, stopped reading the log at a bad checksum or a gap,
or skipped a corrupt record in a segment the tables cover and with it the rest of
that segment: each is a hole in the middle of the state, and an applied index over a
hole names a state that never existed (D-022). Raft's safety argument assumes
persistent state is persistent, so a node that lost it cannot vote or serve: a
refused server traces `RaftRefused` and participates in nothing, no votes and no
responses, until stage E re-seeds it with a snapshot from the leader. It never binds
its socket, so a peer's messages to it reach no one.

**The message codec.** `Message` is `PreVote`, `PreVoteResponse`, `RequestVote`,
`RequestVoteResponse`, `AppendEntries`, `AppendEntriesResponse`, `InstallSnapshot`,
`InstallSnapshotResponse`, `TimeoutNow` for leadership transfer later. The wire form is
one frame: `kind: u8 | term: u64 | from: u64 | fields`, fields length-prefixed, entries
as `count | (term, index, payload_len, payload)*`, everything little-endian like the
engine's records, under `MAX_FRAME_LEN`. A `decode` that fails is a dropped message, not
a panic. The moirae bridge takes a `Decoder`, and `ananke-raft` provides one that turns a
frame into `{"type": "raft.append", "term": …, "from": …, "prevIndex": …, "entries":
n}` and its kin, so the studio labels lanes by message kind and filters by term and
index, which is issue #3's field set.

**The node's tasks.** One server is four tasks under `Environment::spawn`, and this is
where PCT gets something to bite, since every interleaving between them is a real one.
Stage B runs the first three; the `snapshot` task arrives with snapshots (stage E):

- `raft`: owns the core and the timers; one loop over a `race` of the inbox, the tick,
  proposals and completions; executes every output in order, awaiting each `Persist`
  before the `Send`s that follow it.
- `net`: receives frames, decodes, and hands messages to the `raft` task through a
  bounded queue; a full queue drops the oldest heartbeat first, never an
  `AppendEntries` with entries, and records the drop.
- `apply`: takes `Apply(through)` from the core, runs the state machine adapter one
  entry at a time, each a synced batch, and reports the applied index back; the core
  serves read-index reads only from applied state, so this task's lag is visible to
  the checker.
- `snapshot`: takes and streams snapshots on the leader; assembles and installs them
  on a follower; the only task that touches checkpoint directories.

The `net` and `raft` tasks are separate so that a message arriving while the core is
awaiting a persist is a queued message, not a lost one, and so that the interleaving
"persist completes, then two messages arrive in either order" is one the scheduler
chooses. The workload's clients are tasks too, on their own nodes, talking to the
cluster over the simulated network.

Client requests share the servers' socket and inbox, told apart by their first byte.
A server that is not the leader answers `NotLeader` with the leader it knows; the
leader answers once the entry applies, with the same term it was proposed in, and
never otherwise, since an entry replaced by a later leader's may still commit
elsewhere. The network delivers at least once, so a leader keeps the index and term
of every request it proposed while the entry is in its log and does not propose a
copy again; the sweep's first seed found a duplicated compare-and-set applied twice
(D-026). A client that hears nothing does not resend a write; it abandons the
operation as pending and continues as a new process (§4). Exactly-once retries
across leaders need client sessions (thesis §6.3), issue #21.

**What the simulator gains.** Message duplication (issue #1, landed with this
proposal), so that rule 3 and D1 are testable. A per-scenario clock configuration that
sets drift beyond `drift_bound` for the lease runs. Nothing else: partitions, delay,
loss, crashes with the disk model, and crashes between polls are already there.

## 4. The linearizability checker

`sim/lin.rs`, in Rust, over the trace: a Wing-Gong checker with Lowe's partitioning and
Horn and Kroening's memoisation, which is what porcupine does.

**History.** Each client operation is `(client, invoke_t, return_t, Op, Result)`; a
pending operation has no return and may be linearized or discarded. Operations are
`Put(k, v)`, `Get(k) → Option<v>`, `Delete(k)`, `Cas(k, expect, v) → bool` and
`Scan(range) → Vec<(k, v)>`; `Cas` exists so that a double apply or a lost write is
visible as a wrong boolean, not only as a stale value later. The trace closes most
pending operations: `RaftProposed` says which entry a request became, and an
abandoned operation whose entry applied took effect then, so it returns at the apply
with a result the client never saw and the model may give it any; one no leader
proposed cannot have taken effect and leaves the history; one proposed and never
applied stays pending. A pending operation is a candidate at every step of the
search, so closing them is what keeps the search small. The search has a budget of
states per key; exhausting it is reported apart from a violation, and the correct
server must never reach it.

**Partitioning.** Single-key operations partition by key, since the KV model is a
product of independent registers: a history is linearizable iff each key's
sub-history is. Each partition is checked on its own, so the search is small even
over long runs. Scans are multi-key reads and cannot be partitioned; they are checked
after the per-key searches: a scan is consistent iff there is a time `t` within its
invocation window at which, for every key in its range, the value it returned is the
value the key's chosen linearization holds at `t`. Each per-key search records the
linearization it found as a timeline of (time, value), so the scan check is a walk over
timelines, and a scan with no such `t` is a violation named with the key that was off.

**Search.** For one partition, sort operations by invocation time; state is (the set
of linearized operations as a bitmask, the register's value); depth-first search
takes any operation whose invocation is before every unlinearized operation's return,
applies it to the model, and recurses; a state seen before is not searched twice. The
model is a register with the four single-key operations. Pending operations may be
skipped at the end. A violation reports the shortest prefix that cannot be linearized
and the operations in it, which the studio shows as the clients' lanes.

**What is asserted.** Every partition linearizable and every scan consistent, for the
correct variant on every seed; `Variant::ApplyBeforeCommit` and
`Variant::LeaseTrustsTheClock` are the ones this check must catch, since the four log
invariants may hold while a stale read is served.

## 5. Buggy variants shipped from day one

Each is a `Variant` on `ananke-raft`, each breaks one rule with a reference, each must
be caught by the named check on some seeds of a hundred, and the correct variant must
pass every seed. A variant the sweep does not catch is a hole in the sweep, not a
variant to delete.

| Variant | The rule it breaks | What catches it | Needs |
|---|---|---|---|
| `NoPreVote` | thesis §9.6: a rejoining node's election disrupts the leader | pre-vote's property at every heal: the isolated server's term is what it was when the isolation began | partitions |
| `VoteBeforePersist` | Figure 2: persist term and vote before responding | election safety: a crash between the vote and its persist lets the node vote twice in one term | crashes during elections |
| `SendBeforePersist` | the same discipline for `AppendEntries`: append the entry, then respond | commit by majority: an entry a leader committed was not durable on a majority when it did, since `RaftAppend` is traced after the persist; with a crash between the send and the persist, leader completeness | nothing beyond the discipline; crashes between polls for the consequence |
| `ApplyBeforeCommit` | Figure 2: apply only up to the commit index | state machine safety: an index applied under two terms on two servers after a truncation; linearizability: a write acknowledged and lost | the leader isolated with a client |
| `CountOlderTermForCommit` | §5.4.2, Figure 8 | commit by current term: a leader's commit landed on an older term's entry; leader completeness on the seeds where that entry is then overwritten | a follower behind by more than a batch when a leader takes over, so the older entries and the leader's own arrive in separate messages: the sweep runs small batches |
| `TruncateOnEveryAppend` | moirae rule 3 | committed entries stay: a server truncated at or below its own commit index | duplication and reordering (issue #1) |
| `IndexFirstElectionRestriction` | §5.4.1: compare last terms first | leader completeness | crashes and partitions |
| `ResetTimerOnAnyRpc` | moirae rule 5 | timers fire: a follower that heard from no leader of its term and granted no vote for three maximum election timeouts did not campaign | a deposed leader whose heartbeats still arrive one-way while the new leader is down: the stale-leader schedule |
| `LeaseTrustsTheClock` | §1 above: no drift guard | invariant 6, lease safety under drift: the checker reports a stale lease read with no revoke before it | the drift violation |
| `ApplyNotAtomicWithIndex` | §3: the applied index written in a separate batch | state machine safety per node: an entry applied twice after a crash; and `Cas` in the linearizability check | crashes during apply |
| `SnapshotWithoutCurrentLast` | §1: a snapshot installed from a staging directory without `CURRENT` written last | state machine safety after a crash mid-install: the node comes back on a state that never existed | crashes during install |
| `SingleMajorityInJointConsensus` | thesis §4.3: commit needs both majorities | election safety or leader completeness during 3 → 5 → 3 under partition | the membership scenario |

The liveness checks above are bounds, not properties: "a leader exists within k
election timeouts of every heal or crash" with `k` chosen so that the correct variant
never trips it over ten thousand seeds, which is a number the sweep will tell us.

## 6. Order of work, if approved

1. `core.rs` and `message.rs` with the paper's scenarios as tests, no simulator.
2. `store.rs` and `apply.rs` on the engine, with the atomic apply proven by a crash test.
3. `node.rs` and `sim/raft.rs` with elections, replication, crashes and partitions, and
   the four log invariants; the first variants.
4. Reads and `sim/lin.rs`.
5. Snapshots, then joint consensus, each with its scenario and variants.
6. The exit criteria of SPEC §3 and the devlog, "Breaking Raft with moirae".

Each step stops for review.
