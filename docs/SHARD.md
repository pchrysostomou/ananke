# SHARD.md — ananke's ranges and multi-raft

_Status: proposed 2026-09-14, on branch `phase-3-design`. Nothing here is decided until
the owner approves it; the questions in §13 are open, and wherever the text depends on
one it names it, as (Q20). Once approved, each implementation stage turns its part into
a DECISIONS.md entry as it lands, numbered from the footer's next free entry then._

**A stated assumption.** Phase 3 has no transactions: Percolator is Phase 4 (D-006, SPEC
§5). Every "atomic" below is one engine `WriteBatch` on one node (D-024), written by the
apply of one entry of one Raft group. Nothing here is atomic across two Raft groups;
where the design needs two groups to agree, it orders their entries and re-checks every
precondition at apply, where every replica sees the same state. RAFT.md's own
assumption, a disk honest about `fsync`, holds for every group, and the sweeps keep
`p_durable = 1` for the same reason (D-026).

**What this document does not inherit.** Checking the documents against the tree found
four things. Phase 2 is merged and not tagged, and a phase is done only when tagged
(D-011; BOOTSTRAP_PROMPT.md:209-212). RAFT.md §4 describes `Scan(range)` and a scan
check; `ClientOp` has `Put`, `Get`, `Delete` and `Cas` only
(crates/ananke-env/src/trace.rs:742-770) and `sim/lin.rs` checks single keys
(sim/lin.rs:15-24, 212-238). RAFT.md §5 lists `VoteBeforePersist` and
`ApplyNotAtomicWithIndex`; `Variant::BUGS` has sixteen arms and neither
(crates/ananke-raft/src/core.rs:159-176). RAFT.md §3 gives the frame as `kind | term |
from` and lists `src/read.rs`; the code writes `kind | from | term`
(crates/ananke-raft/src/message.rs:4, 383-385) and has no `read.rs`. This document cites
the code where the two differ (Q1). Paths below are relative to the repository root;
`core.rs`, `node.rs`, `store.rs`, `apply.rs`, `client.rs`, `message.rs`, `types.rs`,
`snapshot.rs` and `invariants.rs` are in `crates/ananke-raft/src/`, `engine.rs`,
`wal.rs` and `manifest.rs` in `crates/ananke-storage/src/`, `trace.rs` and `moirae.rs`
in `crates/ananke-env/src/`.

Sources of truth, in order: SPEC §4, which fixes the outline; RAFT.md and the entries it
names, for everything inside one Raft group, which this document changes only where it
says so and asks; the paper and the thesis as RAFT.md cites them. SPEC §4 names "the
Spanner/CockroachDB pattern" (SPEC.md:287-288); for mechanisms it does not spell out,
CockroachDB's range design is the reference — descriptors with a generation, meta
records keyed by a range's end key, a subsume that freezes the right-hand range before a
merge, replicas that exist before they hold data, a snapshot refused when it overlaps
another replica, removed replicas collected by consulting a newer descriptor. A borrowed
mechanism is named as borrowed and is a proposal here, not a source. Where this document
and SPEC disagree, SPEC wins until the owner approves the change (Q20).

## 1. Ranges, descriptors and the meta range

**Ranges and keys (SPEC §0, §2.6).** A range is a contiguous interval `[start, end)` of
the engine's key order over encoded keys, `tenant | table | user_key` (SPEC.md:196-201).
Ranges tile the addressable keyspace: every key outside tenant 0 belongs to exactly one
range at any applied state of that range. Tenant 0 holds each replica's own Raft state
(RAFT.md §3) and belongs to no range. A split key falls between user keys. In Phase 3 no
key carries a version, so every encoded key is a boundary; once Phase 4 writes the
`!version` suffix (D-023), a split key must carry no suffix, so the versions of one user
key never straddle two ranges.

**The descriptor.** Every range has one:

```
RangeDescriptor {
    range:      RangeId,         u64, never reused (§5)
    start, end: Key,             the span [start, end); the last range's end is the
                                 keyspace's end
    generation: u64,             rises at every split, merge and configuration change
                                 of the range (Q6)
    voters:     Vec<NodeId>,     the plain configuration the range last committed;
                                 joint configurations and learners stay Raft's
                                 (RAFT.md §1) and never appear here
    state:      Live
              | Merging { right: RangeId, right_generation: u64,
                          begun: Index }                                 (§6)
              | Subsumed { into: RangeId, begun: Index, index: Index }   (§6)
}
```

`begun` is the index of the `MergeBegin` entry in the left range's log, which names one
merge attempt; `index` is the index of the `Subsume` entry in the right range's log.

Generations carry the rule the rest of the design rests on: **along the ownership of any
key, generations strictly rise.** A split gives both halves the parent's generation plus
one; a merge gives the merged range the larger of the two generations plus one; a
committed configuration change gives the range its generation plus one (Q6). By
induction, a range that owns a key after any change of that key's owner carries a higher
generation than every range that owned it before. So among any set of descriptors, the
one with the highest generation whose span contains `k` names `k`'s owner or an owner no
older than any other in the set names, whatever order the set was learned in. The client
cache (§3) and the meta range (below) both merge by this rule, and check 10 of §8 folds
it over every write.

**Where descriptors live.** In two places, of which one is the authority (Q3):

- *Range-local.* Every replica holds its range's descriptor under tenant 0, beside the
  range's Raft state (key layout Q5), written in the same batch as the apply that
  changes it: a split, a subsume, a merge, the apply of a `C_new`. A replica serves,
  applies and answers `RangeMismatch` from this copy alone (§3). By state machine safety
  every replica of a range holds the same descriptor at the same applied index, and
  check 7 of §8 folds that.
- *Meta.* The meta range holds a record per descriptor it has been told, for addressing.
  It is written after the change it records, by a request to another Raft group, so it
  may lag; it never goes back.

The two cannot be atomic with each other without a transaction spanning two groups, and
Phase 3 has none (D-006). The draft makes the range-local copy the authority and meta an
index that lags and self-corrects; the other options are a commit protocol between two
groups built for Phase 3 alone, or meta as the authority with a range reading it before
serving (Q3).

**The meta range's state machine.** It applies one command, `MetaUpdate { descriptors
}`. For each descriptor `d` in it, every maximal sub-interval of `d`'s span that meta
names by a record of lower generation, or names by nothing, is from then on named by `d`
restricted to it; a sub-interval named at a higher generation is left alone. In the
draft, records live under the meta span keyed by their end key, each carrying its
start, range, generation and voters, and a record partly overwritten is cut at `d`'s
boundaries in the same batch; the key (end or start) and the cut are Q4. The state after
applying a set of updates is a maximum by generation per key and does not depend on
their order, so a `MetaUpdate` may be sent any number of times by
any holder of a real descriptor: a resend whose answer was lost changes nothing, and
D-026's rule against resending a write (DECISIONS.md:851-854) has nothing to protect
here. A lookup of key `k` reads the first record whose end key is above `k`, a bounded
seek the engine lacks (§11).

The leader of a range sends `MetaUpdate` after it applies a change of its descriptor —
both halves at a split, the merged range at a merge, the new voters after a `C_new` —
and the node that sent it resends until the meta range's leader acknowledges it, whether
or not it still leads the range (Q3). A leader also sends its
range's descriptor when it takes office, which repairs an update lost with a leader that
crashed between its apply and its send (whether repair also runs on a timer is Q3). A
record may name a range that no longer exists or replicas that no longer hold it; a
client routed by it is answered `RangeMismatch` with better descriptors (§3). Check 16
folds the meta range.

**Addressing (SPEC §4).** SPEC says range 0 is found through configuration and the meta
range through range 0 (SPEC.md:287-288). The draft assumes two groups (Q4): range 0, the
root, holds the meta range's descriptor, the range-id counter (§5) and the node records
(§2, Q8); range 1 is the meta range and holds the records of every other range; neither
splits nor merges in Phase 3. A record is about 190 bytes for keys of up to 64 bytes and
three voters, so ten thousand ranges are about 2 MB of meta, a small part of one 64 MiB
memtable (engine.rs:154), and one range holds it. Where the root and meta spans sit in
the keyspace is Q5.

## 2. Bootstrapping

**What configuration names.** A server's configuration today is its id, its address, an
address book, the voters a fresh store starts with, the core's and the engine's
parameters and an inbox size (`NodeConfig`, node.rs:122-142). A node's configuration in
Phase 3 names its id and address, the addresses of range 0's replicas, and whether this
node is one of them, a bootstrap node. A client's configuration names range 0's replicas
and nothing else.

**A fresh cluster.** Every bootstrap node whose store is fresh writes, in one synced
batch before its tasks run, the same initial state, computed from configuration alone
(Q7):

- range 0, the root span, range 1, the meta span, and range 2, the rest of the keyspace,
  each at generation 1 with the bootstrap nodes as voters, a hard state of term 0 and no
  vote, an applied index of 0 and its configuration at index 0. This ties the number of
  bootstrap nodes to the replication factor: §7's moves are one-for-one and never
  change a range's voter count, so five bootstrap nodes would leave every range at five
  voters for good. The draft requires as many bootstrap nodes as the replication factor,
  three (Q7, Q32);
- in range 0's state, range 1's descriptor, the range-id counter at 3 and one node
  record per bootstrap node;
- in range 1's state, the meta record for range 2.

This is `initial_voters` generalised: a fresh store starts with what configuration says,
and a store that holds a configuration already ignores it (node.rs:130-135; D-029,
DECISIONS.md:1105-1110). The trace already allows a configuration at index 0
(trace.rs:443). Each node traces `RangeCreated { cause: bootstrap }` for each range
(§8), which gives the checker every range's first descriptor, voters and floor instead
of `1..=servers` (invariants.rs:289-295), and `MetaApplied { index: 0 }` for range 1's
record of range 2, which gives check 16 its first record.

**A node that joins.** It starts with no ranges and knows range 0's addresses. It is
added by an operator's `AddNode`, a write to range 0 (Q8); the rebalancer then moves
replicas to it (§7). It creates a replica when a leader's message first names a range it
does not host, uninitialised until a split or a snapshot gives it state (§5).

**A client's first request.** A cache miss reads range 1's descriptor from range 0 and
then the record for the key from range 1, each a read served by that range's leader by
read-index or lease (RAFT.md §1). Whether these lookups enter the history is Q36; they
are advisory either way (§3).

**What bootstrap does not cover.** Range 0's own replicas are fixed by configuration. If
the rebalancer may move range 0 or range 1, clients and joining nodes need another way
to find range 0; the draft does not move either (Q9).

## 3. Routing and the client cache

**What a request carries.** `tag | client | seq | range | generation | command`: the
range the client routed to and the generation of the descriptor it routed by (Q10). A
reply is `Outcome`, `NotLeader { leader }` or `RangeMismatch { descriptors }`.
`NotLeader` names a node (client.rs:50-59); with the range in the request it is no
longer ambiguous on a node that holds many ranges.

**Where a server checks, and against what.** Always against its own descriptor, never
the client's, in three places:

1. *At receipt.* A node with no replica of `range` answers `RangeMismatch` with every
   descriptor it holds whose span contains the key. A replica whose descriptor does not
   contain the key, or is subsumed, answers `RangeMismatch` with its own descriptor and
   every other it holds that contains the key — at a split, the right half it created in
   the same apply. Nothing is proposed.
2. *At a read's serving.* The core hands a read ready at an index and the server serves
   it from applied state (node.rs:2092-2115); the server checks the key against the
   descriptor at the applied index it serves at, so a split or subsume applied between
   receipt and serving answers `RangeMismatch`. A get never enters the log (RAFT.md §3),
   so this is its last check. Today the server reads the engine's latest state with no
   version pinned (`engine().get`, node.rs:2099) while the `apply` task keeps writing, so
   "the applied index it serves at" is not yet defined: the value, the descriptor and the
   applied index must be read at one engine version (§11, raft item 15).
3. *At apply.* A command whose key lies outside the span of the descriptor in force
   before its index, or whose range is subsumed, applies as nothing: the batch advances
   the applied index and writes no user key, and the outcome is `RangeMismatch`. Every
   replica applies that entry against the same descriptor, so every replica refuses it
   alike, and check 4 of §8 now compares the effect as well as the payload. Today a
   command for any key applies (apply.rs:244-274).

The receipt check is advisory: a leader's applied descriptor lags its log. The apply
check is what makes a split take effect for proposals already in flight — a write the
parent's leader appended after the split entry, before it had applied the split — and is
safety for writes. The read check is safety for reads.

**What `RangeMismatch` guarantees.** A write answered `RangeMismatch` did not take
effect: refused at receipt, it was never proposed; refused at apply, its entry applied
as nothing on every replica. The client may send it again as the same operation, the
same `(client, seq)`, to the range the descriptors name (Q10). A write that gets no
answer is abandoned as pending, as today (D-026; client.rs:12-19), because a retry
across that ambiguity is a second write until client sessions exist, issue #21 (Q11).
The history treats `RangeMismatch` as it treats `NotLeader`: followed inside the client,
never a return (§9).

**Duplicates across ranges.** A leader's record of what it proposed, by `(client, seq)`,
is per server and per log (node.rs:1978-1995, RAFT.md §3). Because a request names its
range, a copy the network delivers to another range's leader is refused at receipt and
never proposed there, and a range's map needs no knowledge of another's.

A resend after a definite `RangeMismatch` is meant to be a new proposal whose earlier
copy applied as nothing, but the map as built does not always let it be one. A leader
skips a request whose `(client, seq)` it recorded while that entry is still in its log,
and answers it only "when that entry applies, once" (node.rs:843-849). Suppose a write
is refused at apply in P after a split, R later merges back into P, and the client
follows R's `RangeMismatch` back to P. If P's leader has not changed and the entry is
not compacted, it finds the record, proposes nothing, and never answers; the client
abandons a write that never took effect. The draft has the leader forget a record once
its entry applies with an effect other than `applied` (§8), which the `apply` task
reports back with the outcome: a copy that applied as nothing took no effect, so
proposing the resend cannot apply one operation twice. The alternative is a resend under
a fresh `seq`, which the history must then pair with the first (Q10).

**The client cache.** An ordered map from end key to descriptor, with a leader hint per
range. It merges by the generation rule of §1: a descriptor learned replaces the cached
entries it overlaps that carry a lower generation, and where an entry of a higher
generation overlaps it, the entry stays and the learned descriptor is dropped for that
part. Descriptors are learned from meta lookups and from `RangeMismatch`. A
`RangeMismatch` that carries nothing evicts the entry and looks the key up again;
`NotLeader` updates the hint; a get with no answer tries another replica, a write with
no answer is abandoned, as the sweep's client loop does today for one group
(sim/raft.rs:3015-3042), which has no cache, no lookup and no `RangeMismatch` to follow
(§11, raft item 16). The cache is advisory: with the three checks above, a stale cache
costs a round trip and never a wrong answer. That is the
rule §10's `TrustStaleDescriptor` breaks.

**How routing converges.** After a split a client with the parent's descriptor sends a
right-half key to the parent, whose `RangeMismatch` carries the right half's descriptor:
routing converges without meta. After a merge a client sends a right-half key to the
subsumed range; a node that still holds it frozen answers with its own descriptor and
the merged range's if it holds that; a node that holds neither answers with nothing and
the client reads meta, which names the merged range once its leader has sent the update
(§6). After a move the client's replicas may have left; `NotLeader` hints and replica
rotation reach the new ones, and `RangeMismatch` with the new voters corrects the cache.

## 4. Multi-raft: one ticker, batched frames, and what breaks at 1 000 and 10 000 ranges

**Today's unit is a group.** One server is one Raft group. `run` binds one socket and
spawns `net` (node.rs:304-346), `apply` (node.rs:1056) and `snapshot` (node.rs:602-603),
and runs the `raft` loop itself, racing its inbox against one `sleep_until` per tick
(node.rs:700-713); its engine spawns a `flusher` (engine.rs:969) and a `wal-writer`
(wal.rs:647). Each `run` takes its own listen address and engine directory
(`NodeConfig`, node.rs:122-142), so N groups in one process are N `run` calls, each with
all of that. Nothing on the wire, in the store or in the trace names a group: a frame is
`kind | from | term | fields` (message.rs:1-8), the Raft keys are fixed names under
tenant 0 (store.rs:65-127), and every `Raft*` event names a server alone
(trace.rs:368-652).

**The constants the arithmetic uses.** A tick is 10 ms (core.rs:377); a leader
heartbeats every 2 ticks, 20 ms (core.rs:374); election timeouts are 10 to 20 ticks
(core.rs:373); the sweep uses the same, `ELECTION_MIN` 100 ms and `TICK` 10 ms
(sim/raft.rs:106-108). A heartbeat is an AppendEntries with no entries: kind, sender and
term, then previous index, previous term, commit, `sent` and an entry count of zero,
53 bytes; its response is kind, sender, term, a success byte and six u64 fields
(previous index, match index, hint, echo, local clock, incarnation), 66 bytes
(message.rs:381-440). A step that appends nothing and changes no term, vote or
configuration persists nothing (core.rs:1278-1298), so an idle follower never syncs. An
engine's memtable is 64 MiB and its log segments 16 MiB (engine.rs:154-155). The log
above the last snapshot lives in the core's memory (D-025, DECISIONS.md:811-812) until
the leader's log is `snapshot_threshold`, 4096 entries, past its last take
(core.rs:381). Only a leader compacts: `maybe_compact` returns at once on any other role
(core.rs:1830-1833), and a follower's log shrinks only by truncation (core.rs:1693) or
when an install replaces its store, since `RaftStore::open` deletes log keys only at or
below the snapshot record (store.rs:686-690). A follower's in-memory log therefore grows
without bound while its range takes writes. An entry of a 64-byte command is 85 bytes on
the wire (message.rs:347-354); in memory an `Entry` is two `u64`s and a `Payload` whose
largest arm is a `Configuration` of three vectors (types.rs:23-30, 67-88), about
88 bytes by its field types (not measured), plus the command's 64 heap bytes, so at
least 152 bytes, and on a follower the command is a slice of its whole AppendEntries
frame (message.rs:324), which it keeps alive. 4096 such entries are at least 608 KiB. A
leader remembers up to 4096 proposals (node.rs:1995). A snapshot chunk is 256 KiB
(core.rs:382), one outstanding per
stream, and a leader streams to every designated follower at once (D-043). The sweep's
inbox holds 128 messages (sim/raft.rs:2859) and drops the oldest heartbeat of any sender
first (node.rs:1918-1962). `RealEnv` queues 1024 frames per destination
(crates/ananke-env/src/real/net.rs:30); `SimEnv` bounds nothing
(crates/ananke-env/src/sim/net.rs:271-275). A frame is at most 16 MiB
(crates/ananke-env/src/net.rs:25). Every frame the simulator carries is a `MessageSent`
record holding its payload and a `MessageDelivered` record
(crates/ananke-env/src/sim/net.rs:150-158, 271-285); a raft sweep run stops at
400 000 records (`TRACE_CAP`, sim/raft.rs:136).

The tables assume three replicas per range, leaders spread evenly, and ranges with no
client load; a per-node figure assumes ten nodes (the balance scenario's node count is
Q30). A loaded case follows the tables. Nothing below is measured.

**Today's shape at scale**, R ranges as R groups of three `run` calls:

| | per range | 1 000 ranges | 10 000 ranges |
|---|---|---|---|
| heartbeats and responses | 200 frames/s | 200 000 frames/s | 2 000 000 frames/s |
| their bytes | 11.9 kB/s | 11.9 MB/s | 119 MB/s |
| core ticks, each a timer wake | 300/s | 300 000/s | 3 000 000/s |
| tasks, six per replica | 18 | 18 000 | 180 000 |
| sockets (`RealEnv` TCP connections, two peers each) | 3 (6) | 3 000 (6 000) | 30 000 (60 000) |
| WAL writers, each its own fsync stream | 3 | 3 000 | 30 000 |
| memtable budget | 192 MiB | 187.5 GiB | 1.83 TiB |
| sweep records from idle heartbeats alone | ≥ 400/s | ≥ 400 000/s | ≥ 4 000 000/s |

At 1 000 ranges this shape does not fit a process, and a sweep run reaches `TRACE_CAP`
within one virtual second of idle heartbeats.

**The proposed unit is a node.** A node owns the tasks, the socket and the engine, and
hosts many groups (Q2, Q14):

- One socket. `net` decodes a frame into messages each tagged with its range, and the
  inbox's policy applies per message (its capacity is Q14).
- One `raft` task holding every core on the node, keyed by range, driven by one ticker:
  each tick steps a `Tick` into every core (unless ranges quiesce, Q12), and each
  message steps into its range's core. After a round — a tick's steps, or the messages
  drained since the last round — the task gathers every `Persist` of the round into one
  `WriteBatch` with `sync: true`, awaits it, and only then hands the round's `Send`s to
  a per-peer outbox, flushed at the end of the round as one frame per peer and cut under
  `MAX_FRAME_LEN`. A core whose step emitted a `Persist` steps nothing more in that
  round, so no step of a core runs on state its own disk does not yet hold. Figure 2's
  discipline (RAFT.md §1) holds per group: no message of a round leaves before every
  persist of the round is durable. The price is that one group's sends wait on other
  groups' persists in the same round, including the sends of a core that persisted
  nothing. Each step still stamps its own decision time (D-047). This is not the order
  RAFT.md §3 and D-026 fix — the `raft` task "executes every output in order, awaiting
  each `Persist` before the `Send`s that follow it" (RAFT.md:551-554;
  DECISIONS.md:831-832) — under which a send that precedes a persist, or belongs to a
  step that persisted nothing, leaves at once. One task or one per range, a round's
  persists merged or awaited per core, the flush per round, per tick or on a timer, and
  the rule that a core stops stepping after a persist are Q41.
- One `apply` task taking each range's applies and snapshot takes in turn, and one
  `snapshot` task whose streams are keyed by range and follower (Q14). Under D-036 a
  take is a job between two applies and "applies wait behind the take" (RAFT.md:177-181),
  so with one `apply` task per node one range's take stalls every range's applies on the
  node. The `snapshot` task sends its chunks on its own socket handle, as today
  (node.rs:602-611), in frames of their own and not through the per-peer outbox; the
  draft keeps that (Q41), so a chunk never shares a frame with a heartbeat.
- One engine, each range's Raft state under tenant 0 keyed by range (Q2, Q5). The WAL
  writer already shares one fsync among everything queued (wal.rs:14-20, D-018).
- A core's private generator is seeded from the node's protocol stream when its
  incarnation starts (node.rs:551, core.rs:932). With many cores on a node a core's seed
  depends on how many drew before it, so a split would move every later range's election
  timeouts, which D-017 exists to prevent (DECISIONS.md:336-341). The draft derives a
  stream per node and range, `n{id}/r{range}/protocol` (Q13).

**Heartbeats (SPEC.md:289-290).** SPEC asks that ten thousand ranges not mean ten
thousand heartbeat streams. Three ways, not exclusive (Q12):

- *Batching*: every group's AppendEntries and response stays as it is, many to a frame.
  The lease's `sent`, `echo` and `local` (RAFT.md §1, as built, D-028), read-index
  rounds, check quorum's per-follower marks and D-049's rule for refused followers stay
  per group and unchanged. Frames per node pair stop growing with ranges; bytes and core
  steps do not.
- *Coalescing*: one heartbeat per node pair per interval carrying each range's term and
  commit. A follower's lease promise is made per request and per group, measured from
  that request's `sent` (RAFT.md:99-106, 126-140); a coalesced heartbeat must carry
  every group's `sent` or restate the promise per node pair and show that sufficient,
  and no argument in RAFT.md or DECISIONS covers that. If a heartbeat entry is range,
  term, commit and `sent`, 32 bytes, and a response entry range, term, a success byte,
  `echo` and `local`, 33 bytes (an assumed layout, with the store incarnation once per
  frame), a range's idle traffic is 100 × 32 + 100 × 33 = 6 500 bytes a second against
  batching's 13 500: 6.5 MB/s over the cluster at 1 000 ranges and 65 MB/s at 10 000, a
  factor of about two. Unless the receiving node answers for its cores without stepping
  them, core steps do not change.
- *Quiescence*: a range whose leader has nothing to send and whose followers have all
  matched stops heartbeating and ticking. A quiesced range's idle steps and bytes go to
  nothing, so an idle node's 500 000 steps a second at 10 000 ranges fall to those of
  the ranges not quiesced; its first proposal pays a wake, the leader's next
  AppendEntries. But a quiesced follower has no timer to notice its leader's death, a
  quiesced leader runs no check quorum (RAFT.md:262-276) and holds no lease, and noticing
  a dead leader needs a signal the tree does not have; CockroachDB uses a node-liveness
  record for it.

The draft assumes batching alone and says below where that stops holding.

**The proposed shape at scale**, batching only. The ranges led on node `a` with a
follower on node `b` number 2R / (N(N − 1)) for N nodes: 22 at 1 000 ranges and 222 at
10 000 on ten nodes. A leader heartbeats on one phase of the two-tick interval, set by
when it took office, and nothing assigns phases, so all of a pair's ranges may share one.
With the draft's flush at the end of each round (Q41), a tick's round sends each ordered
pair at most one heartbeat frame; the responses to one arriving frame leave in one frame
if one round drains the whole frame, and in several if the inbox splits it across drain
rounds, which the 128-message inbox against 200 arrivals a tick at 1 000 ranges can do.
So an ordered pair carries one heartbeat frame and one response frame per tick, 200
frames per second, 18 000 over ten nodes, whatever the range count, only while frames
are drained whole; a flush per tick would make that a bound. A range id adds 8 bytes to
each message (Q10): 61 and 74 bytes.

| | 1 000 ranges | 10 000 ranges |
|---|---|---|
| frames per second, ten nodes, frames drained whole | 18 000 | 18 000 |
| messages in a frame, phases spread evenly / all on one phase | about 11 / 22 | about 111 / 222 |
| largest frame, all on one phase (22 or 222 responses of 74 bytes) | 1 628 B | about 16.4 kB, against 16 MiB |
| bytes per second over the cluster | 13.5 MB | 135 MB |
| bytes out of one node | 1.35 MB/s | 13.5 MB/s, about 108 Mbit/s idle |
| replicas per node | 300 | 3 000 |
| core steps per node per second: ticks, heartbeats as follower, responses as leader | 30 000 + 10 000 + 10 000 | 300 000 + 100 000 + 100 000 |
| steps in one tick of the one `raft` task: ticks and arriving messages | 300 + 200 | 3 000 + 2 000 |
| messages arriving at a node per tick | 200 | 2 000 |
| tasks per node | 6 | 6 |
| memtable budget per node | 64 MiB | 64 MiB |
| in-memory logs of the node's leader replicas, each at the threshold with 64-byte commands (100 or 1 000 × 608 KiB) | about 59 MiB | about 594 MiB |
| in-memory logs of the node's follower replicas under writes | unbounded | unbounded |
| payload bytes a run of an assumed 20 s holds in `MessageSent` records (a run's length is its schedule's, sim/raft.rs:777-790) | about 270 MB | about 2.7 GB |

The tree has no measurement of a core step's cost. If a step costs `c`, an idle tick
costs 500 `c` at 1 000 ranges and 5 000 `c` at 10 000, and the task falls behind its
ticker once that and the round's sync exceed a tick of 10 ms: at 10 000 ranges, once `c`
passes 2 µs with no sync at all. That budget is an assumption the node's first stage must
measure.

**The loaded case.** Suppose some ranges on a node take writes, enough that every round
on the node has at least one `Persist`. Then under the round every round waits for one
synced batch, and every co-hosted range's heartbeats and responses leave only after it,
whether or not their own range wrote anything. The delay each round adds is one sync:
100 µs to 2 ms per disk operation on the sweep's disk (sim/raft.rs:2807-2808); the tree
records no real disk's `fsync` latency, and a shared or failing disk can take far longer.
The delay is the same at 1 000 ranges as at 10 000; what grows with ranges is how many
groups it lands on. Against it: heartbeats every 20 ms and a minimum election timeout of
100 ms (core.rs:373-377); a lease promise measured from `sent` and so shortened by any
delay after the request is built (RAFT.md:126-137); check quorum every minimum election
timeout; and the drift guard's window of 400 ms (core.rs:379), whose fastest response per
window absorbs the delay only if some round in the window synced quickly. A sync of a few
milliseconds costs little. A sync longer than about 80 ms, a minimum election timeout
less a heartbeat interval, is to every range led on that node what a crash is: their
followers' timers fire together.

**What breaks at 1 000 ranges**, batching only:

- Nothing on the wire or in the process while idle: 13.5 MB/s over the cluster and
  50 000 core steps per second per node.
- Under load, a slow sync on one node: one sync past about 80 ms starts about 100
  elections, one per range the node leads, and every leader elsewhere whose majority
  needs that node's responses waits with them; check quorum steps down any whose other
  follower is also slow.
- The sweep's trace. Ten nodes' frames alone are up to 36 000 records a second, so an
  assumed 20 s run passes 400 000 records at any range count once every pair carries
  traffic; 270 MB of payload sits in `Sim`'s trace and is copied at the end
  (sim/raft.rs:3552, 3573), once per seed running in parallel (D-040); the JSONL export
  of every record was already 6.9 % of the raft binary's time at D-046
  (DECISIONS.md:2856-2866).
- The inbox. 200 messages a tick against a capacity of 128: unless the `raft` task
  drains it within the tick, the policy drops the oldest heartbeat of any range
  (node.rs:1918-1962), which costs a follower its timer reset and a leader a promise.
  Each admission also scans the whole queue, once to count its messages and up to twice
  to find a victim (node.rs:1933-1950; queue.rs:103-110 in `crates/ananke-raft/src/`),
  so a tick's admissions cost arrivals × capacity (§11, raft item 11).
- PCT's hint. It counts one poll per node per millisecond
  (crates/ananke-env/src/sim/mod.rs:155-164), which sets the change-point rate (D-016,
  DECISIONS.md:314-318); a node's work grows with its ranges and the hint does not.
- Recovery. With one engine per node (Q2) a lost table refuses every replica on the node
  (store.rs:648-650; engine.rs:1127-1136): 300 re-seeds, each a stream from its own
  leader, up to 300 × 256 KiB = 75 MiB of chunks outstanding toward one node, and the
  install silence D-049 measured at a median of 185.6 ms (DECISIONS.md:3414-3433) falls
  on every range with a replica there at once (Q15). As built the 300 streams cannot run
  at once: a node's receiver stages under one directory and holds one stream, and a chunk
  of another identity abandons the stream in progress (snapshot.rs:92-94, 859-915), so
  they restart each other until §11's raft item 14 keys staging by range.
- Quarantine. Each of those 300 replicas is then on a re-seeded store and never votes
  again (D-035; RAFT.md:523-528). Once two nodes have each lost a table, the ranges with
  replicas on both, 8 of every 120 placements of three replicas on ten nodes, about 67,
  have one replica that can vote: each can keep a leader it has and cannot elect one, and
  those whose leader was on either refused node have none. Until the rebalancer moves
  quarantined replicas off (Q33), that is permanent.
- A node's crash starts about 100 elections, one per range it led, within one election
  timeout.
- Follower logs. A node's 200 follower replicas compact only by install (above), so under
  steady writes their in-memory logs grow until something re-seeds them (§11, raft
  item 13).

**What breaks at 10 000 ranges**, batching only:

- The wire and the CPU of ranges with nothing to do: 135 MB/s over the cluster and
  500 000 core steps a second per idle node. This is the cost SPEC's sentence names, and
  batching does not remove it: coalescing halves the bytes, to about 65 MB/s, and leaves
  the steps; quiescence removes both for quiesced ranges and needs a liveness signal
  (Q12).
- The `raft` task's tick: 5 000 steps per 10 ms tick while idle, more than it can take
  at a step cost above 2 µs.
- Under load, a slow sync on one node: one sync past about 80 ms starts about 1 000
  elections.
- The inbox: 2 000 messages a tick, each admission a scan of the queue.
- A node refusal: 3 000 re-seed streams toward one node, up to 3 000 × 256 KiB =
  750 MiB of chunks outstanding; a node crash: 1 000 elections.
- Quarantine: after two nodes have each lost a table, about 667 ranges with one replica
  that can vote, unable to elect a leader (Q33).
- In-memory logs: about 594 MiB per node for its leader replicas at the snapshot
  threshold, and its 2 000 follower replicas' logs unbounded under writes.
- The simulator: 2.7 GB of payload per run. SPEC asks the simulator for 1 000 ranges
  (SPEC.md:303); 10 000 stays a statement about the design and not a scenario (Q39).

## 5. Split

**The proposal (SPEC §4: "leader proposes split at key `k`").** The parent range P,
`[start, end)` at generation g, is split by its leader, asked by an operator's
`Command::Split { key }`, by a size threshold, or by the sweep's driver (Q18). The
leader first takes a range id for the right half from the counter in range 0, a
compare-and-set on one key in another group; an id taken and never used is a gap and
harmless (Q17). It then checks, for liveness only: `start < key < end`, P's descriptor
`Live`, the configuration in force plain and committed, and no change in flight,
catch-up included (Q23; one change in flight, DECISIONS.md:1099-1104). It proposes
`Split { key, right }`, an entry like any client command: an arm of `Command`
(apply.rs:35-77) the state machine applies, not a `Payload` kind, since the protocol has
no need to know it (types.rs:21-30). The left half keeps P's id and the right half takes
the new one (Q19).

**The apply.** Every replica of P applies the split at the same index `s` and re-checks
there what the leader checked, against state every replica shares: the key inside the
span of P's descriptor in force before `s`, that descriptor `Live`, and the latest
configuration entry at or below `s` plain. A split that fails any check applies as
nothing. Otherwise one synced batch on each replica holds:

- P's applied index, `s`;
- P's descriptor, `[start, key)` at generation g + 1;
- the right half R's descriptor, `[key, end)` at generation g + 1, P's voters, `Live`;
- R's Raft state under R's own keys: a hard state with no vote and the term of Q20; an
  applied index of `s`; a snapshot record of last index `s`, the last term of Q20 and
  P's configuration at `s`, naming no checkpoint; the configuration key; and, under
  Q26's draft of an incarnation and a quarantine flag per replica, copies of those of
  P's replica on this node (under the per-node-store option there are no such keys);
- no user key. With one engine per node R's data is already where it is (Q2). With an
  engine per replica, R's data would have to be copied into a new engine, which no batch
  can be atomic with (§11).

R's Raft state is written only on a node that is a voter of P's configuration at `s`. A
replica of P that is not — a learner P's leader is catching up (D-032), which replays `s`
from P's log like any follower — gets P's shrunk descriptor and, in place of R's state,
a range delete of the right span's keys on its node (§11): otherwise it would hold an R
outside R's configuration, which no rule of §7 ever collects, since its descriptor was
never a member's. The choice between the two is a function of shared state, P's
configuration at `s`, and each node's P descriptor is the same either way. The proposer's
refusal while a change is catching up (Q23) makes this rare; it does not make it
impossible, since a learner added after the split may still replay `s`.

The node then starts R's core from that floor with `Raft::restore_compacted`, which
takes a snapshot index, term and configuration and sets commit and applied to the floor
(core.rs:867-942), and traces, in this order, `RangeSplit`, `RangeDescriptor` for P and
`RangeCreated { cause: split }` for R, then the entry's `RaftApply` (§8). A crash before
the batch is durable re-applies `s` from the log at the restart, exactly once, as every
apply is (RAFT.md §3); a crash after it restarts both replicas from their keys.

**What the two halves inherit.**

| | left half, P's id | right half, R |
|---|---|---|
| span | `[start, key)` | `[key, end)` |
| generation | g + 1 | g + 1 |
| Raft group, log, term, vote | P's, unchanged | a new group; nothing in the log above `s`; term and floor term per Q20; no vote |
| leader | P's | none until elected (Q21) |
| lease | P's leader keeps it; a read for a right-half key is refused at serving (§3) | none; a new leader serves by read-index for two guard windows before its lease (RAFT.md:131-133) |
| configuration | P's; a change catching up on P's leader stays P's (D-032) | P's plain configuration at `s`, no learners |
| data | `[start, key)`, in place | `[key, end)`, in place (Q2) |
| applied index | `s` | `s`, recorded as its snapshot, as SPEC says |
| snapshot record | P's last take, which covers P's whole span | the floor at `s`, with no checkpoint behind it: the first stream asks for a take (node.rs:1722-1735) |
| proposals remembered (node.rs:1978-1995) | P's; entries above `s` for right-half keys apply as nothing (§3) | none |
| pending reads | right-half keys refused at serving | none |
| store incarnation (D-042), quarantine (D-035) | P's replica's | copied from P's replica on the same node (Q26) |
| meta record | P's until the leader's update | none until then; P's record still names R's span at generation g, so meta has no gap (§1) |

The quarantine copy is conservative. A replica is quarantined because its store lost a
vote in some term of its group (D-035); a re-seed of P before the split lost nothing of
R, which did not exist. Q15's draft refuses a whole node at a loss and re-seeds each of
its replicas from its own leader, so each re-seeded replica draws its own incarnation
and carries its own quarantine flag; that is why Q26's draft keeps both per replica, and
why a split copies them. Kept per node store instead, a node that lost one table would be
quarantined in every range for good, replicas added later included (§4).

**How the new group starts.** On each replica at its own apply of `s`, with no message
between replicas. Every replica of R starts from the same state, since each applied the
same entry at the same index. SPEC says the new group "starts at term 1 with the
parent's applied index recorded as its snapshot" (SPEC.md:291-293). Each replica's
applied index when it applies the split is `s` itself, so "the parent's applied index"
is unambiguous. "Term 1" is not: a snapshot records an index and the term of the entry
there (RAFT.md §1; the record under `0 / 3 / snapshot`, RAFT.md §3), the split entry's
term `t_s` is whatever term P's leader proposed it in, and a server's current term is
never below the last term in its log (paper Figure 2: a server that sees a higher term
adopts it). A floor at `(s, t_s)` under a current term of 1 is a state no Raft server
reaches. The options (Q20): (a) R's current term and floor term both `t_s`; (b) current
term 1 and a floor of `(s, 0)`; (c) fixed constants for both, as CockroachDB starts
every right-hand range at the same index and term. The draft assumes (a): it keeps every
invariant RAFT.md states and asks nothing new of `restore_compacted`. If approved, (a)
would supersede SPEC's "term 1" (Q20); until then SPEC's words stand. Option (b) keeps
SPEC's words and asks the core to accept a floor of term 0 above index 0, a state no code
path has been read for.

R has no leader at birth, so its keys are unavailable until some replica's election
timer fires, 100 to 200 ms, and the pre-vote and vote rounds after it. The draft has the
replica of R on the node whose replica of P is leader at the apply start its pre-vote at
once instead of waiting for its timer. That first pre-vote usually fails: it reaches the
other replicas of P within a disk operation or two of the leader's apply, while they
learn that `s` committed only from P's next AppendEntries, up to a heartbeat interval
later, and until each applies `s` its R is a placeholder, which grants no pre-vote
(below). So the draft repeats the pre-vote every heartbeat interval until R has a leader
or the replica's own timer fires; a pre-vote changes no term, so a repeat disturbs
nothing. A pre-vote that reaches an initialised replica of R is granted, since no
replica of R has heard from a leader of R (RAFT.md §1, pre-vote) (Q21).

**The uninitialised replica (Q22).** A node can receive R's messages before it holds R:
its replica of P lags behind `s`, or R is being moved to it (§7). The node creates a
placeholder for R with no descriptor, no span and no Raft state, and traces
`RangeReplicaCreated`. In the draft a placeholder grants no vote or pre-vote,
acknowledges no append, and takes only a snapshot the overlap rule below allows; it
becomes a replica at its node's apply of `s` or at such an install. It cannot vote
twice, because it never votes.

A placeholder must still ask for its snapshot, and silence does not ask. A leader adds a
learner at `next = last + 1` (core.rs:1930-1937) and feeds a snapshot only to a follower
designated snapshot-fed or whose `next` is at or below the compacted prefix
(core.rs:2071); designation needs the follower more than `snapshot_threshold` entries
behind its match and quiet for two minimum election timeouts (core.rs:1446-1455;
RAFT.md:250-257). A silent placeholder of a range whose last index is at most 4096 is
never fed, and any other waits at least two minimum election timeouts. The draft has a
placeholder answer every AppendEntries as a refused server does (RAFT.md:501-511): a
rejection with a hint of 1, an echo of zero and store incarnation 0. The hint moves the
leader's `next` for it to 1, which feeds it the snapshot once the log is compacted and
otherwise draws the previous-index-0 rejection that designates it (D-037). Its answers
count for nothing in a lease or read-index round, as a zero echo does, and for check
quorum only as D-049 counts a refused server's (Q22). A placeholder on a node whose P
still covers its span has its install refused by the overlap rule until P moves on, so
such an answer can start streams that are refused; that cost is part of Q22.

R can still elect: R's voters are P's, P's leader keeps bringing P's replicas up to date,
and each of them applies `s`. The exception is a
replica of P removed before it applied `s`: its node's R is then initialised only by a
snapshot, after the stale P is collected (Q27). The alternative is a placeholder that
votes with a hard state it persists, as CockroachDB's uninitialised replicas do; the
split's apply must then keep that vote.

**The overlap rule (Q27).** A snapshot is installed on a node only if its span overlaps
no other initialised replica there; otherwise it is refused and asked for again later.
It keeps one owner per key per node (check 8 of §8). Without it, a lagging replica of P
that still covers `[key, end)` applies P's entries below `s`, which write right-half
keys over the newer state R's snapshot put there (§10, `SnapshotOverlapsReplica`). The
same rule refuses P's pre-split snapshot, whose span is P's whole span, on a node where
R is initialised; P's leader must then stream a newer one. How the refusal reaches the
leader, and what the leader does, are new protocol steps that RAFT.md does not settle.
Today a retake starts only on the sender's side, when the leader finds no complete
version to stream (`StreamFailed { retake: true }`, node.rs:1710-1735), and a receiver's
ask to start over marks a checkpoint unusable only at its third ask (RAFT.md:195-199).
The draft adds an install answer, `Overlaps`, on which the leader ends the stream and,
after a minimum election timeout, streams again: from a fresh take if its checkpoint lies
below the last split or merge its range applied, since only a newer snapshot can stop
overlapping, and otherwise from the same checkpoint, since the overlap is the receiver's
to clear. The alternatives are to reuse the start-over ask, or an immediate retake at
every refusal (Q27). A snapshot that installs onto a replica that is already
initialised, P's post-split snapshot onto a lagging P for instance, changes that
replica's descriptor and is traced as a `RangeDescriptor` at the snapshot's last index
(§8). A node's replica of P that already applied past a snapshot answers it installed
without switching (RAFT.md:222-224), so a replica of P that applied `s` is never set
back below it. The same rule makes the apply's "R already initialised here" unreachable:
R could have been installed on the node only if no replica of P covered its span, and
then no replica of P is there to apply `s`. The apply asserts it.

**What is atomic with what.**

- Atomic, on each replica, in the apply batch of `s`: P's applied index, both
  descriptors, and R's Raft state. With one engine per node that is the whole split
  (Q2).
- Not atomic, and safe by order and by the checks of §3: the id taken from range 0,
  before and in another group; R's core and its first election, after, volatile, and
  restarted from the batch at a crash; the meta update, after, idempotent, and repaired
  when a leader takes office (§1); every client's cache.
- Across replicas nothing needs to be atomic: each applies `s` to the same state.
- Split with the descriptor update is the atomicity §10's `SplitNotAtomicWithDescriptor`
  breaks. Split with meta is not atomic by design (Q3).

## 6. Merge

**The preconditions (SPEC §4: "only adjacent ranges with identical replica sets").** A
left range L, `[a, k)` at generation g_L, and a right range R, `[k, b)` at generation
g_R, merge into L only if: L's end is R's start; both descriptors are `Live`; the
configurations in force on both are plain, with the same voters and no learners; and
neither has a split or a change in flight. *Identical* means the same voter set in the
configuration each range has committed, and no joint configuration or learner on either.
SPEC's "two-phase with a subsume command" leaves the phases to this document; the draft
uses three entries and an abort (Q24).

**The coordinator.** L's leader, asked by the rebalancer or an operator's
`Command::Merge { right }` (Q24, Q25). It reads R's descriptor from R's leader, not from
meta, which may lag.

**1. `MergeBegin { right: R, right_generation: g_R, voters }` in L's log.** Each replica
of L re-checks at its apply, at index `h`, that L is `Live` and that its configuration in
force is plain with exactly `voters`, and sets L's state to `Merging { right: R,
right_generation: g_R, begun: h }`. `h` names the attempt, and every later entry of the
merge carries it. A merging L refuses splits, and its node refuses `Change` for it;
writes to L go on. The coordinator accepts no `Change` from the moment it proposes
`MergeBegin`, and checks again that its core has no change in flight before it proposes
step 3; a joint entry that reaches L's log after `MergeBegin` anyway is caught at step 3's
apply. A copy of `MergeBegin` that applies after an attempt has ended begins a new
attempt, which a leader of L carries on or aborts like any other it finds.

**2. `Subsume { into: L, begun: h, right_generation: g_R, voters }` in R's log.**
Proposed to R's leader by the coordinator. Each replica of R re-checks at its apply, at
index `f`, that R is `Live` at generation `right_generation` and that its configuration in
force is plain with exactly `voters`, and sets R's state to `Subsumed { into: L, begun: h,
index: f }` in the apply batch. From `f` on, R applies every command as nothing, serves
no read, refuses splits and changes, and answers requests `RangeMismatch` (§3). R's Raft
group keeps running, so its lagging replicas still reach `f`. R's leader answers the
coordinator with R's descriptor at `f` and whether its core has a change in flight.

A configuration entry takes effect when it is appended, not applied (RAFT.md §1), and a
change's catch-up lives in the leader's core, where no apply can see it (D-032). So a
change R's leader accepted before `f` could still append a joint entry after `f`. The
coordinator therefore proceeds only on an answer from a leader of R that has applied `f`
and has no change in flight. From then on the node refuses `Change` for the subsumed R; a
later leader starts with no catch-up (D-032) and, by a rule this design adds to every
range, accepts no `Change` before it has applied its own term's first entry, which lies
above `f`, so it sees R subsumed before it could accept one. R's configuration no longer
changes. L is held the same way from `MergeBegin` on.

No read of R is served stale after `f`. A read R's leader serves from applied state at or
above `f` is refused (§3, check at serving). A read it serves from applied state below `f`
is a read of R's history before the freeze, and L writes R's keys only after step 3, which
follows the commit of `f`. Any other replica of R serving a read would have to be a
leader: an older leader's lease expired before a newer one could be elected (RAFT.md §1,
leases), and a newer leader holds `f` (leader completeness) and serves nothing before it
has applied past its own no-op, which lies above `f`.

**The wait for every right replica.** The coordinator asks each of R's replicas for its
applied index and proposes step 3 only when every one has applied at least `f`. Every,
not a majority: with one engine per node each node's replica of L takes over its own
node's copy of R's data in place (Q2), and a replica of R below `f` would leave its
node's merged range without writes R committed below `f`. A replica of R that never
reaches `f`, its node down, holds the merge until the coordinator aborts it (below). The
check is repeated at step 3's apply on each node, against that node's replica of R.

**3. `Merge { right: R's descriptor at f, begun: h, f }` in L's log.** Each replica of L
applies it at index `m`. If L is not `Merging` with `begun: h`, the attempt has already
ended, by a merge or an abort, and the entry applies as nothing. Otherwise it re-checks:
the carried descriptor is R's, `Subsumed { into: L, begun: h, index: f }` at generation
g_R; L's end is R's start; L's configuration in force at `m` is plain with R's voters.
These checks read only state every replica of L shares, and if any fails the entry
applies as an abort, as `MergeAbort` does below. The last check is this node's own: its
replica of R is initialised, subsumed at `f`, and has applied at least `f`. Otherwise one
synced batch holds:

- L's applied index, `m`;
- L's descriptor, `[a, b)` at generation max(g_L', g_R) + 1, where g_L' is L's
  generation at `m`, `Live`;
- the deletion of R's descriptor and Raft state on this node: its hard state, applied
  index, log keys, configuration key, snapshot record, and under Q26's draft its
  incarnation and quarantine flag, a range delete the engine lacks (§11);
- no user key: R's data is already where L now serves it (Q2).

The node stops R's core and traces, in this order, `RangeMerged`, `RangeRemoved { cause:
merged }` for R and `RangeDescriptor` for L at `m`, then the entry's `RaftApply` (§8).

**A replica that fails only its own node's check.** The coordinator's wait excludes it,
with one exception: a node whose replicas are re-seeded between the wait and its apply of
`m`, as Q15's draft re-seeds every replica of a node that lost a table, can replay `m`
from L's log with its R uninitialised or below `f`. Such a replica of L does not apply
`m`: it stops applying L at `m − 1`, traces `RangeMergeStalled`, and waits for a snapshot
of L at or above `m` (Q24). This is not RAFT.md §3's refusal and borrows none of it:
nothing was lost, so no marker is written (D-044), no engine is quiesced and the node's
other replicas run on, and no quarantine follows, since D-035's reason, a vote the store
may have lost, does not arise. Its log and hard state stay, and it keeps nothing
volatile that a restart needs: restarted, it re-applies up to `m − 1` and stalls again.
To be fed, it answers L's AppendEntries as a placeholder does (§5), with a rejection
hinting index 1, an echo of zero and incarnation 0, and grants no vote while it waits.
The snapshot's span `[a, b)` overlaps this node's replica of R where that replica is
initialised below `f`; the draft makes the one exception to the overlap rule here: a
snapshot of L at or above `m` replaces the replica of R that L's log merged at `m`
(Q27). The alternatives, in Q24: refuse the whole node as RAFT.md §3 refuses a store,
which under Q2's draft quarantines every replica on it (D-035) although nothing was lost;
or have the coordinator wait again, after a re-seed, before a node applies `m`, which
no entry can make a node do.

**Abort and unfreeze.** The coordinator proposes `MergeAbort { right: R, begun: h }` to L
when `Subsume` applied as nothing (R's generation or voters had changed), or when the
wait for R's replicas passes its bound (Q24). It applies only while L is `Merging` with
`begun: h`: then L returns to `Live`, the batch writes an abort record of `h` under L's
tenant-0 keys, kept until L's own state is deleted, and the node traces
`RangeMergeAborted`. Otherwise the attempt has already ended and it applies as nothing.
The same holds for a `Merge` whose shared checks fail.

The coordinator proposes `Unfreeze { from: L, begun: h, index: f }` to R only once it has
seen the abort take effect: its own node's apply moved L from `Merging` with `begun: h`
to `Live` and wrote the abort record. That the abort committed, or that an entry named
`MergeAbort` applied, is not enough. Suppose leader W proposes `Merge` at `m` and loses
office; a new leader X takes office with `m` in its log and not yet applied, sees L
`Merging`, finds one of R's replicas down, and proposes `MergeAbort` at `i > m`. `m`
applies as a merge and `i` as nothing; an unfreeze proposed on reading `i` applied would
leave R and the merged L both owning `[k, b)`. `Unfreeze` applies in R only while R is
`Subsumed { into: L, begun: h, index: f }`, and otherwise as nothing; applied, R returns
to `Live`. Since no `Merge` of `h` can take effect once L holds the abort record of `h`,
anyone who has read that record from a replica of L's applied state may propose the
unfreeze. Check 12 of §8 folds the order.

A new leader of L that takes office with L `Merging` resumes: it asks R's leader, and
completes or aborts. A leader of R whose range has stayed subsumed longer than a bound
asks L's leader whether L holds an abort record of `h`, and proposes `Unfreeze` on one;
this is what unfreezes R after a coordinator that saw its abort take effect lost office
before its unfreeze committed (Q24).

**Resending range commands.** Every entry of the merge names its attempt and re-checks it
at apply, so a copy applies as nothing, and the coordinator resends a command whose
answer was lost until it reads the command's effect. D-026's rule against resending a
write (DECISIONS.md:852-854) protects client writes, whose copies are second writes. One
copy does take effect: a `Subsume` of an attempt that has already ended by abort,
reaching an R that was never subsumed for it or has been unfrozen since. It freezes R
until R's leader's bounded ask unfreezes it, which costs availability, not safety (Q24).

**What the merged range inherits.**

| | the merged range, L's id | what becomes of R's |
|---|---|---|
| span, generation | `[a, b)`, max(g_L', g_R) + 1 | R's descriptor deleted on every node |
| Raft group, log, term, leader, lease | L's | R's group stops on each node at its apply of `m`; its keys are deleted in the same batch |
| configuration | L's, equal to R's by the precondition | — |
| data | `[a, k)` and `[k, b)`, both in place | nothing moves |
| applied index | `m` | R's `f` is folded into nothing: L's log records only `m` |
| snapshot record | L's last take, whose checkpoint covers `[a, k)` below `m`; a follower fed it replays `m`, and passes step 3's own-node check only if its node's R has applied `f`, so L's leader takes a fresh one after `m` rather than stream the old one (Q24) | deleted |
| proposals remembered | L's | dropped; a request naming R finds no replica and is answered `RangeMismatch` (§3) |
| pending reads | L's | refused at the freeze |
| incarnation (D-042), quarantine (D-035) | L's replica's (Q26) | deleted, under Q26's draft of per-replica keys |
| meta record | L's leader sends `[a, b)` at the new generation, which overrides R's record (§1) | overridden |

**What is atomic with what.** Atomic, on each replica: `MergeBegin` in L's apply batch;
`Subsume` in R's; `Merge` — L's descriptor, the deletion of R's state, L's applied index
— in L's; `MergeAbort` in L's; `Unfreeze` in R's. Not atomic, and made safe by order:
`MergeBegin` before `Subsume` before `Merge`, with the wait for every replica of R
between the commit of `Subsume` and the proposal of `Merge`; `Unfreeze` proposed only
once the abort has taken effect on the proposer's node or its record has been read; the
meta update after. A crash re-applies each entry from its log; the coordinator's wait is
volatile and a new leader of L redoes it.

**Why identical replica sets.** On a node with a replica of L and none of R, the merged
range has no data for `[k, b)`: it answers reads there from an empty span, and later
writes build on nothing. On a node with a replica of R and none of L, the frozen data
has no successor and the replica is never collected. Moves (§7) make replica sets
differ, so ranges are first brought to the same replicas by moves; CockroachDB's merge
queue relocates the right-hand range's replicas before it merges. Whether the
coordinator or the rebalancer does that is Q24. §10's `MergeDivergentReplicas` is the
merge that skips the voter-set checks.

## 7. Rebalancing

**Who decides (SPEC §4: "background task on a leaseholder-elected node").**
"Leaseholder" is not a term the tree defines; the only lease in RAFT.md is a Raft
leader's read lease (RAFT.md §1). The draft runs the rebalancer on the node whose
replica of range 0 leads, for as long as it leads (Q29). Two rebalancers at once — an
old leader of range 0 not yet deposed beside a new one — cost liveness, not safety:
every move is a change on the moved range's leader, one change is in flight per group,
and a request for different voters while one is under way is refused (D-029,
DECISIONS.md:1099-1104). The rebalancer's state is volatile; a new one starts by
reading.

**What it optimises (SPEC §4: "range count and leader count").** For each node in range
0's node records (Q8), the replicas it holds and the ranges it leads. The goal is SPEC's
"within 10%"; Q30's draft measures each node's two counts against their means, and what
the 10 % is measured against, and by when, is Q30. How a step chooses its move is not
settled by SPEC, which names only what is balanced (SPEC.md:296-297); one step of the
draft's policy, every choice in it Q42:

- Replicas: take the node holding the most and the node holding the fewest; choose the
  range with the lowest id that has a replica on the first and none on the second, is
  not range 0 or 1 (Q9), is `Live`, and has no change in flight; move that replica from
  the first to the second.
- Leaders: take the node leading the most and the node leading the fewest; choose the
  lowest-id range the first leads with a follower on the second; ask its leader for a
  transfer to it (`Command::Transfer`, apply.rs:64-69; `TimeoutNow`, D-028).
- A node being removed (Q8) has every replica moved off, and a quarantined replica is
  moved off its node (Q33), before any other move.

At most a fixed number of moves are in flight across the cluster and one per range
(Q30). The draft learns replica sets from meta, which lags by one update, and each
node's counts by asking the node, answered from its memory, not from a Raft write (Q31).

**A move, made safe by joint consensus (SPEC §4: "Uses joint consensus per move").**
Moving range X's replica from node `s` to node `d` is one change: the rebalancer sends
X's leader `Change { voters: old − {s} ∪ {d} }`. Never a removal and then an addition. A
change that only removes goes to its joint entry with no catch-up gate
(core.rs:1920-1928), and between the two changes the range runs on one voter fewer,
where a single failure stops it; that is §10's `RemoveBeforeCaughtUp`. The steps:

1. X's leader adds `d` as a learner (D-029). `d` has no replica of X: its node creates
   an uninitialised one on first contact (§5). A learner starts at `next = last + 1`
   (core.rs:1930-1937), so nothing in the leader feeds it a snapshot by itself; the
   placeholder's rejection hinting index 1 does (§5, Q22), and for a range born of a
   split the snapshot is needed from the range's first entry, since its floor is the
   parent's split index. The overlap rule of §5 applies to the install. A node that
   still holds a removed replica of X is not chosen as `d` until it has collected it, so
   a replica's identity in the trace is never reused while its old state exists (Q26).
2. Catch-up in rounds: a round of replication to `d` that ends within a minimum election
   timeout marks `d` caught up (`note_learner_round`, core.rs:2002-2026; RAFT.md §1).
   Caught up means a matched log index, not an applied one; a learner fed a snapshot
   counts once an append after its install succeeds within a round (core.rs:1766-1796,
   2600-2620). Whether a move also waits for `d` to apply is Q28.
3. The joint entry, and once it commits, `C_new` (`after_commit`, core.rs:2033-2058).
   While joint, commits and elections need majorities of both voter sets
   (`Configuration::has_majority`, types.rs:121-127). A leader outside `C_new` steps
   down once it commits (RAFT.md:152-153).
4. At the apply of `C_new`, each replica of X raises its descriptor's generation and
   records the new voters in the apply batch, and X's leader sends the meta update (§1).
5. `s`'s replica is collected only once some replica of X has applied a descriptor at a
   higher generation than `s`'s own that excludes `s`. Applied means committed, so that
   is proof `C_new` committed. Until then its state stays: a truncation that reverts
   `C_new` restores its right to campaign (D-033; RAFT.md:163-168). A replica emptied
   early is a placeholder on its node, which a leader of X re-seeds (§5); the install
   carries the stream's term and no vote (RAFT.md:518-519), so the replica could grant a
   second vote in a term it had voted in. The draft has `s`'s replica, after ten maximum
   election timeouts with no message from a leader of X, ask the leader meta names for
   X's descriptor, and collect itself on such proof; CockroachDB's replica GC consults a
   newer descriptor the same way. The silence's length and whom the replica asks, meta's
   leader for X or the last leader of X it heard from, are Q27. §10's
   `GcBeforeRemovalCommitted` collects it early.
6. The rebalancer learns completion by reading X's descriptor from X's leader. `Change`
   is answered `Done` on accept and its completion shows only as configuration entries
   (node.rs:818-826, 868-886; D-029). A leadership change before the joint entry
   abandons the change (D-032); a rebalancer that still finds the old voters at the old
   generation after a bound traces the move `abandoned` and may begin it again, and a
   request for the voters already under way or in force is answered `Done` and proposes
   nothing (DECISIONS.md:1099-1104).

The rebalancer traces each decision as `RebalanceMove { range, from, to, phase }`, with
phases `begun`, `done` and `abandoned` (§8).

**Membership changes and snapshots on one schedule.** Every move of a range with a
compacted log crosses the membership path and the snapshot path together. Phase 2 has
exercised them apart: "the main sweep never proposes `Change`; the membership scenario
never crosses the snapshot threshold. The interaction … is unit-tested only"
(OVERNIGHT.md:188-191). Whether Phase 2's scenarios cover it before Phase 3 depends on
it is Q34.

**Quarantined replicas.** A replica on a store a re-seed rebuilt never votes again
(D-035). D-035 rejected replacing such a server by a membership change because the
recovery path then had no membership machinery and needed an operator per refusal
(DECISIONS.md:1522-1524). Phase 3 has both, and needs them: with Q15's whole-node
refusal a node that loses one table has every replica re-seeded and quarantined, and
once two nodes have, the ranges with replicas on both can keep a leader they have and
cannot elect one (§4). The draft's rebalancer moves quarantined replicas off, one per
range at a time, before any balancing move, and counts a node's quarantined replicas
apart from the rest (Q33).

**Node add and remove (SPEC exit criterion 2).** A node is added by `AddNode` and
removed by `RemoveNode`, operator writes to range 0 (Q8); a removed node is drained by
moves before it is expected to stop. A node that crashes and stays down is not detected
by the draft: the rebalancer counts it and its replicas until an operator removes it
(Q8).

## 8. The invariants and how the trace checks each

Every state transition of this document emits a trace event, and every check below is a
function of the trace. Today no event names a group: every `Raft*` event names a server
(trace.rs:368-652), and the enum already says it "grows with each phase (range id, …)"
(trace.rs:70-75). The moirae export's `convert` is an exhaustive match
(crates/ananke-env/src/moirae.rs:277-988), so a new event does not compile without its
export line, and SPEC §1.5 already reserves a `range` field in `msg`, `log.data` and
`state.patch` "so a filter can carve a per-range trace" (SPEC.md:117-124).

Changes to existing events, each recorded with its node and two times as today (D-047):

| Event | Change |
|---|---|
| every `Raft*` event about a replica | gains `range`; the events about a node's store are below the table |
| `RaftApply` | gains `key` for a single-key command, and `effect`: `applied` for a client command executed within its range's span, whatever it wrote — a `Cas` whose compare failed returns `Swapped(false)` and writes nothing (apply.rs:260-267) and is `applied`; `out_of_span` or `frozen` for a client command refused at apply (§3); `took` for a range command (§5, §6) that took effect; `aborted` for a `Merge` that applied as an abort; `refused` for a range command whose re-check failed and that applied as nothing; `none` for a no-op or a configuration entry |
| `RaftRead` | moves from the core, which traces it when a read is confirmed (core.rs:1175, 1202), before the server holds the key or has served anything, to the server where it serves (node.rs:2092-2110); keeps `index` and `lease`, and gains `key` and `applied`, the applied index of the engine version it was served from (§3) |
| `RaftProposed` | gains `range` (with every `Raft*` event), which the history's closure needs (§9) |
| `ClientInvoke`, `ClientReturn` | unchanged: the history knows no ranges (§9) |

**Events about a node's store.** Under Q2's draft several existing events describe the
node's one engine, not a replica. `RaftRefused`, `RaftAdopted` and `RaftServerFailed`
stay per node and carry no range. A node's `RaftRefused` clears, for every range, what
checks 2 to 4 hold for that node, as a server's clears it today (invariants.rs:557-559,
595-597), and every span check 8 holds for the node; every replica on the node counts as
refused in the per-range majority rule below. `RaftRecovered`, `RaftReseeded` and
`RaftProgressReset` are per replica and gain `range`, and `RaftRecovered`'s
`incarnation` is the replica's under Q26's draft. A replica's re-seed install traces
`RangeCreated { cause: snapshot }` on its node, which restarts that replica's floors in
checks 2 to 4. How a refused node's per-range installs become one engine again is Q15.

New events:

| Event | When | Fields |
|---|---|---|
| `RangeCreated` | a replica of a range is initialised: at bootstrap, at a split's apply, at an install on a node that held no initialised replica of the range | `range`, `cause`, `parent`, `start`, `end`, `generation`, `voters`, `floor_index`, `floor_term` |
| `RangeDescriptor` | a replica's descriptor changes at an apply; an install onto an initialised replica changes it (with `applied` the snapshot's last index); or it is restated at its node's start | `range`, `index`, `applied`, `start`, `end`, `generation`, `voters`, `state` |
| `RangesRestated` | a node has restated every replica it holds | `ranges` |
| `RangeReplicaCreated` | a node made a placeholder for a range it does not hold (§5) | `range` |
| `RangeSplit` | a replica applied a split that took effect | `range`, `right`, `key`, `index` |
| `RangeSubsumed` | a replica of R applied a `Subsume` that took effect | `range`, `into`, `begun`, `index`, `generation`, `voters` |
| `RangeMerged` | a replica of L applied a `Merge` that took effect | `range`, `right`, `begun`, `index`, `right_index`, `right_generation`, `right_applied` |
| `RangeMergeAborted` | a replica of L applied a `MergeAbort`, or a `Merge` whose shared checks failed, that moved L from `Merging` with `begun` to `Live`; never for one that applied as nothing | `range`, `right`, `begun`, `index` |
| `RangeMergeStalled` | a replica of L stopped at `m − 1` on its own node's check (§6) | `range`, `right`, `index` |
| `RangeUnfrozen` | a replica of R applied an `Unfreeze` that took effect | `range`, `from`, `begun`, `subsumed`, the index of the `Subsume` it ends, and `index` |
| `RangeRemoved` | a replica's state was deleted | `range`, `generation`, `cause`: `merged` or `collected` |
| `RaftLearnerRound` | a leader ended a catch-up round for a learner (§7) | `range`, `learner`, `from_index`, `to_index`, `ticks`, `caught_up` |
| `RaftChangeAccepted` | a leader accepted a `Change` | `range`, `voters`, `applied`, `term` |
| `RangeMismatchSent` | a server answered `RangeMismatch` | `range`, `client`, `seq`, `at`: `receipt`, `read` or `apply`, `descriptors` as (range, generation) |
| `MetaApplied` | a replica of the meta range applied a `MetaUpdate`, and at bootstrap for range 1's initial record (index 0) | `index`, and per descriptor `range`, `start`, `end`, `generation`, `voters` and the sub-intervals it won |
| `RebalanceMove` | the rebalancer began a move, saw it done, or gave it up | `range`, `from`, `to`, `phase` |
| `NodeAdded`, `NodeRemoved` | a replica of range 0 applied an operator's `AddNode` or `RemoveNode` | `node` |
| `ClientSend` | a client sent an operation's request | `client`, `seq`, `range`, `generation`, `to` |
| `ClientMismatch` | a client received `RangeMismatch` | `client`, `seq`, `descriptors` as (range, generation, start, end) |

At a node's start every replica restates its log, as a server does today before
`RaftRecovered` (RAFT.md §2, check 4), and then its descriptor as a `RangeDescriptor`
carrying its applied index; `RangesRestated` closes the node's restatement. The events
of one structural apply are traced in a fixed order, and the checks below rely on it: a
split's `RangeSplit`, P's `RangeDescriptor`, R's `RangeCreated`, then `RaftApply`; a
merge's `RangeMerged`, R's `RangeRemoved { cause: merged }`, L's `RangeDescriptor`, then
`RaftApply`; an install's `RangeDescriptor` for the replica it shrinks before any
`RangeCreated` of an install that follows.

The checks run where RAFT.md §2's do: the folds in one incremental checker, fed the
records since its last look every ten slices of fifty milliseconds and stopping the run
at its first violation, and re-run over the whole trace at the end (D-046); the rest in
`sim/` at the end. Which crate holds the folds is Q40. RAFT.md §2's checks carry over
with their state keyed by range first:

1. **Election safety**, from a map of (range, term) to server. Today the key is the term
   alone (invariants.rs:443-453), and two ranges electing in one term would read as two
   leaders of it.
2. **Log matching**, over logs and floors per (range, server) (invariants.rs:40-49). A
   `RangeCreated` sets its replica's floor as an installed snapshot does
   (invariants.rs:80-106), so a split-born range's log starts at its floor.
3. **Leader completeness**, with the committed set per range; *commit by majority* takes
   each range's first configuration from its `RangeCreated`, where today it takes
   `1..=servers` (invariants.rs:289-295); *commit by current term* and *committed
   entries stay* per (range, server). Leader completeness rescans the committed set at
   every `RaftLeader` (invariants.rs:493-520); keyed by range, the rescan is of that
   range's set.
4. **State machine safety**, a map per range from index to (term, hash, effect): a
   second value is a violation, so two replicas that apply one entry with different
   effects — one writing a key, another refusing it; one taking a split, another
   refusing it — are seen at the second. Per (range, server), applies are consecutive
   from the floor `RangeCreated` or an install sets. A `RangeRemoved` ends a (range,
   server)'s memory the way `RaftRefused` ends a store's today (invariants.rs:551-571,
   574-616), so a replica removed and later added to the same node starts clean (Q26).
5. **Linearizability**, §9.
6. **Lease safety under drift**, per range. As built this is not a fold: it is check 5
   on every seed and a test that runs each drift-exceeded seed with the guard and
   without it (sim/tests/raft.rs:1433-1500). It carries over the same way.

The new checks. Checks 7 to 12, 16, 18 and 19 are properties; 13, 14, 15, 17 and 20 to
22 are rule folds, which see a broken rule the first time it is exercised and not only
when its consequence lands.

7. **Descriptor agreement.** Fold `RangeCreated` of a bootstrap or a split, as the value
   at (range, `floor_index`), and `RangeDescriptor` at an apply, as the value at (range,
   `index`): a map from (range, index) to (start, end, generation, voters, state); a
   second value for one (range, index) is a violation. A restatement at a node's start,
   and an install — its `RangeCreated { cause: snapshot }` or its `RangeDescriptor` —
   carry an index and must equal the map's latest value at or below that index,
   whichever of the two is traced first. This is state machine safety for
   descriptors, and where the single batch of §5's split and §6's merge is proven: a
   node that restarts with a descriptor its applied index cannot hold is seen here. Every
   check below that reads "check 7's map" reads this map; a lookup by (range,
   generation) takes the first value traced at that generation. Cheap, exact.
8. **One owner per key on a node.** Fold `RangeCreated`, `RangeDescriptor`,
   `RangeRemoved`, `RangesRestated`, `NodeCrashed` and a node's `RaftRefused`: per node,
   the spans of its initialised replicas, subsumed ones included, since they still hold
   their data. After every event that changes one, taken in the fixed orders above, no
   two of a node's spans overlap. And a node that is a voter of P's configuration at
   `s`, whose replica of P has applied `s` — traced as `RangeSplit`, as a `RaftApply` of
   `s`, or restated at an applied index at or past `s` — when some replica of P applied
   `s` with effect `took`, holds R initialised from then until a `RangeRemoved` of R or a
   `RaftRefused` on that node; a `RangesRestated` without it is a violation. A node that
   is not a voter there traces no `RangeCreated { cause: split }` of R. Incremental: an
   interval map per node.
9. **Serving within span.** Fold `RaftApply` and `RaftRead` with check 7's map. A
   `RaftApply` whose effect is `applied` and whose key lies outside the span of its
   range's descriptor in force before its index, or whose descriptor is subsumed, is a
   violation; so is one whose effect is `out_of_span` for a key inside it. A `RaftRead`
   whose key lies outside the span of the descriptor at its `applied`, or on a subsumed
   one, is a violation. Cheap, exact.
10. **Owners' generations rise.** Fold the first `RaftApply` of each (range, index) with
    effect `applied` and a key, with the generation of its range's descriptor in force
    before that index from check 7: a map from key to the (generation, range) that last
    wrote it. A write at a lower generation is a violation, and so is a write by a
    different range at a generation not above the recorded one: sibling halves share
    `g + 1`, so without the range a left half writing a key its right sibling had written
    would pass. Reads are not folded: a read whose read index lies below a split may be
    served after the right half has written, and still linearizes before that write
    (§9). Cheap. It is §1's rule, which meta and the client cache merge by.
11. **A merge applies only where its preconditions hold.** Fold `RangeMerged` with
    `RangeSubsumed`, check 7's map, a history per (range, index) of the configuration in
    force, built from each `RaftConfig`'s `index` (check 3 keeps only the latest per
    server, invariants.rs:278-279), and check 4's applied index per (range, server). At
    each `RangeMerged { range: L, right: R, begun: h, index: m, right_index: f }` on node
    `n`: some replica of R traced `RangeSubsumed { range: R, into: L, begun: h, index: f
    }` at the merge's right generation; L's end at `m − 1` is R's start at `f`; L's
    configuration in force at `m − 1` and R's at `f` are the same plain voter set; and
    `n`'s replica of R has applied at least `f`, which `right_applied` must also say. A
    violation otherwise. Learners are not checked here: configuration entries carry none
    (core.rs:1985-1988, 2044-2047), so a `RaftConfig`'s `learners` is always empty; a
    replica of L on a node that lacks R, a learner or not, is caught by the last clause
    if it merges at all.
    Cheap, exact.
12. **A freeze ends only by its merge or an abort that took effect.** Fold
    `RangeSubsumed`, `RangeMerged`, `RangeMergeAborted` and `RangeUnfrozen`, in record
    order, by first apply across replicas. Per attempt `h` of L on R, and per subsume of
    it at `f`: a `RangeUnfrozen { range: R, begun: h, subsumed: f }` not preceded by
    a `RangeMergeAborted { range: L, begun: h }` is a violation; a `RangeMerged` of `h`
    and a `RangeUnfrozen` of `h` at `f` are a violation whichever comes first; so are a
    `RangeMerged` and a `RangeMergeAborted` of one `h`. The first clause sees an unfreeze
    whose apply lands before any abort of its attempt took effect; the proposal itself is
    not traced, so an unfreeze proposed early whose apply happens to follow the abort is
    seen only if the merge wins. Cheap.
13. **Incoming voters are caught up** (a rule fold, D-029). Fold `RaftLearnerRound`, the
    leader's `RaftConfig` and check 2's replayed logs. At a leader's `RaftConfig` that
    is joint and adds voter `d`, the same leader in the same term traced a
    `RaftLearnerRound` for `d` with `caught_up` and `ticks` below the minimum election
    timeout, and `d`'s replayed log, entries or floor, reaches that round's `to_index`.
    A violation otherwise. Cheap.
14. **A replica is collected only after its removal committed** (a rule fold, D-033).
    Fold `RangeRemoved` with check 7's map. A `RangeRemoved { cause: collected }` of
    range X on node `n` at generation g is a violation unless some replica of X traced a
    `RangeDescriptor` above g whose voters exclude `n` before it; check 18 ties those
    voters to the committed `C_new`. One with `cause: merged` is a violation unless `n`'s
    previous record is a `RangeMerged` with `right: X`, the fixed order above. Cheap.
15. **A move keeps the replication factor** (a rule fold, Q32). Fold each range leader's
    `RaftConfig`: a configuration in force, plain or either side of a joint one, with
    fewer voters than the replication factor, on a range whose previous configuration
    had at least that many, is a violation. Asked only in scenarios with at least as
    many nodes as the factor. Cheap.
16. **The meta range never goes back and names only real descriptors.** Fold
    `MetaApplied` with check 7's map, from the bootstrap's `MetaApplied { index: 0 }`.
    Every descriptor a meta apply names — range, start, end, generation and voters —
    equals the value check 7 holds for its range at its generation, first traced before
    the meta apply; and per key, the generation meta names never falls from one meta
    index to the next. Incremental: an interval map.
17. **A client refreshes on a mismatch** (a rule fold). Fold `ClientInvoke`, for each
    operation's key, `ClientMismatch` and `ClientSend`. A `ClientSend` for an operation,
    after a `ClientMismatch` for it that named a descriptor of generation G whose span
    contains the operation's key, to a range at a generation below G, is a violation.
    Cheap.
18. **Descriptor lineage.** Fold `RangeSplit`, `RangeCreated`, `RangeMerged` and
    `RangeDescriptor` with check 7's map and check 11's configuration history, at the
    first apply of each (range, index). At a split of P at `s` with key `k`, where P's
    descriptor before `s` is `[x, y)` at g: P's value at `s` is `[x, k)` at g + 1 with
    P's voters, and R's `RangeCreated { cause: split }` is `[k, y)` at g + 1 with the
    voters of P's plain configuration in force at `s` and floor index `s`. At a merge of
    R into L at `m`: L's value at `m` is `[L's start, R's end at f)` at max(g_L', g_R) +
    1 with L's voters. At any other change of a range's value at index `i`: the span is
    unchanged; the generation rises by exactly one where the entry at `i` is a plain
    configuration following a joint one, and the voters then equal that configuration's;
    anywhere else generation and voters are unchanged and only the state moves, along
    `Live`→`Merging`→`Live` or `Live`→`Subsumed`→`Live`. Installs and restatements are
    check 7's. Cheap, exact.
19. **The ranges tile the keyspace.** Fold check 7's map, in record order by first apply:
    the latest value of every range, with a range dropped at the first `RangeMerged` that
    names it as `right` until a later new value of it is traced. After every event no two
    of those spans overlap, and after every `RaftApply` of a range command they cover the
    addressable keyspace with no gap. Where check 8 is one node, this is the cluster.
    Incremental: an interval map.
20. **A placeholder grants nothing** (a rule fold, Q22). Fold `RangeReplicaCreated`,
    `RangeCreated`, `RangeRemoved` and `RaftVote`. A `RaftVote` with `granted`, vote or
    pre-vote, of range R on node `n` when `n` has traced no `RangeCreated` of R since its
    last `RangeRemoved` of R is a violation. Cheap, exact.
21. **A removal is durable** (a rule fold). Fold `RangeRemoved`, `RangeCreated`,
    `RangeDescriptor` restatements and `RaftRecovered` per node. A restatement or a
    `RaftRecovered` of range X on node `n` after `n`'s `RangeRemoved` of X, with no
    `RangeCreated` of X on `n` between, is a violation: the deletion did not survive the
    crash. Cheap, exact.
22. **Changes are accepted only on a live range, by a leader that has applied its term's
    first entry** (a rule fold, §6). Fold `RaftChangeAccepted` with check 7's map,
    `RaftLeader`, and each leader's `RaftApply`s. A `RaftChangeAccepted` whose range's
    descriptor at its `applied` is `Merging` or `Subsumed`, or whose leader has traced no
    `RaftApply` of an entry of its own term, is a violation. Cheap, exact.

The checks about time run where RAFT.md §2's do: on uniformly scheduled seeds (D-016),
and only for a range whose replicas that are neither refused nor quarantined form a
majority at the end of the run (RAFT.md:370-377), now asked per range rather than per
cluster (sim/raft.rs:967-986). After the last fault heals, a client write to every key
completes within ten maximum election timeouts; today the check takes one minimum over
every write (sim/raft.rs:990-998), which a wedged range beside a live one would pass, so
it is asked per key. After the last heal, meta names every key's current descriptor
within a bound; a move the rebalancer began is traced done or abandoned within a bound;
a subsumed range is merged or unfrozen within a bound (each bound Q39).

In the balance scenario, after the last `NodeAdded` or `NodeRemoved` plus a bound (Q30),
every node's counts are within 10 % as Q30 defines it. The fold: the nodes, from
`NodeAdded` and `NodeRemoved`; each node's replica count, from its `RangeCreated` and
`RangeRemoved`; each node's leader count, from the latest `RaftLeader` of every range,
counted against the leader's node while no later `RaftTerm` of that range on that server
ends its office; evaluated at every event from the bound's end to the run's end.

In `sim/move.rs`'s shape (a), the hold check: a client write to X invoked after the
stayer's `NodeCrashed` and before the hold ends completes before the hold ends. It reads
`NodeCrashed`, the hold's `LinkLimited` and the heal that ends it, and `ClientInvoke` and
`ClientReturn` for X's keys; the hold's length is Q39, and the correct system must pass
it on every seed.

The timer check and pre-vote's property are per (range, server): a reset is a delivered
AppendEntries or chunk of the range's own term, decoded from its batched frame
(sim/raft.rs:1251-1394), or the replica's `RangeCreated`, and pre-vote's term is the
range's (sim/raft.rs:1148-1198). Today the timer check skips only leaders and re-seeded
servers (sim/raft.rs:1372-1375), and only the raft sweep runs it (sim/raft.rs:1034), not
the membership scenario, which is the one with servers outside a configuration. The
sharded sweep has such replicas by design, so the check also skips, per (range, server):
one that is not a voter of its configuration in force (D-033); a placeholder, from its
`RangeReplicaCreated` to its `RangeCreated`; one stalled at a merge, from its
`RangeMergeStalled`; and one after its `RangeRemoved`. A removed replica that waits ten
maximum election timeouts before asking for a newer descriptor (§7) is the first of
these. Pre-vote's property takes, for a range created on the isolated node during the
isolation, the term of its `RangeCreated` as the term the isolation began with.

Decision time and durability time (D-047) divide as in RAFT.md §2. Checks 7 to 16, 18,
19 and 21 are about what was durable and read durability time or record order: every
event they fold is traced once its apply batch is durable. Check 20 folds `RaftVote`,
traced once the vote is durable, as today. Check 17, check 22's `RaftChangeAccepted`
and the rebalancer's `RebalanceMove` are traced as they happen, so their two times are
equal. The timer check and pre-vote's property read decision time, as today.

The pair rule holds for each: a variant in §10 fails each check, the balance, move,
subsume and hold bounds included, and the correct system passes every seed. Every check
is a function of the trace alone, so a failing seed replays in the studio with the
check's own events on screen.

## 9. The linearizability checker across split, merge and rebalance

SPEC's first exit criterion is "Linearizability holds across split/merge/rebalance under
faults" (SPEC.md:302). The checker is `sim/lin.rs` as RAFT.md §4 describes it and as
built: a Wing-Gong search with Lowe's partitioning and memoisation, per key. What
sharding changes is how the history is closed, and nothing about how it is searched.

**History.** Each client operation is `(client, invoke_t, return_t, Op, Result)` over
`Put`, `Get`, `Delete` and `Cas` (trace.rs:742-770), paired by `(client, seq)`
(sim/lin.rs:70-92). A `RangeMismatch` is followed inside the client, like `NotLeader`
(sim/raft.rs:3022-3034), and is never a return; an operation sent to three ranges
returns once. The trace closes pending operations as today, by their entries' fate, with
two changes. Today `RaftProposed` records `(index, term)` and `RaftApply` records the
first apply of `(index, entry_term)` on any server (sim/lin.rs:93-109), so with several
groups in one trace an operation proposed at `(5, 2)` in one range would be closed by
the first apply of `(5, 2)` in any range. The closure is keyed by `(range, index,
term)`. And today an applied entry took effect (sim/lin.rs:8-11); with the apply check
of §3 an entry can apply as nothing. Only an apply whose effect is `applied` closes an
operation, and `applied` is any client command executed within its span, whatever it
wrote, so a `Cas` whose compare failed still closes its operation with the boolean the
model must match (RAFT.md §4). An abandoned operation with a proposal applied to that
effect returns at the earliest such apply's durability time (D-047), with a result the
model may choose; one
whose every proposal applied with another effect did not take effect and leaves the
history, as one never proposed does (sim/lin.rs:113-131); one with a proposal never
applied stays pending. The list of an operation's proposals is already a list
(sim/lin.rs:68, 100-103), which a resend to a second range after a definite
`RangeMismatch` needs.

**Partitioning, and why moving boundaries do not move it.** The KV model is a product of
independent registers, so a history is linearizable iff each key's sub-history is
(sim/lin.rs:15-17, 212-238). Which range served an operation is not part of the
operation: a key's register before a split, served by P, and after it, served by R, is
one register, and its operations are linearized together. So a split, a merge or a move
changes no partition, and the checker needs no knowledge of spans or generations. A
history partitioned by range instead would be wrong: it would check P's and R's halves
of one key's life as two registers, and a write to P lost at the split would be
invisible to both. What the boundaries do touch is confined to three places: the closure
above; multi-key reads; and meta lookups.

*Multi-key reads.* Phase 3's API has none unless `Scan` is added (Q35). A scan across a
boundary is stitched from reads of two groups at two read indices, and nothing in
Phase 3 gives those two a common instant: a key of the first can change after its read
and a key of the second before its read, and RAFT.md §4's scan check — a single time `t`
in the scan's window at which every key's value matches its timeline — would report
that. Without transactions (D-006) a cross-range scan cannot be promised linearizable,
so the draft leaves `Scan` out of Phase 3 and SPEC's distributed scans with Phase 5
(SPEC.md:345-346).

*Meta lookups.* A lookup is a read of range 0 or range 1. The draft keeps lookups out of
the history (Q36): a stale lookup is harmless by §3's design, and check 16 and the meta
convergence bound of §8 are what hold the meta range to account.

**Search.** Unchanged: operations sorted by invocation, state as (the set linearized,
the register's value), depth-first with a memo, pending operations candidates at every
step (sim/lin.rs:297-352), a budget of 2 000 000 states per key whose exhaustion is
reported apart from a violation and must never be reached by the correct system
(sim/lin.rs:159, 173-189). What grows the search is concurrency and pending operations
on one key, not the number of ranges. Splits need several keys per range, and conflicts
need few keys per client; today's workload has two keys (sim/raft.rs:104), which one
split leaves at one per range. The sharded workload's keys, clients and split points are
Q39.

**What is asserted.** Every key linearizable, for the correct system on every seed,
across the splits, merges and moves each scenario makes under its faults. Checks 7 to 22
are the earlier and cheaper catch of every variant in §10; linearizability is the
property SPEC names, so each variant whose named scenario can make it serve or write a
key wrongly — `TrustStaleDescriptor`, `ApplyIgnoresSpan`, `ServeAfterSubsume`,
`MergeBeforeRightApplied`, `SplitNotAtomicWithDescriptor` — is also asserted caught here
on some seed where its consequence reaches a client, so the checker is shown to see what
the folds see. Two variants that serve wrongly in principle are not on the list, because
no scenario of §10 lets the consequence reach a client: `SnapshotOverlapsReplica`, whose
harm needs a lagging P that replays entries below `s`, while `sim/split.rs` keeps the
follower away until P has compacted past `s`; and `MergeDivergentReplicas`, which keeps
step 3's own-node check, so a node with L and no R stalls and is re-seeded with the
merged range's data.

No rebalance variant is on the list either, and the reason is the design's: a move
changes voters, not data. `RemoveBeforeCaughtUp` and `JointBeforeCaughtUp` leave a
range with too few voters that hold its log, which stops commits and loses nothing
committed. `GcBeforeRemovalCommitted` reaches a client only through a second vote: the
emptied replica is re-seeded (§5) with the stream's term and no vote, grants a vote in a
term it had already voted in, and two leaders of that term accept writes; no scenario of
§10 builds that sequence. So the first exit criterion's "rebalance" rests on the correct
system passing this check on every seed across the moves it makes under faults, and the
move variants' catches rest on checks 3, 13, 14, 15 and 21, the hold check and the move
bound; whether to
build a shape that carries `GcBeforeRemovalCommitted` to a client is Q39.

## 10. Buggy variants shipped from day one

Each is a variant on the layer whose rule it breaks, and each breaks one rule with a
reference. Variants are a set whose empty set is the correct system, and a set turns off
exactly its members' fixes (D-045); whether Phase 3's live in `ananke-raft`'s
`Variants`, which has sixteen of thirty-two bits used (core.rs:159-202, 243;
DECISIONS.md:2700-2705), or in a set of the range layer's own, is Q37. The correct
system must pass every seed. Each variant must be caught by the named check on some
seeds. None of the rates below is measured, so no tier is claimed yet: a variant whose
situation a directed scenario builds on every seed is asserted caught on every seed at
every tier, as `sim/quorum.rs`'s are (D-049; RAFT.md:657-660), and each such scenario
asserts per seed that it built the situation; a variant a sweep arm reaches, or a
directed shape reaches only on some seeds, is asserted at the tier its measured rate
supports, and the arm's firing is asserted at every tier (D-041, D-043, D-044;
RAFT.md:651-656).

Every Phase 2 variant is re-asserted on the node of §4, with its combined persist and
batched frames, to the standard its Phase 2 test asserts and no stronger, on the same
arm or directed scenario run with several ranges per node: the sweep variants on the
raft sweep's arms; `RefusedCountsForQuorum` and `RefusedNeverCounts` on a sharded
`sim/quorum.rs`, since the random sweep reaches their situation once in a thousand seeds
(RAFT.md:657-660); `SharedSnapshotDir` at the nightly tier only (RAFT.md:655-656);
`IgnoreIncarnation` as an injection-and-reach assertion, since it is caught on 0 of
10 000 seeds (RAFT.md:681); and the pair `{IgnoreIncarnation, SharedSnapshotDir}` on its
pinned seed (RAFT.md:648-651). `SendBeforePersist` against a round's single synced batch
matters most (Q39). A variant the sweep does not catch is a hole in the sweep, not a
variant to delete.

The scenarios and arms the table names (their shapes, node counts and shares are Q39):

- `sim/shard.rs`, the sharded sweep: five nodes, three of them bootstrap nodes, ranges
  made by splits its driver draws, merges and moves drawn by the same driver, several
  clients over several keys per range, and Phase 2's network and disk faults. Its driver
  draws from a stream `shard`, and each arm from a stream of its own, which D-031
  requires of every new arm (DECISIONS.md:1380-1383):
  - `Fault::MetaReorder` (stream `meta-reorder`): at a split of a range led on node `a`,
    a one-way block from `a` to the meta range's leader's node
    (crates/ananke-env/src/sim/mod.rs:437-444) for a fixed hold (Q39); during the hold
    the arm transfers the left half's leadership to a replica on a third node `c`
    (`TimeoutNow`, D-028) and the driver splits the left half again through `c`, whose
    updates reach meta by an unblocked link. `a` keeps resending the first update until
    it is acknowledged (§1), so it arrives after the second whenever the second split's
    update applies within the hold. The block also stalls every range `a` leads with a
    follower on that node, which is why it is held for a bound. Its test asserts at
    every tier that the arm fired and prints on how many seeds meta applied the second
    split's update before the first's;
  - the workload itself, as the Figure 8 driver's burst is (D-031): a split drawn while
    a burst of right-half writes is pending on the parent's leader; a split drawn while a
    burst of right-half gets waits on the parent's leader's read-index round; a split
    drawn while one client's cache still names the parent and another client writes the
    right half through R; a move drawn onto a range just after a split of it, before its
    log is compacted, so the incoming learner replays `s`; a `Change` sent by the driver
    to a range it is merging; and a crash of a range's leader while a move's learner is
    catching up, which abandons the change (D-032).
- `sim/split.rs`, directed, four nodes, nodes 1 to 3 the bootstrap nodes, in three
  halves, (i) and (ii) with P on nodes 1 to 3:
  (i) *overlap*: a follower `x` of P cut off from before a split until both halves have
  compacted past `s`, P's left half written larger than one snapshot chunk and R's right
  half smaller, then healed under a frame-length limit toward `x` that lies between the
  two (`Sim::limit_frames`, crates/ananke-env/src/sim/mod.rs:446-457, D-049). Chunks
  travel in frames of their own (§4), so the limit separates the streams; the scenario
  asserts per seed that a chunk of R's stream reached `x` while a chunk of P's was
  dropped as oversized.
  (ii) *vote*: the same cut-off `x`; at the heal the node leading R is crashed and `x`'s
  links to the third node healed, its links to the crashed node staying cut, and the
  crashed node restarting after ten maximum election timeouts. Until then R's remaining
  replica can win only with `x`'s vote, and `x`'s R is
  a placeholder throughout: `x`'s P reaches past `s` only by P's post-split snapshot,
  which creates no R, and R has no leader to stream R's. The scenario asserts per seed
  that a pre-vote or vote request of R reached `x` before `x`'s `RangeCreated` of R.
  (iii) *crash*: `Fault::CrashSplitting` (stream `split-crash`). P is first moved onto
  nodes 2 to 4, and the victim is node 4's replica: node 4 holds no replica of range 0
  or 1, so the split's meta update writes nothing there; no other range takes writes;
  and the driver's writes to P stop a heartbeat interval before it proposes the split,
  so `s − 1` is applied on node 4 well before `s` arrives. Node 4's append of `s` is
  synced before `s` commits, and a commit persists nothing (core.rs:1278-1298), so after
  node 4's `RaftCommit` reaching `s` its engine syncs nothing but the apply of `s`. The
  arm learns `s` from the `RaftProposed` of the driver's split request and crashes node 4
  at its first `WalSynced` after that `RaftCommit`, stepping in 250 µs slices as the
  adoption watch does (sim/raft.rs:3850-3862), then restarts it. A
  `RaftApply` is traced only after the whole apply has returned (node.rs:1160-1192), so
  in `SplitNotAtomicWithDescriptor` it follows both batches and cannot aim the crash; the
  first `WalSynced` is the first batch in the variant and the only one in the correct
  system. Writes to both halves resume once the victim restarts, so a restarted node
  whose P still covers the right span can take one. Its test asserts on every seed that
  the arm fired at that sync, and prints how often the restart shows the variant's state,
  since whether the second batch is durable within one slice is the disk's draw (100 µs
  to 2 ms, sim/raft.rs:2807-2808).
- `sim/merge.rs`, directed, every seed, five nodes, two adjacent ranges and a merge, in
  five shapes, (a) to (c) with both ranges on nodes 1 to 3 and (d) and (e) with both on
  all five:
  (a) a move takes R's replica from node 3 to node 4 and the merge is proposed, which the
  correct coordinator refuses; a second move brings it back to node 3, leaving R's
  generation two above L's, and the merge is proposed again, which the correct system
  completes;
  (b) one replica of R cut off from before `Subsume` commits until the first of `Merge`
  applied elsewhere or a fixed hold longer than the coordinator's wait bound (Q24), and
  then L's leadership transferred to that node (D-028); the correct run is asserted to
  trace `RangeMergeAborted` and a later `RangeUnfrozen`;
  (c) R's leader on node 2 and L's on node 1; once `m` commits, a one-way block from
  node 1 to node 2, so node 2 neither learns the commit nor applies `m` while R's leader
  there keeps its lease through node 3; a client whose cache names R reads the right
  span through node 2 from `f` on, while another client writes it through L; the
  scenario asserts per seed that a request naming R reached node 2 while its R was
  subsumed;
  (d) R's leader on node 2 and the coordinator, L's leader, on node 1; node 5 crashed
  before `Subsume` commits, so the coordinator's wait passes its bound; node 1 cut off
  from nodes 3, 4 and 5 but not from node 2 one heartbeat interval before the wait's
  bound ends, so the coordinator still leads when it gives up; node 5 restarted after
  a hold. The coordinator appends `MergeAbort`, which cannot commit, and a new leader of
  L is elected among nodes 2 to 4; R, led from node 2, still commits. The scenario
  asserts per seed that a new leader of L took office with L `Merging`;
  (e) `Merge` before an abort in L's log: R's leader on node 2; the coordinator on node 3
  appends `Merge` at `m` on nodes 1, 2 and 3 and crashes before any of them learns that
  `m` committed; node 5 is down; node 1 is elected L's leader and at once cut off from
  nodes 3, 4 and 5 but not from node 2, so its no-op cannot commit and `m` stays
  unapplied on it; its wait for R's replicas passes its bound and it appends `MergeAbort`
  at `i > m`, while R, led from node 2 with node 4, still commits; after a hold the cut
  heals and nodes 3 and 5 restart, and `m` commits and applies before `i`. The scenario
  asserts per seed that `m` applied as a merge and `i` as nothing, and the correct run
  that no `RangeUnfrozen` of the attempt was traced.
- `sim/move.rs`, directed, every seed: range X, its log compacted, on nodes 1, 2 and 3,
  moved from 3 to 4, in three shapes: (a) the moment node 3 leaves the configuration in
  force on X's leader, a stayer crashes and a frame-length limit toward node 4 holds
  snapshot chunks, over a hold; the scenario asserts per seed that the limit and the
  crash were in place, and in a variant's run whether node 4's chunks were dropped while
  X's appends to node 4 were delivered; (b) X's leader cut off with `C_new` appended on
  node 3 and not committed, and a new leader elected without it, as the membership
  scenario's partition phase is built (sim/membership.rs:565-614); (c) the move
  completed and no range on node 3 taking writes around its collection, so no later
  sync of its engine makes an unsynced deletion durable, `Fault::CrashCollecting`
  (stream `collect-crash`) crashes node 3 within five milliseconds of its `RangeRemoved
  { cause: collected }` and restarts it, asserted per seed to have fired; the catch is
  asserted on some seeds, since what the crash keeps of an unsynced write is the disk's
  draw.
- `sim/balance.rs`: a thousand ranges made by driver splits (Q18), nodes added and
  removed, and the balance check of §8 (its node count, faults, tier and bound Q30,
  Q39).

| Variant | The rule it breaks | What catches it | Needs |
|---|---|---|---|
| `SplitNotAtomicWithDescriptor` | §5: a split's apply writes P's applied index, both descriptors and R's Raft state in one batch; the variant writes the applied index in P's usual batch and the descriptors and R's state in a second batch after it | check 7 at the restart after a crash between the two: the node restates P at an applied index past `s` with P's old span; check 8 at the same restart: the node holds no R; linearizability on the seeds where that node's P leads and accepts a right-half write R also accepts | `sim/split.rs` (iii), `Fault::CrashSplitting` |
| `ApplyIgnoresSpan` | §3: the apply check is where a split takes effect for proposals in flight, and what makes a `RangeMismatch` at apply definite; the variant applies a command whatever its key | check 9 at the first applied write outside the span; check 10 when R has written the key first: a write by P at R's generation; linearizability: a write P acknowledged that R's readers never see | the sharded sweep's split under a pending right-half burst |
| `ReadCheckAtReceiptOnly` | §3: a read is checked against the descriptor at the applied index it is served at; the variant checks it at receipt only, so a split applied between receipt and serving goes unseen | check 9 at the first `RaftRead` whose `applied` lies at or past the split, for a right-half key | the sharded sweep's split under a pending burst of right-half gets |
| `TrustStaleDescriptor` | §3: a server checks a key against its own descriptor at receipt and at serving; the variant takes the range a request names as proof that the key is in it and checks neither, keeping the apply check. This is the owner's "client that trusts a stale descriptor", placed where trust can do harm: a client's cache is advisory by design and a stale one costs a round trip unless a server honours it (Q38) | check 9 at the first read served for a key outside the serving range's span; linearizability: a read through P after a split returns what R's writes have replaced | the sharded sweep's workload: a split while one client's cache still names the parent and another client writes the right half through R |
| `ClientIgnoresMismatch` | §3: a client merges the descriptors a `RangeMismatch` carries into its cache before it sends again; the variant resends to the range and generation it had (Q38) | check 17 at the first resend; the liveness bound of §8 on uniform seeds | any split with load: the sharded sweep |
| `SnapshotOverlapsReplica` | §5: a snapshot installs on a node only where it overlaps no other initialised replica; the variant installs every one | check 8 at the install's `RangeCreated { cause: snapshot }` | `sim/split.rs` (i) |
| `UninitialisedReplicaVotes` | §5: a placeholder grants no vote or pre-vote (Q22); the variant grants them from a placeholder, with nothing durable | check 20 at the first grant | `sim/split.rs` (ii) |
| `PlaceholderAcknowledges` | §5: a placeholder acknowledges no append (Q22); the variant answers an AppendEntries at its `next` as matched, with nothing in any log | commit by majority, check 3: a commit counted node 4, which traced no `RaftAppend` of the entry | `sim/move.rs` (a) |
| `SplitCreatesRightOnNonVoter` | §5: R's state is written only on a voter of P's configuration at `s`, and a learner gets a range delete; the variant writes R on every replica that applies `s` | check 8's clause that a node that is not a voter of P's configuration at `s` traces no `RangeCreated { cause: split }` of R | the sharded sweep's move just after a split |
| `MergeDivergentReplicas` | SPEC §4 and §6: identical replica sets. The variant drops every voter-set check — the coordinator's, `MergeBegin`'s and `Subsume`'s `voters` re-checks, and `Merge`'s check that L's configuration has R's voters — and keeps adjacency, the states, the wait for every replica of R and each node's own check | check 11 at the first `RangeMerged` anywhere, on a node that holds both: L's configuration at `m − 1` and R's at `f` differ. Not linearizability: each node's own check stalls a replica of L with no R and re-seeds it with the merged range's data | `sim/merge.rs` (a) |
| `MergeBeforeRightApplied` | §6: the coordinator waits for every replica of R to apply `Subsume`, and `Merge`'s apply checks its own node's; the variant keeps every voter-set check, waits for a majority and checks nothing on the node | check 11: `right_applied` below `f`; linearizability: that node's L lacks writes R committed below `f` and serves reads without them once it leads | `sim/merge.rs` (b) |
| `MergeTakesLeftGeneration` | §1 and §6: a merged range's generation is max(g_L', g_R) + 1; the variant gives it g_L' + 1 | check 18 at the merge's apply; check 10 at the merged range's first write to a right-span key R wrote at a higher generation; meta's convergence bound, since meta keeps R's record over the merged one | `sim/merge.rs` (a) |
| `ServeAfterSubsume` | §6: a subsumed range serves nothing and applies nothing from `f`; the variant's R keeps serving reads and applying writes | check 9 at the first read or write on a subsumed descriptor; linearizability: a read through R returns a value L has overwritten | `sim/merge.rs` (c) |
| `UnfreezeBeforeAbortCommitted` | §6: `Unfreeze` is proposed only once the abort has taken effect on the proposer's node, or its record has been read; the variant proposes `Unfreeze` in the round it appends `MergeAbort` | in `sim/merge.rs` (d), check 12's first clause, at the unfreeze's apply, before any abort of the attempt took effect; in (e), where the merge wins, check 12's second clause and check 19 (R and the merged L overlap), and on the seeds where both then take writes to the right span, check 10 and linearizability | `sim/merge.rs` (d) and (e) |
| `MergeNotResumed` | §6: a new leader of L that takes office with L `Merging` asks R and completes or aborts; the variant's new leader does nothing | the bound of §8 on a subsumed range being merged or unfrozen | `sim/merge.rs` (d) |
| `ChangeWhileFrozen` | §6: a node refuses `Change` for a range that is merging or subsumed, and a leader accepts none before it has applied its term's first entry; the variant accepts one whenever its core has none in flight, as today | check 22 at the acceptance | the sharded sweep's `Change` to a range it is merging |
| `RemoveBeforeCaughtUp` | §7 and D-029: a move is one change whose joint entry waits for the incoming replica's catch-up; the variant's rebalancer removes the outgoing replica and then adds the incoming one | check 15 at the removal's configuration; the hold check of §8: the correct move commits through the other stayer and the caught-up node 4, and the variant, on nodes 1 and 2 with one crashed, commits nothing until the crashed one restarts | `sim/move.rs` (a) |
| `JointBeforeCaughtUp` | D-029, RAFT.md §1: learners first, the joint entry once a round to each is shorter than a minimum election timeout; the variant proposes the joint entry at once | check 13; the hold check of §8: node 4 holds nothing when the stayer crashes and its snapshot is held, so the range commits nothing where the correct move's node 4 already holds the log | `sim/move.rs` (a) |
| `GcBeforeRemovalCommitted` | §7 and D-033: a removed replica is collected only once a descriptor that excludes it at a higher generation is applied, since a truncation reverting `C_new` restores its rights; the variant collects when the replica appends `C_new` | check 14 | `sim/move.rs` (b) |
| `RemovalNotDurable` | §7: a collection's deletion is a synced batch; the variant writes it unsynced | check 21 at the restart: the node restates the collected replica | `sim/move.rs` (c), `Fault::CrashCollecting` |
| `MoveNeverRetried` | §7: a rebalancer that finds the old voters after a bound traces the move abandoned and may begin it again; the variant keeps waiting on a change a leadership change abandoned (D-032), counted in flight, and traces nothing | the bound of §8 on a move being traced done or abandoned | the sharded sweep's leader crash during a learner's catch-up |
| `RebalancerIgnoresLeaders` | SPEC §4: the rebalancer balances range count and leader count; the variant balances replicas only | the balance check of §8: leaders gathered where splits put them (Q21) stay there | `sim/balance.rs` |
| `MetaOverwritesByArrival` | §1: the meta range keeps, per key, the descriptor of the highest generation; the variant stores each update as it arrives | check 16 at the first key whose named generation falls; the meta convergence bound of §8 when the regressed record names a range that no longer exists | `Fault::MetaReorder` |

The checks about time in §8 are bounds, not properties: chosen so the correct system
never trips them over ten thousand seeds, as RAFT.md §5 chooses its own. A bound the
correct system trips is a model error to fix, not a bound to widen (D-030, D-039;
SPEC.md:270-280). Every bound this document names is Q39 until a run has measured the
correct system against it.

## 11. What the layers below must provide

Each item is something the design above needs and the tree does not have, with where the
tree stands. Several depend on Q2, one engine per node or one per replica; the list
assumes one per node and says what changes otherwise.

**Storage (`ananke-storage`).**

1. *Ranges' Raft state in one engine.* The Raft keys are fixed names under tenant 0 —
   `hard`, `applied`, `reseeded`, `incarnation`, the log by index, `config`, `snapshot`
   (store.rs:65-127) — so two groups in one engine collide on every key. The key layout
   needs a range id (Q5). This is the Raft crate's layout on the engine's opaque keys:
   the engine has no notion of tenant: no source file of `ananke-storage` mentions one,
   and the encoding is built by the Raft crate (store.rs:76-83).
2. *A bounded, ordered seek.* `scan` returns every key of a span as one `Vec`, with no
   limit, no reverse and no "first key at or after `k`" (engine.rs:1051-1074);
   `RaftStore::open` already loads a whole log that way (store.rs:683-698). A meta
   lookup, the first record whose end key is above `k` (§1), and a split key chosen from
   a range's keys (Q18) need one.
3. *A range delete.* Deleting a merged range's Raft state, a collected replica's span, or
   the right span on a node outside the right half's configuration (§5) is one tombstone
   per key in a `WriteBatch` today (engine.rs:285-329, 976-995), each
   kept until it reaches the bottom level or no older write lies below it
   (compaction.rs:10-15).
4. *A checkpoint of a span.* `Engine::checkpoint` copies every table in service and the
   memtables at one version (engine.rs:1138-1226) and takes no key range. A range's
   snapshot needs the span's user keys and that range's Raft keys at one version.
5. *An install of a span into a live engine.* The only install today replaces the whole
   store directory at the server's next start, before the engine opens
   (snapshot.rs:305-449), and putting tables into a live engine is crate-private
   (`manifest_edit`, `install`, engine.rs:1434-1479). A range's install must remove the
   span's keys and add its tables in one manifest switch while the node's other ranges
   keep running, with the new tables' sequence numbers above the live engine's. An
   install today ends the server's run-loop incarnation and reopens the engine
   (node.rs:999-1002, 494-497), which in a shared node would restart every range on it;
   the store incarnation of D-042 is kept (node.rs:990-993).
6. *A checkpoint that does not stall every range.* A checkpoint holds the turnstile, so
   no flush or compaction runs meanwhile (engine.rs:1150; D-024, DECISIONS.md:736-737).
   With one engine per node one range's take stalls every range's flushes, and with one
   `apply` task per node (Q14) every range's applies, which wait behind a take (D-036).
7. *An approximate size of a span*, for a size-triggered split (Q18) and the
   rebalancer's load (Q31). Tables carry their key bounds and sizes (manifest.rs:40-58);
   nothing sums them over a span.
8. *Loss attributed below the engine, or not.* A dropped table, a fallback, a head gap
   or a stopped log refuses the whole store (store.rs:644-650) and quiesces the whole
   engine (engine.rs:1127-1136; D-022, D-044). With one engine per node, one lost table
   refuses every replica on the node. Whether a loss can be attributed to the ranges
   whose keys the lost table covered — its `first_key` and `last_key` are all the engine
   records of it (manifest.rs:54-57) — is Q15. Under Q15's draft the node is refused
   whole: its marker says lost (D-044) and its engine is quiesced, so no range's re-seed
   can install into that engine. What the node re-seeds into is not provided: a fresh
   engine adopted at a start once every range's install has staged, or a fresh engine
   opened at once with each range installed live (item 5) as its stream completes, which
   must square with D-041's rule that a directory that held a store never opens fresh.
   Either needs a durable per-replica mark of which ranges are still refused on the new
   engine, which nothing has today (Q15). §6's stalled replica is not a refusal and needs
   none of this.

With one engine per replica, items 1, 4, 5, 6 and 8 reduce to what exists, and in their
place: split and merge must copy data between engines, which no batch can be atomic
with, and memory and tasks grow with replicas (§4).

**Raft (`ananke-raft`, or the range layer above it, Q40).**

1. *A range on every frame and every client message.* A frame is `kind | from | term |
   fields` (message.rs:1-8, 381-385); a request is `client | seq | command` and a reply
   `Outcome` or `NotLeader` (client.rs:39-59). The batch frame of §4 and a studio
   decoder that yields several messages per frame (`studio`, message.rs:659) are new.
2. *A node that runs many groups.* `run` is one group: one socket, one core, one ticker
   (node.rs:304-346, 700-713). §4's node steps many cores on one ticker, gathers a
   round's persists into one synced batch before any of the round's sends — `execute`
   awaits one persist per step today (node.rs:2045-2090), the order RAFT.md §3 and D-026
   fix — keeps a core that persisted from stepping again in the round, flushes a
   per-peer outbox at the end of each round, and leaves snapshot chunks in frames of
   their own (Q41).
3. *Commands for ranges.* `Command` is `Put`, `Delete`, `Cas`, `Get`, `Transfer`,
   `Change` (apply.rs:35-77); §5 to §7 add `Split`, `MergeBegin`, `Subsume`, `Merge`,
   `MergeAbort`, `Unfreeze`, `MetaUpdate`, `AddNode`, `RemoveNode`, and the lookups
   `Descriptor`, `Applied` and `Summary`, which are reads.
4. *Descriptors in apply.* `apply_command` writes the command and the applied index in
   one batch and checks no key against anything (apply.rs:244-274). §3's apply check,
   the `effect` it traces, and §5's and §6's batches go there.
5. *`RangeMismatch`* beside `NotLeader` (client.rs:50-59), and §3's checks at receipt
   and at a read's serving (node.rs:827-841, 2092-2115).
6. *A group started from a floor with no checkpoint.* `Raft::restore_compacted` takes a
   snapshot index, term and configuration (core.rs:867-942); a stream to a follower that
   finds no complete checkpoint at the recorded index asks for a take
   (node.rs:1722-1735). Both exist; a split-born range relies on them together.
7. *Placeholders and collection.* A server not in any configuration sits quiet only if
   its process runs (node.rs:130-135); nothing creates a replica for a range a node does
   not host on first contact, and nothing removes one (D-029: removed servers "are never
   told to shut down", DECISIONS.md:1157-1159).
8. *A completion signal for changes, and a guard on accepting them.* `Change` is answered
   `Done` on accept and completion is visible only as configuration entries
   (node.rs:818-826, 868-886; D-029). The rebalancer reads descriptors instead (§7),
   which item 4 provides. A leader accepts a change today whenever its core has none in
   flight (DECISIONS.md:1099-1104); §6 adds that it accepts none from the moment it
   proposes `MergeBegin`, none before it has applied its own term's first entry, and
   none for a range that is merging or subsumed, and check 22 needs the acceptance traced
   as `RaftChangeAccepted`.
9. *The learner round in the trace.* `note_learner_round` marks a learner caught up
   (core.rs:2002-2026) and traces nothing; check 13 needs `RaftLearnerRound`.
10. *A seed per range.* A core's generator is seeded from the node's protocol stream at
    its incarnation's start (node.rs:551, core.rs:932); §4 and Q13.
11. *An inbox for many ranges.* One inbox bounded by message count, dropping the oldest
    heartbeat of any sender first (node.rs:1918-1962), 128 in the sweep
    (sim/raft.rs:2859), against §4's 200 and 2 000 arrivals a tick (Q14). Its admission
    scans the queue on every arriving message: `count` is a linear filter and
    `remove_first` a linear search, called up to twice (node.rs:1933-1950;
    `queue.rs:103-110`), so a tick costs arrivals × capacity. Before its capacity grows
    with ranges it needs admission in constant or logarithmic time, with a kept count of
    messages and an index of heartbeats by sender and range.
12. *The checker keyed by range.* `invariants::Checker` keys every map by server, term
    or index (invariants.rs:253-282) and takes `1..=servers` as the first configuration
    (invariants.rs:289-295); §8's checks 1 to 4 need range keys and `RangeCreated`, and
    checks 7 to 22 are new, each under the incremental checker's equivalence test with a
    variant that trips it (sim/tests/raft.rs:2105-2189).
13. *Follower compaction.* Only a leader compacts (core.rs:1830-1833), and a follower's
    log shrinks only by truncation or an install (core.rs:1693; store.rs:686-690), so
    with two thirds of a node's replicas followers, their logs grow without bound under
    writes (§4). A follower must compact to its own applied checkpoint, or to one its
    leader names, so that a node's replicas have bounded logs.
14. *Snapshot files and receivers per range.* The staging directory is one per engine
    directory (`install`, snapshot.rs:92-94), a version directory is named by index and
    take alone (`snap-<index>-<take>`, snapshot.rs:112-114), and one `Assembler` per
    snapshot task holds one stream, abandoning it for a chunk of another identity
    (snapshot.rs:859-864, 906-915; node.rs:1398, 1803). `sweep_versions` deletes every
    unpinned version directory the store's single snapshot record does not name
    (snapshot.rs:193-228). With one engine per node, two ranges' takes at one index
    collide, one range's sweep deletes other ranges' checkpoints, and §4's re-seeds
    toward one node restart each other. Staging, versions and the sweep need keying by
    range; the receiver needs one assembly per (range, sender), under a cap (Q14); and
    §5's `Overlaps` answer is new.
15. *A read served at one version.* The core traces `RaftRead` when it confirms a read
    (core.rs:1175, 1202), holding neither the key, which stays in the server's `reads`,
    nor the applied index the read is served at; the server then serves from the
    engine's latest state (node.rs:2092-2110). §3's read check and check 9 need the
    value, the descriptor and the applied index read at one engine version
    (`Engine::snapshot`, `get_at`, engine.rs:1015, 1040), and the serving event traced
    by the server with that version's applied index.
16. *A client for ranges.* The client (client.rs) and the sweep's client loop
    (sim/raft.rs:2931, 3015-3042) route to a server by leader hints and rotation alone.
    §3's cache, its merge by generation, the lookups through range 0 and range 1, the
    following of `RangeMismatch` and the `ClientSend` and `ClientMismatch` events are
    new in both; item 5 is the server's side only.
17. *Replicas that answer as refused without being refused.* §5's placeholder and §6's
    stalled replica answer AppendEntries with a rejection hinting index 1, an echo of
    zero and incarnation 0 (RAFT.md:501-511), grant nothing and take a snapshot; today
    only a server in re-seed mode does, and it holds no store. The merge's entries
    carry their attempt (§6), and a `MergeAbort` that takes effect writes an abort record
    under L's keys.

**Env (`ananke-env`) and the simulator.**

1. *The trace events of §8.* A `range` on every `Raft*` event about a replica, `effect`
   and `key` on `RaftApply`, `RaftRead` moved to the server with `key` and `applied`,
   and the new events, each with its
   export line in `convert` (trace.rs:368-652; moirae.rs:277-988). Every pinned trace
   hash moves once, deliberately, with the reason in the commit (CLAUDE.md, "Every state
   transition that matters emits a trace event").
2. *A stream per node and range*, `n{id}/r{range}/protocol`, if Q13 chooses it (D-017).
   A node's streams are derived by `node_stream` as `n{id}/{label}`
   (crates/ananke-env/src/sim/state.rs:159-165) and made when the node is added
   (crates/ananke-env/src/sim/mod.rs:282-283). Protocol code reaches randomness only
   through `Environment::rng` and `sched_rng` (crates/ananke-env/src/env.rs:28-35), so a
   labelled stream per range needs a new method on the trait, implemented by `SimEnv` as
   a derived stream and by `RealEnv` from OS entropy, not a change to the simulator
   alone.
3. *A bounded queue per destination in `SimEnv`.* D-015 says each destination has one
   (DECISIONS.md:261-263); `RealEnv` has it (crates/ananke-env/src/real/net.rs:30, 133,
   208), `SimEnv` delivers into an unbounded per-socket queue
   (crates/ananke-env/src/sim/net.rs:271-275). With many ranges sharing one socket the
   sweep would not see the drops `RealEnv` takes (Q16).
4. *A trace a large run can hold.* Every frame is recorded with its payload
   (crates/ananke-env/src/sim/net.rs:150-158) and every poll as a record; the export
   writes one line per poll unless built `without_polls` (moirae.rs:97-106). §4's 270 MB
   of payload per run at 1 000 ranges needs a record that carries, per contained
   message, what the checks read — range, kind, term, and for an AppendEntries or a chunk
   whether it resets a timer — in place of the frame's bytes, and a cap other than
   400 000 (sim/raft.rs:136) (Q39). Dropping payloads outright is not open: the timer
   replay and other sweep checks decode `MessageSent` payloads with `Frame::decode`
   (sim/raft.rs:1264-1299, 1846-1853, 2237-2246, 2470-2482, 3623-3655), and each would
   have to change.
5. *A run-length hint that counts work*, not nodes
   (crates/ananke-env/src/sim/mod.rs:155-164; D-016).
6. *Nodes added to and removed from a running simulation.* `Sim::add_node` exists
   (crates/ananke-env/src/sim/mod.rs:263-277) and `Sim::crash` kills every task of a
   node (crates/ananke-env/src/sim/mod.rs:490); a node removed for good is a crash with
   no restart. Server addresses in the sweep are `10.0.0.<id>` with the id as a `u8`
   (sim/raft.rs:144-148), which bounds a scenario at 255 nodes.
7. *Scenarios in the determinism test.* `sim/tests/parallel.rs` hashes a seed alone
   against the same seed among its neighbours for echo, the WAL, the engine and the raft
   sweep (sim/tests/parallel.rs:6-39), not for the membership or quorum scenarios; every
   Phase 3 scenario belongs there.
8. *Leader-relative faults per range.* `leader_now` returns the server of the latest
   `RaftLeader` of any group (sim/raft.rs:2878), and every leader-relative arm reads it;
   an arm aimed at a range must choose the range from its own stream.
9. *The history's closure keyed by range and by effect.* `sim/lin.rs` closes a pending
   operation by the first `RaftApply` of its `(index, entry_term)` on any server, and
   takes every applied entry as having taken effect (sim/lin.rs:8-11, 93-109). §9 needs
   the closure keyed by `(range, index, term)` and closing only on effect `applied`.
10. *The new scenarios' arms.* `Fault::CrashSplitting`, `Fault::MetaReorder` and
    `Fault::CrashCollecting` (§10) are new, each on its own stream (D-031), with
    `sim/shard.rs`, `sim/split.rs`, `sim/merge.rs`, `sim/move.rs` and `sim/balance.rs`.

## 12. Order of work, if approved

1. The trace events and the checker keyed by range, with today's single group as one
   range: no behaviour change; the pinned hashes move once.
2. Storage: the range-keyed layout, the bounded seek and the range delete, each with its
   crash test; then the span checkpoint and the live install, with the sweep of
   Phase 1's engine extended to them.
3. The node of §4 with static ranges made at bootstrap: the shared ticker, batched
   frames, the round of Q41, follower compaction, snapshot files and receivers keyed by
   range; a core step's cost measured against §4's budget; every Phase 2 variant
   re-asserted on a run with several ranges per node, to its Phase 2 standard (§10).
4. Descriptors, the apply check, `RangeMismatch`, the client cache, range 0 and the meta
   range; `TrustStaleDescriptor`, `ClientIgnoresMismatch`, `MetaOverwritesByArrival`.
5. Split, with `sim/split.rs` (i) and (ii) and its variants, `ReadCheckAtReceiptOnly`
   among them.
6. Placeholders, collection and moves by joint consensus, with `sim/move.rs`,
   `Fault::CrashCollecting` and their variants; then `sim/split.rs` (iii), which moves P
   first, with `Fault::CrashSplitting` and `SplitNotAtomicWithDescriptor`.
7. Merge, with `sim/merge.rs` and its variants.
8. The rebalancer and `sim/balance.rs`, with `MoveNeverRetried` and
   `RebalancerIgnoresLeaders`; the exit criteria of SPEC §4; the devlog.

Each step stops for review.

## 13. Questions for approval

Every design choice above that SPEC, RAFT.md and DECISIONS.md do not settle. Each names
its options, the one the draft assumes where the text needs one, and why it is open.
None is decided by this document; an approved answer becomes a DECISIONS.md entry, and
an answer that changes SPEC §4 or RAFT.md supersedes the text it changes with a forward
pointer, as D-048 did (DECISIONS.md:3206).

**Q1. Phase 2's tag, and RAFT.md's drift, before Phase 3's code.** (a) Tag and publish
Phase 2 per D-011, and correct RAFT.md where it describes what the code does not have
(`Scan`, `VoteBeforePersist`, `ApplyNotAtomicWithIndex`, the frame's field order,
`src/read.rs`), before any Phase 3 code; (b) start Phase 3's first stage beside the tag.
Draft: (a) for code; this document is design only. Open because D-011 defines when a
phase is done but no entry says whether the next may start before it, and the drift has
no entry.

**Q2. One engine per node, or one per replica.** (a) One per node: split and merge are
one batch and move no data, one WAL and one memtable budget per node; it needs the span
checkpoint, live install and range delete of §11, a lost table refuses every replica on
the node (Q15), and one range's take stalls every range's flushes (and, with one `apply`
task, its applies, Q14). (b) One per replica, today's shape: a loss refuses one range;
split and merge copy data with no atomic primitive, and tasks, memory and fsync streams
grow with replicas (§4). (c) A few
engines per node, ranges assigned to them. Draft: (a). Open because SPEC §3 puts one
group's log in the engine (SPEC.md:244-245) and nothing covers many groups; checkpoint
(D-024), refusal (D-044), quarantine (D-035) and incarnation (D-042) are each defined
per store directory.

**Q3. Which copy of a descriptor is the authority, and how meta is kept.** (a) The
range-local copy, with meta an index merged by generation and repaired by each leader on
taking office, with or without a periodic repair; (b) a commit protocol for Phase 3
alone that updates a range and the meta range together; (c) meta as the authority, every
server reading it, or holding a lease on its record, before serving. Draft: (a), repair
on taking office only, with the node that sent an update resending it until
acknowledged whether or not it still leads. Open because SPEC §4 says descriptors are
"stored in a meta range" and nothing about atomicity, and cross-shard atomic commit is
Phase 4 (D-006).

**Q4. Addressing levels.** (a) Range 0 the root, holding the meta range's descriptor,
and range 1 the meta range, neither splitting; (b) range 0 is the meta range; (c) two
levels with a meta range that splits, as CockroachDB's meta1 and meta2. Also the meta
record's layout: keyed by a range's end key, so a lookup is the first record above `k`,
or by its start key, so a lookup is the last record at or below `k`; and the write rule,
that a record a newer descriptor partly overwrites is cut at that descriptor's
boundaries in the same batch, or left whole and resolved at lookup. Draft: (a), end
key, cut in the batch. Open because SPEC's "range 0 is found via config, then meta range
via range 0" (SPEC.md:287-288) implies two groups and says nothing of what else range 0
holds or whether meta splits; ten thousand ranges fit one meta range (§1), so (a) and (b)
both serve Phase 3.

**Q5. The keyspace's layout.** Which tenant holds the root and meta records (SPEC §6
puts Phase 5's catalog "in a system tenant", SPEC.md:340; user data is tenant 1 today,
apply.rs:23-24); how per-range Raft keys sit under tenant 0 — the range id after the
table, `0 / t / <range: u64 BE> / name`, or a table per range; whether a range may span
tenants. Draft: one reserved system tenant below every user tenant for root and meta,
the range id after the table, ranges may span tenants. Open because SPEC §2.6 fixes
`tenant | table | user_key` and RAFT.md §3's tenant-0 table is for one store. Changing
tenant 0's keys is a format change; D-027 and D-043 each changed a format on the ground
that no store was released (DECISIONS.md:947-950, 2317-2320), which holds while Phase 2
is unpublished.

**Q6. What raises a generation.** (a) Split, merge and every committed configuration
change; (b) split and merge only, with voters versioned apart. Draft: (a), which orders
replica sets by the same number clients and meta merge by and lets check 14 prove a
removal committed. Open because SPEC §4 names no generation.

**Q7. Bootstrapping a fresh cluster.** (a) Every bootstrap node writes the same initial
state from its configuration, as `initial_voters` does today; (b) one node initialises
on an operator's command and the others join by snapshot. And how many bootstrap nodes
there are: exactly the replication factor, since every initial range takes the
bootstrap nodes as voters and §7's one-for-one moves never change a voter count; or any
number, with each initial range's voters a replication-factor-sized subset chosen by a
stated rule. Draft: (a), and as many bootstrap nodes as the replication factor. Open
because SPEC says only "found via config"; (a) relies on every bootstrap node being
configured alike, which nothing checks.

**Q8. Nodes: membership, removal, failure.** Membership by (a) operator records in range
0, (b) static configuration, or (c) liveness records nodes write to range 0. Removal by
draining the node with moves. A node down for good (i) goes undetected until an operator
removes it, or (ii) is detected by liveness and its replicas replaced. Draft: (a),
drain, (i). Open because SPEC's exit criterion says "after node add/remove"
(SPEC.md:303) and not how, and node identity beyond an address (D-015) is undefined.

**Q9. Placement of ranges 0 and 1.** (a) Fixed on the bootstrap nodes, never moved, and
a bootstrap node never removed; (b) movable, with range 0 found some other way than
configuration. Draft: (a). Open because configuration is static and SPEC finds range 0
through it, while the exit criterion's node removal may want a bootstrap node gone.

**Q10. Requests, `RangeMismatch`, retries.** A request carries (range, generation), or
the key alone and the server chooses; `RangeMismatch` carries every descriptor the node
holds for the key, or nothing; the checks run at receipt, serving and apply, or at fewer
places (a receipt check alone lets a split miss proposals in flight, §3); a write
answered `RangeMismatch` is sent again as the same operation, or abandoned; a range id
is 8 bytes on the wire. For a resend as the same operation: the leader forgets its
record of a `(client, seq)` once its entry applies with an effect other than `applied`,
or the resend takes a fresh `seq` that the history pairs with the first; without one of
them, a resend that reaches a leader still holding the earlier entry is never proposed
nor answered (node.rs:843-849, §3). Draft: the first of each, and the leader forgets.
Open because SPEC §4 names the error and "they refresh" and no more, and D-026 covers a
write with no answer, not a write with a definite one.

**Q11. Client sessions in Phase 3.** (a) Not in Phase 3: a write with no answer is
abandoned, as today; (b) issue #21 brought into Phase 3, so a retry after a split, merge
or move is safe. Draft: (a). Open because BOOTSTRAP_PROMPT.md:124 schedules issue #21
"Before Phase 4", while splits and moves make unanswered writes more common, and the
rule against widening a phase (CLAUDE.md) weighs against (b).

**Q12. Heartbeats.** Batching, coalescing, quiescence, or a combination (§4). At 10 000
ranges on ten nodes, idle: batching carries 135 MB/s over the cluster and steps each
node's cores 500 000 times a second; coalescing, with the entry layout §4 assumes, about
65 MB/s and the same steps; quiescence, nothing for the ranges it quiesces, a wake on
their next proposal, and a liveness signal the tree does not have. Draft: batching only,
which holds at 1 000 ranges and not at 10 000 (§4). Open because SPEC
asks that ten thousand ranges not mean ten thousand heartbeat streams and not how, and
coalescing and quiescence both touch the lease's per-request promise (RAFT.md §1, D-028)
and check quorum's per-follower counting (D-049), whose arguments are made for one
group.

**Q13. A seed per range.** (a) A named stream per node and range,
`n{id}/r{range}/protocol`; (b) core seeds drawn from the node's protocol stream, as now.
Draft: (a). Open because D-017 names streams per node; (b) lets a split move every later
range's election timeouts, against D-017's purpose (DECISIONS.md:336-341), and (a) is an
environment change: a new `Environment` method for a labelled stream, in `SimEnv` and
`RealEnv` both (§11, env 2).

**Q14. Tasks, inbox and streams on a node with many ranges.** One `apply` task per node
or per range; an inbox per node sized by the ranges it hosts, bounded in bytes, or one
per range; a cap on the snapshot streams one node sends or receives at once, against
D-043's stream to every designated follower at once, and on the streams one node
assembles at once, one per (range, sender). One `apply` task per node means one range's
snapshot take stalls every range's applies on the node, since applies wait behind a take
(D-036, RAFT.md:177-181). An inbox whose capacity grows with ranges also needs admission
that does not scan the queue (§11, raft item 11). Draft: one `apply` task per node; the
inbox and the caps are left to this question. Open because RAFT.md §3's four tasks,
D-026's inbox and D-043's streams were built for one group.

**Q15. A loss in a shared engine.** (a) A lost table or log record refuses the whole
node, each replica re-seeded by its own leader; (b) only the ranges whose spans the lost
table's key bounds, or the lost log records, touch are refused. Under (a): what the node
re-seeds into, since its engine is quiesced (D-044) — a fresh engine adopted once every
range's install has staged, or a fresh engine opened at once with ranges installed live
as they arrive, against D-041's rule that a directory that held a store never opens
fresh — and a durable mark per replica of which ranges are still refused (§11, storage
8). And the lasting cost: every re-seeded replica is quarantined (D-035), 300 on a node
at 1 000 ranges and 3 000 at 10 000, so once two nodes have each lost a table, about 67
or 667 ranges can keep a leader they have and cannot elect one until Q33's moves take the
quarantined replicas off (§4). Draft: (a), the second way back to one engine. Open
because D-022 refuses any hole in the middle of the state and D-044 makes the refusal
durable per directory, and neither considered a store holding several state machines.

**Q16. A bounded queue per destination in `SimEnv`.** Add it before one socket carries
many ranges, or leave `SimEnv` unbounded. Draft: add it. Open because D-015 states the
bound and only `RealEnv` implements it (§11), and adding it moves every simulation's
drops.

**Q17. Allocating range ids.** (a) A counter in range 0, taken by compare-and-set before
a split; (b) an id derived from the parent's id and the split index. Draft: (a). Open
because SPEC is silent; (a) costs a write to range 0 per split, and (b) needs no write
but builds each id from its parent's, so ids are not one fixed-width counter.

**Q18. What splits, where.** An operator's command; a size threshold (§11 storage 7); a
load threshold; the sweep's driver. For the balance scenario: ranges pre-split at
bootstrap, or made by splits under load. Draft: operator and driver only, and the
balance scenario's ranges made by driver splits. Open because SPEC says a leader
"proposes split at key `k`" and not why or which `k`.

**Q19. Which half keeps the parent's id.** The left, as CockroachDB's does, or the
right. Draft: the left. Open because SPEC is silent, and it decides whose meta record
and whose client caches stay right.

**Q20. The right half's starting term and floor.** (a) Current term and floor term both
the split entry's term, which would supersede SPEC's "term 1"; (b) term 1 and a floor of
`(s, 0)`, keeping SPEC's words and asking the core to accept a floor of term 0 above
index 0, which no code path has been read for; (c) fixed constants. Draft: (a). Open
because SPEC.md:291-293's "term 1 with the parent's applied index recorded as its
snapshot" conflicts with a snapshot's record of an index and that entry's term (RAFT.md
§1) unless the split entry's term is 1, and changing SPEC §4 needs an entry.

**Q21. The right half's first election.** Timers only; the replica on the node whose
parent replica leads starts its pre-vote at once and then waits for its timer; or it
starts at once and repeats every heartbeat interval until R has a leader or its timer
fires. The first pre-vote usually meets placeholders, which grant nothing (Q22), since
the other replicas learn that `s` committed up to a heartbeat interval after the
leader applies it. Draft: at once and repeated. Open because SPEC is silent; it trades up
to an election timeout of unavailability after every split for leaders gathered on the
parent leader's node, which the rebalancer then spreads.

**Q22. Uninitialised replicas.** (a) No vote, no pre-vote, no acknowledgement; (b) voting
from a hard state they persist, as CockroachDB's do, with the split's apply keeping the
vote. And, under (a), how a placeholder asks for its snapshot, since a silent one is never
designated (core.rs:1446-1455, 1930-1937, 2071): it answers an AppendEntries with a
rejection hinting index 1, an echo of zero and incarnation 0, as a refused server does
(RAFT.md:501-511), counted for nothing in a lease or read-index round and for check
quorum only as D-049 counts a refused server's; or a new designation trigger in the
core. The rejection can start streams the overlap rule then refuses while the node's
parent replica still covers the span. Draft: (a), with the rejection. Open because D-033
has a server with no configuration grant votes "by the usual rules" (RAFT.md:163-168);
that was written for a server with a store, and a placeholder has none, so (a) departs
from it for placeholders.

**Q23. Splits, merges and configuration changes together.** A split refused at proposal
while a change is catching up and at apply while the configuration is joint; or allowed,
the right half inheriting a joint configuration and losing the change's volatile
catch-up. A replica of P that is not a voter of P's configuration at `s` creating no R
and deleting the right span's keys (§5), or creating R as a learner of R's. A leader
accepting no `Change` before it has applied its own term's first entry, nor from the
moment it proposes `MergeBegin` (§6), or the node reading the range's state some other
way. Draft: refused; no R and a range delete; the first-entry rule and the refusal from
`MergeBegin`'s proposal. Open because "one change in flight" (DECISIONS.md:1099-1104) is
per group and silent on splits and merges, and learners appear in no configuration entry
(D-032), so no apply can see them.

**Q24. The merge protocol.** The coordinator: L's leader, or the rebalancer. Waiting for
every replica of R at `f`, or a majority with R's data carried in the merge entry. Abort
and unfreeze, or no abort, R frozen until the merge completes. How an attempt is named:
by `MergeBegin`'s index `h`, carried by every later entry, or by a generation R's
unfreeze raises. When `Unfreeze` may be proposed: once the abort has taken effect on the
proposer's node, or on reading L's durable abort record of `h`; not once the abort is
merely committed or applied, which a `Merge` earlier in L's log defeats (§6). Who
unfreezes a range subsumed for an attempt whose coordinator is gone: a new leader of L on
taking office, R's leader after a bound, or both. Range commands whose answers are lost:
resent until their effect is read, each applying as nothing when its attempt has ended,
which departs from D-026's rule for writes and leaves one copy, a late `Subsume`, that
freezes R for a bound; or not resent. A replica of L whose node lacks R at `f` when it
applies `m`, reachable when a node is re-seeded between the wait and its apply: stalls
without refusing, answering as a placeholder until a snapshot of L at or above `m`
replaces it; or is refused as RAFT.md §3 refuses a store, which under Q2 (a) refuses
and quarantines every replica on the node (SPEC §3, D-044, D-035) although nothing was
lost. L's pre-merge snapshot: not streamed after the merge, or streamed and followed by a
re-seed. Equal replica sets before a merge: made by the rebalancer, or by the
coordinator. Draft: L's leader, every replica, abort and unfreeze, `h`, on effect or
record, both, resent, stall, not streamed, the rebalancer. Open because SPEC says
"two-phase with a subsume command" and nothing more.

**Q25. What merges.** An operator's command and the sweep's driver; a size threshold;
the rebalancer merging small neighbours. Draft: operator and driver only. Open because
SPEC is silent and the balance criterion needs no merge.

**Q26. Replica identity and per-replica state.** (a) A replica is (range, node),
`ServerId` stays the node's number (types.rs:11-13), a `RangeRemoved` ends its identity
in the trace, and a replica must be collected before its range is added back to the
node; (b) a replica id fresh per addition. Also: incarnation and quarantine per node
store or per replica; whether a split's right half copies the parent replica's
quarantine (draft copies); whether a merged range takes R's. Draft: (a), per replica,
copies, does not take R's. Open because D-042 notes that a wiped server
is "a new member for the membership path" only as outside Phase 2's model
(DECISIONS.md:2064-2067), and in Phase 3 removing and re-adding a replica on one node is
a normal move. The draft keeps both per replica because Q15's draft re-seeds each
replica of a refused node from its own leader, so each draws its own incarnation, and a
quarantine kept per node store would leave a node that lost one table voteless in every
range for good, replicas later moved to it included (§4, §5).

**Q27. Collecting removed replicas, and the overlap rule.** Collection: the replica asks
its range's leader after silence and collects itself on a newer descriptor that excludes
it; the leader tells removed replicas; or each node sweeps its replicas against meta. If
the replica asks: after how long a silence (the draft's ten maximum election timeouts is
unmeasured), and whom, the leader meta names for the range or the last leader it heard
from. Overlap: a snapshot overlapping an initialised replica is refused and asked for
later, or installed after deleting the overlapped replica; if refused, how the refusal
reaches the leader (a new `Overlaps` answer, or the start-over ask of RAFT.md:195-199)
and what the leader does (stream again after a minimum election timeout, from a fresh
take only if its checkpoint predates its range's last split or merge; or retake at once;
or wait for the normal threshold); and whether a snapshot of a merged L at or above `m`
may replace the node's replica of the R it merged (§6). Draft: the replica asks, ten
maximum election timeouts, the leader meta names; refused, `Overlaps`, stream again
after a timeout; the merged snapshot replaces R. Open because D-029 leaves removed
servers "never told to shut down — an operator's business, left to the backlog"
(DECISIONS.md:1157-1159), and the mechanism is an unfiled candidate
(BOOTSTRAP_PROMPT.md:218).

**Q28. "Caught up" for a move.** D-029's log round as built; or also an applied index at
or past the round's end; or also a completed install. Draft: the round as built. Open
because D-029's round was built for the membership scenario, whose learners are never
fed snapshots (OVERNIGHT.md:188-191), and a moved replica usually is.

**Q29. Where the rebalancer runs.** Range 0's leader; the meta range's leader; the
holder of a separate lease record in range 0. Draft: range 0's leader. Open because
SPEC's "leaseholder-elected node" names a lease the tree does not define.

**Q30. The rebalancer's goal, and SPEC's 10 %.** Ten percent of what: each node's
replica and leader counts against their means, or the ratio of largest to smallest.
After how long: a bound after the last add or remove, measured on the correct system
before it is asserted (D-039). How many nodes (§4's per-node figures assume ten), under
which faults, at which tier (D-040), and how many moves at once. Draft: counts against
the mean; the rest left to this question. Open because SPEC.md:303 gives the number and
nothing else.

**Q31. What the rebalancer reads.** Replica sets from meta and counts asked of each
node; node summaries written to range 0; or everything from meta. Draft: the first. Open
because SPEC is silent.

**Q32. The replication factor.** Three for every range, or per range. Draft: three, with
as many bootstrap nodes (Q7). Open because no document names one; Phase 2's scenarios run
three and five servers, and check 15 needs a number.

**Q33. Quarantined replicas.** The rebalancer ignores quarantine, or moves quarantined
replicas off, which is D-035's rejected alternative made possible. Ignoring it, with
Q15's whole-node refusal, leaves about 67 of 1 000 ranges, or 667 of 10 000, with one
replica that can vote once two nodes have each lost a table: each can keep a leader it
has and cannot elect one (§4). Draft: moves them off, one per range at a time, before any
balancing move. Open because D-035 rejected replacement for want of machinery Phase 3 has
(DECISIONS.md:1522-1524).

**Q34. Membership changes and snapshots on one schedule.** Extend Phase 2's membership
scenario past the snapshot threshold first, or let `sim/move.rs` be the first schedule
that crosses both. Draft: neither assumed. Open because the gap is recorded as an
unfiled candidate (OVERNIGHT.md:188-191) and no entry decides it.

**Q35. `Scan` in Phase 3.** No scan; a single-range scan refused with `RangeMismatch`
across a boundary; a cross-range scan with a weaker guarantee stated. Draft: no scan.
Open because RAFT.md §4 describes a scan the code lacks, SPEC puts distributed scans in
Phase 5 (SPEC.md:345-346), and a cross-range scan has no linearizable form without
transactions (§9).

**Q36. Meta lookups in the history.** Out, or in as reads checked by the search. Draft:
out. Open because SPEC is silent; putting them in tests meta's reads, but the meta
range's state machine is a maximum by generation, not a register (§1), so the search's
model would need a second kind.

**Q37. Where Phase 3's variants live.** More bits in `ananke-raft`'s `Variants(u32)`,
sixteen used, which §10's twenty-three would bring to thirty-nine, past its width; a set
of the range layer's own; or both carried together. Draft: not assumed. Open because
D-045 fixed the width "before the width is a decision to revisit"
(DECISIONS.md:2704-2705).

**Q38. The stale-descriptor variant.** Ship `TrustStaleDescriptor`, the server honouring
the range a request names, and `ClientIgnoresMismatch`, the client resending to its old
range; or only one. Draft: both. Open because the owner's request names the client,
while §3's design makes a client's staleness harmless unless a server trusts it, so the
rule whose breach reaches linearizability is the server's.

**Q39. Scenarios, faults, bounds and cost.** The sharded sweep's node count (five
assumed), keys per range, clients, and split, merge and move draws; each scenario's
faults — Phase 2's full model, or the subsets a directed scenario needs, as
`sim/quorum.rs` chose (DECISIONS.md:3346-3353); every time bound's value, measured on
the correct system first; the balance scenario's tier, run length, trace (payloads kept
or not, polls exported or not, its record cap); the holds of `Fault::MetaReorder`, of
`sim/merge.rs` (b), (d) and (e), of `sim/move.rs` (a) and of its hold check; whether
each Phase 2 variant's re-assertion on the node of §4 runs at every tier or one, given
that §10 holds each to its Phase 2 standard; whether to build a shape that carries
`GcBeforeRemovalCommitted` to a client through a second vote after a re-install (§9);
and the budget: premerge is meant to take about a quarter of an hour (D-040,
DECISIONS.md:1417-1420), D-046 brought it to 7 minutes 26 seconds
(DECISIONS.md:2845-2848), and every scenario of §10 adds runs per seed. Draft: five
nodes; the rest left to this question. Open because SPEC's exit criteria name
properties, not scenarios, and cost is a constraint the owner has enforced: the adoption
storm was cut to one seed in four when it took premerge from about thirteen minutes to
forty (DECISIONS.md:1896-1913).

**Q40. Crate boundaries.** `ananke-shard`, "Range management, multi-raft"
(BOOTSTRAP_PROMPT.md:75), holds the node of §4, descriptors, split, merge, the meta
state machine, the rebalancer and §8's new checks, with `ananke-raft` keeping the core,
store, snapshots and a checker keyed by range; or the node that runs many groups lives
in `ananke-raft` and `ananke-shard` holds only ranges. Draft: not assumed. Open because
the crate does not exist yet and `run` in node.rs today is both the tasks and the group.

**Q41. The node's `raft` task and its round.** One `raft` task per node holding every
core, or one per range. A round's persists merged into one synced batch that holds every
send of the round, or each core's persist awaited before that core's own sends, as
RAFT.md §3 and D-026 order a step's outputs (RAFT.md:551-554; DECISIONS.md:831-832). The
per-peer outbox flushed at the end of each round, once per tick, or on a timer. A core
that persisted steps nothing more in the round, or steps on with its later outputs held.
Snapshot chunks in frames of their own on the snapshot task's socket handle, as today
(node.rs:602-611), or batched with the round's sends. Draft: one task per node, merged
persists, a flush per round, the stop rule, chunks unbatched. Open because SPEC.md:289-290
settles only "a shared Raft ticker and message batcher per peer pair". The merged persist
changes RAFT.md §3's order of execution: a send that precedes a persist, or belongs to a
core that persisted nothing, waits on every other group's persist in the round, which
under load puts one sync's latency on every co-hosted range's heartbeats (§4). §4's
frame counts hold only while a round drains whole frames; a flush per tick would make
them a bound. Chunks batched with heartbeats would lose small messages with large ones
under a frame-length limit and blur the directed scenarios of §10.

**Q42. How the rebalancer chooses a move.** SPEC names what is balanced, range count and
leader count (SPEC.md:296-297), and nothing about how. Choices: pair the node holding
the most replicas with the node holding the fewest, or score every node pair by the
improvement a move makes; break ties by the lowest range id, or by a draw from the
rebalancer's stream, or by range size (§11, storage 7); transfer leaders in the same
step as replica moves, or only once replica counts are within the band; drain a node
being removed, and move quarantined replicas off (Q33), before any other move, or
interleave them; and run a step always, or only while some node is outside the 10 %
band (Q30). Draft: most to fewest, lowest id, leaders in the same step, drain and
quarantine first, a step always. Open because the owner asked what the rebalancer
optimises, and the draft's greedy pairing is one policy among several with different
move counts and convergence times, none measured.
