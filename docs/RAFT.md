# RAFT.md — ananke's Raft

_Status: approved 2026-09-05, with the lease section rewritten as approved. Each
implementation stage turns its part into a DECISIONS.md entry as it lands: A and B on
2026-09-05 and 2026-09-06 (D-025, D-026, D-027), C on 2026-09-06 (D-028), D on
2026-09-08 (D-029, with D-032 and D-033), E on 2026-09-09 (D-030, with D-035 to
D-038); the entries since are D-039 and D-041 to D-049. This document describes the
server as those entries leave it._

**A stated assumption.** Raft's safety argument here assumes the disk is honest about
`fsync`: a sync the disk acknowledged but did not do loses the hard state or the log's
tail, and that loss is undetectable by construction from the server's own disk, so a
server that comes back short of what it promised can vote twice in a term or drop a
committed entry. The sweep runs with `p_durable = 1` for that reason (D-026); bit rot
and torn writes stay on, and the engine's checksums turn them into a refusal. What can
be done instead, recovery that knows the protocol and repairs the local log from the
other replicas, is Alagappan et al., *Protocol-Aware Recovery for Consensus-Based
Storage* (FAST 2018), issue #23.

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
so a leader on the wrong side of a partition stops serving. A server on a store a
re-seed rebuilt grants no pre-vote and no vote for the rest of its life on that store,
since the state it lost may have held a vote in a term it cannot know (D-035, §3).

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

A follower's log can lose entries it acknowledged: a refused server is rebuilt from a
snapshot that may end below what a leader matched on its lost store (§3). So the
leader's `match_index` is monotone for one store, not for one follower (D-042). Every
store carries a store incarnation, 1 for a store started fresh, carried across an
install into a live store, and drawn afresh, never 1, for a store a re-seed rebuilt;
`AppendEntriesResponse` and `InstallSnapshotResponse` carry the responder's, 0 from a
refused server, which has no store. A leader records the number each follower's
`AppendEntriesResponse` carries, and the one an `Installed` answer carries; chunk
acknowledgements go to the snapshot task, which does not read the number and tells the
core only that a stream progressed (D-049). The first
answer only records. An answer carrying a different one resets that follower's progress before the answer is otherwise processed —
`match_index` to 0, `next_index` to one past the leader's last index, the pipeline, the
probe and any snapshot designation cleared, traced `RaftProgressReset` — and the leader
replicates to it at once, as on a heartbeat, so a rejection's hint walks the probe back
from where the rebuilt log ends rather than from a match that log no longer holds. An
install's completion resets the same way before its match is recorded. The numbers are
compared for inequality only.

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

As built (D-028): a request carries the leader's clock at its sending, `sent`, and the
response echoes it and adds the follower's clock, `local`; the offset the guard sees
is `local − sent`, which includes the one-way delay. The guard takes the fastest
response of each window of `guard_window` as the window's observation, the way an NTP
clock filter does, and compares it with the first window's: movement beyond
`drift_bound` times the time between the two windows revokes, and what jitter the
fastest response still carries counts as movement. A follower is trusted only from its
first steady comparison, so a fresh leader serves by read-index for two windows. The
promise a follower's response makes runs from `sent`: the minimum election timeout
less one tick, scaled down by the bound, less a tick of margin. The lease is the
promise, among trusted followers, that a majority with the leader expires latest. The
promise holds because a follower that has heard from its leader within its minimum
election timeout ignores a vote request altogether, term and all (thesis §6.4.1): with
pre-vote, a candidate that reached a real election already has a majority that has not
heard. A vote request marked as a leadership transfer (thesis §3.10) is the one
exception, since the leader asked for it: `TimeoutNow` makes the target campaign at
once without a pre-vote, and the sweep uses it to hand leadership to the server with
the slowest clock, the only way such a server ever leads. A server on a re-seeded store
makes no promise (D-035): its responses echo zero, and the leader counts such a
response for nothing in the lease or a read-index round, since that server grants votes
to nobody and a vote majority need not cross it.

**Membership changes by joint consensus (thesis §4.3).** A change from C_old to C_new
is two log entries: `C_old,new` and, once that is committed, `C_new`. While the joint
entry is in the log, uncommitted or committed, elections and commits need majorities
of both configurations. A server uses the latest configuration entry in its log,
committed or not. A leader that is not in C_new steps down once `C_new` is committed,
and does not count itself for the majorities that commit it. Servers being added first
catch up as non-voting learners (thesis §4.2.1): they receive entries and snapshots but
count for nothing until a round of replication to them takes less than an election
timeout, at which point the leader proposes the joint entry. The catch-up is the
leader's alone and volatile (D-032): the learners, their rounds and the target voters
live in the core of the leader that accepted the change and reach neither the log nor
the store, and a leader that steps down, or a new one taking office, starts with none.
A leadership change before the joint entry exists therefore abandons the change, and
the operator asks again, which a request for the same voters makes harmless (D-029);
from the joint entry on, the change is in the log and drives itself on whichever
leader holds it. One change is in flight at a time. A server that is not a voter of
its configuration in force — a learner, a server with no configuration yet, a removed
server — cannot win an election and does not start one: its election timeout resets
its timer, and it ignores `TimeoutNow` (D-033). It still grants votes and pre-votes by
the usual rules and follows any leader that appends to it, and a truncation that
reverts the entry excluding it restores its right to campaign. The exit criterion,
3 → 5 → 3 under partition (SPEC §3), is a scenario in `sim/membership.rs` with a leader
on the minority side during the change: the change completes both ways, and no gap
between completed client operations, partition windows taken out, is longer than ten
maximum election timeouts (D-029). The worst gap at 10 000 seeds was 549.359683 ms before D-058.

**Snapshots (thesis §5, SPEC §3).** A snapshot is an `Engine::checkpoint` of the state
machine's store at an applied index, with the index, term and configuration at that
point written into the store's reserved tenant before the checkpoint's `CURRENT`. A
take is a job on the `apply` task's own queue (D-036): between two applies that task
writes the record under `<prefix> / 3 / snapshot` with its own applied index and the term of
the entry it last applied, synced, and then checkpoints, so no apply lands between the
record and the copy and the record is exact by construction; applies wait behind the
take. Every take goes to a directory of its own, `snap-<index>-<take>`, numbered by a
take counter the record carries, so two takes at one index are two directories and no
take rewrites a directory a stream is reading (D-043). On the node that name gains the
range, `snap-r<range>-<index>-<take>`, and the staging directory gains the range and the
sender, `staging-r<range>-s<sender>`, because a node's ranges apply streams of commands
of their own and two of them taking at one index is ordinary; a range's sweep then
proposes only its own versions for deletion (SHARD.md §11 raft 14; D-075, proposed).
A take asked for at the index
the record already names answers with the recorded version when this store took it
and it is complete; a take asked for because a stream found no usable checkpoint is
always a fresh version, unless a take is already in flight, which the stream then
waits for rather than asking for another (D-043). A leader takes a snapshot when its
log past its last take exceeds `snapshot_threshold` entries and it has applied past
that take, once it has led for two minimum election timeouts (D-030); a follower that
needs a snapshot before one exists gets one taken on demand.

`InstallSnapshot` streams the checkpoint's files in order, in chunks of
`snapshot_chunk` bytes, each chunk naming the file, its offset and the total, with one
chunk outstanding per stream (D-030). The receiver's acknowledgement names the next
byte it wants, and a chunk unanswered for half a minimum election timeout is resent
from there, so a resend after loss resumes from the last acknowledged offset of the
last file rather than from zero. Eight resends give the stream up; a receiver that asks
to start over has the stream restarted from its first byte, twice, and at the third
such ask the leader counts the checkpoint as unusable. A stream's identity is its
sender, the leader's term and the snapshot's last index and term, and a change of any
of them, a new leader's stream included, starts the receiver's staging over (D-030). The
one-group receiver holds one stream and abandons it for a chunk of another identity; the
node holds one assembly per (range, sender), under a per-node cap on what is assembled at
once, so a chunk that is not an assembly's own never disturbs it and the streams over the
cap wait rather than restarting one another. A slot the cap frees is granted to a stream
that is asking for it, never reserved for one that asked earlier and may since have been
replaced as leader; and a chunk that starts an assembly over is answered with that
restart even when it is its stream's last, since the directory it would be installed from
has just been started over (Q14; D-075, proposed). An
acknowledgement that takes a stream past the furthest point any acknowledgement had
taken it is that stream's progress, and the task marks the follower for the core, which
reads the marks before each tick; a duplicate, the answer a resend gets and ground a
restart covers again are not progress, and the `Installed` answer, which acknowledges
the final chunk, is (D-049). A
stream opens the newest complete version of the index the core asked for — complete
meaning the checkpoint's own `CURRENT` is there, since the record precedes the
checkpoint and may name a take still in flight — and reads that version for its whole
life: a leader that has since taken a newer snapshot finishes streaming the pinned one,
and a follower then found below the compacted prefix is fed the newer one after it
(D-043). A leader streams to every designated follower at once, each stream on its own
deadline, so no follower waits behind another's stream (D-043). A version is deleted
once it is neither the record's nor read by a stream: at every incarnation's start
before its tasks run, after every completed take and after every stream ends (D-043).

The receiver assembles a stream under `install/` in its data directory. It holds the
streamed `CURRENT` aside in memory, so the staging directory is never a store while
the stream runs, and syncs each completed file with its directory entry (D-030,
D-038). On the final chunk it verifies
every staged table with the engine's own checks and the staged snapshot record against
the stream's identity. The `raft` task then quiesces the `apply` task, so the applied
index is final, and either answers installed without switching, when the store already
holds everything the snapshot carries, or hands the receiver's identity to the
`snapshot` task, which writes it into the staged store as one repair table and a
successor manifest: the receiver's term and vote, the applied index at the snapshot,
the snapshot record, the log tail it keeps — kept only when its entry at the snapshot's
last index carries the snapshot's last term — the `<prefix> / 2 / config` key consistent with
that tail, the quarantine flag, set on a re-seeded history and tombstoned otherwise,
the store incarnation, and tombstones for the leader's log keys the tail does not
replace (D-030, D-035, D-038, D-042). Only when the repair is durable is the staged
`CURRENT` written, tmp-and-rename, the commit point of every store switch (D-024,
D-038), and the install retires the server's incarnation.

The server's next start adopts the staged store before its engine opens, copying
before it switches and switching before it deletes (D-041). A staging directory with
no `CURRENT` is an install that never finished and is swept. One whose `CURRENT` does
not parse, names a manifest that is missing or does not decode, or lists a table that
is not there is damage, and the server is refused with nothing touched (§3). Otherwise
the staged tables and a manifest, renumbered past every table and manifest already in
the store directory, are copied in and synced with the directory; `CURRENT` is switched
to the copied manifest, tmp-and-rename; the store marker is written whole (§3, D-044);
and only then are the old store's files removed, then the staging directory's own
`CURRENT`, after which the staging can never win again, and then the rest of the
staging. Until the switch is durable the old `CURRENT` names the old store whole, and a
crash anywhere before the staging `CURRENT` is gone re-runs the adoption on the same
staged bytes.

A leader compacts the Raft log to its last checkpoint once every follower has matched
it, is being streamed the snapshot, or is designated snapshot-fed (D-037). A follower is
designated when it is more than `snapshot_threshold` entries behind and has answered
nothing for two minimum election timeouts, or at once when it rejects an AppendEntries
whose previous index is 0: index 0 is consistent with every log, so only a server with
no log rejects it, which is a refused server asking to be re-seeded (§3). A designated
follower, or one whose `next_index` falls at or below the compacted prefix, is fed
through the snapshot path; a successful append acknowledgement, a completed install or
a change of the follower's store incarnation clears the designation.

A follower compacts too, to its own applied index, and takes a checkpoint only when it
must stream one (D-065). Once its log is more than `snapshot_threshold` entries past
its prefix, its next tick asks the `apply` task for a snapshot *record* at the applied
index: the record alone, written synced between applies, so its last index, that
entry's term and the configuration in force at it are exact, as a take's are (D-036),
and with no checkpoint under it. The step's persist then deletes the prefix, after the
record is durable, in the order a leader's compaction uses. The prefix that swallows
the configuration entry in force leaves that configuration as the revert floor, as it
does on a leader (D-029). A follower waits for no follower and holds off for nothing:
the leader's two-election-timeout hold-off exists because a checkpoint stalls every
apply for its duration, and a record is one synced batch. A record with no checkpoint
under it is a shape the store already held — an install's repair writes one, and a
crash between a take's record and its checkpoint leaves one — so a server that later
has to stream finds no complete version of that index and asks for a take, which is the
path a leader has used since the first install. A follower's applied index never passes
its commit index, so the prefix it drops holds nothing uncommitted. The record itself
traces nothing of its own: the transition is the compaction, which the core traces as
`RaftCompacted` once the prefix's deletes are durable, and a crash between the two is
reported by the re-statement of the next open, where a take's crash window is
reported. The leader's own
rules are unchanged: its threshold take, its hold-off, and D-037's condition for
compacting.

**Timing.** Election timeouts are drawn per node per election from the node's
protocol stream over `[election_timeout_min, 2 × election_timeout_min)`; heartbeats
every `election_timeout_min / 5`. The simulator's clocks skew and drift per node, so
nothing compares timestamps from two nodes except the lease guard above, which is
built to. Check quorum runs on the leader's ticks: every minimum election timeout it
asks whether a majority answered since the last time, and steps down
(`RaftQuorumLost`) if not, so a leader on the wrong side of a partition stops serving
within two of them. A follower that answered from a store counts. A refused server's
rejection (§3) counts only in a window in which a chunk of the leader's re-seed stream
to it was acknowledged, the `Installed` answer included: a refused server answers every
AppendEntries whatever becomes of its re-seed, so its rejections say it is alive, not
that the leader is getting anywhere with it. A leader whose majority needs a refused
follower whose stream made no progress in a window steps down, naming the follower in
the step-down's `uncounted`; and a leader kept in office by a re-seed stream cannot
commit through that follower until its install completes (D-049). A refused server
answers nothing at all while it verifies, repairs and adopts the install it staged, so a
leader whose majority needs it can lose its office in that silence under any counting,
which is the re-seed's own cost (D-049). A server whose timer fires while it cannot lead
— not a voter of its configuration in force (D-033), or on a re-seeded store (D-035) —
draws a new timeout and stays a follower. A server that switches to an installed store
starts its new incarnation with a freshly drawn timeout, as a restart does (D-039).

## 2. The five invariants and how the trace checks each

Raft emits a trace event for every state transition that the invariants read. The
events, each recorded with its node and two times (D-047). Every event about a
replica carries `range`, the group it is of, and the three about the node's store —
`RaftRefused`, `RaftAdopted` and `RaftServerFailed` — carry none (SHARD.md §8,
D-069); today a node runs one group, `node::SINGLE_GROUP`:

| Event | When | Fields |
|---|---|---|
| `RaftTerm` | the current term changes | `term`, `role`, `received`: when the peer's message the step took reached the server, if it took one (D-050, proposed) |
| `RaftVote` | a vote or pre-vote is granted or refused | `term`, `candidate`, `granted`, `pre` |
| `RaftLeader` | a node becomes leader | `term`, `last_index` |
| `RaftAppend` | an entry is written to the log | `index`, `entry_term`, `hash` of the payload |
| `RaftTruncate` | a conflict removes entries | `from_index` |
| `RaftCommit` | the commit index advances | `index` |
| `RaftApply` | an entry is applied | `index`, `entry_term`, `hash`, `key` where the entry names one, `effect`: `applied` for a client command executed in its range, `none` for a no-op or a configuration entry; SHARD.md §8's other five values belong to what later stages build (D-069) |
| `RaftConfig` | a configuration entry takes effect | `index`, `old`, `new`, `joint` |
| `RaftSnapshot` | a snapshot is taken or installed, or a durable prefix is re-stated at an open. A replica's compaction emits none of its own: it writes a record and the core traces `RaftCompacted`, and the record surfaces here only as the re-statement of the next open, with `taken` false (D-065) | `last_index`, `last_term`, `taken` |
| `RaftRead` | a read is served, traced by the server that serves it, not by the core that confirmed it (D-069) | `index`, `lease`, `key`, `applied`: the applied index of the engine version the value was read at, taken at that one version |
| `RaftLeaseRevoked` | the guard revoked a lease | `follower`, `offset_moved` |
| `RaftQuorumLost` | a leader stepped down for want of a majority | `term`, `uncounted`: the refused followers whose rejections went uncounted for want of re-seed progress (D-049) |
| `RaftTransfer` | a leader sent TimeoutNow | `to` |
| `RaftRecovered` | a server starts on what its store held | `term`, `applied`, `last_index`, `incarnation` |
| `RaftProposed` | a leader made a client's request an entry | `client`, `seq`, `index`, `term` |
| `RaftRefused` | a server's store lost state, or its staged install is damaged, and it waits to be re-seeded (§3) | `reason` |
| `RaftServerFailed` | a server stopped on an I/O error | `reason` |
| `RaftInboxDropped` | a full inbox dropped a message | `kind` |
| `RaftReseeded` | a server starts on a store a re-seed rebuilt (D-035) | — |
| `RaftAdopted` | a server adopted a completed install at its start (D-041) | — |
| `RaftProgressReset` | a leader forgot a follower's progress on a new store incarnation (D-042) | `follower`, `incarnation` |
| `RaftSnapshotStreams` | a leader opened a snapshot stream (D-043) | `to`, `streams` |
| `RaftSnapshotReused` | a take answered with the recorded version (D-043) | `last_index`, `take` |
| `RaftSnapshotDeleted` | a checkpoint version nothing reads was deleted (D-043) | `last_index`, `take` |
| `RaftMatchStarted` | a leader's `matched` for a follower rose for the first time under the incarnation the follower's answer carried (D-042, D-069) | `follower`, `incarnation`, `matched` |
| `RaftLearnerRound` | a leader ended a catch-up round for a learner (D-029, D-069) | `learner`, `from_index`, `to_index`, `ticks`, `caught_up` |
| `RaftChangeAccepted` | a leader accepted a `Change`, holding none in flight (D-029, D-069) | `voters`, `applied`, `term` |
| `ClientInvoke` | a client operation starts | `client`, `seq`, `op` |
| `ClientReturn` | it returns | `client`, `seq`, `result` |

Each event carries the node's persistent term, so the studio's per-term filter (issue
#3) needs nothing more. Every check reads the trace of a run. Checks 1 to 4 and the
three rule folds below are folds in `ananke_raft::invariants`, run as one incremental
checker, `invariants::Checker`, that keeps every check's state across calls: the sweep
feeds it only the records since its last look, every ten slices of fifty milliseconds
of virtual time, and stops the run at its first violation; and at the end of the run
the same folds run again over the whole trace from its first record, a second opinion
on every seed (D-046). The rest are in `sim/` and run at the end.

Every one of checks 1 to 4 is a property of *one Raft group* and is keyed by the group
each event names (PROPOSED D-071; SHARD.md §8): the leader of each (group, term), the
log and floor of each (group, server), the committed set of each group, and what each
group's index applied as. The crate names no range — a group is an opaque id,
`node::SINGLE_GROUP` while a server runs one group — and the checker is fed each event
with the node that traced it, since `RangeCreated` and `RangeRemoved` name their range
and their node and no server (`invariants::Traced`).

1. **Election safety.** Fold `RaftLeader` events: a map from (group, term) to the node
   that became leader in it; a second node for one of those is a violation. Two groups
   electing in one term are two elections. Cheap, exact.
2. **Log matching.** Reconstruct each *replica's* log — one per (group, server) — from
   `RaftAppend`, `RaftTruncate` and `RaftSnapshot` events as a map from index to
   (term, hash). After every event, for the replica it touches and every other replica
   of that group holding the same index with the same term, every index below must
   agree in term and hash. One node's two groups hold two logs and share nothing.
   A `RangeCreated` sets its replica's floor as an installed snapshot does, so a group
   born above index 0 starts at that floor with no log below it (PROPOSED D-071).
   Incremental: the new entry's index is the only one that can newly violate it.
3. **Leader completeness.** From `RaftCommit` on a leader, the set of committed
   (index, term, hash) *per group*. At every `RaftLeader` event, the new leader's
   reconstructed log must contain every entry committed in an earlier term **of that
   group**, which is the set the rescan reads. Also checked, as the property
   that makes rule 2 of moirae's list bite: an entry counted committed must have
   been appended on a majority of the configuration then in force, which the
   reconstructed logs and `RaftConfig` events show. A group's first configuration —
   the one in force on a replica that has traced no `RaftConfig` — is the voters of its
   `RangeCreated`, and servers 1 through the cluster's count for a group whose creation
   the trace does not hold (PROPOSED D-071).
4. **State machine safety.** Fold `RaftApply`: a map **per group** from index to
   (term, hash, effect); a second value for an index of a group is a violation, so two
   replicas that apply one entry to different effects — one executing a client's
   command, another refusing it — are seen at the second (PROPOSED D-071). Per (group,
   server), applied indices must be the
   consecutive integers from the floor a `RangeCreated` or an install sets, so an entry
   applied twice or skipped shows here as
   well, which is where the applied index being written in the same batch as the
   entry's writes (§3) is proven. A `RangeRemoved` ends that (group, server)'s memory
   the way a node's `RaftRefused` ends its store's, so a group removed from a node and
   created there again starts clean. An apply durable at a crash but not yet traced
   shows at the restart as `RaftRecovered` carrying an applied index past the last
   traced apply; the entries between are what the server's log holds there, and
   are checked like any other. For the same reason a restarting server re-states
   its durable log, a truncation at its end and an append per entry, before
   `RaftRecovered`, so the trace's picture of every log is the disk's. An installed
   snapshot raises the server's applied floor to its last index, a snapshot the
   server took moves nothing, and a refusal removes the floor, so every applied
   index a restart restates past the floor must be held by its restated log: that
   is how both a store switched to without its repair and a refusal a restart
   forgot are seen (D-030, D-038, D-044). The refusal is the node's and clears every
   group on it; the entries a recovered applied index accounts for are read from the
   replica's log, which holds the entry and not what applying it did, so they agree
   with any effect.
5. **Linearizability of the KV API.** The history is the `ClientInvoke` and
   `ClientReturn` pairs with their virtual times, checked by the checker in §4. An
   operation that never returned, because its client's node crashed or the run ended,
   is kept as pending and may take effect or not, as porcupine treats it. An entry is
   an entry of one group, so the closure that ends a pending operation is keyed by
   (range, index, term) (§4; PROPOSED D-071).
6. **Lease safety under drift.** For every `RaftRead` with `lease` set, served while
   the simulator's clock drift between the leader and some voter exceeded the
   configured `drift_bound` over the lease period, either a `RaftLeaseRevoked` event on
   that leader precedes the read, or the read is a linearizability violation reported
   by check 5. Stated the other way: on every seed with the drift violation on, the
   run passes only if no lease read was served stale. The invariant is a fold over
   `RaftRead`, `RaftLeaseRevoked` and the simulator's clock configuration, and the
   checker in §4 is what decides staleness; the guard's sufficiency is never assumed.
   The read's event is the serving server's since D-069, so what the fold sees is
   exactly the reads a client was answered from, each with the key and the applied
   index of the engine version it was answered at. It is per range, and carried over as
   what it is: not a fold, but check 5 on every seed — whose closure is keyed by range —
   and the test that runs each drift-exceeded seed with the guard and without it
   (SHARD.md §8, check 6; PROPOSED D-071).

Three more folds check the rules behind the properties directly, so a broken rule is
seen the first time it is exercised and not only when its consequence happens to
land: *commit by majority*, every entry a leader commits was durable, as `RaftAppend`
after the persist says, on a majority when it did; *commit by current term* (§5.4.2),
a leader's commit index only ever lands on an entry of its own term; *committed
entries stay*, no server truncates at or below its own commit index. Two checks are
about time and run only on seeds the simulator scheduled uniformly, where no task can
be starved (D-016), and only of a *range* whose replicas that are neither refused nor on a
re-seeded store form a majority at the end of the run (PROPOSED D-071; SHARD.md
§8): a refused replica can only be
re-seeded by a leader and a re-seeded one never votes, so a range whose impaired
replicas are not a minority cannot elect a leader if it loses the one it has, which is
the availability D-035 gives up and not a liveness failure (D-030, D-035), and it is
also where `IgnoreIncarnation`'s wedge would stall a commit (§5, D-042). A node's
refusal is its whole store's, so every replica on it counts as refused. After the last
fault heals, a client write to **every key** of such a range completes within ten
maximum election timeouts: a single minimum over every write is passed by a wedged
range beside a live one, so the bound is asked of each key some client wrote to after
the heal (PROPOSED D-071). And a
running replica that is not leading its range and not on a re-seeded store, and has gone two
maximum election timeouts,
scaled by its own node's clock rate, without a reset, has started an election (moirae rule
5, D-028). The check is per (range, server): a replica's timer is its own, so one
range's heartbeats do not stand in for another's silence on the same node (PROPOSED
D-071). A reset is the delivery of an AppendEntries or an `InstallSnapshot` chunk of
that replica's term or later, whoever sends it (D-030); a vote it granted; a campaign;
its start; its `RangeCreated`; its step-down as leader; and the restatement of an
install on a server that
never went down, whose new incarnation draws a fresh timer (D-039). A replica's
`RangeRemoved` ends its timer: there is no replica left to campaign. *Running* means
holding a live incarnation: one ends at a shutdown, a crash, or a **completed install**,
which retires the incarnation and leaves the server adopting the staged store with no
core and no election timer until its restatement starts the next one, so the bound is
not asked of it across that window any more than it is of a crashed server before its
restart (PROPOSED D-063, which supersedes D-039's arm in the check). One check is
pre-vote's own property (thesis §9.6), asked of each of the isolated node's replicas
(PROPOSED D-071): a replica on a server the schedule isolated has, at the heal,
the term it had when the isolation began — a term is a range's, and one range's
election says nothing of another's. A range created on the isolated node during the
isolation takes the term of its `RangeCreated` as the term the isolation began with,
since the replica did not exist at the start. An isolation in which the node was
refused, or that replica was re-seeded or completed an install, is skipped, since the
install restates the term the stream carried (D-030). The violation names the server,
the terms and the window and not the range, which forty-four pinned assertions take word
for word and a run of this stage has one of; the stage that gives a node many ranges
moves those pins and names it there.

A record carries two times (D-047). A server traces a step's events once what they
report is durable (D-026), so a record's time is its durability time; beside it the
record carries its decision time, when the step that produced it was taken. The stamp
is taken before every step of the core, as the `apply` and `snapshot` tasks take each
piece of work, when re-seed mode has staged a whole stream, and at a refusal, and the
decision time equals the durability time for everything traced as it happens; the
studio's export writes it as `decidedNs` where the two differ. A check about what was
durable when reads the durability time or the records' order: the log invariants, the
rule folds, and the history, in which an abandoned operation returns at its entry's
durable apply (§4). A check about why a server acted reads the decision time.
Pre-vote's property reads the server's term by when each change was decided, so a rise
decided on a message delivered before the isolation began and traced after it is not
the isolated server's election; its skip reads the durability time, since the
restatement it stands in for is traced after the install is durable. The timer check
replays the records in decision order, so no bound is measured past a reset the server
had already made. A term-raising message delivered before an isolation but stepped
inside it, queued behind a persist or an install, is decided inside the window, so the
term record also says when the server received the message its step took (D-050,
proposed, issue #32): the pre-vote check does not flag an isolation whose every term
change decided inside it was taken from a message received by its start. A change from
a step that took no peer's message — a campaign on the server's own timer without
pre-vote, a restatement, a completion — carries no receipt and is flagged as before; a
candidacy stepped from a granting PreVoteResponse, or from a TimeoutNow, carries the
receipt of that message and is excused when it was received by the isolation's start,
like any change a message caused.

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
  src/core.rs       the pure state machine of the protocol, the read-index and lease
                    rules included (§1, D-053); Variant and the set Variants (§5)
  src/store.rs      persistent state in the engine
  src/invariants.rs the checks of §2 as folds over trace events, one incremental
                    Checker
  src/node.rs       the tasks that run one server
  src/apply.rs      the state machine adapter: commands to engine batches
  src/snapshot.rs   checkpoints as snapshots, chunked both ways
  tests/            the paper's scenarios against the core: Figure 8, moirae's ten
                    rules, the D1 replay, a joint change with a leader outside C_new
sim/raft.rs         the crash-and-partition sweep: the five checks, the workload
sim/lin.rs          the linearizability checker
```

**The pure core.** `core.rs` is a state machine with no I/O, in the shape of raft-rs's
`RawNode`: `Raft::step(&mut self, input) -> Outputs`, where an input is a message from
a peer, a tick, a proposal, a read request, a completed persist or apply, or a snapshot
stream's progress or end, and the
outputs are a list of `Send(to, Message)`, `Persist(PersistBatch)`, `Apply(through:
Index)`, `ReadReady(request, index)` and `Snapshot(take | install)`. The core is where
Figure 2 lives and where the paper's scenarios are unit tests without a simulator. It
is generic over nothing: the node task does the I/O.

**The log in the engine.** SPEC §3 puts the Raft log in the storage engine under a
reserved tenant, tenant 0 in the §2.6 key encoding. A store is opened for one Raft
*group*, under the key prefix `0 / <group: u64 BE>`; every key of the group is
`prefix / <purpose: u64 BE> / name`, with SHARD.md §13's Q5 purposes in the place
RAFT.md's table ids had, so a group's whole Raft state is one key interval and
several groups share one engine without sharing a key. Today's one group is group
2, SHARD.md §2's range 2 (`node::SINGLE_GROUP`); Stage B gives a server a group per
range. The layout, the group and the format record below are D-060, PROPOSED, which
this section describes and which the entry decides (Stage A item 6, Q5 and Q40).

| Key | Value |
|---|---|
| `0 / g / 0 / hard` | current term, vote |
| `0 / g / 0 / applied` | the applied index |
| `0 / g / 0 / reseeded` | present on a store whose history a re-seed rebuilt, carried by every later install on it: the quarantine (§3, D-035) |
| `0 / g / 0 / incarnation` | the store incarnation (§1, D-042) |
| `0 / g / 1 / <index: u64 BE>` | the entry: term, payload |
| `0 / g / 2 / config` | the latest configuration entry's index and content |
| `0 / g / 3 / snapshot` | last snapshot's index, term and configuration, whether it was taken here, the store's take counter (D-043), and its checkpoint directory |

Purposes 4 and up are unassigned: #21's session table (Q11) and a range descriptor
each take one inside the group's interval and move no key. User data is tenant 2
(`apply::USER_TENANT`, SHARD.md §1) and tenant 1 is the system tenant, which nothing
in Stage A writes; a range may span tenants.

**The format record.** Nothing in the engine says which layout a store directory
holds, so the store records it in a file of its own, `RAFT-FORMAT`, beside the store
marker: two checksummed copies of the format version in one block, 37 bytes each at
offsets 0 and 37. Format 1 is ananke-raft 0.3.0's, which records none; format 2 is
the layout above, the only one this build reads or writes. The name, the magic, the
copy's shape and offsets are permanent, so any build can read any build's version.

The record is read before anything writes: a directory holding a store's files and
no record is ananke-raft 0.3.0's and is refused unread, with nothing written to it
— no log segment, no marker, no lost mark — and so is one recording any other
version, older or newer, naming both (D-059). A directory holding nothing but a
record that cannot be read, or nothing at all, is fresh, and its record is its first
write, before the engine creates an entry there; a record with one damaged copy is
healed in place at the start; a record that cannot be read at all beside a store is
lost state, refused and re-seeded like any other loss, and the adoption that
follows rewrites it. A directory holding entries that are neither is refused as
foreign rather than started as a new store.

Every checkpoint carries its own record, written after `Engine::checkpoint`, and a
checkpoint without one is incomplete; the stream sends it first, the assembler reads
it before it opens a staged table, and both adoptions read the staged record before
their first write, so a snapshot of another format is never staged and never
adopted.

Appending entries is a `WriteBatch` with `sync: true` of the entries and, when the
term or vote changed with them, `hard`; the batch's future resolving is the persist the
core waits for before sending. The hard state and the applied index are separate keys
because separate tasks write them, the `raft` task and the `apply` task, and neither
waits for the other. Truncation is deletes of the conflicting indices in the
same batch as the entries that replace them. The core holds the log in memory and feeds
a follower behind it from there; the store reads the log back, a `scan` over the index
range, only when it opens (D-053).
Applying entry `i` is one `WriteBatch` with the command's writes under the user's
tenant and the applied index in `applied`: the two are durable together, so a crash
between them cannot exist, and an entry is applied exactly once whatever the crash
schedule. A get never enters the log: the server hands it to the core as a read, and
the core answers it by the lease or after a heartbeat round, once the read index is
applied (§1). A `Transfer` command is an operator's request the server acts on
directly, never an entry. The Raft log is compacted by deleting indices at or below a snapshot's;
the engine's compaction reclaims the space in its own time.

The engine is opened with `allow_manifest_fallback` and `allow_head_gap` off, and the
store refuses a recovery that dropped an unreadable table, fell back to an older
manifest, discarded a log head, stopped reading the log at a bad checksum or a gap,
or skipped a corrupt record in a segment the tables cover and with it the rest of
that segment: each is a hole in the middle of the state, and an applied index over a
hole names a state that never existed (D-022). Two starts are refused before the
engine opens at all: one on a staged install whose `CURRENT` exists but does not
parse, names a manifest that is missing or does not decode, or lists a table that is
not there, since that install may be the only copy of a state the leader has compacted
past (D-041); and one on a store directory whose marker says the store lost state, or
says it was a store and holds no `CURRENT` that parses (D-041, D-044). The marker,
`RAFT-STORE`, is a file in the store directory written through the filesystem, never
through the engine. It is written whole once the engine and the store have opened; at
every refusal, before the refusal is traced, it is rewritten in place to say the store
lost state and why, synced with its directory, so a refusal outlives the process that
made it; anything but the whole store's line reads as lost; and once it says lost, only
an adoption writes it whole again (§1, D-044). A refused engine does no more work: one
whose recovery lost writes never starts its flusher, and one the store refused is
quiesced, so no flush, manifest switch or deleted log segment can make the lost store
look whole to the next start (D-044).

The start runs its checks in this order (D-059, D-060): the format record, read
before anything writes; a fresh directory's record; the adoption, with the staged
install's own format checked before its first write; the heal of a record with one
damaged copy; a record that cannot be read, which is lost state; the store marker;
and then the engine and the store. A format refusal stops the server, traced as
`RaftServerFailed`: the store lost nothing and is not this server's to replace.

Raft's safety argument assumes persistent state is persistent, so a server that lost
it neither votes nor serves until a snapshot rebuilds it. A refused server traces
`RaftRefused` and runs in re-seed mode (D-030). Its socket is bound, as every server's
is before it adopts an install or opens its store, and it answers every AppendEntries,
whatever its term and entries, with a rejection carrying the request's term and
previous index, a hint of 1, an echo of zero, so no lease promise or read confirmation
is measured from it, and store incarnation 0, since it has no store (D-042). That
rejection is how a leader learns the server needs a re-seed. It is a sign of life and
not of a store: check quorum counts it only in a window in which a chunk of the
leader's stream to the server was acknowledged (§1, D-049). A leader that recorded
another incarnation for it forgets its progress (§1, D-042), and the hint moves the
leader's next index for it back to 1: a leader whose log is compacted then feeds it the
snapshot (§1), and one whose log is not sends an AppendEntries whose previous index is
0, whose rejection designates it snapshot-fed (D-037). A leader that kept a stale match
for it would probe no lower than that match, so that trigger is reached past a stale
match only through the reset. It grants nothing: vote and pre-vote requests,
`TimeoutNow` and every response are dropped unanswered, and it runs no election timer.
It serves nothing: a client's request gets no answer, not even `NotLeader`, and the
entries of an AppendEntries go into no log. The one thing it takes is its own re-seed:
it assembles an `InstallSnapshot` stream and acknowledges it chunk by chunk as any
receiver does (§1). The install it completes carries the stream's term, no vote, no
log tail, the quarantine flag and a freshly drawn store incarnation; its answer that
the snapshot is installed carries that incarnation, and the server starts again on the
adopted store: the adoption, the marker, the open and a new incarnation. A crash before
the staged `CURRENT` is written leaves the lost mark, and the next start refuses again.
On the rebuilt store the server is quarantined for good (D-035): it replicates, applies
and counts for commit majorities, but grants no vote and no pre-vote, never campaigns
and makes no lease promise, across any number of restarts and any later install on
that history, since the state it lost may have included a vote.

**The message codec.** `Message` is `PreVote`, `PreVoteResponse`, `RequestVote`,
`RequestVoteResponse`, `AppendEntries`, `AppendEntriesResponse`, `InstallSnapshot`,
`InstallSnapshotResponse` and `TimeoutNow`, for leadership transfer (D-028). The two
responses a follower's store answers for, `AppendEntriesResponse` and
`InstallSnapshotResponse`, also carry the responder's store incarnation, stamped by the
server on the way out like the clock (§1, D-042). The wire form is one frame:
`kind: u8 | from: u64 | term: u64 | fields` (D-053). A fixed-width field is written
bare, and a variable-length one, a file name or a chunk's data, after its `u32` length;
entries are `count: u32 | (term: u64 | index: u64 | payload)*`, a payload a `u8` tag
followed, for a command, by `len: u32 | bytes` and, for a configuration, by the voters,
a `u8` flag with the new voters when joint, and the learners, each list `count: u32 |
ids`. Everything is little-endian like the engine's records, under `MAX_FRAME_LEN`. A
`decode` that fails is a dropped message, not a panic. The moirae bridge takes a
`Decoder`, and `ananke-raft` provides one, `message::studio`, that turns a frame into
`{"type": "raft.append-entries", "from": …, "term": …, "prevIndex": …, "entries": n}`
and its kin (D-053), so the studio labels lanes by message kind and filters by term and
index, which is issue #3's field set.

**The node's tasks.** One server is four tasks under `Environment::spawn`, and this is
where PCT gets something to bite, since every interleaving between them is a real one.
The server runs as a sequence of incarnations, one per store it runs on: at each start
a completed install is adopted (§1), the engine and the store open, and the `raft`,
`apply` and `snapshot` tasks run on that store until the server stops or an install
retires the incarnation (D-030). The socket, the inbox and the `net` task live across
incarnations and re-seed mode, so a message that arrives between two incarnations
waits in the inbox for the next:

- `raft`: owns the core and the timers; one loop over a `race` of the inbox, the tick,
  proposals and completions; executes every output in order, awaiting each `Persist`
  before the `Send`s that follow it. It stamps a decision time before each step of the
  core and traces the step's events with it once they are durable (§2, D-047).

  On the node (SHARD.md §4, Q41) one `raft` task holds *every* core, keyed by range, on
  one ticker, and keeps that order per core rather than for the task: the outputs that
  precede a core's `Persist` and all outputs of a core that persisted nothing leave
  before the round's sync, the round's persists are submitted together so the WAL
  writer's group commit syncs them once, and everything a core produced after its
  `Persist` — its sends, `Apply`, `ReadReady`, `ReadDropped`, snapshot actions and its
  trace events — waits for *that core's own* persist. A core whose persist is
  outstanding steps nothing: the messages for it are taken from the inbox and held,
  counted against the node's byte bound *and refused against it*, and a tick that falls
  due meanwhile is held as one tick, every missed tick stepped when the persist resolves
  and none collapsed. The node's bound covers what it holds because the task drains its
  queue to empty on every wake, so a bound that asked only about the queue would bind
  nothing (D-074, proposed). What the hold does *not* bind is a message larger than the
  whole bound: no emptying could ever make room for one, so it is admitted whatever the
  node holds, exactly as it is into an empty queue (D-072). Binding that case on the hold
  refuses every retransmission of it alike — a node behind any outstanding sync is
  holding something — and the range would never replicate again.

  The entries an `Apply` names are read from the core **at the step that named them**,
  not when the node comes to execute it: a deferred `Apply` runs after the replay has
  stepped that core further, where the one-group server's `execute` runs between two
  steps of the one core. An index the core does not hold fails the node rather than
  being passed over. This is `ananke-shard`'s `round` and `node` modules, beside the
  one-group server described here, which keeps working exactly as this section says
  until the sweeps move to the node (D-073, proposed).
- `net`: receives frames, decodes, and hands messages to the `raft` task through a
  bounded queue, each with a stamp of when it was received, which a term change the
  message causes carries (D-050, proposed); a full queue drops the oldest heartbeat
  first, never an `AppendEntries` with entries, and records the drop.
- `apply`: takes `Apply(through)` from the core, runs the state machine adapter one
  entry at a time, each a synced batch, and reports the applied index back; the core
  serves read-index reads only from applied state, so this task's lag is visible to
  the checker. On the node it is still one task, taking every range's jobs one at a
  time in the order they were queued (SHARD.md §4, Q14; D-073, proposed), and the node
  keeps the highest index it has handed the task per range, so the jobs partition the
  committed log — every index once, no gap and no repeat. It also takes every snapshot,
  as a job between two applies — on the node too, where the node routes a
  `SnapshotAction::Take` to this task and not to `snapshot` — so it is the task that
  writes checkpoint versions (§1, D-036, D-043).
- `snapshot`: streams checkpoints on the leader, one stream per designated follower,
  each pinned to the version it opened, and deletes the versions no stream reads after
  each take and each stream's end (§1, D-043); assembles and verifies arriving streams
  on a follower and, once the `raft` task has quiesced `apply` and handed it the
  receiver's identity, writes the repair and the staged `CURRENT` (§1, D-038).

  On the node (SHARD.md §4; Q14, Q41) it is one task keyed by (range, follower) on the
  way out and (range, sender) on the way in. Nothing caps the streams it sends, so a
  leader feeds every designated follower of a range at once (D-043); a per-node cap
  bounds what it assembles, and a (range, sender) over the cap is told to restart and
  takes the first slot that frees. Its chunks go in frames of their own on a socket
  handle of its own, never through the per-peer outbox, so a 256 KiB chunk never spends
  the frame a round's heartbeats needed (Q41). Its install is not the adoption of §1:
  every install on the node is the live install of the range's two key intervals into
  the running engine, with the range's repair carried in the same manifest switch, and
  no incarnation ends and no engine reopens. `RaftAdopted` on the node records only a
  node taking a fresh directory as its store after a whole-node refusal, and never a
  replica's install (D-066; D-075, proposed).

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
sets drift beyond `drift_bound` for the lease runs. Nothing else for the faults:
partitions, delay, loss, crashes with the disk model, and crashes between polls are
already there. The sweep later needed four more things of the environment: a read of
the simulated disk's durable namespace, `Sim::durable_names`, which aims a crash at an
adoption's first durable change (D-041); a copy of the trace from a given record on,
`Sim::trace_from`, which lets a fault driver and the incremental checker read only the
records since their last look (D-044, D-046); a decision time on every trace
record beside its durability time (§2, D-047); and a frame-length limit on one direction
of a link, `Sim::limit_frames`, a path-MTU black hole that loses a stream's chunks and
passes its heartbeats and their rejections, which the check-quorum re-seed scenario
blocks a stream with (D-049).

## 4. The linearizability checker

`sim/lin.rs`, in Rust, over the trace: a Wing-Gong checker with Lowe's partitioning and
Horn and Kroening's memoisation, which is what porcupine does.

**History.** Each client operation is `(client, invoke_t, return_t, Op, Result)`; a
pending operation has no return and may be linearized or discarded. Operations are
`Put(k, v)`, `Get(k) → Option<v>`, `Delete(k)` and `Cas(k, expect, v) → bool`, each on
one key; there is no scan (D-053). `Cas` exists so that a double apply or a lost write is
visible as a wrong boolean, not only as a stale value later. The trace closes most
pending operations: `RaftProposed` says which entry a request became, and an
abandoned operation whose entry applied took effect then, so it returns at the apply,
at the time the apply was durable, the latest its effect can have become visible
(D-047), with a result the client never saw and the model may give it any; one no
leader proposed cannot have taken effect and leaves the history; one proposed and never
applied stays pending. An entry is an entry of one Raft group, so a proposal and an
apply are matched by `(range, index, term)`: with several groups in one trace, an
operation proposed at (5, 2) in one range must not be closed by another range's apply
of its own (5, 2) (PROPOSED D-071; SHARD.md §9). A pending operation is a candidate
at every step of the
search, so closing them is what keeps the search small. The search has a budget of
states per key; exhausting it is reported apart from a violation, and the correct
server must never reach it.

**Partitioning.** Every operation is on one key, and the KV model is a product of
independent registers, so the history partitions by key: a history is linearizable iff
each key's sub-history is. Which range served an operation is not part of the
operation, so a key's register is one register whichever range served it and no
boundary moves the partition (SHARD.md §9). Each partition is checked on its own,
so the search is small
even over long runs, and each per-key search returns the linearization it found as a
timeline of (time, value). There is no scan and no scan check: scans are multi-key
reads, and the distributed scans that read across ranges are SPEC §6's, in Phase 5
(D-053).

**Search.** For one partition, sort operations by invocation time; state is (the set
of linearized operations as a bitmask, the register's value); depth-first search
takes any operation whose invocation is before every unlinearized operation's return,
applies it to the model, and recurses; a state seen before is not searched twice. The
model is a register with the four single-key operations. Pending operations may be
skipped at the end. A violation reports the shortest prefix that cannot be linearized
and the operations in it, which the studio shows as the clients' lanes.

**What is asserted.** Every partition linearizable, for the correct variant on every
seed (D-053); `Variant::ApplyBeforeCommit` and `Variant::LeaseTrustsTheClock` are the
ones this check must catch, since the four log invariants may hold while a stale read
is served.

## 5. Buggy variants shipped from day one

Each is a `Variant` on `ananke-raft` and breaks one rule with a reference. A server
carries a set of them, `Variants`, whose empty set is the correct server; a set turns
off exactly its members' fixes and no others, so two bugs run in one server with no
third behaviour between them (D-045). The pair `{IgnoreIncarnation, SharedSnapshotDir}`
runs that way, as the negative control for a wedge that would need both bugs, and is not
asserted over a sweep tier (D-045). On the tree with the key layout (D-060) no seed of
the first thousand catches it, or either half of it, so no seed pins its catch; seed 132,
which pinned it on the tree before, now asserts that absence with its reason (680 held it
before D-056). The correct variant must pass every seed. Each
other variant must be caught by the named check on some seeds, at every tier unless its
window is thinner than the gate's twenty seeds show: `AdoptionAsBuilt` asserts its catch
from the hundred-seed tier (D-041), `RefusalNotDurable` from the thousand-seed tier, with
its first catch there, seed 158, pinned at every tier (D-044, D-056, D-060),
`LeaseTrustsTheClock` from the thousand-seed tier, its stale read being caught on about
4 % of seeds (D-061), and `SharedSnapshotDir` asserts its liveness catch only at the
nightly's ten thousand (D-043, D-047); each of their tests asserts at every tier that
its fault fired. A variant caught on under 5 % of seeds asserts its catch from the
thousand-seed tier and never below it (CLAUDE.md, D-061).
`RefusedCountsForQuorum` and `RefusedNeverCounts` are caught by the directed re-seed
scenario, `sim/quorum.rs`, which builds their situation on every seed and is asserted to
catch each on every seed at every tier; the random sweep reaches it too rarely to
catch either, once in a thousand seeds (D-049).
`IgnoreIncarnation`'s test asserts at every tier that the variant was injected and, from
a hundred seeds, that the sweep reached the state its wedge is built on (D-042). Each
test prints its catch rate. A variant the sweep does not catch is a hole in the sweep,
not a variant to delete.

The table's seventeen rows are `Variant::BUGS` (D-053). Two rules have no variant of their
own. The term and the vote durable before a vote is answered is `SendBeforePersist`'s
rule, since that variant sends every message of a step, a vote's answer included, before
the step's persist. The applied index written in the batch of its entry's writes (§3) has
no known-buggy variant: its crash test,
`an_entrys_writes_and_the_applied_index_are_durable_together` in
`crates/ananke-raft/tests/store.rs`, runs the correct store alone.

| Variant | The rule it breaks | What catches it | Needs |
|---|---|---|---|
| `NoPreVote` | thesis §9.6: a rejoining node's election disrupts the leader | pre-vote's property at every heal: the isolated server's term is what it was when the isolation began | partitions |
| `SendBeforePersist` | Figure 2, thesis §3.8: the term, the vote and the log durable before any message that depends on them; the variant sends a step's messages, a vote's answer and an append's acknowledgement alike, before the step's persist (D-053) | commit by majority: an entry a leader committed was not durable on a majority when it did, since `RaftAppend` is traced after the persist; with a crash between the send and the persist, leader completeness | nothing beyond the discipline; crashes between polls for the consequence |
| `ApplyBeforeCommit` | Figure 2: apply only up to the commit index | state machine safety: an index applied under two terms on two servers after a truncation; linearizability: a write acknowledged and lost | the leader isolated with a client |
| `CountOlderTermForCommit` | §5.4.2, Figure 8 | commit by current term: a leader's commit landed on an older term's entry; leader completeness on the seeds where that entry is then overwritten | a follower behind by more than a batch when a leader takes over, so the older entries and the leader's own arrive in separate messages: the sweep runs small batches |
| `TruncateOnEveryAppend` | moirae rule 3 | committed entries stay: a server truncated at or below its own commit index | duplication and reordering (issue #1) |
| `IndexFirstElectionRestriction` | §5.4.1: compare last terms first | leader completeness | crashes and partitions |
| `ResetTimerOnAnyRpc` | moirae rule 5 | timers fire: a follower that heard from no leader of its term and granted no vote for two maximum election timeouts, scaled by its clock's rate, did not campaign | a follower cut off from receiving that pre-votes into the majority every timeout, each request declined, while the leader is down: the stale-sender schedule. Under check quorum a deposed leader's heartbeats stop within a timeout, so this is the stale sender that lasts |
| `LeaseTrustsTheClock` | §1 above: no drift guard | invariant 6, lease safety under drift: the checker reports a stale lease read with no revoke before it | the lease trial: leadership transferred to the server with the slowest clock, its lease formed, then cut off with a reading client while the fast followers elect and write; and a slow clock severe enough that its lease outlives their timers |
| `SnapshotWithoutCurrentLast` | §1: the staged `CURRENT` written last, after the repair (D-038); the variant writes the streamed `CURRENT` the moment it arrives, after the `RAFT-FORMAT` record the stream sends first (D-060) | state machine safety after a crash mid-install: the restart adopts a store carrying the leader's own tenant 0, which restates a *taken* snapshot and an applied index its restated log cannot account for, a state that never existed | crashes during install: on half the seeds `Fault::CrashInstalling` isolates a follower until it falls behind the threshold and crashes it two to twenty milliseconds after the stream's final chunk is delivered (D-030) |
| `SingleMajorityInJointConsensus` | thesis §4.3: commit needs both majorities | election safety or leader completeness during 3 → 5 → 3 under partition | the membership scenario |
| `AdoptionAsBuilt` | §1: the adoption copies and switches before it deletes, a damaged staging `CURRENT` is refused, and a marked store never opens fresh (D-041); the variant removes the old store first, sweeps a damaged staging `CURRENT` as debris, and neither checks nor writes the marker | committed entries stay: a voter restarts on a fresh store and restates a truncation from index 1 below its commit index | the adoption crash storm, `Fault::CrashAdopting`, on one seed in four: the install crash's setup, then sixteen to thirty-two crashes, each the moment the adoption's first change to the store directory is durable, each a roll of the disk's rot on the staging `CURRENT` (D-041) |
| `IgnoreIncarnation` | §1: a leader forgets a follower's progress when its store incarnation changes (D-042); the variant records the incarnation and never resets | the liveness check, were the wedge to stall a commit where the bound is asked (the configuration under Needs is not): a re-seeded follower below the match its leader recorded is never counted again while that leader leads. Caught on 4 of 10 000 seeds before D-047 (nightly run 34711427220) and on 0 of 10 000 after it (runs 34731272921 and 34749071877); D-047 moved no schedule, so the drop is itself evidence that the four were timing artefacts of the pre-vote check, as D-047 measured each directly | a follower refused and re-seeded below the match its leader recorded, while the third server is unavailable; after the last heal a server is unavailable only by refusal, and a refused server beside a re-seeded one is where §2 does not ask the liveness bound (D-035) |
| `SharedSnapshotDir` | §1: every take its own version, a stream pinned to one, and a stream to every designated follower at once (D-043); the variant rewrites one directory per index under whatever stream reads it and streams to one follower at a time | the liveness check: a take lands on the directory a live stream reads, that stream never completes, and a second designated follower waits behind it with no entries, so neither follower counts and nothing commits. Only the liveness check's catches count: a linearizability search that exhausts its budget proves nothing (D-047). The *shape* short of the stall — a re-take into a directory a live stream has open, at an index the follower never installs at afterwards — is reached on 13.5 % of a thousand seeds against the correct server's 0, and is asserted from the hundred-seed tier (D-060) | a take at an index a live stream is reading, which no fault forces; `Fault::RetakeUnderStream`, on one seed in four, builds the shape around it: a designated follower's stream running while the leader's other follower is cut off |
| `RefusalNotDurable` | §3: a refusal is marked in the store directory before anything else, and a refused engine does no work (D-044); the variant keeps the refusal in the process alone and its engine flushing | state machine safety: a server restarts on a store its refused engine flushed into self-consistency, a `RaftRecovered` after a `RaftRefused` with no install between, whose applied index its log does not hold | `Fault::CrashRefused`, on every seed: three to five rounds, each crashing the victim inside a memtable flush it has begun or, when it already sits refused, sixty to a hundred and sixty milliseconds into the round, and restarting it (D-044) |
| `RefusedCountsForQuorum` | §1: a refused follower's rejection counts for check quorum only in a window in which the leader's re-seed stream to it had a chunk acknowledged (D-049); the variant counts it whatever the stream does, the leader as built | the re-seed scenario's blocked half: with the leader's other follower cut off and the stream to the refused one lost to a path-MTU black hole, the leader keeps its office through the whole hold where the correct leader steps down within two windows and three ticks | a follower refused and being re-seeded while the leader's other follower is away, which `sim/quorum.rs` builds on every seed: the refusal made by the store's lost mark at a restart, the other follower cut off the moment the leader opens the stream |
| `RefusedNeverCounts` | the same rule from the other side: nothing from a refused follower counts, the alternative D-049 rejected | the re-seed scenario's open half: the leader steps down mid-re-seed and, the re-seeded server never voting (D-035) and the other follower away, commits nothing after the install, where the correct leader keeps its office and commits | the same, with the stream open, on a disk that takes no time, since on the sweep's the refused server's silence while it repairs and adopts deposes the leader under any counting |
| `FollowerNeverCompacts` | §1 above: a replica that is not leading compacts to its own applied index once its log is more than `snapshot_threshold` entries past its prefix (D-065); the variant is the server as it was built before that, whose follower log shrinks only by truncation or install | the follower-log bound, `raft::FOLLOWER_LOG_MULTIPLE` × `SNAPSHOT_THRESHOLD`, asked of every replica on every seed: caught on 5 of the first 1 000 seeds (116, 429, 512, 577, 757), by that bound on all five, with 878 entries — 73 × the threshold — the largest, on seed 512. The rate is half a per cent, far under the gate's twenty seeds, so the pair is pinned at seed 512 rather than swept (D-061): `a_replica_that_never_compacts_outgrows_the_follower_log_bound` | a replica that trails its leader and is never streamed a snapshot, over a run long enough for the lag to pass 64 × the threshold |

The checks about time in §2 are bounds, not properties: a client write within ten
maximum election timeouts of the last heal, and an election within two of a server's
last reset, chosen so that the correct variant never trips them over ten thousand
seeds. Each of the three times ten thousand seeds tripped the timer bound on the correct
server, what was wrong was the check's model of the protocol, not the bound, and widening
the bound would have dulled its catch of `ResetTimerOnAnyRpc` (seeds 164 and 385, D-030
and D-039; seed 2605 of nightly run 35111624618, the adoption window, PROPOSED D-063).
At ten thousand seeds the correct server passes both (nightly runs 34731272921 and
34749071877, D-047; and run 35111624618 tripped the timer bound alone, on the one seed
D-063 closes).

## 6. Order of work, if approved

1. `core.rs` and `message.rs` with the paper's scenarios as tests, no simulator.
2. `store.rs` and `apply.rs` on the engine, with the atomic apply proven by a crash test.
3. `node.rs` and `sim/raft.rs` with elections, replication, crashes and partitions, and
   the four log invariants; the first variants.
4. Reads and `sim/lin.rs`.
5. Snapshots, then joint consensus, each with its scenario and variants.
6. The exit criteria of SPEC §3 and the devlog, "Breaking Raft with moirae".

Each step stops for review.
