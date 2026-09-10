# Phase 2: breaking Raft with moirae

_September 2026. DRAFT — Phase 2 is not yet tagged; the numbers below are from the
overnight branch's sweeps and the nightly noted at the bottom._

## What Phase 2 built

Raft, the extended-paper version, as `ananke-raft`: a pure protocol core stepped by
inputs and answering with outputs in an order the server must keep; the hard state,
the log, the configuration and the snapshot record in the storage engine under a
reserved tenant; one server as four tasks under the `Environment`; and a single-shard
key-value store on top, serving puts, deletes, compare-and-sets and linearizable
reads. The variant is the hard one on purpose: pre-vote, leader leases with a drift
guard the simulator actively attacks, joint-consensus membership changes with
learners catching up first, and snapshots streamed as resumable chunks of an engine
checkpoint. RAFT.md is the design; D-025 through D-031 record how each stage landed.

The deliverable, as in Phase 1, is not the protocol. It is the sweep: three servers
and two clients under message drops, duplication, reordering, delay, clock skew and
drift, partitions, one-way blocks, and crashes with the full disk fault model, checked
after every crash and at the end against the four log invariants of Figure 3, three
folds of the rules behind them, lease safety under drift, and a Wing-Gong
linearizability checker over the clients' history. Twelve deliberately broken servers
run beside the correct one, and each must be caught. A variant the sweep cannot catch
is a hole in the sweep — that rule shaped this phase more than any other, and two of
the three bugs below came from taking it seriously.

## Seed 42: the network wrote twice

The first seed the stage B sweep ever ran. The simulated network duplicated a client
request, the leader proposed both copies, and a compare-and-set applied twice. The
client was told about the second application — a failing swap — while the first had
already changed the value. The linearizability checker named the key.

Compare-and-set is in the client model precisely for this: a double apply shows up as
a wrong boolean immediately, not as a stale value some later read might notice. The
fix is leader-side deduplication by client and sequence number while the entry is in
the log (D-026). The network delivering at least once is not a fault injection
exotic; it is what real networks do. The sweep found it before the first hundred
virtual seconds of the phase had run.

## The lease catch that silently died

Stage E turned on snapshots, and the correct server kept passing every seed. What
failed was the negative control: `LeaseTrustsTheClock` — the server without the drift
guard, which the checker must catch serving a stale lease read — went from being
caught on a fifth of drift-exceeded seeds to being caught on none.

The mechanism took an evening to find. The sweep's lease trial hands leadership to
the server with the slowest clock and cuts it off with a reading client while the
fast majority elects and writes; the stale read only exists if a write lands inside
the window. With a snapshot threshold of twelve entries, the freshly elected
majority-side leader took its first checkpoint the instant it won — its entire
history was past the threshold — and the checkpoint's apply stall pushed the one
write that had to land inside the window past the client's sixty-millisecond budget.
No invariant failed. The code was correct. The *test* had lost its teeth, and only
the pair rule noticed: a known bug that stops being caught is a red sweep, whatever
the correct server does.

The fix is in the core, not the harness: a fresh leader defers its first
threshold-driven snapshot by two minimum election timeouts (D-030), which is sensible
behavior on its own — a leader's first duty is replication, not housekeeping. The
catch rate came back. The lesson is the one this project is built on: the harness is
a piece of engineering with its own failure modes, and the buggy variants are how it
tests itself.

## Seed 60: two ways down, priced

Stage E's rule for a server whose store lost state is conservative to the point of
paranoia: it is re-seeded from a leader's snapshot, but it never votes, pre-votes,
campaigns or extends a lease again on that store, recorded durably (PROPOSED D-035).
The quorum arithmetic says this is safe — a commit majority and a vote majority must
intersect in a voter, and the quarantined server is not one. Seed 60 sent the bill.

Bit rot beheaded a WAL segment on one server — a covered skip, then "expected record
214, found 238" — and the store refused to start, as it must: an applied index over a
hole names a state that never existed (D-022). A second server was already under the
re-seed quarantine from earlier in the run. The third pre-voted forever: a
quarantined server grants nothing, and a refused server can only be re-seeded *by* a
leader, which now could not exist. Liveness failed with zero code bugs. That is the
availability price of D-035's conservatism, and the sweep now states it honestly:
liveness is asked only of majorities that can actually elect.

The investigation paid twice over. It exposed two real bookkeeping bugs in the
quarantine itself — an ordinary snapshot install onto a quarantined receiver used to
*clear* the quarantine, and the flag could ride inside a leader's checkpointed
tenant 0 and quarantine a healthy receiver. Both fixed; the flag now sticks to a
store's history and is explicitly tombstoned in the install repair. The full answer
to re-seeding — recovery that repairs a local store from the other replicas'
knowledge — is Alagappan et al.'s protocol-aware recovery (FAST 2018), issue #23.

## Also found on the way

- The issue #22 sketch for a batched Figure 8 driver cannot work at this sweep's
  speeds: commit knowledge lags appends by one round trip, so no crash timing opens
  a bigger-than-a-batch gap on the surviving follower. What opens the window is the
  crashed leader itself, steered back into the lead — a restart's commit index is
  volatile, and rebuilding it from batch acknowledgements below its no-op is exactly
  the §5.4.2 mistake (D-031). Caught on 58 of 100 seeds with batching at the default.
- `voters()` returned every member, learners included — latent while learners were
  always empty, a counting bug the moment stage D made them real (D-029).
- The snapshot staging directory was assembled without `sync_dir`, so the simulated
  crash took every staged file and the mid-install-crash variant was uncatchable —
  the directory-entry-loss model is what makes an acknowledged chunk offset honest
  (D-030).
- A leadership change during learner catch-up quietly abandons the membership
  change; the operator's idempotent retry is the recovery (PROPOSED D-032).
- The ten-thousand-seed nightly's first two correct-server failures, seeds 164
  and 385, were both the liveness timer check misreading a follower being fed
  a snapshot — once because it did not count `InstallSnapshot` as the leader's
  contact, once because it did not know an install's completion rebuilds the
  core with a fresh timer. Neither was a protocol bug; both are checker
  refinements (D-030, PROPOSED D-039), and both seeds are pinned in the gate.
  A hundred seeds never reached a follower that far behind a compacting leader;
  ten thousand did.

## The sweep by the numbers

At 1000 seeds in release (`scripts/premerge.sh`, seeds in parallel, D-040; the
10k verdict is pending on the GitHub nightly): every one of the nine sweep-tested
variants caught — SendBeforePersist, TruncateOnEveryAppend and NoPreVote on
1000/1000, ApplyBeforeCommit 895, ResetTimerOnAnyRpc 438, CountOlderTermForCommit
406 under default batching, SnapshotWithoutCurrentLast 326,
SingleMajorityInJointConsensus 275, and the guardless lease server caught serving a
stale read on 41 of the 503 drift-exceeded seeds with the guard revoking on every
one of them. The membership scenario completed its 3 → 5 → 3 change under
partition on all 1000 seeds, worst availability gap 469 ms against a 2 s bound.
The correct server: green on every seed of both sweeps — 4820 partitions, 3165
crashes, 580 refusals and 17 046 lease revocations later. The Phase 1 sweeps hold
at a thousand too. A thousand seeds take a quarter of an hour on a laptop; that
is the tier a branch is merged on, and ten thousand are the nightly's.

## What is deferred, deliberately

Client sessions for exactly-once retries across leaders (thesis §6.3) are issue #21,
needed before Phase 4. Protocol-aware recovery is issue #23. Six PROPOSED entries at
the bottom of DECISIONS.md await review — the re-seed quarantine above is the one
with teeth. The tag waits for all of that review; this phase is code-complete, not
done, and D-011 is clear about the difference.
