# Phase 2: breaking Raft with moirae

_September 2026. The close of Phase 2; the tag follows this branch's ten-thousand-seed
run._

## What Phase 2 built

Raft, the extended-paper version, as `ananke-raft`: a pure protocol core stepped by
inputs and answering with outputs in an order the server must keep; the hard state, the
log, the configuration and the snapshot record in the storage engine under a reserved
tenant; one server as the `raft`, `net`, `apply` and `snapshot` tasks under the
`Environment`; and a single-shard key-value store serving puts, deletes, compare-and-sets
and linearizable reads. The variant is the hard one on purpose: pre-vote, leader leases
with a drift guard the simulator attacks, joint-consensus membership changes with
learners catching up first, and snapshots streamed as resumable chunks of an engine
checkpoint. RAFT.md is the design; D-025 to D-031 record how each stage landed.

The deliverable, as in Phase 1, is the sweep: servers and clients under drops,
duplication, reordering, delay, clock skew and drift, partitions, one-way blocks, and
crashes with the disk fault model, the disk honouring `fsync` as Raft's safety argument
assumes (D-026), checked against the log invariants of the
paper's Figure 3, folds of the rules behind them, lease safety under drift, and a
Wing-Gong linearizability checker over the clients' history. The core carries fourteen
deliberately broken variants, thirteen of them with a sweep of their own.

This post is about what the sweep taught me about itself: the thousand-seed tier went
green and ten thousand found what it could not reach; pinned seeds went on passing after
they had stopped reaching anything; a family of catches was the checker reading a
record's durability time as its decision time; and my diagnosis of one seed was wrong.

## Seed 42: the network wrote twice

The first seed the stage B sweep ran. The simulated network duplicated a client request,
the leader proposed both copies, and a compare-and-set applied twice. The client was told
about the second application, a failing swap, while the first had already changed the
value, and the linearizability checker named the key. The fix is leader-side
deduplication by client and sequence number while the entry is in the log (D-026). A
network that delivers at least once is not an exotic fault; it is what networks do.

The same entry set a rule this post comes back to. A server awaits a step's persist
before the sends that follow it, and a node traces the step's events only once that
persist is durable, so the trace says what is durable. That answers one question. Late in
the phase the checker asked it another.

## A thousand seeds green, and what ten thousand found

The thousand-seed tier, `scripts/premerge.sh`, arrived with the overnight branch at
`ea6fe7d` (D-040) and was green there: the correct server passed every seed of both Raft
sweeps. The branch (PR #24) was merged three minutes after its ten-thousand-seed nightly
was dispatched, before the verdict. Run 34496762339 then failed three seeds on that tree,
all at or above a thousand and so outside the tier by construction:

- **Seed 6325, a real bug** (D-041): the adoption of a staged install was not
  crash-safe. It removed the old store before the copies were durable, swept a staging
  `CURRENT` damaged by bit rot as debris, and a directory emptied that way opened as a
  fresh store. A crash inside the adoption left a voter that remembered nothing.
- **Seed 5909, two real bugs** (D-043): re-takes of a snapshot at one index wrote one
  shared directory under a stream still reading it, so the stream never completed, and a
  leader streamed to one follower at a time, so the other designated follower waited
  behind it. No client write completed after the last heal. How I first read it is
  below.
- **Seed 7381, the checker.** Its snapshot floor only ever rose, so a server re-seeded
  from a snapshot below its lost store's floor was still held to that floor, and the
  checker reported its log for not holding an applied index it had recovered. An
  installed snapshot now sets the floor exactly (`cb15eb1`).

Earlier, on the overnight branch and before D-040 made GitHub the only place ten thousand
run, two local ten-thousand-seed runs had found seed 164, on `1373601` (the tree of
`48e5276` on main), and seed 385, on `f54b468` (`635aea0`). Both were the timer check.
Seed 164's follower was flagged for 399.26 ms against a 399.19 ms bound while
InstallSnapshot chunks of its term arrived: the check counted only AppendEntries as
contact, and now counts an InstallSnapshot of its term too (D-030). Seed 385's follower
campaigned 25 ms past its bound after an install had rebuilt its core with a fresh timer,
which the check did not know (D-039). Both seeds are below a thousand; the tier did not
exist yet.

The fixes for the nightly's three went through the thousand-seed premerge, and it failed
on seed 687 (D-044). A server refused its store for lost state, as it must, but the
refusal lived only in the process and its engine kept working: a flush rewrote the
manifest without the lost table and deleted the log segment that held the evidence. The
next restart opened clean, a voter with a hole in its state machine. A refusal is now
durable, and a refused engine does no work.

So the tiers above CI's hundred seeds found four real server bugs: three out of the ten
thousand, the adoption and 5909's two, and the fourth out of the thousand once those were
fixed. Every other seed the ten thousand failed the correct server on was the checker:
164, 385 and 7381 were rules it lacked, and 1885 and 2023 were a timestamp it misread.

## Pinned seeds that no longer reached what they were pinned for

After those fixes six seeds were pinned: 164, 385, 687, 5909, 6325 and 7381. All six
asserted only green: that the correct server passed, and for 5909 that
`IgnoreIncarnation`, `SharedSnapshotDir` and the pair passed too. A seed's schedule moves
whenever a fault arm is added, a record changes size or a draw changes, and a pin that
asserts only green keeps passing after its seed has stopped reaching the situation it was
pinned for.

The audit (PR #30) ran each seed under the correct server and its buggy variants,
regenerated 164 and 385 byte for byte on the commits they were found on, and had every
reading challenged by an adversarial reviewer. Five seeds no longer reached what they
were pinned for: 164, 385, 7381, 6325 and 687. Seed 164 now elects a different leader
after the 9.691 s partition; seed 6325's install finishes 318 ms before the crash that
once landed inside its adoption; seed 687's bit rot lands on a checkpoint copy instead of
the live table, and `AdoptionAsBuilt` and `RefusalNotDurable` pass their seeds too. Seed
5909 no longer reached its wedge either, and seed 680, which `SharedSnapshotDir` alone
wedges, pins the wedge now. Part of this was on record: the pins for 5909 and 687 said
they held a seed green rather than replaying it. The audit added the measurement.

The rule is in CLAUDE.md now. A pinned seed asserts its mechanism: the buggy variant
still fails it, and the correct server's trace still reaches the situation the fix
handles; or, where the schedule has moved away, the pin asserts that situation absent,
with the reason in its comment, so the day it returns the test says so. Each of the six
does one or the other. The audit stated the absence pins' limit: they prove the situation
is gone, not that the fix handles it when it comes.

It also corrected seed 164's record. The seed came from a local run, not the nightly; its
follower was about sixty-eight entries behind, not two hundred; and in the stretch the
check flagged nobody led, since server 3 had lost its quorum at 12.936 s and the 21
chunks were its leftover stream. D-048 records the correction to D-030's account.

## Seeds 1885 and 2023: durability time read as decision time

Nightly run 34711427220, on `14c3e17`, failed the correct server on seed 1885, *pre-vote:
server 1 raised its term from 8 to 10 while isolated*, and on seed 2023; the scheduled
run on that tree, 34747423004, failed both again. The server had done nothing wrong. A
RequestVote of term 10 reached server 1 2.530703 ms before the isolation began; it
adopted the term and persisted it; and because a node traces a step's events once the
persist is durable, the rule from seed 42's entry, the record was stamped 48.446 µs
inside the window. Every isolating fault partitions at the instant it fires, so a rise
stepped just before is stamped just after. No message reached the server inside the
window.

The pre-vote check and the timer check read that durability time as the moment the server
decided. D-047 gives every record both times: `TraceRecord::at` stays the durability
time, and `TraceRecord::decided` is when the step was taken. A check about why a server
did something reads the decision time; a check about what was durable reads the other.
The moirae export carries it as `decidedNs` in a log line's data where the two differ.
Taking the stamp moves no schedule, measured: the echo scenario's pinned hash is still
`19f19201df99a799`, and seed 42's trace is byte-identical to the previous tree's once the
field is removed, 100 992 lines with 4 744 carrying it.

Every sweep of the raft scenario but the lease trials now reports what reading decision
time moved, twelve in all; neither membership sweep does. At ten thousand seeds (run
34749071877) it removed 28 catches and added none. Two were the correct server's failures,
1885 and 2023. The other 26 had counted as catches of known-buggy variants:
`IgnoreIncarnation` 4, `SharedSnapshotDir` 7, `ResetTimerOnAnyRpc` 8, `AdoptionAsBuilt`
5, `SnapshotWithoutCurrentLast` 1 and `ApplyBeforeCommit` 1. Of the 28, 27 were pre-vote
straddles, decided 11.7 µs to 2.53 ms before an isolation and traced 48 µs to 3.36 ms
after it, and one was a timer catch: in seed 5153 under `ResetTimerOnAnyRpc`, server 2
granted a vote at its delivery at 8.262757 s, inside its bound, and the vote was traced at
8.265322 s, the first record past it.

The 26 were false positives credited to the sweep. `IgnoreIncarnation` fell from 4 of
10 000 to 0 and `SharedSnapshotDir` from 13 to 6, and since no schedule moved, the drop is
itself evidence that the four `IgnoreIncarnation` catches were timing artefacts, as D-047
had measured each one to be. Neither D-042's nor D-043's text claimed them as catches of
its bug, but the printed rates carried them, and `SharedSnapshotDir`'s test counted every
catch toward the one it required at ten thousand; it now requires a liveness catch. Seeds
164, 385 and 7381 are not in this family: they were rules the checker lacked, and a record
with two times would have prevented none of them.

## Seed 5909: two bugs, then one

Run 34496762339, seed 5909. The last commit was 329 at 13.43 s, and nothing committed for
the remaining 5.4 s of the run. Server 2 was streamed snapshot 329 from 13.872 s, 735
chunks with 15 resumes from offset 0, and between 15.261 and 15.370 s the leader re-took
329 five times into `/raft/snap-329`, the directory the stream was reading; the stream
never completed.
Server 3, refused and re-seeded, was designated snapshot-fed, received no chunk, and
rejected 302 heartbeats with hint 334 from the term-11 leader's election at 14.757 s.

On 11 September D-043's Context and the pin already had server 3 designated
snapshot-fed and queued behind server 2's never-completing stream. Beside that I read a
stale match index: a re-seeded follower loses entries it acknowledged, a leader's
`matched` only rises, so the leader never probes below it and discards every answer. By
that evening I had written that the wedge needed both bugs at once, a stale `matched` on
one follower and a never-completing stream to the other, each fix removing one necessary
condition. That reading motivated D-042, store incarnations, and D-045, which made
`Variant` a set so one server could carry both bugs. D-043's Context and the pin also
carried a wrong timeline for server 2's stream, since 7.46 s, 744 chunks and six resumes,
and D-043's Context counted 204 rejections where there were 302 from the election. And when PR #28 called seed 687 the fourth bug the sweeps
had found, D-042 was one of the three before it.

The audit measured the nightly's trace. No stale `matched` occurred: the term-11 leader
was elected after server 3's last acknowledgement, index 333 at 13.86 s, and a new leader
builds every follower's progress at `matched: 0`. Server 3 sent it 302 rejections and no
success. It went uncounted for the reason D-043's Context had already given, designated
snapshot-fed and queued behind server 2's scrambled stream, so D-043's two bugs alone
explain 5909 and the stale `matched` was a second cause that never occurred. Seed 680
agrees: `SharedSnapshotDir` alone fails it with the byte-identical message the pair gives.
Over seeds 0..999 in release the pair is caught on 1 of 1000, seed 680,
`SharedSnapshotDir` alone on the same seed, `IgnoreIncarnation` alone on 0, and the pair
on 0 seeds that neither single catches. The pair has no sweep and has not run at ten
thousand.

The diagnosis was mine, and the error was one of method: I read the trace for a story that
fit both followers and wrote it into three entries that already held server 3's last
acknowledgement at 13.86 s, the term-11 election at 14.76 s and a count of its rejections,
without asking what a leader elected after that acknowledgement knew of server 3, or
whether server 3 ever answered that leader with a success. It stood from `b2e8f91` on 11
September to `7e5fbd4` on 12 September and shaped PRs #27 to #29 in between.

D-042 and D-045 stayed decided on their own reasoning. A re-seed can make a follower lose
entries it acknowledged, which breaks the assumption behind a monotone `matched`, and
today's correct run of 5909 takes D-042's path: server 3 refused at 18.696 s, its
progress reset at 18.698 s, re-seeded at 18.970 s. Under `IgnoreIncarnation` the seed
reaches the stale state and its pin asserts it; no seed has failed on it. D-045's set
stands because before it no run could put two bugs in one server; at a thousand seeds it
has not yet caught anything a single variant misses.

## What the sweep is now

Run 34749071877, on `9b5995d`; the code on this branch differs from it only in comments,
two message strings, one test name and docs. The correct server passed all 10 000 seeds
through 53 953 partitions, 113 224 crashes, 31 580 refusals, 188 189 snapshot installs
and 10 148 606 commits. The variants: `SendBeforePersist` 10 000, `NoPreVote` 9 999,
`TruncateOnEveryAppend` 9 995, `ApplyBeforeCommit` 8 902, `CountOlderTermForCommit`
4 413, `ResetTimerOnAnyRpc` 3 462, `SnapshotWithoutCurrentLast` 3 303,
`SingleMajorityInJointConsensus` 2 720, `AdoptionAsBuilt` 646, `RefusalNotDurable` 132,
and `SharedSnapshotDir` 6, 4 by liveness and 2 linearizability searches out of budget,
which prove nothing. `IgnoreIncarnation` is caught on 0, with its precondition reached on
6 257 seeds, and its sweep asserts the bug is injected. Drift exceeded 1000 ppm on 5 023
seeds; the lease guard revoked on all of them, and without it a stale read was served on
472. The Phase 1 variants in the same run: `NoWalBeforeMemtable` 9 825,
`ReleaseBeforeManifest` 6 482, `DeleteBeforeManifest` 5 893, `AckBeforeSync` 10 000,
`NoChecksum` 9 744, `NoSyncDir` 9 150.

A thousand seeds, the tier a branch merges on, took 7 min 26 s when D-046 made the safety
re-check incremental, down from 29 minutes. Ten thousand run nightly on GitHub.

## Phase 2 exit criteria

SPEC §3 names three.

- **All five invariants hold across 10k seeds with the full network and disk fault
  model.** Green on
  [run 34749071877](https://github.com/pchrysostomou/ananke/actions/runs/34749071877)
  on `9b5995d`, and on run 34731272921 on `bd93ed3` before it, with one exception to
  "full": the disk honours `fsync` (D-026). Torn writes, bit rot and lost directory
  entries stay on; lost syncs are issue #23. The tag follows this branch's own
  ten-thousand-seed run.
- **Membership change from 3 → 5 → 3 nodes under partition, no availability loss
  beyond one election timeout.** Not met as worded. The change completes both ways on all
  10 000 seeds, but the sweep's bound is ten maximum election timeouts, 2 s (D-029), and
  the worst gap is 549.36 ms, above one 200 ms maximum election timeout.
- **Devlog post showing a real bug found and its trace.** This post. The failing traces
  of 5909, 6325 and 7381 are run 34496762339's artifacts, kept until 2026-12-09.

## What is open

- **Issue #32.** A term-raising message delivered before an isolation but stepped inside
  it, behind a persist or an install, is decided inside the window, and the pre-vote
  check would still flag it. No correct-server seed of the ten thousand has.
- **Issue #33.** A timer catch that decision time removes is printed, not asserted, and a
  removed pre-vote catch is asserted only to share its run with a straddle, not matched
  to its own isolation. Seed 5153 was checked by hand.
- **Issue #23.** The sweep's disk honours `fsync` because Raft's safety argument assumes
  it, and a refused server waits for a snapshot. Protocol-aware recovery (Alagappan et
  al.) would repair a store from the other replicas instead.
- **SPEC §3's availability criterion.** The as-built check allows ten maximum election
  timeouts and the worst gap is 549.36 ms; the SPEC says one election timeout. Either the
  server closes the gap to one election timeout and the check is tightened to match, or
  SPEC's wording changes; until one does, the criterion is not met.
