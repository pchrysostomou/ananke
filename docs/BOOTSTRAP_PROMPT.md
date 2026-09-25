# BOOTSTRAP_PROMPT.md — ananke

> Paste this into a fresh Claude Code / agent session to bootstrap or resume work on ananke.
> It is the single source of truth for *what we are building and why*. SPEC.md is the
> source of truth for *how*. DECISIONS.md is the log of *why we chose this over that*.

## What ananke is

**ananke** is a distributed SQL database written in Rust, built from the ground up to be
*deterministically testable*. Every source of non-determinism — disk, network, clock,
randomness, thread scheduling — goes through a single `Environment` abstraction, so the
exact same code that runs in production also runs inside a deterministic simulator driven
by **moirae** (github.com/pchrysostomou/moirae), a DST framework with a visual trace replay
studio.

The thesis: correctness in distributed systems comes from being able to *reproduce* every
failure. ananke is designed so that any bug found under simulation can be replayed
byte-for-byte, stepped through in the moirae studio, and turned into a regression test.

ananke and moirae are one project in two repos. ananke is moirae's flagship consumer;
moirae is ananke's test harness. Each justifies the other.

## Who is building it

Prodromos Chrysostomou — MSc Software Systems Engineering (UCL), starting MSc Information
Security (UCL) in October 2026. Author of moirae. Solo developer with AI-assisted
workflows. Interests: distributed systems, security, open source (curl, Redis, Home
Assistant contributions).

This is a long-horizon project (12–18 months) built alongside studies. Each phase must be
independently shippable and blog-able. Optimise for *finished layers*, not breadth.

## Non-negotiable principles

1. **Determinism first.** No direct calls to `std::time`, `std::fs`, `tokio::net`,
   `rand`, or thread spawning outside the `Environment` trait. CI fails on violation
   (clippy lint + `disallowed-methods`).
2. **Every component is testable in isolation under simulation** before it is wired into
   the cluster. Storage engine gets crash-injection tests before Raft exists. Raft gets
   partition tests before sharding exists.
3. **No unsafe outside `storage/`**, and every `unsafe` block has a `// SAFETY:` comment
   and a Miri run in CI.
4. **Protocol-level compatibility with moirae's trace format.** Every state transition
   that matters emits a moirae trace event. If it can't be seen in the studio, it didn't
   happen.
5. **Security is a phase, not an afterthought** — but the architecture (tenant boundaries,
   key hierarchy hooks, authenticated node identity) is present from Phase 0 as
   placeholders so it doesn't require a rewrite.
6. **Ship small.** A phase is done when it is tagged, published to crates.io, and has a
   devlog post. Not before.

## Phases

| Phase | Deliverable | Done when |
|---|---|---|
| 0 | Deterministic runtime, `Environment` trait, moirae bridge | A toy echo server runs identically under real and simulated env, trace visible in moirae studio |
| 1 | Storage engine (LSM) | 10k crash-injection simulations pass; recovery is byte-identical |
| 2 | Raft | Linearizable single-shard KV under partitions, clock skew, disk faults; joint-consensus membership changes |
| 3 | Multi-raft sharding | Range splits/merges/rebalances under load without losing linearizability |
| 4 | Transactions | Snapshot isolation across shards (Percolator-style), verified by elle |
| 5 | SQL layer | `CREATE TABLE`, `INSERT`, `SELECT` with `WHERE`/`JOIN`/`ORDER BY`, secondary indexes |
| 6 | Security | mTLS between nodes, per-tenant encryption at rest, RBAC, tamper-evident audit log |
| 7 | External verification | Jepsen-style harness, published results, fuzzing corpus |

Full detail per phase in SPEC.md.

## Repository layout (target)

```
ananke/
  crates/
    ananke-env/        Environment trait + real & simulated implementations
    ananke-storage/    LSM engine
    ananke-raft/       Raft consensus
    ananke-shard/      Range management, multi-raft
    ananke-txn/        MVCC, transaction coordinator
    ananke-sql/        Parser, planner, executor
    ananke-net/        Wire protocol, mTLS, node identity
    ananke-server/     Binary: assembles a node
    ananke-cli/        Client CLI
  sim/                 Simulation scenarios (moirae-driven)
  docs/
    SPEC.md
    DECISIONS.md
    RAFT.md            Formal-ish description of the Raft variant used
    devlog/
```

## Working agreements for an AI agent session

- Read SPEC.md and DECISIONS.md before writing code. If a design question isn't
  answered there, propose an answer as a DECISIONS.md entry *before* implementing.
- Never widen scope inside a phase. If something is tempting, add it to
  `docs/BACKLOG.md` with one line of justification.
- Every PR: tests under simulation, clippy clean, `cargo doc` builds without warnings.
- Prefer boring, well-documented Rust. This is a project meant to be read.
- When stuck on a distributed-systems question, cite the paper (Raft, Percolator,
  Calvin, Spanner, FoundationDB testing talk) in the DECISIONS.md entry.

## Current status

_Update this section at the end of every session._

- Branch `phase-3-stage-c-split` (2026-09-25), stacked on `phase-3-stage-c-range-ids`
  (PR #135), PROPOSED D-100, **Stage C's split, the first of its four sub-slices:
  proposed by a range's leader with an id from its node's block, re-checked and applied
  by every replica in one batch, the right half's core started from its floor and
  hurried to its first election, and check 18's split clause** (SHARD.md §5, Q18–Q21,
  Q23, Q26; §8's check 18; §10's `IdBlockResumed` caught, `MetaOverwritesByArrival`
  and `ApplyIgnoresSpan` alone reached). `Command::Split { key, right }` and
  `Outcome::Refused(SplitRefusal)` on the wire; the proposal at the leader's receipt —
  system range, a change under way (`Raft::changing`, Q23), not `Live`, the key outside
  the span, no id in the block (PROPOSED D-092's A, as proposed) each refused with its
  reason and no entry, a follower handing the ask to its core with no id taken — and
  the apply on every replica: the re-check against shared state, one synced batch with
  P's applied index and both halves' descriptors at g + 1, R's Raft state at the floor
  `(s, 1)` on a voter of P's configuration at `s` and a range delete of the right span
  on a replica that is not, `RangeSplit`, `RangeDescriptor`, `RangeCreated { split }`
  traced, R's store opened beside P's, its core from `restore_compacted` handed to the
  `raft` task as `Local::RangeAdded` and given `Input::Campaign` where the node led P
  (Q21: the pre-vote now and every heartbeat interval while no leader is heard, a
  candidacy waiting its timeout); the node's tables dynamic behind a lock, the
  `snapshot` task's plan following both halves' spans; a restart reopening every group
  the engine holds from its descriptor (`discover_ranges`, §5); the meta update for
  both halves from the node that led P; check 18's split clause (`SplitLineage`) under
  the range layer's checker; the sweep's `Fault::Split` on every node schedule and
  `Fault::SplitFromRestarted`, §10's shape for `IdBlockResumed`, with the split's
  coverage folded (`splits_of`). Five faults on the correct path found and fixed on the
  way in (the entry's findings): the hurried pre-vote burning a term a heartbeat, a
  restart not reopening a right half, the host keeping the old core on a right half's
  install, an install of the parent writing over the right half's keys (a stale read
  through R), a follower taking an id for a split it would not append, and D-099's
  `meta` task numbering its requests from 0 at every start so a restarted node's refill
  was dropped as a copy. Measured before asserted, at the gate's twenty: 31 splits took
  effect, 62 right halves reopened by a restart, 129 installs onto a half after its
  split; `IdBlockResumed` caught on 14 of 20 (12 by check 18) on the restarted
  proposer's second split, asserted at every tier; `MetaOverwritesByArrival` 3 of 20 by
  check 16, asserted from the thousand's share; `ApplyIgnoresSpan` alone 1 of 20 by
  check 9 — 5 of 100 at the thousand, D-061's 5 % on the nose — asserted from the
  nightly's share of a thousand. **Every node schedule moved**: seed 493's schedule
  moved off D-099's hold, re-pinned as the absence with its reason, and **the thousand
  names seed 633** of the three holds it counted, pinned in D-091's shape (server 2's
  replica of range 0, the root).
  The premerge is green at a thousand seeds in 2 447 s on this session's container
  against D-099's 2 332, the node binary 1 010 s where it took 878 (two more arms on
  every schedule), every rate tabled in the entry against D-099's: 1 630 splits took
  effect, 101 072 writes through a right half, 3 906 right halves reopened, 7 989 halves
  installed after their split, 101 writes refused at apply after a split; the restarted
  proposer's second split done on 594 seeds; `IdBlockResumed` 71 of 100. The
  placeholders (Q22), the overlap rule (Q27) and the split's other variants are
  C5b–C5d's.
- Branch `phase-3-stage-c-range-ids` (2026-09-25), stacked on `phase-3-stage-c-meta`
  (PR #134), PROPOSED D-099, **Stage C's range ids leased in blocks** (SHARD.md §5, Q17;
  §8's check 18, its range-id clause; §10's `IdBlockResumed`). `Command::Refill { node,
  run }`, an entry `ananke-raft` reads nothing of; range 0's lease record per node
  (`system::LeaseRecord`, the run nonce and the block's first and last id) beside the
  counter; `ananke_shard::ids::grant`, range 0's apply of a refill — the block of
  `id_block` ids at the counter to the asking run, the record and the counter past the
  block in one batch with the entry's index, `RangeIdsLeased` traced, effect `took`,
  the record answered back — refused as a mismatch off range 0; `ids::IdBlocks`, the
  node's block in memory: the run nonce drawn at every start, a grant adopted only if
  it carries this run's nonce and each block once, ids taken in order from memory so a
  restart abandons the rest; the refill on the `meta` task, asked when the ids left are
  at or below `refill_at` and none is outstanding, resent every minimum election
  timeout until a grant of this run lands, the lease record read back before each
  resend where the node holds range 0 (§5's second rule's other case, PROPOSED D-092's
  restatement); the block size and threshold `ServerConfig`'s, eight and two until the
  sharded sweep measures them; the lease record read at the start, where
  `IdBlockResumed` adopts whatever run's block it names; check 18's range-id clause,
  the part without a split — grants by first apply, blocks disjoint, every replica of an
  index granting the same — under the range layer's checker. Nothing takes an id on
  this tree; the exhausted block's answer is the split slice's, as D-092 is ruled.
  Measured before asserted: range 0 granted 107 blocks over 20 seeds, the fewest on a
  seed 3, one per node at its start and one per restart; `IdBlockResumed` caught on no
  seed, the absence asserted with its reason, and seen injected at 60 grants to its
  nodes against 107 to the correct node's over 55 crashes; the checker's agreement with
  its six folds at 480 prefixes. **Every node schedule moved** (the nonce, the refill):
  seed 368's schedule moved off the hold D-098 pinned, re-pinned as the absence with a
  guard that every live install traces its state, its snapshot and its restatement at
  one instant — **which found** the `snapshot` task reading a switched range's span
  from the configured user ranges, D-098's host finding on the task: a system range's
  switch traced no state read back and D-091's arm was silent for every one since the
  meta range first compacted; the task reads the hosted six. The correct node's sweep
  counts the holds the arm answers for on every seed and names the first, where the pin
  moves. The premerge is green at a thousand seeds in 2 332 s on this
  session's container against D-098's 2 308, the node binary 878 s where it took 834
  (the correct node's sweep replaying the timer check a second time on every seed, its
  row re-weighed 387.4 → 547.2 cpu s), every rate tabled in the entry against D-098's:
  5 408 blocks granted, fewest 3 a seed; the lease variant's catch 6 of 1 000 where D-098
  measured 12, recorded for the owner. **The thousand named seed 493** as the hold
  D-091's pin waited for, and it is pinned in D-091's shape.
- Branch `phase-3-stage-c-meta` (2026-09-25), stacked on `phase-3-stage-c-routing`
  (PR #133), PROPOSED D-098, **Stage C's meta build: the root and the meta range**
  (SHARD.md §1; Q3, Q4, Q36; §8's check 16; §11, raft 3). `Command::MetaUpdate
  { descriptors }`, an entry whose descriptors are bytes `ananke-raft` does not read
  (Q40), and `Command::Lookup { key }`, a read that asks about a key and touches none, so
  neither is checked against a span; `ananke_shard::meta`, the meta range's state
  machine — for each descriptor of an update every maximal sub-interval of its span that
  a record of lower generation names, or that nothing names, is from then on named by
  it, a record partly overwritten cut in the same batch, the descriptors of one update
  composed over one map and the batch the difference, `MetaApplied` traced with what
  each won and the effect `took` or `none`; every node's `meta` task, handed a user
  range's descriptor when a core sends the first `AppendEntries` of a term the node had
  not led it in, sending `MetaUpdate` to the meta range's leader under the node's own
  client id and resending every minimum election timeout until acknowledged, whether or
  not the node still leads (Q3), and never on the answer itself (the first build's
  storm: 585 000 messages in eight simulated seconds on seed 1); a `Lookup` asked of
  range 0 answering the meta range's descriptor and of range 1 the first record whose
  end key is above the key, by the bounded seek at the read's version, traced as a read
  on the system range that served it, which check 9 holds to nothing (Q36); the sweep's
  client looking a key up through ranges 0 and 1 on a miss and the node cluster's second
  client starting knowing range 0 alone, D-097's interim gone; check 16
  (`MetaNeverGoesBack`) under the range layer's checker, the bootstrap's `MetaApplied`
  traced after the creations; every node scenario folding the range layer's checks, the
  install and re-seed scenarios included, which found D-096's install creation tracing
  the configured keys raw; `MetaOverwritesByArrival` with no path on this tree, caught on
  no seed and seen injected by the spans its updates win. Measured before asserted: 113
  lookups served and 1 917 meta applies with none `took` over 20 seeds, both floors
  asserted; D-097's rates held under the lookups; `RefuseOneRangeOnly` caught 8 by the
  fan-out clause and 12 by state machine safety on the meta range, the second new; the
  checker's agreement with its five folds at 480 prefixes. **Found by the twenty**: the
  client's lookups under an operation's `seq` were paired with it by the history's
  closure (`LOOKUP_SEQ_BASE`), and a re-seeded replica of a system range started its
  state machine empty, diverging at the first update (the re-seed writes its index-0
  state from configuration). Every node schedule moved and the pins hold (272 and 516
  absent at 40 and 23 live installs, the re-seed's seed 1 at 2 chunks); seed 42's
  one-group JSONL is D-097's. **Found at a thousand seeds**, on the correct node: the
  meta range's leader compacts now, so a system range was streamed a snapshot for the
  first time, and the host's `installed` looked the range's span up in the configured
  user ranges, not the hosted six — the switch landed in the store and the old core was
  kept, `InstallKeepsTheOldCore`'s own behaviour, and the next apply named an index the
  compaction had removed (9 of 1 000 seeds; the host reads the hosted six); and, on
  seed 368, a node streamed five snapshots at once held range 2's core through four
  other installs and the timer check flagged it — D-091's live-install arm reads the
  switch's two records as one only at one instant, and D-097's descriptor read-back sat
  between them, so the arm had matched no node install since and its exemption, and the
  absence seeds 272 and 516 asserted through it, were silent; the read-back precedes the
  state now, and the install is decided where the hold is taken. The premerge is green at a
  thousand seeds in 2 308 s on this session's container against D-097's 2 246, the node
  binary 834 s where it took 782, every rate the node's sweeps assert tabled in the entry
  against D-097's: 5 541 lookups served and 94 113 meta applies over the thousand, none
  `took`; the lease variant's catch 12 of 1 000 where D-097 measured 3, recorded for the
  owner; seed 368 pinned as D-091's hold; two `node` rows weighed on this tree.
- Branch `phase-3-stage-c-routing` (2026-09-25), stacked on `phase-3-stage-c-bootstrap`
  (PR #132), PROPOSED D-097, **Stage C's routing** (SHARD.md §3; §8's checks 7, 9, 10 and
  17; §9; §11, raft 4, 5, 16), in the two commits §12 names as moving pinned schedules and
  hashes. The first: a client request carries the generation of the descriptor it routed
  by beside the range (Q10), `Reply::RangeMismatch { descriptors }` beside `NotLeader`
  carrying descriptors as bytes `ananke-raft` does not read (Q40), and
  `ananke_shard::client::Cache`, the client's descriptor cache merged by §1's generation
  rule, which the sweep's client routes by on the node cluster and resends under a fresh
  `seq` after a mismatch, the operation's own kept for its invoke and return (§9). The
  second: the three checks on the server against its own descriptors — at receipt (a node
  with no replica or a replica whose span lacks the key answers `RangeMismatch` with every
  descriptor it holds for the key, where D-076 failed the run), at a read's serving (the
  descriptor read at the value's engine version), at apply (a keyed command outside the
  descriptor in force before its index applies as nothing with effect `out_of_span` and
  the client is answered `RangeMismatch`, the leader keeping its record) — every one
  traced `RangeMismatchSent`; `ClientSend` and `ClientMismatch` from the sweep's client on
  both clusters; the history closed by `invoked` and by effect `applied`;
  `ananke_shard::invariants` with checks 7, 9, 10 and 17 as folds, one `Checker` fed at
  every look of the incremental checker on the node cluster and folded over the whole
  trace by every node run; §10's `TrustStaleDescriptor`, `ApplyIgnoresSpan` and
  `ClientIgnoresMismatch` (`ReadCheckAtReceiptOnly` comes with the split); and every other
  client of the node cluster starting stale, with §2's one-range map at generation 0, so
  every seed refuses requests at receipt and converges through `RangeMismatch` alone.
  Measured before asserted: `TrustStaleDescriptor` caught on 20 of 20 by check 9 and the
  apply check refusing its trusted writes on 17 of 20 (Q10's path, resent under a fresh
  `seq`, on the correct apply code); the pair with `ApplyIgnoresSpan` caught on 20 of 20
  with a write applied in the wrong range on 17; `ApplyIgnoresSpan` alone on no seed, the
  absence asserted with its reason; `ClientIgnoresMismatch` on 20 of 20 by check 17; the
  checker's agreement with its folds at 480 prefixes. **Every node schedule moved** with
  both commits and the pins hold (272 and 516 absent, the re-seed's seed 1 held back);
  seed 42's one-group JSONL moves with the client events to 13 241 905 bytes and
  `c5b8d814…`, recorded in the entry. **Found in D-096**: its variant test never asked
  that the correct membership node passes check 7's first step, and it did not — a node
  not named at bootstrap traced its interim replica's creation with empty voters; fixed
  here, the creation naming the range's voters on every node, and asserted. **Found at a
  thousand seeds**: the same interim replica held no descriptor and, leading a range it
  had been caught up to by the log alone, served a stale client's read unchecked (check 9,
  seed 652); every node not named at bootstrap now writes its interim replicas'
  descriptors from configuration at its first start (`interim_state`). The premerge is
  green at a thousand seeds in 2 246 s on this session's container against D-096's 2 149,
  the node binary 782 s where it took 753, every rate the node's sweeps assert tabled in
  the entry against D-096's; four `node` rows weighed on this tree.
- Branch `phase-3-stage-c-bootstrap` (2026-09-25), stacked on
  `phase-3-node-folds-equivalence` (PR #131), PROPOSED D-096, **Stage C's first slice: the
  bootstrap** (SHARD.md §2; Q7, Q9, Q32; §1's range-local descriptor). A node's
  configuration names range 0's replicas, the bootstrap nodes; a bootstrap node whose
  store holds no digest writes the initial state in one synced batch before its tasks run
  — every hosted range's configuration at index 0, first incarnation and descriptor at
  generation 1 under a new `PURPOSE_DESCRIPTOR`, range 0's meta descriptor, counter, node
  records and digest, range 1's record per user range — and hosts **ranges 0 and 1** as
  Raft groups beside the user ranges, tracing `RangeCreated` for six replicas and
  `MetaApplied { index: 0 }`. Every span the node names is an interval of encoded keys
  (`Range::span`), the system ranges' in tenant 1. A node not named keeps Stage B's start,
  replicas of an empty configuration, until this stage's placeholders. The variant beside
  it, `AnyFreshNodeBootstraps`, takes a fresh store for a bootstrap with the address book
  as voters and is caught by check 7's agreement fold on the membership scenario's five
  nodes (1 000 of 1 000 over a whole thousand, measured; the test runs a tenth of the
  tier, D-082's share); on three nodes it has nothing to do and its trace is
  byte-identical, asserted as the absence. **Every node schedule moved**, as §12 said the
  bootstrap commit would: seeds 272 and 516 no longer reach D-091's live-install hold (a
  probe over a thousand release seeds found none that does) and their pin asserts the
  absence both ways with its reason; the re-seed shape reads six replicas, the four user
  ranges by a stream each as D-081 asserts and the two system ranges by the log from index
  1, since their leaders compacted nothing, with the arm aimed at a streamed range's mark;
  `sim/tests/ranges.rs` counts six; the apply spreads read the user ranges alone. It also
  caught a bootstrap bug on every seed: a re-seeded bootstrap node bootstrapped again over
  the directory its re-seed built, so the bootstrap is confined to the configured
  directory on a start that did not re-seed. Sweeps: the premerge green at a thousand
  seeds in 2 149 s on this session's container against `main`'s 1 760 s, the node binary
  753 s where `main`'s took 487, every rate the node's sweeps assert within a seed or two
  of `main`'s at the same tier (tabled in the entry), the four node-family binaries' shard
  rows re-weighed on this tree.
- Branch `phase-3-node-folds-equivalence` (2026-09-25), off `main` at 527adcd, PROPOSED
  D-095, the owner's ruling (b) at Stage B's close: **the node's apply-lag, cross-range
  hold and coverage folds run under the incremental checker's equivalence test**, the
  line of Stage B's exit the tag names as unmet. `sim/folds.rs` gives each an
  incremental form beside D-082's whole-trace reading, kept as the reference — the
  hold's in one pass, since a hold's window ends before the apply that reports it —
  and `the_node_folds_agree_with_their_whole_trace_readings` holds fold to reading at
  eight prefixes of `min(seeds, 100)` seeds over four node variants in turn. The lag
  keeps the one verdict there is, §4's heartbeat on each range's median, asked per run;
  the hold and the coverage have no rule to break, so they are compared value for
  value on runs a variant has moved. The tripping variant is new:
  **`ApplyWaitsForEveryRange`**, Q14's grouping built as a wait, caught by the lag
  verdict on **100 of 100** seeds (worst range median 6.32 s against 20 ms) and by the
  run's other checks on 44. **The equivalence held on every seed at every prefix.** Two
  findings for the owner: the hold's median under that variant stays at 2.5 ms, since a
  stalled apply's hold is still the one job before it, which is why a threshold on the
  hold would be the wrong instrument (D-082 made it a figure, not a bound); and the
  correct node trips the per-run 20 ms median on a range on 2 of 25 seeds at a hundred,
  which the pooled per-range median the sweep asserts (3.0 to 3.2 ms at a thousand)
  does not show. Two shard rows, weighed here.

- Merge update (2026-09-24): PR #109 (`phase-3-stage-b-stream-variants`, PROPOSED
  D-086, carrying #123's D-089 and D-091) merged `origin/main` at c178682 under the
  owner's ruling, to be gated, nightly-run and merged by the same session. Six files
  conflicted and were resolved the house way: `docs/DECISIONS.md` as the ordered union
  D-084 to D-091 with one footer at D-092 (no number collided); the shard table as a
  checked union of 129 rows, each in its own side's shard, every header recomputed (the
  night is now bounded by shard 3 at about 15 % over the ideal, which is the re-placement
  question D-086's re-weigh note already leaves with the owner); both sides' status
  bullets; and `node.rs`/`round.rs`, where both sides had fixed the same two bugs — taken
  as D-081's versions, which carry a variant each, with D-086's provenance beside them.
  **Three absence checks the merge made stale were re-read, not deleted** (D-086, "On the
  merge with `main`"): the node's snapshot threshold is each scenario's own — 12 for the
  raft-arms sweep that measures the path, `1 << 30` for `sim/membership.rs` and the
  sharded `sim/quorum.rs`, whose rates were measured with it unreached (extending D-084
  and D-085 to the path is the owner's); a store refused on the node is **counted and
  D-077's fan-out asserted of it**, after #123's nightly reached one on 1 of 10 000 seeds
  against a tripwire that said #86 was not in the tree (0 on the merged tree's thousand);
  and every "the day PR #NNN lands" clause naming a landed PR was re-read, with three
  tests renamed to what is true. **`READS_OUTSTANDING` re-measured by D-076's own rule
  on the merged tree**: the worst a replica held over a thousand seeds in release was 16
  under the raft arms on the node (seed 644), 19 on the sharded quorum scenario, 4 on
  `ranges` and membership, so the bound is **38**, and the high-water mark is now traced
  (`RaftReadsOutstanding`) and printed at every tier. The owner's provenance is in point
  12 — the original 4 was measured on a node that never ran `RetakeUnderStream`, #109
  brought that arm onto the node, and the bound was never wrong for the tree it was
  measured on, *at the tier it was measured at*: `main`'s own nightly on c178682 (run
  36008940158, seed 297) found 9 against 8 with no such arm, and the same trip was
  reported there as five variants caught, which D-091's attribution on this branch is
  shown to close. The alternative the owner declined — not counting a client's
  superseded retries — is issue #125, due in Phase 4. `RefusedReadLeft`'s pair, re-measured
  with the new bound: **0 of 1 000** on `sim/ranges.rs` at 38, whose runs are too short
  for the leak to reach it, so the catch is asserted under the raft arms on the node —
  **108 of 1 000 (10.8 %)**, 8 of 100, 2 of 20, every one by the bound — from the
  hundred-seed tier, and `ranges` keeps the leak's firing at every tier (D-076 point
  12). **The read bound turned out to be a wedge's second reporter**: under
  `SharedSnapshotDir` a wedged range's clients retry into the bound before `check()`
  reaches liveness, and on non-uniform schedules the bound is the wedge's only reporter,
  so the two wedge tests read the wedge from the liveness fold itself and tell a trip
  with no wedge apart by running the correct node on the seed (the node's failure, or
  the variant's load — seeds 41 and 74 at the thousand tier); 20 of 100 wedged by the
  check's own reading, the pair the stream half alone on 28 of 100 seed for seed, no
  assertion moved (D-086). And D-081's re-seed shape, run for the first time on a tree
  with this branch's stream fixes, has its `ServeBeforeRefusedMark` plant run past the
  runaway cap on seed 7 (401 151 records against 350 130 on `main`, the re-seeded node
  now serving), which hid clause (c)'s evidence behind the cap: (c) is asked before the
  cap, by D-081's own reasoning for asking it before (a); the correct shape is green on
  all 200 of its cap (D-086). **The first nightly on the merged tip (run 36049438254)
  was red on one seed of ten thousand, 3164, on both shards that run it** — and the
  mechanism is the per-key liveness reading, not the node: the key's only post-heal
  write was a CAS issued 47 ms before the run's end and proposed by a live leader, which
  `writes_after_heal_by_key` read as "none completed"; `677cad3` fails it identically,
  masked there by the tripwire that panicked first. The reading now leaves out a write
  pending at the run's end inside the bound, as its own comment claimed (D-086); every
  figure re-measured, `SharedSnapshotDir` 20 → 17 of 100 and the pair 28 → 25. The
  same run held the read bound at ten thousand — worst 19 against 38, the tier's own
  measurement — and counted the one refusal (seed 6695, a torn record) with D-077's
  fan-out holding. The nightly is re-dispatched on the fixed tip and the merge follows
  its verdict.

- Branch `phase-3-stream-arms-aimed`, the owner's second ruling applied
  (2026-09-24): **the timer check gains a fourth arm for the node's live install**
  (PROPOSED D-091), and **a catch on the node is attributed to the variant's own
  violation**. PROPOSED D-089's per-range aim reached a situation nothing before it did
  and the correct node tripped the timer bound in it on seeds 272 and 516; the owner
  ruled for the arm rather than a widening. An install into a live store keeps its
  incarnation (D-042, D-066), so D-063's arm — written for a run-loop incarnation that
  ends at the completion — does not apply. What stops the node's core is the **hold**:
  the node holds the one range it is installing, from the repair's capture to the
  manifest switch, and a held core takes no tick and has no timer to fire (D-066, which
  said this re-keying would be owed). The arm is fenced at both ends by named events —
  it **opens** on the install's decision, carried by the completion record and read at
  its decision time, so the exemption is never wider than the hold, and **closes** on
  the restored replica's restatement, with a crash's `RaftTerm` and a `RangeRemoved` as
  the two closes every stretch already has. It does not exempt the stretch before the
  hold, the node's other ranges, a take, a completed install with no read-back (D-063's
  staged one), or anything after the restatement; each is asserted on hand-built records
  in `sim/raft.rs`. **The correct node is green on all thousand seeds** and the two are
  pinned with their mechanism both ways
  (`seeds_272_and_516_are_a_live_installs_hold_and_the_fourth_arm_answers_for_them`).
  The second half of the ruling: every `caught`-style assertion in `sim/tests/node.rs`
  now names the check it expects (RAFT.md §5's `What catches it`), so a catch by an
  unrelated violation **fails rather than passes**, and an absence test reports a run
  that failed some other check as the node's own failure and not as the variant's catch
  — which is what one timer gap in the correct node's run was being read as, on three
  different variants. The three rates the ruling names were re-measured over the share of a
  thousand seeds their assertions use, before and after: `IgnoreIncarnation` **2 → 0**,
  `RefusalNotDurable` **2 → 0** and `AdoptionAsBuilt` **1 → 0** violations, and **none of
  the five was ever by a check the variant breaks** — the before column was three false
  catches of one bound. The arm takes nothing from the check: over the correct node's
  thousand it adds no gap and removes exactly those two, and `ResetTimerOnAnyRpc` — the
  variant the timer check is written for — is still caught on **50 of 100**, all fifty by
  the timer check, with no gap added or removed. Every one of 11 949 holds the arm opened
  over that thousand closed, 11 946 on the restatement at its own switch. Five mutations
  planted, five caught. Nothing is widened: `TIMER_TIMEOUTS`, the bound and
  every trace and schedule are untouched.

- Branch `phase-3-stream-arms-aimed`, stacked on `phase-3-stage-b-stream-variants`
  (2026-09-23): **the two stream arms aim their victim at a range it lags**
  (PROPOSED D-089), which is the owner's ruling on what D-086 took to them.
  `Fault::CrashInstalling` and `Fault::RetakeUnderStream` drew a victim from one stream
  and a range from another, so on a node the arm reached its situation only where the
  two draws coincided: the install crash reached the final chunk of the range it drew on
  **0 of 100** seeds. The range is resolved against the trace now, after the isolation
  and the heal, and lands on one the victim is behind that range's leader's compacted
  prefix of, preferring the drawn range wherever it qualifies. The arm fires on **14 of
  100** and **122 of 1 000**, so `SnapshotWithoutCurrentLast`'s injection is asserted
  from a **hundred seeds** instead of the nightly's ten thousand — the hole the ruling
  was about. Its **catch is still 0 of 1 000**, now over 122 firings rather than one,
  and is still asserted nowhere: the arm is no longer the reason, and that goes to the
  owner. `SharedSnapshotDir`'s four rates were re-measured and three did not move (the
  fault 100/100, the arm 1/100, the catch 26/100; the scramble 29 → 30 of 100), and no
  tier moves with this. The correct node's sweep floors the install arm's firing and
  asserts each arm's aims cover more than one range, which is what a constant aim is
  caught by. `Cluster::OneGroup` is byte-identical to the base — seed 42's JSONL hashes
  to `05a18a8e6f57159a703bdc0c8b37a9e52f9d083f154760abd24a53bfa6f5ea86` on both, which
  is **not** the hash D-086 records: the merge with `phase-3-stage-b-wiring` moved it by
  145 bytes before this slice, and D-086 is corrected rather than rewritten.
  **The aim reaches a situation nothing before it did, and the correct node trips the
  timer bound in it on 2 of the first 1 000 seeds** — 272 and 516, where the base is green
  on all thousand. A replica being fed a snapshot of one range completes a **live**
  install inside the window with no chunk of that range delivered in it, and none of the
  timer check's three reset arms covers a node whose install keeps its incarnation
  (D-042, D-066), so it neither hears a leader nor campaigns. No safety fold fails on
  either seed. Nothing is widened: both are pinned with their mechanism, and
  whether the check owes a fourth arm is the owner's ruling. **The gate is green and the
  thousand-seed tier is red on those two seeds until it is made.** *The owner made it,
  and the bullet above is the answer: PROPOSED D-091 on this branch, whose pin replaces
  this one's — the two seeds are green for the reason the ruling gives.*

- Branch `phase-3-stream-restarts-cap-wait` (2026-09-23), PROPOSED D-090, stacked on
  PR #107: the node gets RAFT.md:210-212's restart bound — `Outbound`'s `Counted`
  carrying the two counters both bounds are made of, `STREAM_RESTARTS = 2`, the third ask
  counting the checkpoint unusable — and the answer that keeps it from counting the wrong
  thing, `SnapshotStatus::Waiting` for a cap-wait.
  **Two bounds were measured and one of them is a finding, not a fix.** `CHUNK_RESENDS`
  was going to be corrected 4 to 8, the number RAFT.md:210 states and the node's own
  comment claimed — and the correct node trips it at *both* numbers, 3 781 give-ups at 4
  and 2 112 at 8 over 32 seeds, because the receiver answers nothing while an install
  decision is pending and the sender counts that silence as a lost chunk. Left at 4 and
  filed as **issue #120** rather than widened (D-030, D-039). And on the cap-wait's own
  bound: bounding a cap-wait by `CHUNK_RESENDS` ran to ten consecutive waits on one
  stream with four ranges over a cap of two, so a cap-wait is now counted against no
  give-up bound at all — a slot is granted to a stream *that is asking* (RAFT.md:218-220),
  so a bound on the asking is a bound on the mechanism. Measured at 100 seeds in release
  with the cap at two over four ranges: 800 cap-waits on the correct node against 2 151 as
  it stood, and a worst single stream, receiver-side, of six asks in a row against
  forty-four; 800 of 800 installs and 100 of 100 green in all three configurations. The
  pair is deterministic in `ananke_shard::install` — `CapWaitIsAStartOver`, which a node of
  one range cannot be wrong about, and `RestartsNotCounted` — and the review of this branch
  added a third check beside them, for what the node *records* rather than what it decides:
  the resend counter a cap-wait must clear and the restart counter a start-over must
  increment were both deletable with the whole suite green, and are not now.

- Branch `phase-3-lease-tier-nightly` (2026-09-23), PROPOSED D-088, off `origin/main` at
  `03e1829`: the owner's ruling on `LeaseTrustsTheClock`'s tier on the node. Its catch —
  a stale read found by linearizability — asserts from the **nightly's ten thousand**
  instead of the thousand-seed tier, because the node's own rate is **78 of 10 000
  (0.78 %)** and **6 of 1 000 (0.60 %)**, re-measured on this tree, against the one-group
  server's 4.0 %. At 0.78 % a thousand seeds catch none with probability 4.0e-4 and ten
  thousand with 9.8e-35, where the one-group assertion this tier was copied from sits at
  1.9e-18. One comparison changes, `if seeds >= 1000` to `if seeds >= 10_000`; the rate
  keeps printing at every tier and the one-group assertion is untouched. The assertion was
  *proved to fire*: with the sweep stubbed to no seeds it fails at 10 000 and passes at
  1 000, and the same stub with the gate at 100 000 passes at 10 000 — the stream-variants
  slice's bug, reproduced deliberately so this slice could be shown not to have it.

- Branch `phase-3-d049-refused-mark-key` (2026-09-23), PROPOSED D-087, on the owner's
  ruling on issue **#116**: D-049's rule is keyed on **a refused mark the answer carries**
  rather than on a rejection stamped incarnation 0. The incarnation says *which* store
  answered (D-042); whether that store holds anything of the log is a different question,
  and the old key answered it by a coincidence the one-group server has and D-077's node
  does not. `AppendEntriesResponse` gains a `refused` bit, carried in the byte `success`
  already occupied so no frame changed length and no schedule could move; the one-group
  re-seed loop sets it literally and the core sets it from `Raft::refused()`, *quarantined
  and holding nothing of this log yet*. **The one-group server did not move**: all four
  D-049 tests are figure for figure identical at 20, 100 and 1 000 seeds, including the
  sweep's-disk control's failing-seed lists. **The node has the rule's site now**: 282
  marked rejections over 1 000 seeds where the old key saw 0, the 6 679 D-085 counted
  splitting exactly into 6 397 unmarked and 282 marked, 0 answers from no store, and a
  step-down naming a follower `uncounted` where D-085 measured none in 1 394. The pair,
  `RefusedCountsForQuorum` and `RefusedNeverCounts`, is **still caught 0 of 1 000 there**,
  now for one reason and not two: nothing on the node compacts, so a re-seeded replica
  answers a marked rejection and then a success inside the same window. §10's exit
  criterion for the pair on a sharded `sim/quorum.rs` is still owed, and its one remaining
  blocker is PR #107's wiring. **After review**, two mutations this slice had not
  anticipated are closed: the mark computed once per node and stamped on its other three
  replicas — caught 20 of 20 and 1 000 of 1 000 by a new per-range clause, the dual of the
  one that was already there — and `Raft::refused()` with its `quarantined` conjunct
  dropped, caught by a new gate-tier test that reads the predicate itself, which no test in
  the tree did.

- Branch `phase-3-stage-b-quorum` (2026-09-23), PR #104, PROPOSED D-085: `origin/main`
  merged in (carrying PR #86's whole-node refusal and re-seed, D-077), and the sharded
  `sim/quorum.rs` built on the node — `quorum::node_run`: three nodes of four ranges, one
  `mark_store_lost` refusing the node and all four replicas it holds, the third node cut
  off, and check quorum asked of each range's own leader about that one node. The correct
  system passes every seed at every tier (0 of 1 000 in release);
  `NodeVariant::RefuseOneRangeOnly` is caught 1 000 of 1 000. **The finding**: D-049's own
  pair, `RefusedCountsForQuorum` and `RefusedNeverCounts`, still has **no site** on the
  node and PR #107 will not give it one. The rule is keyed on a rejection stamped
  incarnation 0 — a server with *no store* — and D-077's node rebuilds every range's store
  in a fresh engine before it serves, so it never sends one: 0 such rejections over 1 000
  seeds, `uncounted` empty in all 1 394 step-downs, both variants caught 0 of 1 000. The
  hazard D-049 fixed therefore returns on the node the day a leader can compact past a
  re-seeded replica; that goes to the owner beside issue #103. Stage B's first exit
  criterion for `sim/quorum.rs` is met for the scenario and still owed for §10's pair.
  (Issue #116 was filed on this finding and the owner ruled: the key changed rather than
  the re-seed. PROPOSED D-087, above, is that change and its re-measurement.)

- Branch `phase-3-stage-b-stream-variants`, stacked on the snapshot wiring
  (2026-09-22): **the node reaches the stream path**, and Phase 2's four stream
  variants are measured on it (PROPOSED D-086). The node's `snapshot_threshold` drops
  from `1 << 30` to the one-group sweep's 12 and `Fault::CrashInstalling` and
  `Fault::RetakeUnderStream` come back to `Schedule::draw_on_the_node`, so
  `sim/tests/node.rs` asserts the snapshot path **reached** on every seed where D-082
  asserted it absent. Reaching it found **five node bugs, all fixed here and none of
  them a bound widened**: the applied watermark left at zero at every start
  (`Cores::insert`), a live install leaving the `apply` task's applied state behind and
  the store's own caches stale (`RaftStore::restate_after_install`), and
  `SnapshotAction::Record` — D-078's follower compaction — routed to the `snapshot`
  task, which drops it, so a replica that asked once never asked for another snapshot
  again and wedged its range the moment it led. That last one tripped the **liveness
  bound on the correct node** on 3 of the first 100 seeds, and fixing it uncovered a
  fifth: a stream opened only on a snapshot record whose index matched the ask exactly,
  and the `apply` task rewrites that record on every take, so an install could lose the
  race forever — `sim/install.rs`'s seed 7, at any run length. A sixth fault was in this
  slice's own variant translation and the review found it: the node's take did not empty
  its version directory, so `SharedSnapshotDir`'s re-take *failed* instead of rewriting
  and every fold of its symptom read zero. With it fixed the variant's fault fires on every seed and its
  wedge is caught by the liveness check on 30 of 100 — 26 of 100 after the merge below,
  where one group's is 4 of 10 000
  — so it is asserted at Phase 2's own tiers and no stronger. With the five fixed the
  correct node passes every seed at the gate's twenty and CI's hundred, over 5 912
  snapshot actions at a hundred seeds (59.1 a seed, fewest on any one seed 30; 5 849,
  58.5 and 27 after the merge below). `Cluster::OneGroup` is untouched: seed 42's JSONL still hashes to
  `445f970010f9d493d489d7627543b77cccba863cb5675af4602686f5de182217`.
  **The variant tiers go to the owner**: `SnapshotWithoutCurrentLast` is caught on 0/100
  and 0/1 000 with its arm firing on 0/100 and 1/1 000, so its catch is not re-asserted
  and its one arm assertion moves to ten thousand — **nothing below the nightly asserts
  that variant**; `SharedSnapshotDir`'s
  fault fires on every seed and is asserted at every tier, its liveness catch (30/100,
  26/100 after the merge below) sits at Phase 2's own ten thousand, and two assertions
  move up on this cluster's measured rates — the scramble (17/100, 29/100 after the
  merge, at which Phase 2's hundred-seed tier would hold: to the owner) to a thousand
  and the aimed arm (1/100, 21 of 1 000) to ten thousand; and
  `IgnoreIncarnation` and the pair are blocked on PR #86's whole-node refusal, since a
  live install deliberately keeps a store's incarnation and only a re-seed changes one.

- Merge update (2026-09-23): branch `phase-3-stage-b-stream-variants` merged its base
  `phase-3-stage-b-wiring` to resolve PR #109's conflicts, carrying in D-077's
  whole-node re-seed, D-080's linearizability change, D-082's verification pass and
  D-083's adversarial-review fixes. The one code conflict is `Task::open`: the base made
  it require a **complete** checkpoint and made the node's take write D-060's checkpoint
  record; this branch rewrote the same lookup as `ananke_shard::snapshot::find_version`,
  keyed by range and by the index the core asked for. The merged code keeps
  `find_version` and D-083's completeness holds inside it — it is checked on every
  candidate, not on the record's directory alone — so an incomplete version is skipped
  for a complete take of the same index and answered with `retake` when there is none.
  Four of D-082's six `*_is_not_re_asserted_on_the_node_yet` tests are superseded by
  this branch's four real assertions, as D-082 said they should be, and their shard rows
  go with them; `AdoptionAsBuilt` and `RefusalNotDurable` keep theirs. The four
  `SharedSnapshotDir` rates were re-run on the merged tree at the share of 100: fault
  100/100 and arm 1/100 unmoved, scramble 17 → 29/100, catch 30 → 26/100. The correct
  node's own asks moved with them — 5 849 over 100 seeds against 5 912, 58.5 a seed
  against 59.1 — and the take-counter property it stands on is unmoved at 0 of 100.

- Branch `phase-3-stage-b-membership` (2026-09-22), stacked on
  `phase-3-stage-b-sweeps` (PR #101, PROPOSED D-082): **`sim/membership.rs` on the
  node** (PROPOSED D-084), the second of Stage B's first exit criterion's three
  scenarios. The scenario is one body of code driven against either of D-082's two
  clusters: five one-group servers as before, byte for byte — seed 7's JSONL hashes to
  `24358ff2…` on this branch and on the base — or five nodes of **four ranges each**,
  3 → 5 → 3 on **every** range. The half of issue #46's extension four ranges make
  possible is built and asserted on every seed: **a change of a range while another
  range on the same node is changing**. The half they cannot reach — a joining server
  fed by a snapshot in its learner phase — stays asserted on the one-group server and
  is asserted *absent with its reason* on the node, whose `snapshot` task is not wired
  (that slice follows D-082's). `SingleMajorityInJointConsensus` is re-asserted on the
  node at 14 of 100 seeds against the one-group server's 19, above D-061's five per
  cent, so its tier does not move. Liveness and availability became **per range** on
  measured bounds (a range's gap 1.33 s at 1 000 seeds against 5 s; its first write
  after a heal 1.81 s against 6 s). Eleven mutations were planted one at a time and ten
  caught, six of them only because a guard was added for them; the review's round found
  three checks asserting less than their names claimed, and the fixes are a **witness**
  on the overlap fold (the answer checked against the trace read backwards — 0 of 100
  unwitnessed correct, 38 of 100 for a fold that never clears its state), the side the
  partition actually cut, and the ranges the transfer was asked for. **Two findings**,
  both from the nightly's shard 3, red on 33 of 10 000 seeds, each seed re-run and
  classified: 30 are **issue #81** — a voter removed and re-added inside one leader's
  term, whose fix is PR #89 — and **3, seeds 6097, 7759 and 7887, were this slice's own
  per-seed overlap clause**, tripped by **the scenario's own parameter and not by the
  node**. `RANGE_STAGGER_MAX_MS` was 25 ms, the top of the range its own doc comment
  names as the one that serialises the four changes. **The parameter was fixed, not the
  clause** (D-030, D-039), and the value chosen on a ten-candidate measurement at 1 000
  seeds: 2 ms, one of only two caps leaving no seed one step from failure, against
  16.6 % of seeds at 25 ms. At 2 ms the overlap is witnessed on 1 000 of 1 000 and the
  gate and CI are green; the ten-thousand tier is re-run on the fixed tip.


- Merge update (2026-09-22): branch `phase-3-stage-b-compaction` merged `origin/main`
  to resolve PR conflicts, carrying in PROPOSED D-073 and D-074 from main and keeping
  this branch's PROPOSED D-078.

- Phase 3, Stage B in progress (2026-09-21). Merged: PR #71 the trace of §8 (D-069),
  PR #75 the checks keyed by range (D-071), PR #76 the node's wire (D-072, crate
  `ananke-shard`: `RangeId`, range-tagged batch frames, the per-peer outbox keyed by
  range, the byte-bounded inbox). Branch `phase-3-stage-b-tasks`: the node's **tasks and
  Q41's round** (PROPOSED D-073) — `ananke_shard::round`, the discipline alone, and
  `ananke_shard::node`, one `raft` task holding every core keyed by range on one ticker
  and one `apply` task over every range's jobs one at a time, with five node variants
  beside the correct round (`ananke_shard::variant`). Measured: a core's idle step costs
  **32 ns** against a pass bound of 20 µs (Apple M2, 8 cores, load 11, release, host
  time, `cargo run --release -p ananke-shard --example step-cost`), so one `raft` task
  holds 1 000 ranges' idle ticks in 0.2 % of a 10 ms tick; the replay burst after a slow
  persist is under 1 % of a tick at every disk latency the tree models and first fills a
  tick at a sync of about 10.4 s; a round with `p` persisting cores costs up to `1 + p`
  frames per peer, which is the one figure D-073 puts to the owner.
  Branch `phase-3-stage-b-ranges`, stacked on the snapshot slice: **four ranges on every
  node** (PROPOSED D-076). `ananke_shard::server::run` is the node as a running server —
  one engine, one socket, one inbox, one `raft` task on one ticker, one `apply` task, and
  a Raft store and a core per range, with the ranges fixed at bootstrap from
  configuration and each traced `RangeCreated { cause: bootstrap }`, each core seeded
  from `n{id}/r{range}/protocol` (D-057's first caller), and a range on every client
  message. `sim/ranges.rs` runs three nodes of four ranges under crashes and isolations
  whose range each arm draws from its own stream, and its trace is put through the checks
  D-071 keyed: the first trace in the tree with more than one range to be wrong about.
  Two checks were wrong and are fixed here, both caught by that sweep on its first run:
  `match_starts_are_first_rises` was keyed by the node, and the timer check's replay read
  a delivered frame as one group's rather than as the batch frame a node sends. The
  single-group server in `ananke-raft` is untouched and `sim/raft.rs`'s arms,
  `sim/membership.rs` and `sim/quorum.rs` still run on it: moving them onto the node
  needs the snapshot, re-seed and compaction paths the other Stage B slices build, so
  Stage B's first exit criterion is owed.
- Phase: 2 CODE-COMPLETE, not tagged. Overnight (2026-09-09), branch
  `phase-2-overnight`, merged to main as PR #24: stages D and E of RAFT.md's order
  and issue #22 landed (D-029, D-030, D-031), every commit gated, the correct server
  green on 100 release seeds of both sweeps with all nine sweep-tested variants
  caught.
  D: joint consensus with learners first, the configuration in force from the log
  with the `0/2/config` key, one change in flight, the 3→5→3-under-partition
  scenario (worst availability gap 469 ms against the 2 s bound at 1000 seeds;
  549.36 ms at 10 000, as SPEC §3 now states the criterion),
  `SingleMajorityInJointConsensus` caught 28/100. E: snapshots as resumable chunked
  streams of `Engine::checkpoint`, the compacted log in the core, the staged
  install committing by CURRENT-last, LostState-refused servers re-seeded under a
  durable vote quarantine (D-035), `SnapshotWithoutCurrentLast` caught
  26/100. #22: the Figure 8 driver, `CountOlderTermForCommit` caught 42/100 with
  `max_batch` at its default (was 0/100 batched). Local 10k runs on 1373601 and
  f54b468 found two correct-server false positives of the timer check against
  snapshot-fed followers (seeds 164 and 385, both pinned; D-030 and D-039). The
  sweeps now run their seeds in parallel and in four tiers, 20 / 100 / 1000
  (`scripts/premerge.sh`) / 10 000 on GitHub only (D-040). `docs/OVERNIGHT.md` is
  the session's full record.
  Before Phase 4: issue #21, client sessions.
- Nightly follow-up (2026-09-11), merged to main as PR #27: the ten-thousand-seed
  nightly's three failing seeds were fixed and pinned in the gate — 6325 by D-041
  (the crash-safe adoption and the store marker), 5909 by D-043 (versioned snapshot
  takes, pinned streams, a stream per designated follower), 7381 by a checker fix
  (an installed snapshot sets the floor exactly, D-030) — with D-042 (store
  incarnations) beside them. Then on main: D-044, a refusal is durable and a refused
  engine does no work (PR #28), and D-045, a variant is a set, with D-046, the
  sweep's safety re-check keeps its state (PR #29).
- Pinned-seed audit (2026-09-12), PR #30, branch `phase-2-pinned-audit` off main at
  `14c3e17`: CLAUDE.md now requires a pinned seed to assert its
  mechanism, never just green. Every pinned seed (164, 385, 680, 687, 5909, 6325,
  7381) asserts it through trace predicates on `sim::raft::Report`; 680 and the
  D-042 reset on 5909 reach their situation today, and the rest assert its
  absence and say why. Seed 5909's wedge was measured to be D-043's alone, with
  no stale `matched`; D-039, D-042, D-043 and D-045 carry the factual
  corrections.
- Decision and durability times (2026-09-13), PR #31, branch `phase-2-d047` stacked
  on `phase-2-pinned-audit`: D-047. Every trace record carries the
  time its step was decided beside the time it was traced (`TraceRecord::decided`,
  `Environment::decision` / `trace_decided`, `decidedNs` in a moirae `log` line's
  data), and the pre-vote and timer checks read decision time. The nightly
  34711427220's correct-server failures on seeds 1885 and 2023, and its eleven
  variant catches of the same trace-timestamp gap, are pinned for that reason;
  seeds 164, 385 and 7381 were rule gaps this does not bear on. No schedule moved
  (the echo golden hash is unchanged and the raft traces differ only by the new
  field), and every 100-seed rate is unchanged. At ten thousand seeds (runs
  34731272921 and 34749071877) every test passed and reading decision time removed
  28 catches and added none: 27 pre-vote straddles and one timer catch whose granted
  vote was traced past the bound, each named in D-047.
- Decided (2026-09-13), branch `phase-2-decided` at the tip of `phase-2-d047`, so
  carrying the commits of PRs #30 and #31: the owner approved every PROPOSED entry
  — D-032, D-033, D-035 to D-039 and D-041 to D-047; D-034 was never used — some
  with amendments, and each is now a decided entry, its code markers reading
  `D-0xx`. D-030's account of seed 164 is superseded by D-048, not edited: a local
  run, the follower about sixty-eight entries behind, no leader in the stretch the
  check flagged. RAFT.md §3 now says what a refused server does: it binds its
  socket and rejects every AppendEntries, grants and serves nothing, and takes only
  its own re-seed (D-030, D-037, D-042). D-047 keeps its two limits of evidence and
  its three open points stated, citing issues #33 and #32. The devlog,
  `docs/devlog/02-phase-2.md`, is rewritten from its draft. Not merged.
- Stage A, B, C record: A: the
  pure core, the codec with the studio decoder, the state under tenant 0 with the
  applied index in the batch and a refusal of any recovery that lost state, the four
  log invariants as folds, the paper's Figures 7 and 8 and moirae's rules as tests
  (D-025). B: the server as `raft`, `net` and `apply` tasks in `ananke-raft`'s
  `node.rs`, a single-shard key-value store with gets through the log, the
  linearizability checker in `sim/lin.rs`, and the sweep in `sim/raft.rs` under
  drops, duplicates, reordering, skew and drift, partitions, one-way blocks and
  crashes with the disk model, checking the log invariants, three rule folds,
  linearizability, pre-vote's property and two liveness bounds on uniform seeds; the
  correct server passes every seed and each of the six stage-B variants is caught
  (D-026). No reads by read-index, no leases, no membership changes, no snapshots.
- Phase 1 record: done, tagged v0.2.0 (2026-09-05). The WAL (D-018, D-019), the
  memtable and engine (D-020, D-021), SSTables with the manifest and log truncation
  (D-022), versions, snapshots, `scan` and leveled compaction (D-023), write batches,
  writes without a sync and checkpoints (D-024) are done. A store is refused rather
  than opened as a state that never existed: a missing log head and an unreadable
  `CURRENT` or manifest fail `open` unless the caller allows the discard or a fallback
  onto an intact manifest (D-022). `sim/wal.rs` and `sim/engine.rs` run the §2.8
  crash property with every §1.3 fault on, filesystem latency, crashes between polls,
  batches, unsynced writes, scans at snapshots, checkpoints opened fresh after the
  crash, and compaction under crashes; the correct code passes every seed, each of
  the three known-buggy engines and three known-buggy logs is caught. Sweeps run
  `ANANKE_SEEDS` seeds: 20 at the gate, 100 in CI, 10 000 nightly, plus a nightly
  deep-levels run of `ANANKE_DEEP_SEEDS`.
- Phase 1 exit criteria (SPEC §2.8), with evidence:
  1. Property test, random ops and random crash points, recovered state equals the
     model: `sim/engine.rs` and `sim/wal.rs`, run by `sim/tests/engine.rs` and
     `sim/tests/wal.rs`; the model is `Model::state_after`, a `BTreeMap` fold over
     the writes the faults left; three buggy engine variants and three buggy log
     variants are caught alongside. Met.
  2. 10k seeds green in CI nightly: `.github/workflows/nightly.yml`, every sweep in
     release at `ANANKE_SEEDS=10000` and the deep-levels run at 1000; run
     33986588539 on commit `85b78df`, green in 33 minutes. Met.
  3. Bench over 200k writes/s single-threaded on the real environment, a sanity
     number: `cargo run --release -p ananke-storage --example bench`, 468 writes/s
     with a sync per write, 33 523 without, 299 169 in batches of a hundred without
     (one laptop disk, 2026-09-05). Met in the shape the flag exists for.
- Last tag: v0.2.0. `ananke`, `ananke-env` and `ananke-storage` 0.2.0 on crates.io.
  Devlog: `docs/devlog/01-phase-1.md`.
- Merged (2026-09-13): PRs #30, #31 and #34; `main` at cd411b4 has dc603ea's tree,
  green at ten thousand seeds on `dc603ea` (run 34769934684). SPEC §3's membership
  criterion is now worded as the ten-timeout bound the check asserts, with the
  measured worst gap of 549.359683 ms.
- Released (2026-09-14): Phase 2 tagged `v0.3.0` on the merge of the release
  commit, with `ananke`, `ananke-env`, `ananke-storage` and `ananke-raft` at 0.3.0
  (`ananke-raft`'s first version), published by the owner. Evidence: ten thousand
  seeds green on `cd411b4` (run 34839613587) and on `a8656e8` with D-049 (run
  34852980174), the tree of `main` at 94c6a54; D-049's ten-thousand-seed record and
  the version bump follow it.
- Phase 2 backlog (2026-09-14/15), branch `phase-2-backlog` off `main` at 94c6a54, not
  merged, every commit gated, three PROPOSED entries (D-050, D-051, D-052) awaiting the owner. Issue #32:
  D-050, a term record carries when the message its step took was received
  (`receivedNs`), and the pre-vote check excuses a change taken from a message received
  before the isolation; a directed scenario (`Fault::IsolateOnTermRaise`) reaches the
  shape on 299 of 1 000 seeds, seed 4 pinned, `NoPreVote` caught on every seed; no
  schedule moved; the receipts correct D-047's causes for 3863 and 1252. Issue #33:
  D-051, a removed catch is asserted against the isolation or the flag it names, at
  every tier, and the nightlies' 28 removed catches are run through it. Issue #37 was
  not implemented: both candidate fixes are larger than the guide allows. Answering
  during repair and adoption with the refused rejection counts for nothing under
  D-049's rule once the stream has ended (no acknowledgement moves it), and a leader
  that recorded the `Installed` answer's incarnation would reset the follower's
  progress on each incarnation-0 answer (D-042) and could re-designate and re-stream
  it; making those answers count needs a new status on the wire and a D-049 rule with
  its own bound. A faster adoption has to cut two phases at once, since the median
  `Installed` to `RaftAdopted` (59.0 ms) and `RaftAdopted` to `RaftReseeded` (65.2 ms)
  already exceed the 100 ms window together; the copy that dominates the first is what
  D-041's crash safety rests on (a crash re-runs the adoption on the same staged bytes),
  and the sweep of old versions in the second is what D-043 requires before any task
  runs. Performance: D-052. Measured first (`sample`); the per-run JSONL export was
  27.24% of the raft binary and is now written when asked for; the adoption-crash watch
  (79% of the path comparisons) reads a durable-namespace version first; the pre-vote
  check reads one pass. `scripts/premerge.sh` at 1 000 seeds, warm build: 547.55 s on
  dbaec73 (mean load 11.20), 449.89 s with the export lazy (load 11.81), 374.64 s on the
  final tree 1ef6d7e (load 13.90); the raft binary 353.17 s, 320.61 s, 287.46 s across
  the three changes. (A first before-figure of 573.15 s was taken at load 15.98 and is
  not used.) The allocator is spread over the simulation with no single
  caller to take out, and `leader_now` (4.54%) and the engine model (about 4% of the
  premerge) were left under the 5% bar. Review of PR #41 (0ed490a and the docs commit
  after it): D-051's timer assertion now reads the reason off both replays at the flag
  record (a reset, the flag record or the server's status moved), exact for any trace;
  D-047's straddle reads `decided <= from < at`; the directed term-raise sweeps go
  through `checked`; D-050's rule is restated for candidacies stepped from a
  PreVoteResponse or TimeoutNow; D-052's figures compare equal loads.
- Phase 3 design and Stage A (2026-09-15/16), branch `phase-3-stage-a` off `main` at
  1570206 with `main` merged back in at a250b42, not merged, every commit gated, the
  owner's review pending at Stage A's exit criteria. The design, SHARD.md, is approved
  (PR #48) with the stage plan of §12 and the answers of §13; PR #50 added every stage's
  green ten-thousand-seed nightly to its exit and Stage A's v0.3.0-store decision, and
  PR #51 the owner's answers to the six post-approval choices. Stage A's seven items:
  D-053, RAFT.md corrected where it described what the code lacks (Q1); D-054, the
  install of a span into a live engine with its crash test, which is Q2's criterion and
  is met in the simulator; D-055, the seek, the range delete and the span checkpoint,
  with the engine sweep over all four primitives; D-056, `SimEnv`'s bounded drop-oldest
  queue per (socket, destination), which moved every pinned hash and schedule and whose
  re-audit re-pinned the `{IgnoreIncarnation, SharedSnapshotDir}` pair from seed 680 on
  seed 132 (Q16); D-057, `Environment::range_rng`, a named stream per node and range,
  called by nothing until Stage B (Q13); D-058, `sim/membership.rs` past the snapshot
  threshold, which meets issue #46 for the snapshot feed and the configuration key and
  defers the truncation revert floor and the kept-tail repair to issue #56 (Q34);
  D-059, the owner's decision that a store in 0.3.0's format is refused at open, with
  the v0.3.0 tag's own store as a checked-in fixture; and D-060, the key layout of Q5 —
  a group's Raft state under `0 / <group: u64 BE> / <purpose> / name`, user data in
  tenant 2, the store parameterised by that prefix (Q40), and the format version in a
  `RAFT-FORMAT` record read before anything writes, the format checked before lost
  state, older and newer versions refused naming both. D-061 is the owner's rule of
  2026-09-15: a variant caught on under 5 % of seeds asserts its catch at the
  thousand-seed tier, never at the gate's twenty (CLAUDE.md), with the audit of every
  such assertion; it moved the lease's stale read, the WAL's betrayed cut, the
  membership scenario's election while joint, `RefusalNotDurable` (D-056) and
  `reverts_to_a_prefix` (D-058). The layout commit moved every Raft schedule and every
  pinned trace hash, and re-audited every pinned seed in one commit;
  `RefusalNotDurable`'s catch moved from seed 119, which now refuses nothing, to seed
  158. `scripts/premerge.sh` at a thousand seeds: 593.87 s at mean load 20.87, against
  D-055's 540.37 s and D-052's 374.64 s. Issues filed: #55, #56, #57 (the nightly's
  300-minute budget). A six-lens adversarial review with an independent skeptic per
  lens, including mutation testing of the new `ananke-raft` code, ran before the exit;
  its findings are fixed in 3927b56, 5698992, 5bd3574 and ae54a20.
- Phase 3 Stage B, the checks keyed by range (2026-09-20), branch
  `phase-3-stage-b-checks` off `main` at 0d76236, one of the stage's builds and not the
  stage: D-071 keys checks 1 to 4 by the group each event names, over a generic group
  key in `ananke-raft`; the history's closure by `(range, index, term)`; the timer check
  and pre-vote's property per (range, server); the checks about time per range and the
  write bound per key. It changes no behaviour, moves no pinned hash and no schedule.
  With one group a wrongly keyed check is invisible to every sweep, so each keyed check
  has a hand-made two-range case of its own and every wrong key was planted in a
  throwaway copy and shown to fail one: the table is in D-071.
  `scripts/premerge.sh` at a thousand seeds: green in 713.53 s on AC power at mean load
  29.64. The per-key write bound runs 214 ms under its 2 s on the worst key of the
  thousand seeds — a *tenth* of the bound, one of its ten election timeouts — where the
  minimum over every write ran 886 ms under it.
  An adversarial review re-ran the whole oracle and found one major gap and eight smaller
  points, fixed in fc21689 and 8ce19a3. The major one: the majority carve-out's case
  asserted the helper and not the key, so replacing both of the carve-out's *uses* with
  the cluster-wide reading passed every tier; the new case
  `a_live_ranges_gap_is_flagged_though_the_range_beside_it_has_no_majority` drives the
  consuming end and is the only case that fails under that mutation. The oracle is re-run
  whole and logged (23 rows, none green), `Report::ranges_of` loses a `RangeCreated` arm
  that could only misfire, check 4's mismatch message names the payload beside the term,
  and D-071 now says plainly that **nothing in the tree emits `RangeCreated` or
  `RangeRemoved`**, that both are trusted unconditionally until §8's check 7 exists, and
  that a refusal over-impairs by marking the node down for every range of the run — for
  the node's PR to close. The premerge at a thousand seeds on the fixed tree: green in
  625.01 s on AC power, with `slowest_write_after_heal` and every variant rate identical,
  so nothing moved.
- Phase 3 Stage B, first slice (2026-09-21), branch `phase-3-stage-b-wire`: the node's
  wire, as PROPOSED D-072. The crate `ananke-shard` exists (Q40) and holds four modules
  and nothing else: `RangeId`; the batch frame, which carries several messages each
  tagged with its 8-byte range id (Q10) and wraps `ananke-raft`'s codec byte for byte, at
  12 bytes a message and 5 a frame; the per-peer outbox, one frame a peer a flush cut
  under `MAX_FRAME_LEN`, with the overflowing message left to start the next flush's
  frame and a message larger than a frame refused at the push; and the node's inbox, one
  per node, bounded in the wire bytes each message occupied, admitting in constant time
  and refusing the arrival rather than dropping anything it has admitted. The studio
  decoder shows a frame of six messages of three ranges as six messages. `ananke-raft`
  does not depend on the new crate and still names no range. The admission examines 0
  queue entries at every length from 1 to 4 096, with room and full — the measurement
  Stage B asks for, counted rather than timed — against today's admission, which scans
  the queue. Ten planted bugs, ten caught, no survivors (the table is in D-072). Nothing
  in `sim/` changes and the single-group server is untouched. The premerge is still owed
  on AC power: the laptop was on battery throughout (D-070).
- The node's snapshot wiring is built (PROPOSED D-083): `ananke_shard::server::run`
  runs the `snapshot` task keyed by range and follower beside `net`, `answers` and
  `apply`. A take is the `apply` task's and checkpoints the range's own key intervals;
  a stream is a `Sender` per (range, follower) in frames of its own; a completed stream
  is D-066's live install of the range's two spans with its repair in one manifest
  switch, with the range held across the switch and its replica replaced by the one the
  switch built. `net` diverts `InstallSnapshot` before the inbox, which closes issue
  #96. `sim/tests/install.rs` is the directed evidence: five voters, four ranges, two
  late followers, eight streams and eight installs on every seed it runs.
- Next concrete task: re-asserting the four Phase 2 variants on the node
  (`SnapshotWithoutCurrentLast`, `IgnoreIncarnation`, `SharedSnapshotDir` and the pair),
  which the wiring above unblocks, and the directed re-seed shape (D-067). Issue #72 is
  now live rather than latent — the first live install on a node's engine makes the
  straddling read reachable (D-069, D-054) — and issue #103, which the wiring answers by
  routing a stream's answers by range, is recorded in D-083. Issue #37: a refused server's silence while it verifies,
  repairs and adopts its re-seed deposes the leader when the third server is away.
  Open follow-ups: issue #32 (the pre-vote check: a message delivered before an
  isolation but stepped inside it) and issue #33
  (assert the timer catches that decision time removes, not only print them).
  Still unfiled from OVERNIGHT.md's backlog candidates: membership changes and
  snapshots on one schedule, a studio metric for stream health, RAFT.md §5's
  Figure 8 driver touch-up, and an operator mechanism to retire a removed server.
  Issues #22 (D-031) and #25 (D-046) are closed with their resolving commits.
- Fault-model tests follow the CLAUDE.md pattern: a known-buggy variant the sweep
  must catch beside the correct one it must pass (`Journal::sync_dir_on_rotate`,
  `wal::Variant`).
- Phase 1 gate record: `FsFaults::p_bitrot` flips one bit per block per crash
  (`BlockRotted`); creating, removing and renaming a file is durable only after
  `sync_dir`, and a crash keeps a random prefix of each directory's pending operations
  (`DirectoryEntryLost`); `SimConfig::poll_budget` fails a run whose task is polled too
  often at one instant (`PollBudgetExceeded`, then a panic naming the task). The echo
  protocol keeps a checksummed journal (`ananke_server::echo::Journal`) that syncs
  every few records and rotates without `sync_dir`, and `sim/echo.rs` checks what the
  restarted node found against the trace. Pinned trace body hash: `fcbe82ee7a0ba672`
  (`19f19201df99a799` until D-056's send queue moved every schedule).
- Phase 0 record: `Environment` with `Clock`, `FileSystem`, `Network`, `Rng`; `RealEnv`
  on tokio; `sim::Sim` / `SimEnv` with the §1.3 torn-write and lost-fsync model and the
  §1.4 drop / delay / partition model; D-013 to D-017; the moirae bridge through
  `moirae-trace` and `moirae-sched` 0.0.1 (moirae ADR-009, format v2); `sim/echo.rs`
  with its pinned trace hash and the studio fixture in the moirae repo. Devlog:
  `docs/devlog/00-phase-0.md`.
