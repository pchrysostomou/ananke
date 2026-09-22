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
  term, whose fix is PR #89 — and **3, seeds 6097, 7759 and 7887, are this slice's own
  per-seed overlap clause**, which the correct system therefore trips. Its cause is
  this slice's `RANGE_STAGGER_MAX_MS` at 25 ms, the top of the range its own doc
  comment names as the one that serialises the four changes; with the stagger at zero
  the overlap is 100 of 100. **Not widened**: it goes to the owner with two candidate
  fixes (the parameter, or D-058's tiering) and the clause left failing. The gate and
  CI tiers are green.
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
- Next concrete task: Phase 3 Stage B's second slice, the node's tasks — the `raft` task
  over many cores on one ticker in Q41's round, the `apply` task, the snapshot task, and
  the scenarios' four ranges a node — on top of the wire above. Issue #72 is open on the
  first live install (D-069). Issue #37: a refused server's silence while it verifies,
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
