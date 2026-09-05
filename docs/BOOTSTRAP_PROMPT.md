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

- Phase: 1 in progress (2026-09-05). The gate is closed; the WAL (D-018, D-019) and
  the memtable with the engine in front of the log (D-020) are done. `sim/wal.rs` and
  `sim/engine.rs` run the §2.8 crash property with every §1.3 fault on and crashes
  between polls; the correct code passes every seed, each known-buggy variant is
  caught. Sweeps run `ANANKE_SEEDS` seeds: 20 at the gate, 100 in CI, 10 000 nightly.
  SSTables are not started.
- Last tag: v0.1.0
- Next concrete task: the SSTable (SPEC §2.4) as the engine's flush sink, replacing
  `Retain`, then the manifest and compaction. Stop for review after each.
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
  restarted node found against the trace. Pinned trace body hash: `19f19201df99a799`.
- Phase 0 record: `Environment` with `Clock`, `FileSystem`, `Network`, `Rng`; `RealEnv`
  on tokio; `sim::Sim` / `SimEnv` with the §1.3 torn-write and lost-fsync model and the
  §1.4 drop / delay / partition model; D-013 to D-017; the moirae bridge through
  `moirae-trace` and `moirae-sched` 0.0.1 (moirae ADR-009, format v2); `sim/echo.rs`
  with its pinned trace hash and the studio fixture in the moirae repo. Devlog:
  `docs/devlog/00-phase-0.md`.
