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

- Phase: 0 (in progress)
- Last tag: none
- Done: steps 1–4. Workspace, CI (clippy `disallowed-methods` / `disallowed-types`
  from `clippy.toml`, `scripts/check-direct-io.sh`, GitHub Actions), D-013 clock
  types, D-014 seeded hash maps, D-015 message-oriented `Network` with non-blocking
  `send`; `Environment` trait with `Clock`, `FileSystem`, `Network`, `Rng`; `RealEnv`
  on tokio (positional fs, TCP transport with reconnect, OS entropy); `sim::Sim` /
  `SimEnv`: single-threaded deterministic executor with seeded random scheduling,
  virtual clock with per-node skew and drift, seeded per-node RNG, in-memory network
  with drop / delay (reorder) / partition, in-memory filesystem with torn writes and
  lost fsync applied at `Sim::crash`, trace to an in-memory `Vec<TraceRecord>` with a
  stable text dump. Determinism test: same seed, byte-identical trace.
- Step 5: `sim/echo.rs` runs 3 nodes under drops, delays, skew, a symmetric
  partition, a one-way block and a crash/restart; `sim/tests/echo.rs` asserts
  byte-identical trace text for equal seeds and runs 100 consecutive seeds against
  the scenario's invariants as a simulator smoke test.
- The echo protocol lives in `ananke_server::echo` and runs unchanged under the
  simulator and as `ananke-server echo --listen .. --peers ..` on `RealEnv`;
  `crates/ananke-server/tests/echo_cluster.rs` runs three real processes and checks
  the protocol invariants. `ananke_env::race` draws its poll order from the
  environment's RNG so no side can starve the other.
- Phase 0 exit criteria still open: the moirae bridge (trace opens in the studio).
- moirae bridge, design approved (three proposals, 2026-09-05): integer-nanosecond `t`
  with a header `unit` field in trace format v2; D-016 hybrid scheduling (uniform half
  the seeds, PCT the other half, fair coin, liveness only on uniform seeds) and D-017
  named RNG substreams with `Environment::sched_rng` and `race` taking the environment;
  crates `moirae-trace` and `moirae-sched` in the moirae repo. The moirae side is done
  on branch `rust-crates` (PR open): ADR-009, format v2, both crates with fixture and
  PCG32 parity tests, CI. Placeholders still need publishing to crates.io.
- Next concrete task: the ananke side of the bridge, after the moirae PR is reviewed:
  D-016 and D-017 entries, substreams, message ids, `Sim::restart` and partition/heal
  events, the bridge module writing JSONL through `moirae-trace`, the hash-pinned CI
  check, and the committed `echo-42.jsonl` fixture opening in the studio via a test.
  Node ids: pick 1-based ananke ids or one mapping function with a round-trip test, and
  say which in the bridge code. Verify writer is Phase 0; the Replay scheduler is v2.
