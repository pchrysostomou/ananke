# ananke — guide for agent sessions

ananke is a distributed SQL database in Rust, built from the ground up to be
deterministically testable under moirae. Read these three documents in full, in this
order, before doing anything:

1. [docs/BOOTSTRAP_PROMPT.md](docs/BOOTSTRAP_PROMPT.md) — *what* we are building and
   why: principles, phases, target repository layout, current status.
2. [docs/SPEC.md](docs/SPEC.md) — *how*. Phase sections are frozen once that phase is
   tagged; later changes need a DECISIONS.md entry.
3. [docs/DECISIONS.md](docs/DECISIONS.md) — *why this over that*. Never delete an
   entry; supersede it. The next free entry number is at the bottom.

Deferred ideas live in [docs/BACKLOG.md](docs/BACKLOG.md).

## Working agreements

- **Read SPEC.md and DECISIONS.md before writing code.** If a design question is not
  answered there, propose a DECISIONS.md entry *before* implementing and wait for
  approval.
- **Never widen scope inside a phase.** Anything tempting goes in `docs/BACKLOG.md`
  with one line of justification.
- **Determinism first.** No direct `std::time`, `std::fs`, `std::net`, `tokio::net`,
  `tokio::time`, `tokio::fs`, `rand`, `std::collections::HashMap`, or thread/task
  spawning outside `crates/ananke-env`. Time is `ananke_env::Instant` / `WallTime`
  (D-013); hash maps are `DetHashMap` / `DetHashSet` seeded from `Environment::rng()`
  (D-014). The banned paths are listed in `clippy.toml`; only `ananke-env`'s `real`
  module, plus the edge files `time.rs` and `collections.rs`, may carry
  `allow(clippy::disallowed_*)`. `scripts/check-direct-io.sh` is the textual second
  check and fails if any other file carries the allow. Both run in CI.
- **No `unsafe` outside `crates/ananke-storage`.** The workspace lint is
  `deny(unsafe_code)`; `ananke-storage` is the only crate permitted to
  `#![allow(unsafe_code)]`. Every `unsafe` block carries a `// SAFETY:` comment and
  gets a Miri run in CI.
- **The gate is one command.** `scripts/gate.sh` runs rustfmt, clippy, the direct-I/O
  check, `cargo doc` and every test in sequence under `set -euo pipefail`. No commit is
  made unless `scripts/gate.sh` has exited 0 on the exact tree being committed, run as
  that single command, never as separate shell lines whose failures can be missed. CI
  runs the same checks as parallel jobs.
- **Every state transition that matters emits a trace event.** If it can't be seen in
  the moirae studio, it didn't happen. A scenario's trace is `Sim::to_moirae` JSONL;
  CI pins its hash, and a deliberate change updates the constant in the same commit and
  says why (`sim/tests/echo.rs`).
- **Every fault-model test runs a known-buggy variant and a correct one.** The buggy
  variant must be seen to fail under the sweep and the correct one must pass under the
  same seeds. A sweep that only passes may not be injecting the fault; one that only
  fails may be failing correct code; the pair proves the fault model distinguishes a
  bug from correct code. The shape: a config flag or variant enum on the code under
  test (`Journal::sync_dir_on_rotate`, the WAL's variants), a scenario `Report::check`
  that expects different things of each, and a sweep test that asserts both. Ship the
  correct default.
- **Every published crate carries copies of `LICENSE-MIT` and `LICENSE-APACHE`** in
  its own directory (copies, not symlinks) so `cargo package` bundles them.
- **Prefer boring, well-documented Rust.** This is a project meant to be read.
- **When stuck on a distributed-systems question,** cite the paper (Raft, Percolator,
  Calvin, Spanner, FoundationDB testing talk) in the DECISIONS.md entry.
- **At the end of every session,** update the "Current status" section at the bottom
  of `docs/BOOTSTRAP_PROMPT.md`.

## Layout

```
crates/ananke/         Placeholder crate reserving the name on crates.io
crates/ananke-env/     Environment trait; real/ (RealEnv on tokio); sim/ (Sim + SimEnv); moirae.rs (trace export)
crates/ananke-server/  Node binary + library of the protocols it runs (echo for Phase 0)
crates/ananke-storage/ Storage engine: crc32c, wal, memtable, sst, manifest, engine (Phase 1)
sim/                   Simulation scenarios; scenario files sit directly in sim/ (echo.rs first)
docs/                  SPEC, DECISIONS, BACKLOG, BOOTSTRAP_PROMPT, devlog/
scripts/               gate.sh (run before every commit), check-direct-io.sh
clippy.toml            Banned I/O paths (disallowed-methods / disallowed-types)
.github/workflows/     CI: rustfmt, clippy + direct-io check, cargo doc, cargo test
```

## Verification commands

```
scripts/gate.sh          # the only command that precedes a commit
```

It runs, in order: `cargo fmt --all -- --check`, `cargo clippy --workspace
--all-targets --all-features -- -D warnings`, `scripts/check-direct-io.sh`, `cargo doc
--workspace --no-deps` with warnings as errors, `cargo test --workspace --all-targets`
and the doctests.
