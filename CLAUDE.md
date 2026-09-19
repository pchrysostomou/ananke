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

Deferred ideas are GitHub issues labelled by phase; [docs/BACKLOG.md](docs/BACKLOG.md) says how.

## Working agreements

- **Read SPEC.md and DECISIONS.md before writing code.** If a design question is not
  answered there, propose a DECISIONS.md entry *before* implementing and wait for
  approval.
- **Never widen scope inside a phase.** Anything tempting becomes a GitHub issue with
  its phase label and one line of justification (`docs/BACKLOG.md`).
- **Determinism first.** No direct `std::time`, `std::fs`, `std::net`, `tokio::net`,
  `tokio::time`, `tokio::fs`, `rand`, `std::collections::HashMap`, or thread/task
  spawning outside `crates/ananke-env`. Time is `ananke_env::Instant` / `WallTime`
  (D-013); hash maps are `DetHashMap` / `DetHashSet` seeded from `Environment::rng()`
  (D-014). The banned paths are listed in `clippy.toml`; only `ananke-env`'s `real`
  module, plus the edge files `time.rs` and `collections.rs`, may carry
  `allow(clippy::disallowed_*)`. `scripts/check-direct-io.sh` is the textual second
  check and fails if any other file carries the allow; it also confines `rayon`, the
  sweep driver's thread pool, to `sim/parallel.rs` (D-040). Both run in CI.
- **No `unsafe` outside `crates/ananke-storage`.** The workspace lint is
  `deny(unsafe_code)`; `ananke-storage` is the only crate permitted to
  `#![allow(unsafe_code)]`. Every `unsafe` block carries a `// SAFETY:` comment and
  gets a Miri run in CI.
- **The gate is one command.** `scripts/gate.sh` runs rustfmt, clippy, the direct-I/O
  check, `cargo doc` and every test in sequence under `set -euo pipefail`. No commit is
  made unless `scripts/gate.sh` has exited 0 on the exact tree being committed, run as
  that single command, never as separate shell lines whose failures can be missed. CI
  runs the same checks as parallel jobs.
- **Every commit is the author's.** Commits are authored and committed as
  `pchrysostomou <prodromosch@hotmail.co.uk>`, never with a `Co-Authored-By` or any
  other AI trailer. Before any push, `git log --format='%an %cn' main..HEAD | sort -u`
  must print that one line and nothing else; if anything else appears, stop and fix
  the history before it leaves the machine.
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
- **A pinned seed asserts its mechanism, never just green.** A test that pins a seed a
  sweep found must assert the specific thing the seed was pinned for: that the
  buggy variant still fails it with the violation it was pinned for, and that the
  correct server's trace still reaches the situation the fix handles; or, when a
  later change has moved the seed's schedule away from that situation, that the
  situation is absent — asserted, with the reason in the comment — so the day the
  schedule reaches it again the test says so and the pin can be upgraded. A bare
  `check().unwrap()` on a pinned seed proves only that the seed is green. A seed's
  schedule moves whenever a fault arm is added, a record changes size or a draw
  changes, so a pin that asserts only green silently stops meaning anything.
- **An assertion belongs where the statistics support it.** A variant caught on under
  5% of seeds asserts its catch at the premerge tier, `seeds() >= 1000` (the premerge
  and the nightly), never at the gate's twenty or CI's hundred. At 3%, twenty seeds
  catch nothing more than half the time and a hundred about one time in twenty, so an
  assertion there fails a tree with nothing wrong the day a change redraws the
  schedules; a thousand miss about once in 10^13. What every tier still asserts is the
  fault's firing, the fault injected or the shape it aims at reached, wherever that is
  itself well above 5%, so a sweep that passes is known to have injected it; and every
  tier prints the catch rate. A coverage counter asserted above zero, a state the
  correct server's sweep must reach, is the same draw and follows the same rule. The
  rate is over the seeds the assertion actually sees: a variant run on a share of the
  tier (`high_rate_share` in `sim/tests/engine.rs`) over that share, and a counter of
  events over the seeds that saw one, not the events. The owner's rule and the audit
  of every such assertion are D-061.
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
crates/ananke-raft/    Raft: core (pure step function), message (codec, studio decoder), store (tenant 0), apply, invariants (Phase 2)
sim/                   Simulation scenarios; scenario files sit directly in sim/ (echo.rs, engine.rs, wal.rs, raft.rs) with the linearizability checker lin.rs
docs/                  SPEC, DECISIONS, BACKLOG, BOOTSTRAP_PROMPT, devlog/
scripts/               gate.sh (run before every commit), check-direct-io.sh
clippy.toml            Banned I/O paths (disallowed-methods / disallowed-types)
.github/workflows/     CI: rustfmt, clippy + direct-io check, cargo doc, cargo test
```

## Verification commands

```
scripts/gate.sh          # the only command that precedes a commit: 20 seeds, debug
scripts/premerge.sh      # before asking for a merge: 1000 seeds, release, ~15 min
git log --format='%an %cn' main..HEAD | sort -u   # before any push: one line, pchrysostomou pchrysostomou
```

The gate runs, in order: `cargo fmt --all -- --check`, `cargo clippy --workspace
--all-targets --all-features -- -D warnings`, `scripts/check-direct-io.sh`, `cargo doc
--workspace --no-deps` with warnings as errors, `cargo test --workspace --all-targets`,
`scripts/check-nightly-shards.sh` and the doctests. A new sweep in `sim/tests` names its
nightly shard in `scripts/nightly-shards.txt` in the commit that adds it (D-064). The sweeps' four tiers (D-040): 20 seeds at the gate, 100 in CI,
1000 under `scripts/premerge.sh` on the machine in front of you, 10 000 in the
nightly workflow on GitHub — the only place ten thousand run. Every sweep runs its
seeds in parallel through `ananke_sim::sweep` (`sim/parallel.rs`, the one file
outside `ananke-env` allowed host threads); each seed's simulation is independent,
so a trace and a failing seed mean the same whichever way the sweep ran.
