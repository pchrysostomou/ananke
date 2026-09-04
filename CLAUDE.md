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
  `tokio::time`, `tokio::fs`, `rand`, or thread/task spawning outside
  `crates/ananke-env`. The banned paths are listed in `clippy.toml`; only the real
  implementation module inside `ananke-env` may carry
  `#[allow(clippy::disallowed_methods, clippy::disallowed_types)]`.
  `scripts/check-direct-io.sh` is the textual second check. Both run in CI.
- **No `unsafe` outside `crates/ananke-storage`.** The workspace lint is
  `deny(unsafe_code)`; `ananke-storage` is the only crate permitted to
  `#![allow(unsafe_code)]`. Every `unsafe` block carries a `// SAFETY:` comment and
  gets a Miri run in CI.
- **Every change:** tests under simulation, clippy clean, `cargo doc` without
  warnings, rustfmt clean.
- **Every state transition that matters emits a trace event.** If it can't be seen in
  the moirae studio, it didn't happen.
- **Prefer boring, well-documented Rust.** This is a project meant to be read.
- **When stuck on a distributed-systems question,** cite the paper (Raft, Percolator,
  Calvin, Spanner, FoundationDB testing talk) in the DECISIONS.md entry.
- **At the end of every session,** update the "Current status" section at the bottom
  of `docs/BOOTSTRAP_PROMPT.md`.

## Layout (Phase 0)

```
crates/ananke/         Placeholder crate reserving the name on crates.io
crates/ananke-env/     Environment trait + RealEnv + SimEnv
crates/ananke-server/  Node binary (placeholder until the echo server is wired)
sim/                   Simulation scenarios; scenario files sit directly in sim/
docs/                  SPEC, DECISIONS, BACKLOG, BOOTSTRAP_PROMPT, devlog/
scripts/               check-direct-io.sh
clippy.toml            Banned I/O paths (disallowed-methods / disallowed-types)
.github/workflows/     CI: rustfmt, clippy + direct-io check, cargo doc, cargo test
```

## Verification commands

```
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
scripts/check-direct-io.sh
cargo fmt --all -- --check
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
```
