# Contributing to ananke

ananke is built to be read as much as run. These are the working agreements every change
follows; they are short because the tools enforce most of them.

## Before you write code

Read, in order, [docs/BOOTSTRAP_PROMPT.md](docs/BOOTSTRAP_PROMPT.md) for what is being
built and why, [docs/SPEC.md](docs/SPEC.md) for how, and [docs/DECISIONS.md](docs/DECISIONS.md)
for why this over that.

**Design changes go through DECISIONS.md first.** If SPEC.md and DECISIONS.md do not
answer a design question, write a `D-nnn` entry with context, the decision, the
alternatives considered and the consequences, and open a pull request with the entry
before the code. Entries are never deleted; a later entry supersedes an earlier one and
says so. When stuck on a distributed-systems question, cite the paper.

**Scope stays inside the phase.** Anything tempting that the current phase does not need
becomes a GitHub issue labelled with its phase, not a change. The issues labelled
`good first issue` are the approachable ones; `help wanted` marks the ones that need a
design conversation first.

## The one command

```sh
scripts/gate.sh
```

runs rustfmt, clippy with warnings denied, the direct-I/O check, `cargo doc` with
warnings as errors, and every test, in that order, under `set -euo pipefail`. No commit is
made unless it has exited 0 on the exact tree being committed, run as that one command.
CI runs the same checks as parallel jobs on every push; a nightly job runs the
simulation sweeps at ten thousand seeds in release.

## Determinism first

Nothing outside `crates/ananke-env` touches the real world directly. `std::time`,
`std::fs`, `std::net`, `tokio::net`, `tokio::time`, `tokio::fs`, `rand`,
`std::collections::HashMap` and thread or task spawning are banned by `clippy.toml`;
time is `ananke_env::Instant`, hash maps are `DetHashMap` seeded from the environment,
and everything goes through the `Environment` trait so that the same code runs under the
real environment and the simulator. `scripts/check-direct-io.sh` fails if any file other
than the real environment's module carries an `allow` for those lints.

`unsafe` is denied everywhere except `crates/ananke-storage`, which so far has none.
The first block there brings a `// SAFETY:` comment and a Miri job in CI with it.

## Every fault-model test comes as a pair

A test that injects faults must run a known-buggy variant that the sweep catches and a
correct one that it passes, under the same seeds. A sweep that only passes may not be
injecting the fault; one that only fails may be failing correct code. The pair is what
shows the fault model tells a bug from correct code. The shape is a `Variant` enum on the
code under test, a scenario `Report::check` that expects different things of each
variant, and a sweep test that asserts both; the correct variant is the default that
ships. `Journal::sync_dir_on_rotate`, `wal::Variant` and `engine::Variant` are the
existing examples.

## Every state transition emits a trace event

If a change cannot be seen in the moirae studio, it did not happen. Add a
`TraceEvent` and its bridge line in `crates/ananke-env/src/moirae.rs` with the code, and
keep the event's fields plain: the sweeps' oracles read them. `sim/echo.rs` pins the hash
of its trace; a deliberate change updates the constant in the same commit and says why.

## Seeds

Sweeps run `ANANKE_SEEDS` consecutive seeds: 20 by default at the gate, 100 in CI,
10 000 in the nightly. A failing seed writes its trace to `sim/out/<scenario>-<seed>.jsonl`;
`npx moirae replay` opens it in the studio. A seed that found a bug stays in the gate as
a named test, with the bug in its doc comment.

## Style

Boring, documented Rust. `missing_docs` is a warning promoted to an error in CI, so every
public item has a sentence. Comments say why, not what. Commit messages say what changed
and why, in prose, and name the DECISIONS entry they implement.

## Licence

Contributions are accepted under the project's dual licence, MIT or Apache-2.0 at the
user's option, and every published crate carries copies of both licence files.
