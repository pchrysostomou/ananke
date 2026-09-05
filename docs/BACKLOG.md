# BACKLOG.md — ananke

Things that were tempting but are out of scope for the current step. One line each,
with the reason for deferring. Promote an item by moving it into SPEC.md.

## Required before Phase 1 starts

These are demanded by SPEC §1.3 and the Phase 1 exit criteria (10k crash-injection
seeds), so Phase 1 cannot begin without them.

- **Bit rot** — after a crash any block of a file may be corrupted; the storage engine's
  checksums must catch it (SPEC §1.3).
- **Directory-entry loss** — `rename` / `create` / `remove` without a following
  `sync_dir` may be lost at crash; `SimFs` currently makes directory entries
  immediately durable (SPEC §1.3).
- **Poll budget for `Sim::run_until`** — a task that wakes itself without ever yielding
  to time loops forever; a configurable budget must turn that into a failing test, not
  a hung 10k-seed nightly run.

## Required before Phase 2 starts

Demanded by SPEC §1.4 and by the D-015 rationale (protocols are written against loss
and reorder from the start), so Raft cannot be tested honestly without it.

- **Message duplication** — deliver a message more than once with a configurable
  probability (SPEC §1.4).

## Before the Phase 0 tag

- **Publish `moirae-trace` and `moirae-sched`** and switch ananke's `[workspace.dependencies]` from git revisions to versions; `cargo publish` refuses git dependencies (D-011).

## General

- **Replay scheduler (bridge v2)** — a `moirae_sched::Scheduler` that feeds a recorded decision log back instead of the PRNG, asserting each recorded choice is in the runnable set; needs a decision-log export mode that also records `race` coins. `Sim::verify_moirae` covers the Phase 0 need.
- **`ananke trace filter`** — carve a per-range or per-transaction sub-trace (`range`, `term`, `index`, `txn` fields) before the studio sees it; the studio stays protocol-agnostic. Phase 2, when there is a range to filter by.

- **README.md** — nothing to show before the Phase 0 tag; write it alongside the first devlog post.
- **Lint canary in CI** — a test that compiles a deliberate `std::fs` call in a scratch crate and asserts clippy rejects it, so a broken `clippy.toml` cannot pass silently; verified by hand in Phase 0 step 2 instead.
- **`RealFs` on Windows** — positional I/O uses `std::os::unix::fs::FileExt`; a `std::os::windows::fs::FileExt` branch is a small `cfg` split, deferred until anyone needs a Windows build.
- **Per-op filesystem latency, slow links and bandwidth caps** — SPEC §1.3 / §1.4 performance-shaped faults; not needed for correctness testing until there is something to measure.
- **`RealRng` cost** — every call is a `getrandom` syscall and `race` draws one bit per poll; switch to a CSPRNG reseeded from the OS if it ever shows in a profile.
