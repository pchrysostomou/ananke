# BACKLOG.md — ananke

Things that were tempting but are out of scope for the current phase. One line each,
with the reason for deferring. Promote an item by moving it into SPEC.md.

## Deferred

- **README.md** — nothing to show before the Phase 0 tag; write it alongside the first devlog post.
- **Lint canary in CI** — a test that compiles a deliberate `std::fs` call in a scratch crate and asserts clippy rejects it, so a broken `clippy.toml` cannot pass silently; verified by hand in Phase 0 step 2 instead.
- **`RealFs` on Windows** — positional I/O uses `std::os::unix::fs::FileExt`; a `std::os::windows::fs::FileExt` branch is a small `cfg` split, deferred until anyone needs a Windows build.
- **Remaining §1.3 / §1.4 faults** — bit rot, directory-entry loss (renames are immediately durable in `SimFs`), per-op fs latency, message duplication, slow links and bandwidth caps. Step 4 was scoped to torn writes, lost fsync, drop, delay and partition; add the rest before Phase 1 crash tests need them.
- **Sim run guard** — `Sim::run_until` loops forever on a task that wakes itself without ever yielding to time; a configurable poll budget would turn that into a failing test instead of a hang.
