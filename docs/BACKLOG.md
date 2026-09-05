# BACKLOG.md — ananke

Things that were tempting but are out of scope for the current step. One line each,
with the reason for deferring. Promote an item by moving it into SPEC.md.

## Required before Phase 2 starts

Demanded by SPEC §1.4 and by the D-015 rationale (protocols are written against loss
and reorder from the start), so Raft cannot be tested honestly without it.

- **Message duplication** — deliver a message more than once with a configurable
  probability (SPEC §1.4).

## General

- **Replay scheduler (bridge v2)** — a `moirae_sched::Scheduler` that feeds a recorded decision log back instead of the PRNG, asserting each recorded choice is in the runnable set; needs a decision-log export mode that also records `race` coins. `Sim::verify_moirae` covers the Phase 0 need.
- **`ananke trace filter`** — carve a per-range or per-transaction sub-trace (`range`, `term`, `index`, `txn` fields) before the studio sees it; the studio stays protocol-agnostic. Phase 2, when there is a range to filter by.

- **README.md** — nothing to show before the Phase 0 tag; write it alongside the first devlog post.
- **Lint canary in CI** — a test that compiles a deliberate `std::fs` call in a scratch crate and asserts clippy rejects it, so a broken `clippy.toml` cannot pass silently; verified by hand in Phase 0 step 2 instead.
- **`RealFs` on Windows** — positional I/O uses `std::os::unix::fs::FileExt`; a `std::os::windows::fs::FileExt` branch is a small `cfg` split, deferred until anyone needs a Windows build.
- **Per-op filesystem latency, slow links and bandwidth caps** — SPEC §1.3 / §1.4 performance-shaped faults; not needed for correctness testing until there is something to measure.
- **`RealRng` cost** — every call is a `getrandom` syscall and `race` draws one bit per poll; switch to a CSPRNG reseeded from the OS if it ever shows in a profile.
- **Lost fsync in the echo sweep** — `sim/echo.rs` keeps `p_durable` at 1.0; the Phase 0 unit tests cover lost fsync and the journal invariants hold under it, so it waits for the WAL's crash tests, where it matters.
- **Trace names for a file after a lost rename** — `BlockRotted` and `WriteTorn` name the inode's last visible path; after directory-entry loss its durable name may differ (`/echo/journal.prev` for a file the restarted node reads as `/echo/journal`). Add inode ids, or the durable name, when the studio needs to follow a file across a crash.
- **Unreachable inodes in `NodeFs`** — a rename over an existing entry or an unlink leaves the old inode in the map, still subject to bit rot at later crashes; harmless for short runs, garbage-collect when a 10k-seed run's memory shows it.
- **Streaming WAL recovery** — `Wal::open` reads each segment whole; stream it record by record when segments are large enough for that to matter.
- **CRC-32C with SSE4.2** — the `crc32c` crate is several times faster than the table in `ananke_storage::crc32c`; switch if a profile shows the checksum (D-018).
