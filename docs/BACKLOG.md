# BACKLOG.md — ananke

Things that were tempting but are out of scope for the current phase. One line each,
with the reason for deferring. Promote an item by moving it into SPEC.md.

## Deferred

- **README.md** — nothing to show before the Phase 0 tag; write it alongside the first devlog post.
- **Lint canary in CI** — a test that compiles a deliberate `std::fs` call in a scratch crate and asserts clippy rejects it, so a broken `clippy.toml` cannot pass silently; verified by hand in Phase 0 step 2 instead.
- **`RealFs` on Windows** — positional I/O uses `std::os::unix::fs::FileExt`; a `std::os::windows::fs::FileExt` branch is a small `cfg` split, deferred until anyone needs a Windows build.
