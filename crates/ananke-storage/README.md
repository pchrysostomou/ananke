# ananke-storage

The storage engine of [ananke](https://github.com/pchrysostomou/ananke): a write-ahead
log and, as Phase 1 proceeds, an LSM tree, all generic over the `ananke-env`
`Environment` so the same code runs on a real disk and under the deterministic
simulator with torn writes, lost fsyncs, bit rot and lost directory entries.

Read the repository's `docs/SPEC.md` §2 for the contract and `docs/DECISIONS.md` for
why it is shaped this way.

Licensed under MIT or Apache-2.0, at your option.
