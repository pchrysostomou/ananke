# ananke-env

The `Environment` trait through which every source of non-determinism in
[ananke](https://github.com/pchrysostomou/ananke) flows: clock, filesystem, network,
randomness and task spawning. Two implementations: `RealEnv` on tokio, and `sim::Sim` /
`SimEnv`, a single-threaded deterministic simulator with a virtual clock, seeded streams,
an in-memory network with drops, delays and partitions, and an in-memory disk with torn
writes and lost fsyncs. A simulation's trace exports as a
[moirae](https://github.com/pchrysostomou/moirae) trace and opens in the moirae studio.

Phase 0 of ananke: the deterministic runtime. Read the repository's `docs/SPEC.md` for the
contract and `docs/DECISIONS.md` for why it is shaped this way.

Licensed under MIT or Apache-2.0, at your option.
