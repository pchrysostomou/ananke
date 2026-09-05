# ananke-raft

Raft for [ananke](https://github.com/pchrysostomou/ananke), in three parts: a pure
protocol core stepped by inputs and producing ordered outputs, with no I/O in it; the
persistent state under a reserved tenant of the ananke storage engine, where an entry's
writes and the applied index land in one batch; and the server that runs a core under
the ananke `Environment`, so that the code that runs on real sockets and disks is the
code the simulator partitions, delays, duplicates and crashes.

The design is `docs/RAFT.md` in the repository: the variant with its sources, the
invariants the crash sweep folds from the trace, and the known-buggy variants the sweep
must catch. Licensed under MIT or Apache-2.0, at your option.
