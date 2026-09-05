# ananke

A distributed SQL database in Rust, built from the ground up to be deterministically
testable: every source of non-determinism goes through one `Environment` abstraction, so
the code that runs in production also runs inside a deterministic simulator whose traces
open in the [moirae](https://github.com/pchrysostomou/moirae) studio.

Phase 0 ships the deterministic runtime as the `ananke-env` crate. This crate reserves the
name and becomes the public facade once there is a database to expose. Development happens
at <https://github.com/pchrysostomou/ananke>.

Licensed under MIT or Apache-2.0, at your option.
