//! `ananke-server` assembles one ananke node. This library half holds the protocols the
//! binary runs, so that `sim/` drives exactly the same code under the simulator that
//! `ananke-server` runs on `RealEnv` (BOOTSTRAP_PROMPT.md, principle 1).

pub mod echo;
