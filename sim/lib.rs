//! Simulation scenarios for ananke.
//!
//! Each scenario is a module in this directory that builds a small cluster under
//! [`ananke_env::sim::Sim`], injects faults, and returns a report with the trace it
//! produced. Integration tests under `tests/` drive the scenarios and check
//! properties such as byte-identical traces for equal seeds (SPEC.md §1.6).

pub mod echo;
pub mod wal;
