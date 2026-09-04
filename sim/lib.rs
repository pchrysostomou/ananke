//! Simulation scenarios for ananke.
//!
//! Each scenario is a module in this directory (for example `echo.rs`) that builds a
//! small cluster under the simulated environment, injects faults, and returns the
//! trace it produced. Integration tests under `tests/` drive the scenarios and check
//! properties such as byte-identical traces for equal seeds (SPEC.md §1.6).
