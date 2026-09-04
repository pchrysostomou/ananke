//! The [`Network`] trait.
//!
//! The shape of node-to-node transport (message-oriented datagrams versus byte streams)
//! is pending DECISIONS.md D-015. Until it is decided this is a marker trait, so that
//! [`Environment`](crate::Environment) is complete and `RealEnv` can be exercised.

/// Node-to-node transport. Shape pending D-015.
pub trait Network: Send + Sync + 'static {}
