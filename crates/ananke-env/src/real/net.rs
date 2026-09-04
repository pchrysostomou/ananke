use crate::Network;

/// Placeholder until DECISIONS.md D-015 settles the transport shape.
#[derive(Clone, Copy, Debug, Default)]
pub struct RealNet;

impl Network for RealNet {}
