//! The protocol's vocabulary (RAFT.md §3): terms, indices, servers, entries and
//! configurations.

use bytes::Bytes;

/// A term, counting from 1; 0 is "no term yet".
pub type Term = u64;
/// A log index, counting from 1; 0 is "before the log".
pub type Index = u64;

/// A server of the group. In the simulator it is the node's number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServerId(pub u64);

impl std::fmt::Display for ServerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "s{}", self.0)
    }
}

/// What an entry carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Payload {
    /// A leader's first entry of its term: nothing to apply, something to commit.
    Noop,
    /// A state machine command, opaque to the protocol.
    Command(Bytes),
    /// A configuration change (joint consensus, RAFT.md §1); Phase 2 stage D.
    Config(Configuration),
}

impl Payload {
    /// A hash of the payload, the same on every server, for log matching in the
    /// trace: FNV-1a over a tag and the bytes.
    #[must_use]
    pub fn hash(&self) -> u64 {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        let mut feed = |bytes: &[u8]| {
            for &byte in bytes {
                hash = (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
            }
        };
        match self {
            Payload::Noop => feed(&[0]),
            Payload::Command(bytes) => {
                feed(&[1]);
                feed(bytes);
            }
            Payload::Config(config) => {
                feed(&[2]);
                for server in config
                    .voters
                    .iter()
                    .chain(config.new_voters.iter().flatten())
                {
                    feed(&server.0.to_le_bytes());
                }
                feed(&[3]);
                for server in &config.learners {
                    feed(&server.0.to_le_bytes());
                }
            }
        }
        hash
    }
}

/// One log entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// The term of the leader that created it.
    pub term: Term,
    /// Its position in the log.
    pub index: Index,
    /// What it carries.
    pub payload: Payload,
}

/// The group's membership. `new_voters` is set while a joint configuration is in
/// force (RAFT.md §1); learners receive entries and count for nothing.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Configuration {
    /// The voters of the configuration, or of the old one when joint.
    pub voters: Vec<ServerId>,
    /// The voters of the new configuration, while joint.
    pub new_voters: Option<Vec<ServerId>>,
    /// Non-voting members catching up.
    pub learners: Vec<ServerId>,
}

impl Configuration {
    /// A plain configuration of `voters`.
    #[must_use]
    pub fn of(voters: &[ServerId]) -> Self {
        Self {
            voters: voters.to_vec(),
            new_voters: None,
            learners: Vec::new(),
        }
    }

    /// Every server that receives entries: voters of both configurations and the
    /// learners, each once.
    #[must_use]
    pub fn members(&self) -> Vec<ServerId> {
        let mut all: Vec<ServerId> = self
            .voters
            .iter()
            .chain(self.new_voters.iter().flatten())
            .chain(&self.learners)
            .copied()
            .collect();
        all.sort_unstable();
        all.dedup();
        all
    }

    /// Whether `granted` is a majority of every voter set in force: of the one set,
    /// or of both while joint.
    #[must_use]
    pub fn has_majority(&self, granted: &[ServerId]) -> bool {
        let majority_of = |set: &[ServerId]| {
            let count = set.iter().filter(|s| granted.contains(s)).count();
            count * 2 > set.len()
        };
        majority_of(&self.voters) && self.new_voters.as_deref().is_none_or(majority_of)
    }

    /// Whether `server` votes in some configuration in force.
    #[must_use]
    pub fn is_voter(&self, server: ServerId) -> bool {
        self.voters.contains(&server)
            || self
                .new_voters
                .as_ref()
                .is_some_and(|new| new.contains(&server))
    }
}
