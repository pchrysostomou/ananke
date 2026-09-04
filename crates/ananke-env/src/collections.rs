//! Hash maps with a seeded hasher (DECISIONS.md D-014).
//!
//! `std::collections::HashMap` seeds its hasher from OS entropy per map, so iteration
//! order differs between runs and would break byte-identical traces. [`DetHashMap`] and
//! [`DetHashSet`] are the same maps with a [`DetState`] whose SipHash-1-3 keys come from
//! the environment's [`Rng`]: reproducible under simulation, OS entropy in production,
//! never a compile-time constant.
//!
//! This file is one of the three sanctioned mentions of a banned type outside `real`:
//! the aliases below must name `HashMap` and `HashSet` themselves (see clippy.toml and
//! `scripts/check-direct-io.sh`).
#![allow(clippy::disallowed_types)]

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::BuildHasher;

use siphasher::sip::SipHasher13;

use crate::Rng;

/// A [`BuildHasher`] with SipHash-1-3 keys drawn from an [`Rng`].
#[derive(Clone, Copy)]
pub struct DetState {
    k0: u64,
    k1: u64,
}

impl DetState {
    /// Draws fresh keys from `rng`.
    pub fn from_rng(rng: &impl Rng) -> Self {
        Self {
            k0: rng.next_u64(),
            k1: rng.next_u64(),
        }
    }
}

impl BuildHasher for DetState {
    type Hasher = SipHasher13;

    fn build_hasher(&self) -> SipHasher13 {
        SipHasher13::new_with_keys(self.k0, self.k1)
    }
}

impl fmt::Debug for DetState {
    // The keys are deliberately not printed: they are what makes the map HashDoS
    // resistant, and they have no business in a log line.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DetState(..)")
    }
}

/// A `HashMap` with a seeded hasher. Construct with [`det_hash_map`] or
/// `HashMap::with_hasher`.
pub type DetHashMap<K, V> = HashMap<K, V, DetState>;

/// A `HashSet` with a seeded hasher. Construct with [`det_hash_set`] or
/// `HashSet::with_hasher`.
pub type DetHashSet<T> = HashSet<T, DetState>;

/// An empty [`DetHashMap`] seeded from `rng`.
pub fn det_hash_map<K, V>(rng: &impl Rng) -> DetHashMap<K, V> {
    HashMap::with_hasher(DetState::from_rng(rng))
}

/// An empty [`DetHashSet`] seeded from `rng`.
pub fn det_hash_set<T>(rng: &impl Rng) -> DetHashSet<T> {
    HashSet::with_hasher(DetState::from_rng(rng))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rng::tests::Scripted;

    fn order_for(seed: [u64; 2]) -> Vec<u32> {
        let mut map: DetHashMap<u32, ()> = det_hash_map(&Scripted::new(seed));
        for k in 0..64 {
            map.insert(k, ());
        }
        map.keys().copied().collect()
    }

    #[test]
    fn same_keys_give_same_iteration_order() {
        assert_eq!(order_for([1, 2]), order_for([1, 2]));
    }

    #[test]
    fn different_keys_give_different_iteration_order() {
        assert_ne!(order_for([1, 2]), order_for([3, 4]));
    }

    #[test]
    fn set_works_the_same_way() {
        let mut set: DetHashSet<&str> = det_hash_set(&Scripted::new([9, 9]));
        assert!(set.insert("a"));
        assert!(!set.insert("a"));
        assert!(set.contains("a"));
    }

    #[test]
    fn debug_hides_the_keys() {
        assert_eq!(
            format!("{:?}", DetState::from_rng(&Scripted::new([7, 8]))),
            "DetState(..)"
        );
    }
}
