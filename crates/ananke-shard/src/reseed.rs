//! Q15's whole-node refusal and its re-seed: which directory a node opens, and which
//! one it builds its re-seed in.
//!
//! A node owns one engine (Q2). A loss in it is therefore not one range's problem: the
//! node is refused *whole*, every replica on it with it, and it rebuilds every range it
//! holds from its leaders' snapshots (SHARD.md §11, storage 8; §12's "A loss in the
//! shared engine"). What this module owns is the part of that path that is a decision
//! about names rather than about bytes — which directory the node opens at a start, and
//! which directory a re-seed builds in — because two rules meet there and neither is
//! obvious:
//!
//! - **D-041: a directory that held a store never opens fresh.** The refused directory
//!   stays marked lost and quiesced for the rest of the run. Nothing reopens it, nothing
//!   deletes it, and no re-seed installs into it. A node that re-seeded in place would
//!   be opening, fresh, a directory that held a store — and a crash part way through
//!   would leave it neither the old store nor the new one.
//! - **D-066: at start a node opens the newest directory not marked lost.** Having
//!   refused into a new directory, the node must find that directory again at its next
//!   start, and must not find the refused one. "Newest" is the generation in the name,
//!   not a timestamp: the simulator has no wall clock a directory listing could be
//!   ordered by, and a generation is what the node itself chose.
//!
//! The two together fix the naming. A node's configured directory is generation 0, and
//! each re-seed opens the generation after the highest *present*, lost or not
//! ([`next_generation`]) — never the lowest free one, because a generation a lost store
//! held is a name D-041 has spent. Then a start takes the highest generation whose
//! marker does not say lost ([`newest_not_lost`]).
//!
//! Nothing here touches a disk: the caller lists the parent and reads each marker, and
//! hands the names and their verdicts in. That keeps the decision assertable without a
//! simulation, as [`mod@crate::snapshot`]'s and [`mod@crate::round`]'s are, and it is
//! how each of the known-buggy alternatives below is caught beside the correct one.
//!
//! What this module does *not* decide is the order of the re-seed itself — the fresh
//! engine opened at once, each range's store created in it, its durable refused mark
//! written before it serves, each range installed live as its stream completes. That is
//! [`mod@crate::server`]'s, where the disks and the sockets are.

use std::path::{Path, PathBuf};

use crate::variant::{NodeVariant, NodeVariants};

/// The generation of a node's configured engine directory: the one configuration names,
/// before any refusal.
pub const FIRST_GENERATION: u64 = 0;

/// One engine directory beside a node's configured one, as a listing found it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Candidate {
    /// Its generation: [`FIRST_GENERATION`] for the configured directory itself.
    pub generation: u64,
    /// Its path.
    pub path: PathBuf,
    /// Whether its marker says the store there lost state
    /// (`ananke_raft::store::is_marked_lost`).
    pub lost: bool,
}

/// The name generation `generation` takes beside `base`.
///
/// Generation 0 *is* `base`: a node that has never been refused keeps the directory
/// configuration gave it, so nothing about an unrefused node's layout changes. Later
/// generations are siblings — `base-g1`, `base-g2` — which is what §12 means by "a fresh
/// engine in a new directory beside the refused one": beside it, in the same parent, and
/// never inside it, since a directory inside the refused one would be inside a directory
/// that is quiesced.
// PROPOSED(D-077): Q15's whole-node refusal, and the re-seed per replica.
#[must_use]
pub fn generation_dir(base: &Path, generation: u64) -> PathBuf {
    if generation == FIRST_GENERATION {
        return base.to_path_buf();
    }
    let name = base.file_name().map_or_else(
        || String::from("engine"),
        |name| name.to_string_lossy().into_owned(),
    );
    base.with_file_name(format!("{name}-g{generation}"))
}

/// The generation `name` is of `base`, if it is one of `base`'s at all.
///
/// `base` itself is [`FIRST_GENERATION`]; `base-g<n>` is `n`. A name that is neither —
/// a staging directory, a version directory, anything else the node keeps beside its
/// engine — is not a generation and is `None`, so a listing of a busy parent does not
/// have to be filtered before it gets here.
// PROPOSED(D-077): Q15's whole-node refusal, and the re-seed per replica.
#[must_use]
pub fn generation_of(base: &Path, name: &str) -> Option<u64> {
    let base_name = base.file_name()?.to_string_lossy().into_owned();
    if name == base_name {
        return Some(FIRST_GENERATION);
    }
    let rest = name.strip_prefix(&base_name)?.strip_prefix("-g")?;
    // A leading zero would make `-g01` a second name for generation 1, and two names
    // for one generation is exactly what D-041 cannot have.
    if rest.is_empty() || (rest.len() > 1 && rest.starts_with('0')) {
        return None;
    }
    let generation = rest.parse::<u64>().ok()?;
    (generation != FIRST_GENERATION).then_some(generation)
}

/// The directory a start opens: the newest generation whose marker does not say lost
/// (D-066).
///
/// `None` means every directory present is marked lost, which on the correct node cannot
/// happen — the node marks the old one lost only once the new one is its to open — and
/// the caller treats it as the node having nothing to open rather than as a reason to
/// reopen a lost one.
///
/// [`NodeVariant::OpenNewestEvenIfLost`] is the same choice without the marker, which is
/// the way to get D-066 wrong that reads most like a simplification: it opens the newest
/// directory whatever its marker says, so a node that was refused and crashed before its
/// new directory held anything reopens the refused one — fresh, since its store is gone
/// — and D-041's rule is broken by the restart rather than by the re-seed.
// PROPOSED(D-077): Q15's whole-node refusal, and the re-seed per replica.
#[must_use]
pub fn newest_not_lost(candidates: &[Candidate], variants: NodeVariants) -> Option<&Candidate> {
    if variants.contains(NodeVariant::OpenNewestEvenIfLost) {
        return candidates
            .iter()
            .max_by_key(|candidate| candidate.generation);
    }
    candidates
        .iter()
        .filter(|candidate| !candidate.lost)
        .max_by_key(|candidate| candidate.generation)
}

/// The generation a re-seed builds in: one past the highest present, lost or not.
///
/// Counting the lost ones is the whole of D-041 in this module. A refused directory is
/// still there — marked lost, quiesced, never deleted — so its generation is spent, and
/// the next re-seed must step over it rather than into it. A node refused twice in a run
/// therefore goes 0, 1, 2, and generation 1 is never opened again even though nothing
/// live is using it.
///
/// [`NodeVariant::ReuseLostGeneration`] is the alternative that looks equivalent and is
/// not: the lowest generation not in use, which after a second refusal hands back the
/// first refusal's directory, opening fresh a directory that held a store.
// PROPOSED(D-077): Q15's whole-node refusal, and the re-seed per replica.
#[must_use]
pub fn next_generation(candidates: &[Candidate], variants: NodeVariants) -> u64 {
    if variants.contains(NodeVariant::ReuseLostGeneration) {
        let mut generation = FIRST_GENERATION;
        while candidates
            .iter()
            .any(|candidate| candidate.generation == generation && !candidate.lost)
        {
            generation += 1;
        }
        return generation;
    }
    candidates
        .iter()
        .map(|candidate| candidate.generation)
        .max()
        .map_or(FIRST_GENERATION, |highest| highest + 1)
}

/// The directory a refused node re-seeds into, given what is beside it.
///
/// [`NodeVariant::ReseedIntoRefusedDir`] re-seeds in place, which is the version D-041
/// exists to forbid: the refused directory is marked lost, and a fresh store opened in
/// it is a store whose directory's marker says its state was lost.
// PROPOSED(D-077): Q15's whole-node refusal, and the re-seed per replica.
#[must_use]
pub fn reseed_dir(
    base: &Path,
    refused: &Path,
    candidates: &[Candidate],
    variants: NodeVariants,
) -> PathBuf {
    if variants.contains(NodeVariant::ReseedIntoRefusedDir) {
        return refused.to_path_buf();
    }
    generation_dir(base, next_generation(candidates, variants))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> PathBuf {
        PathBuf::from("/n1/engine")
    }

    fn candidate(generation: u64, lost: bool) -> Candidate {
        Candidate {
            generation,
            path: generation_dir(&base(), generation),
            lost,
        }
    }

    fn correct() -> NodeVariants {
        NodeVariants::correct()
    }

    #[test]
    fn generation_zero_is_the_configured_directory_and_later_ones_are_beside_it() {
        assert_eq!(generation_dir(&base(), 0), PathBuf::from("/n1/engine"));
        assert_eq!(generation_dir(&base(), 1), PathBuf::from("/n1/engine-g1"));
        assert_eq!(generation_dir(&base(), 7), PathBuf::from("/n1/engine-g7"));
        // Beside, never inside: a re-seed directory shares the refused one's parent.
        assert_eq!(
            generation_dir(&base(), 2).parent(),
            base().parent(),
            "a re-seed directory is beside the refused one, not inside it"
        );
    }

    #[test]
    fn a_generation_name_round_trips_and_nothing_else_is_one() {
        for generation in [0, 1, 2, 9, 10, 4096] {
            let dir = generation_dir(&base(), generation);
            let name = dir
                .file_name()
                .expect("a name")
                .to_string_lossy()
                .into_owned();
            assert_eq!(generation_of(&base(), &name), Some(generation));
        }
        // The node's other directories live beside the engine's and are not
        // generations of it.
        for name in [
            "engine-g",
            "engine-g01",
            "engine-gx",
            "engine-staging-1-s2",
            "engine-snap-4-0",
            "engineer",
            "other",
            "",
        ] {
            assert_eq!(
                generation_of(&base(), name),
                None,
                "{name} is not a generation"
            );
        }
    }

    #[test]
    fn a_start_opens_the_newest_directory_not_marked_lost() {
        // D-066: generation 2 is the live one; 0 and 1 are refusals behind it.
        let present = [candidate(0, true), candidate(1, true), candidate(2, false)];
        let opened = newest_not_lost(&present, correct()).expect("a directory to open");
        assert_eq!(opened.generation, 2);

        // The pair: the variant opens the newest whatever its marker says. Here the
        // node was refused into generation 3 and crashed before it held anything, so
        // the correct node opens 2 and the variant opens the refused 3 — fresh.
        let crashed = [candidate(2, false), candidate(3, true)];
        assert_eq!(
            newest_not_lost(&crashed, correct())
                .expect("a directory to open")
                .generation,
            2,
            "the correct node steps back over a lost directory"
        );
        let variant = NodeVariants::of(&[NodeVariant::OpenNewestEvenIfLost]);
        assert_eq!(
            newest_not_lost(&crashed, variant)
                .expect("a directory to open")
                .generation,
            3,
            "OpenNewestEvenIfLost opens the refused directory, breaking D-041"
        );
    }

    #[test]
    fn a_reseed_steps_over_every_generation_present_including_the_lost_ones() {
        // One refusal: 0 is lost, the re-seed builds 1.
        let once = [candidate(0, true)];
        assert_eq!(next_generation(&once, correct()), 1);
        assert_eq!(
            reseed_dir(&base(), &generation_dir(&base(), 0), &once, correct()),
            PathBuf::from("/n1/engine-g1")
        );

        // Twice: 0 and 1 are both lost, and the re-seed builds 2 rather than
        // reopening 1. This is the case the variant gets wrong.
        let twice = [candidate(0, true), candidate(1, true)];
        assert_eq!(next_generation(&twice, correct()), 2);
        let variant = NodeVariants::of(&[NodeVariant::ReuseLostGeneration]);
        assert_eq!(
            next_generation(&twice, variant),
            0,
            "ReuseLostGeneration hands back a directory a lost store held"
        );
        assert_eq!(
            reseed_dir(&base(), &generation_dir(&base(), 1), &twice, variant),
            PathBuf::from("/n1/engine"),
            "and the directory it hands back is the one D-041 spent first"
        );
    }

    #[test]
    fn the_reseed_never_builds_in_the_refused_directory() {
        let present = [candidate(0, true)];
        let refused = generation_dir(&base(), 0);
        assert_ne!(
            reseed_dir(&base(), &refused, &present, correct()),
            refused,
            "D-041: a directory that held a store never opens fresh"
        );
        let variant = NodeVariants::of(&[NodeVariant::ReseedIntoRefusedDir]);
        assert_eq!(
            reseed_dir(&base(), &refused, &present, variant),
            refused,
            "ReseedIntoRefusedDir re-seeds in place"
        );
    }

    #[test]
    fn a_generation_a_reseed_took_is_never_offered_to_a_later_one() {
        // The run of a node refused three times: every directory it ever opened is
        // distinct, which is the property D-041 asks of the whole run rather than of
        // one refusal.
        let mut present: Vec<Candidate> = vec![candidate(0, false)];
        let mut taken: Vec<PathBuf> = vec![generation_dir(&base(), 0)];
        for _ in 0..3 {
            let live = newest_not_lost(&present, correct()).expect("a live directory");
            let refused = live.path.clone();
            let generation = live.generation;
            for candidate in &mut present {
                if candidate.generation == generation {
                    candidate.lost = true;
                }
            }
            let next = reseed_dir(&base(), &refused, &present, correct());
            assert!(
                !taken.contains(&next),
                "{} was opened before",
                next.display()
            );
            taken.push(next.clone());
            present.push(Candidate {
                generation: next_generation(&present, correct()),
                path: next,
                lost: false,
            });
        }
        assert_eq!(taken.len(), 4);
    }
}
