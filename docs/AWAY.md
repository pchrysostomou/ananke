# AWAY.md — the unattended session of 14–15 September 2026

What landed while the owner was away, what did not and why, every PROPOSED entry, and
every open question. Nothing was merged, published or force-pushed. No tag has been
pushed yet; see Track 0.

## What landed, as pull requests (none merged)

| PR | Branch | What | Gate | Premerge 1000 | 10 000 seeds |
|---|---|---|---|---|---|
| #38 | `phase-2-release` | The 0.3.0 release commit: workspace 0.3.0, `ananke-raft` public for the first time, README, devlog status and BOOTSTRAP say Phase 2 is released | green | green (dry-run publish of all four crates also green) | run 34885583684 on `4044b7b`: green, every catch rate identical to run 34852980174 |
| #39 | `phase-2-d049-10k` | D-049's ten-thousand-seed record, checked seed by seed | green | — (docs only) | — (docs only; the figures are from runs 34852980174, 34839613587, 34885298800, 34885317292) |
| #40 | `phase-3-design` | `docs/SHARD.md`, the Phase 3 design proposal | green | green | — (docs only) |
| #41 | `phase-2-backlog` | Issues #32 and #33, the performance hot spots; issue #37 left with reasons | green on every commit | green | run 34901799989 on `1ef6d7e` and run 34908018220 on the tip `0557590`: both green, rates identical to the release run |
| this PR | `phase-2-devlog-final` | The Phase 2 devlog against the release tree, D-049's story, and this file | green | — (docs only) | — |

Merge notes:
- **This branch sits on #38.** It is stacked on `phase-2-release`, so it merges after #38.
- **#38 and #41** both edit the status section of `docs/BOOTSTRAP_PROMPT.md`. Whichever merges second needs a small rebase.
- **#39 and #41** both touch the end of `docs/DECISIONS.md`: #39 appends to D-049 just above the footer, and #41 appends D-050..D-052 after it. Expect a conflict next to the footer in whichever merges second. Resolve it by keeping both texts; the footer reads D-053.

## Track 0 — in flight

- **The #36 seed-by-seed check: done, and it passed.** Two temporary branches off `cd411b4` and `a8656e8` printed every seed's trace hash and verdict at 10 000 seeds for the correct server and the seven variants whose figures moved (runs 34885298800 and 34885317292). Their catch counts reproduce both nightlies' exactly.
  - 983 seed and server pairs differ between the two trees.
  - Each was re-run on both trees. Every one first differs at an `ananke.raft.quorum-lost` naming an uncounted refused follower, where the earlier tree has no step-down.
  - The nine catch changes are among the 983:
    - `CountOlderTermForCommit` +4025 +5860 +9983 −7830
    - `ResetTimerOnAnyRpc` +1677 +4245 +9765
    - `RefusalNotDurable` +4734
    - `SnapshotWithoutCurrentLast` −1537
  - The record is in D-049, in PR #39.
  - Both temporary branches are deleted, locally and on GitHub.
- **The version bump** was rebuilt on `main` at 94c6a54 as PR #38, and left open.
- **The tag has not been pushed,** because #38 was still open when this file was written. A watcher in the session tags `v0.3.0` on #38's merge commit and pushes the tag once the owner merges. If the session has ended by then, the tag is the owner's to push, and the release notes below are the tag message.
- **The release notes' evidence statement is stated exactly, not as the owner worded it.** The owner asked for "cd411b4's tree plus the changes in #35, #36 and the bump, each with its own green run". #35 (docs and one comment) had no ten-thousand-seed run of its own. Its commit is under #36's head `a8656e8`, whose run covers both. The bump has its own run.

Release notes, to be the annotated tag message of `v0.3.0`:

```
Phase 2: Raft, tested under deterministic simulation

ananke, ananke-env, ananke-storage and ananke-raft at 0.3.0; ananke-raft
is published for the first time.

The ten-thousand-seed evidence is cd411b4's tree plus the changes after it,
each covered by a green run of its own tree:
- cd411b4 (the Phase 2 close-out): nightly run 34839613587;
- #35 (SPEC §3's membership criterion as asserted, forward pointers, the
  README) and #36 (D-049, check quorum and refused followers), together
  at a8656e8, #36's head, which contains #35's commit: run 34852980174;
- the 0.3.0 release commit 4044b7b (#38): run 34885583684.
The disk honours fsync (D-026); lost syncs are issue #23. Worst membership
availability gap 549.359683 ms against the 2 s bound SPEC §3 states.
```

## Track A — `docs/SHARD.md` (PR #40)

- **What the document is.** 2 113 lines in RAFT.md's shape, marked proposed, with no Phase 3 code. It covers:
  - ranges, descriptors and the meta range;
  - bootstrapping and routing;
  - multi-raft, with what breaks at 1 000 and 10 000 ranges worked out from today's constants (arithmetic, not measurements);
  - split, merge and rebalancing;
  - the invariants as folds over named trace events;
  - linearizability across moving boundaries;
  - 23 buggy variants, including the four the owner named: `SplitNotAtomicWithDescriptor`, `MergeDivergentReplicas`, `TrustStaleDescriptor`, `RemoveBeforeCaughtUp`;
  - what storage, raft and env must provide;
  - an order of work;
  - 42 questions for approval.
- **How it was produced.** Three research surveys, one author, five review lenses and one revision; all 65 review findings were fixed.
- **Differences from the request.**
  - The owner asked for invariants "exactly as RAFT.md §4 does". In RAFT.md the folds are §2 and §4 is the linearizability checker. SHARD.md follows both.
  - No DECISIONS entries were written, following RAFT.md's own proposal (`cd4a891`). The open choices are the numbered questions instead.

## Track B — the backlog (PR #41)

- **#32 → PROPOSED D-050.**
  - The `net` task stamps each frame's receipt, and a term record carries the receipt of the message its step took (`receivedNs` in the export).
  - The pre-vote check excuses an isolation only when every term change in it came from a message received before it began.
  - A directed scenario, `Fault::IsolateOnTermRaise`, reaches the shape on 2 893 of 10 000 seeds; seed 4 is pinned. `NoPreVote` is caught on every seed.
  - No schedule moved.
  - The correction it makes to D-047's figures: on seeds 3863 and 1252 the messages were received 140.67 and 144.43 ms before their steps, not 2.48 and 16.31 ms. The inbox is first in, first out.
- **#33 → PROPOSED D-051.** A removed pre-vote catch must match a straddle on the isolation it names. A removed timer catch must be explained by one of three shapes read off both replays at the flag record: a reset moved back, the flag moved back, or the server's status moved. The assertions run at every tier and on the directed sweep.
- **#37: not implemented.** Both candidate fixes exceed the size guide.
  - (a) Answering during repair and adoption needs a new wire status, a change to D-049's rule and inbox handling inside the re-seed and adoption paths.
  - (b) A faster adoption must remove about 124 ms of median silence, and the copy that dominates it is what D-041's crash safety rests on.
  - The owner decides: a protocol change, or revisiting D-041.
- **Performance → PROPOSED D-052.**
  - Profile: `sample`, 300 raft seeds. The JSONL export was 27 % inclusive and was written for every run.
  - It is now written only on demand. The adoption watch reads a durable-namespace version first, and the pre-vote check does one pass.
  - Premerge at 1 000 seeds, warm build: 547.55 s at load 11.20 → 374.64 s at load 13.90, 31.6 % less.
  - Not changed: the allocator (18.6 % inclusive, spread over the simulation with no single caller to remove), and, each under 5 % of the premerge, `leader_now`, the engine's export and `Model::state_after`.
- **Review.** Three independent reviewers read D-050 to D-052 and found one major and six minor problems, all fixed:
  - the major one: D-051's timer assertion could have failed a real removal;
  - D-047's straddle predicate had an off-by-one boundary, fixed at the same time.

## Track C — the devlog (this PR)

The devlog now reads against the release tree:
- figures from run 34885583684, with the pre-D-049 figures that D-049 moved beside them;
- the variant count;
- exit criterion 2 asked on uniformly scheduled seeds;
- issue #37 open.

It also gains a two-paragraph section on D-049. **One place differs from the owner's request.** The owner asked for it as "the clearest example in the project of a decision changed by evidence rather than argument". The record I could check says the rule changed on the argument (the three-server case). The measurement came after, and decided what D-049 may claim: the argument holds during the stream by a wide margin (84 % against 24 % of re-seeds losing the leader before the install), and little of that survives the adoption (98 % against 92 %, issue #37). The section says that, rather than the owner's phrase. The session in which the pushback was made left no transcript, so its account is the owner's, and the section says so.

## What I did not do, and why

- **Merge, publish, force-push.** Never; the rules forbid them.
- **Push the tag.** #38 was still open. See Track 0.
- **Issue #37.** Over the size guide; see Track B.
- **Phase 3 code.** The owner forbade it; SHARD.md is design only.
- **Performance changes under 5 % of the premerge.** Left, with their shares measured.
- **Premerge on #39 and on this branch.** Both are docs only, and the gate ran on each commit.
- **Re-measure the `{IgnoreIncarnation, SharedSnapshotDir}` pair over 0..999 after D-049.** The devlog now says its figure is from before D-049 and only seed 680 was re-checked.

## Every PROPOSED entry awaiting approval

| Entry | PR | Decision |
|---|---|---|
| D-050 | #41 | A term's record carries when the message its step took was received |
| D-051 | #41 | A removed catch is asserted against the isolation or the flag it names |
| D-052 | #41 | A scenario's moirae JSONL is written when it is asked for |

No PROPOSED entries on #38, #39, #40 or this branch.

## Every open question

- **D-050..D-052: approve or not.** On approval, D-047 takes its forward pointer from D-050 and D-051, SPEC §1.5 needs a sentence on `receivedNs`, and issues #32 and #33 can close.
- **Issue #37.** A wire-protocol change (answering during repair and adoption), or revisiting D-041's copy, or leaving it.
- **D-052's lost check.** Sweeps no longer export every seed's trace, so a trace that could not be written as moirae v2 shows only when it is asked for. The only failure moirae-trace 0.0.2 can return is a non-object `msg`, `patch` or `data`, which the decoders never produce.
- **D-050's coverage.** No natural sweep seed reaches the shape; only the directed scenario does.
- **The D-049 story's framing.** Keep the section as written, or reword it to the owner's phrase.
- **The release notes' wording.** #35 is covered by #36's run rather than its own.
- **SHARD.md's 42 questions (§13 of `docs/SHARD.md`):**
  - Q1–Q10:
    1. Phase 2's tag and RAFT.md's drift before Phase 3's code.
    2. One engine per node or per replica.
    3. Which descriptor copy is the authority, and how meta is kept.
    4. Addressing levels.
    5. The keyspace's layout.
    6. What raises a generation.
    7. Bootstrapping a fresh cluster.
    8. Node membership, removal, failure.
    9. Placement of ranges 0 and 1.
    10. Requests, `RangeMismatch`, retries.
  - Q11–Q20:
    11. Client sessions in Phase 3.
    12. Heartbeats.
    13. A seed per range.
    14. Tasks, inbox and streams on a node with many ranges.
    15. A loss in a shared engine.
    16. A bounded queue per destination in `SimEnv`.
    17. Allocating range ids.
    18. What splits, where.
    19. Which half keeps the parent's id.
    20. The right half's starting term and floor.
  - Q21–Q30:
    21. The right half's first election.
    22. Uninitialised replicas.
    23. Splits, merges and configuration changes together.
    24. The merge protocol.
    25. What merges.
    26. Replica identity and per-replica state.
    27. Collecting removed replicas, and the overlap rule.
    28. "Caught up" for a move.
    29. Where the rebalancer runs.
    30. The rebalancer's goal, and SPEC's 10 %.
  - Q31–Q42:
    31. What the rebalancer reads.
    32. The replication factor.
    33. Quarantined replicas.
    34. Membership changes and snapshots on one schedule.
    35. `Scan` in Phase 3.
    36. Meta lookups in the history.
    37. Where Phase 3's variants live.
    38. The stale-descriptor variant.
    39. Scenarios, faults, bounds and cost.
    40. Crate boundaries.
    41. The node's `raft` task and its round.
    42. How the rebalancer chooses a move.

## Local worktrees left for cleanup

`~/Desktop/ananke-audit`, `ananke-d047`, `ananke-decided`, `ananke-tagprep`, `ananke-d049`,
`ananke-d049-10k`, `ananke-release`, `ananke-shard`, `ananke-backlog` and `ananke-devlog`,
one per branch above. Remove each with `git worktree remove` once its PR is merged or
closed.
