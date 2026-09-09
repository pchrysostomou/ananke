# OVERNIGHT.md — the 2026-09-08 → 09 session, Phase 2 to code-complete

Stop condition reached: **Phase 2 done except the tag**, as asked. Everything below
is on branch `phase-2-overnight` (never `main`), every commit gated by
`scripts/gate.sh` on its exact tree, nothing pushed, no tags, no publishes, no
moirae changes.

## What landed, in order

| Commit | What |
|---|---|
| `9d2bfec` | raft sweep: two lease trials per schedule (stage C review item) |
| `6d91cad` | env: trace events for stages D and E (`RaftConfig`, `RaftSnapshot`) |
| `ddd75b7` | raft sweep: the Figure 8 driver and the batched sweep (issue #22, D-031) |
| `cfb2196` `46a305c` `938afa5` | stage D: joint consensus — core, the `0/2/config` key, the Change command, the address book apart from the voters, the 3→5→3 scenario (D-029) |
| `8b79ca8` | merge of stage D onto the batched sweep |
| `c9fc814` `75e24fc` `101db4e` `51754ad` | stage E: snapshots — compacted log, codec, the snapshot task, streaming and the staged install, the re-seeded server, the quarantine, snapshot-aware invariants (D-030) |
| `1373601` | merge of stage E onto D+#22: nine files reconciled; the install repair now writes `0/2/config` (proven by a negative control); two latent composition bugs fixed (the truncation revert floor on a compacted log; the take-record's configuration following applied `Config` entries) |

Work ran as three parallel worktree branches (`phase-2-overnight-22`, `-d`, `-e`),
merged in that order through the gate; the branches are kept for inspection.

Also written: `docs/devlog/02-phase-2.md` (the Phase 2 devlog DRAFT, three best
bugs chosen and told), and the "Current status" section of BOOTSTRAP_PROMPT.md.

## Catch rates per variant

At 100 seeds, release, on the final tree (`1373601`); the correct server passes
all 100 seeds of both sweeps:

| Variant | Caught | Catching check |
|---|---|---|
| SendBeforePersist | 100/100 | commit majority |
| TruncateOnEveryAppend | 100/100 | committed entries stay |
| NoPreVote | 100/100 | pre-vote's isolation property |
| ApplyBeforeCommit | 92/100 | state machine safety |
| ResetTimerOnAnyRpc | 44/100 | timers fire |
| CountOlderTermForCommit | 42/100 | commit by current term — **with `max_batch` at its default**; 0/100 before the D-031 driver |
| SingleMajorityInJointConsensus | 28/100 | commit majority / leader completeness, membership scenario |
| SnapshotWithoutCurrentLast | 26/100 | state machine safety after a crash mid-install |
| LeaseTrustsTheClock | stale read caught on 2 of 52 drift-exceeded seeds; the guard revoked on all 52; "neither" on 0 | invariant 6 via the checker |

Context for the lease line: the two-trial change (`9d2bfec`) measurably doubled the
single-trial catch (2→4 of 46 at 100 seeds on the pre-D/E tree); the rate moves a
few counts with every schedule reshuffle (D-031 changed every seed's draws), which
is exactly why the nightly's 10 000 seeds are the number that matters.

Membership scenario: grows and shrinks completed on 100/100 seeds, 248 learners
promoted, 6 elections while joint, 37 step-downs of a leader outside C_new, worst
completion gap 469 ms against the 2 s bound. Snapshot coverage: 3116 taken, 1292
installed, 4941 streams resumed, 2722 compactions, 46 completed
refusal → re-seed → applying-again cycles.

**Nightly at 10 000 seeds — two runs so far, both red on the correct server,
both false positives of one check; the third run is pending (numbers below are
still the 100-seed ones).**

- Run 1 (`1373601`, 2h45m awake): 15 of 16 suites green; the correct server
  failed at **seed 164** — the timers-fire check flagged a follower two hundred
  entries behind a compacting leader, fed only by `InstallSnapshot` chunks, as
  starved; the leader was reaching it every few milliseconds and a leader was
  elected 40 ms after the flag. Checker gap: the check counted only
  `AppendEntries` as the leader's contact. Fixed in `f54b468` (D-030 stanza),
  verified on seeds 0–199. Every variant test passed at 10k, but their rates
  were swallowed by cargo's output capture (`nightly.yml` lacks `--nocapture`).
- Run 2 (`f54b468`): the correct server failed at **seed 385**, the sibling
  case — a follower cut off alone mid-install, whose install completion rebuilt
  its core with a fresh timer (stage E's incarnation switch), campaigning 25 ms
  past the bound. The check now treats the install's restatement as the
  leader's contact: **PROPOSED D-039**, a checker-semantics decision for your
  approval; seeds 164 and 385 are pinned in the gate. The run was killed at ~14%
  because its verdict was already red and the Mac had been sleeping through
  most of its wall-clock (lid closed, on battery).
- Run 3: launched after this commit under `caffeinate -i` with `--nocapture`
  (~3 h awake); its per-variant rates and verdict go here.
<!-- TODO(nightly-v3): paste per-variant rates at 10k, the correct-server
verdict, any further seed, and wall-clock. -->

## Every PROPOSED entry (in DECISIONS.md under "PROPOSED — needs approval")

- **D-032** — learner catch-up state is leader-local and volatile; a leadership
  change abandons the membership change and the operator's idempotent retry
  recovers it.
- **D-033** — a server that is not a voter of its configuration in force does not
  campaign (learners, empty-config servers, removed servers).
- **D-035** — a re-seeded (LostState-refused, snapshot-fed) server never votes,
  pre-votes, campaigns or extends a lease again on that store, durably
  (`0/0/reseeded`); quorum-intersection argument written out; cites Alagappan et
  al. FAST 2018 / issue #23; alternative: wipe + joint-consensus re-add (now
  implementable since stage D landed).
- **D-036** — snapshot-metadata exactness: the take runs in the apply task between
  applies; record synced into the live store, then `Engine::checkpoint`.
- **D-037** — when a follower stops blocking compaction and is designated
  snapshot-fed (behind by more than the threshold and silent for two minimum
  election timeouts, or rejecting at index 1).
- **D-038** — the staged install commits by CURRENT-last and is adopted by
  idempotent copy at open; no directory renames.
- **D-039** — for the timer check, a completed snapshot install counts as the
  leader's contact (the install was leader-initiated and the server was busy
  finishing it): the install's `RaftRecovered` restatement resets the check's
  clock like a crash restart's. From the nightly's seed 385; protocol unchanged.

D-034 is an unused number (allocation gap between parallel branches); the footer
says next entry D-040. Every PROPOSED site is marked `// PROPOSED(D-0xx)` in code.
Nothing was recorded as decided: D-029/D-030/D-031 record only what RAFT.md already
approves plus as-built detail and what the sweep found, per the house convention.

## Every seed (or find) that found a bug this session

| Where | What it found | Outcome |
|---|---|---|
| #22 dev | The issue's sketched driver shape cannot open the window (commit knowledge lags by one round trip); the restarted leader's volatile commit index is what opens it | D-031's driver design |
| #22 dev, 20-seed gate | New fault arm drawn from the shared stream reshuffled every seed; lease catch went 0/20 | The own-stream rule, recorded in D-031 |
| stage D (by review) | `voters()` returned members(), learners included — latent counting bug | Fixed in `cfb2196` |
| stage E, compile/gate | Predecessor's `Result<u64>` vs `Result<()>`; a rustdoc private link | Fixed |
| stage E, seed 9 (20-seed) | Timers check flagged the re-seeded server for never campaigning — that is D-035's design | Sweep check exempts re-seeded servers |
| stage E, threshold tuning | **Lease catch collapsed to 0**: a fresh leader's instant first checkpoint stalled applies past the write window | Core fix: first take deferred two minimum election timeouts (D-030); devlog bug #2 |
| stage E, seed 8 | An install completing inside an isolation window re-stated the term there; pre-vote check misfired | Isolation check skips refusal/re-seed/install windows |
| stage E, variant work | Crash-aiming watcher fired on stale done-chunks; and the staging dir was never `sync_dir`'d so staged files vanished at the crash and the variant was uncatchable | Harness fix + real fix: per-file directory sync (D-030) |
| stage E, seed 60 (100-seed) | **Double outage**: rot-refused server + quarantined server = no electable majority; liveness failed with no code bug; investigation exposed two quarantine hygiene bugs (install cleared it; leader's checkpointed flag could quarantine a healthy receiver) | Liveness asked only of electable majorities; quarantine sticks to store history and is tombstoned in the repair (`51754ad`); devlog bug #3 |
| E-merge (by review) | Truncation reverted config to the *initial* configuration on a compacted log; take-records carried a spawn-time-frozen configuration | Both fixed in `1373601` |
| merge negative control | Install repair without the `0/2/config` write: every installed store refused at open | The required fix, with a test that fails without it |
| **nightly, seed 164** | First correct-server failure ever: the timer check read a snapshot-fed follower as starved; `InstallSnapshot` was not counted as the leader's contact | Checker fix `f54b468`, D-030 stanza; pinned |
| **nightly, seed 385** | Its sibling: a follower alone mid-install, the install's incarnation switch reset its timer, campaigned 25 ms past the bound | Checker models the switch — PROPOSED D-039; pinned |

## What was NOT done, and why

- **The tag, crates.io, releases** — explicitly out of scope ("finish Phase 2
  except the tag"); D-011's bar (exit criteria in CI, tag, devlog published) is
  not met until you review.
- **Push** — `git push` has no credentials on this machine (osxkeychain has
  nothing for github.com); per your rule I stopped at the push and ask: push the
  branch (and the three sub-branches if you want them) after review.
- **RAFT.md / SPEC.md edits** — both frozen/approved; two touch-ups now warranted
  but not made: RAFT.md §5's CountOlderTermForCommit "Needs" column predates the
  D-031 driver, and §5's table row wording for the sweep's batch size.
- **Membership changes × snapshots on one schedule** — the main sweep never
  proposes `Change`; the membership scenario never crosses the snapshot
  threshold. The interaction (config-key repair, revert floor) is unit-tested
  only. Suggested issue below.
- **Three sweep-untested variants** — `VoteBeforePersist`,
  `IndexFirstElectionRestriction`, `ApplyNotAtomicWithIndex` have core/unit
  coverage but no sweep catch-rate test; pre-existing state, unchanged.
- **BACKLOG issues not filed** (no credentials): candidates —
  membership+snapshots on one schedule; checkpoint-directory GC (`snap-<n>` dirs
  never deleted); a multi-stream snapshot sender; a studio metric for stream
  health; RAFT.md §5 driver touch-up; an operator mechanism to retire a removed
  server.
- **Moirae** — no changes needed; no PR opened.

## Operational notes

- Three agent instances died to a 600 s no-output watchdog mid-build and were
  resumed from their on-disk state; no work was lost beyond wall-clock.
- The second 10k run lost most of its wall-clock to the Mac sleeping (battery,
  lid closed: sleep → 2 s DarkWake → sleep, 13:28–14:32); the third runs under
  `caffeinate -i`. A 10k run here needs ~3 h awake.
- The nightly's catch rates are only visible with `--nocapture`; suggested
  follow-up: add it to `nightly.yml`'s test command so CI reports them too.
- The `ananke-22/-d/-e/-int` worktrees are left in place beside the repo for
  archaeology; `git worktree remove` them when done.
