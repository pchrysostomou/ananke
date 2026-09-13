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
| `f54b468` | raft sweep: an InstallSnapshot resets the timer check — seed 164 of a local ten-thousand-seed run (D-030 stanza) |
| `74b93c5` | raft sweep: seed 385, the install's restatement resets the timer check (PROPOSED D-039); seeds 164 and 385 pinned; this record, the devlog draft and BOOTSTRAP's status committed |
| (this commit) | sim: seeds in parallel through `ananke_sim::sweep` with its proof test, the four tiers with `scripts/premerge.sh`, the nightly's `--nocapture` (D-040); the 1000-seed numbers |

Work ran as three parallel worktree branches (`phase-2-overnight-22`, `-d`, `-e`),
merged in that order through the gate; the branches are kept for inspection.

Also written: `docs/devlog/02-phase-2.md` (the Phase 2 devlog DRAFT, three best
bugs chosen and told), and the "Current status" section of BOOTSTRAP_PROMPT.md.

## Catch rates per variant

**1000 seeds, `scripts/premerge.sh` (release, seeds in parallel), on the final
tree; 10k pending on the GitHub nightly.** The correct server passes all 1000
seeds of both Raft sweeps, and the Phase 1 sweeps stay green at 1000 too. The
100-seed column is the CI tier on the same code (`1373601`).

| Variant | 1000 seeds | 100 seeds | Catching check |
|---|---|---|---|
| SendBeforePersist | 1000/1000 | 100/100 | commit majority |
| TruncateOnEveryAppend | 1000/1000 | 100/100 | committed entries stay |
| NoPreVote | 1000/1000 | 100/100 | pre-vote's isolation property |
| ApplyBeforeCommit | 895/1000 | 92/100 | state machine safety |
| ResetTimerOnAnyRpc | 438/1000 | 44/100 | timers fire |
| CountOlderTermForCommit | 406/1000 | 42/100 | commit by current term — **with `max_batch` at its default**; 0/100 before the D-031 driver |
| SnapshotWithoutCurrentLast | 326/1000 | 26/100 | state machine safety after a crash mid-install |
| SingleMajorityInJointConsensus | 275/1000 | 28/100 | commit majority / leader completeness, membership scenario |
| LeaseTrustsTheClock | stale read caught on 41 of 503 drift-exceeded seeds; the guard revoked on all 503; "neither" on 0 | 2 of 52 | invariant 6 via the checker |

Phase 1 at 1000 seeds: engine — NoWalBeforeMemtable 984, ReleaseBeforeManifest
627, DeleteBeforeManifest 570, the correct engine green with 114 refusals; log —
AckBeforeSync 1000, NoChecksum 964, NoSyncDir 909, the correct log green; the
echo journal never vanished on 1000 seeds under the correct variant.

Context for the lease line: the two-trial change (`9d2bfec`) measurably doubled the
single-trial catch (2→4 of 46 at 100 seeds on the pre-D/E tree); the rate moves a
few counts with every schedule reshuffle (D-031 changed every seed's draws), which
is exactly why the nightly's 10 000 seeds are the number that matters.

Membership scenario at 1000 seeds: grows and shrinks completed on 1000/1000,
2456 learners promoted, 46 elections while joint, 432 step-downs of a leader
outside C_new, worst completion gap 469 ms against the 2 s bound, slowest write
after a heal 617 ms. The main sweep at 1000: 4820 partitions, 3165 crashes, 1143
Figure 8 drivers, 580 refusals, 17 046 lease revocations, 104 174 lease reads.

**Local runs at 10 000 seeds — two runs so far, both red on the correct server,
both false positives of one check; the third run is pending (numbers below are
still the 100-seed ones).**

> Corrected 2026-09-13: runs 1 and 2 were local ten-thousand-seed runs on this
> machine, not the GitHub nightly, so seeds 164 and 385 are local finds, here, in
> *What landed* and in the seed table below. The GitHub nightly's ten thousand
> seeds on `ea6fe7d` (run 34496762339) failed on seeds 5909, 6325 and 7381. Seed
> 164's follower was about sixty-eight entries behind, not two hundred, and no
> leader was reaching it in the stretch the check flagged; nor was it the first
> correct-server failure (seeds 9 and 60 below came before it). The footer line
> under *Every PROPOSED entry* now gives the next free number as it stood.
> `1373601`, `f54b468` and `74b93c5` exist only in the local clone; the same trees
> on `main` are `48e5276`, `635aea0` and `7e0792f`. See D-048.

- Run 1 (`1373601`, 2h45m awake): 15 of 16 suites green; the correct server
  failed at **seed 164** — the timers-fire check flagged as starved a follower
  about sixty-eight entries behind a compacting leader (appended through 208, the
  leader's log through 276), fed only by `InstallSnapshot` chunks; no leader was
  reaching it in the stretch the check flagged — the 21 chunks in it, from
  12.9405 s, were the leftover stream of server 3, which had lost its quorum at
  12.936 s — and a leader was elected 40 ms after the flag. Checker gap: the
  check counted only `AppendEntries` as the leader's contact. Fixed in `f54b468`
  (D-030 stanza), verified on seeds 0–199. Every variant test passed at 10k, but
  their rates were swallowed by cargo's output capture (`nightly.yml` lacks
  `--nocapture`).
- Run 2 (`f54b468`): the correct server failed at **seed 385**, the sibling
  case — a follower cut off alone mid-install, whose install completion rebuilt
  its core with a fresh timer (stage E's incarnation switch), campaigning 25 ms
  past the bound. The check now treats the install's restatement as the
  leader's contact: **PROPOSED D-039**, a checker-semantics decision for your
  approval; seeds 164 and 385 are pinned in the gate. The run was killed at ~14%
  because its verdict was already red and the Mac had been sleeping through
  most of its wall-clock (lid closed, on battery).
- Run 3 was started and then killed by decision: ten thousand seeds are no
  longer a laptop's job. The sweeps now run their seeds in parallel and in four
  tiers (D-040) — 20 at the gate, 100 in CI, 1000 under `scripts/premerge.sh`,
  10 000 only in the nightly on GitHub — and the numbers above are the
  1000-seed tier's. The clean 10k verdict is the GitHub nightly's to give, on
  this branch by `workflow_dispatch` once pushed and nightly on `main` after the
  merge; its catch rates are now printed into the job log.
- `scripts/premerge.sh` on this machine (4 performance + 4 efficiency cores):
  **~12.7 min of tests** — raft binary 667–705 s, engine 81 s, wal 8 s, the rest
  seconds — with an instant warm build; under the 15-minute target. Two runs
  gave identical catch rates and coverage, field for field. (Both wall-clock
  readings said 28 min because the lid was closed mid-run and the Mac took a
  fourteen-minute *Clamshell Sleep* each time; libtest's clock stops during
  sleep, `date` does not. `caffeinate -i` does not prevent clamshell sleep.)
- The raft sweep is about four times heavier since stage E: 100 seeds take 81 s
  on this machine against 23 s on the pre-E tree, because snapshots on every
  schedule lengthen every trace and the sweep's periodic safety check
  (`advance` in `sim/raft.rs`) clones the whole trace so far every ten slices
  and re-runs every fold from scratch — quadratic in trace length, and the
  hottest frames in a profile (`Vec<TraceRecord>::clone`, the drops, `replay`).
  Backlog: fold incrementally over the new suffix. The parallel driver neither
  caused nor cures this; it matches the old regime on a full binary and beats
  it at the tail (672 s for the raft binary at 1000 seeds against roughly 990 s
  extrapolated from the sequential runs).

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
  clock like a crash restart's. From seed 385 of a local ten-thousand-seed run;
  protocol unchanged.

D-034 is an unused number (allocation gap between parallel branches); the next
entry is D-041, though the footer, not moved when D-040 was added, read D-040. Every
PROPOSED site is marked `// PROPOSED(D-0xx)` in code.
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
| **local 10k run, seed 164** | The first correct-server failure a ten-thousand-seed run of the raft sweep produced: the timer check read a snapshot-fed follower as starved; `InstallSnapshot` was not counted as the leader's contact | Checker fix `f54b468`, D-030 stanza; pinned |
| **local 10k run, seed 385** | Its sibling: a follower alone mid-install, the install's incarnation switch reset its timer, campaigned 25 ms past the bound | Checker models the switch — PROPOSED D-039; pinned |

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
  server; **the sweep's periodic safety check made incremental** (it re-folds
  the whole trace every ten slices; quadratic, and the dominant cost of a raft
  seed since stage E — see the operational notes).
- **Moirae** — no changes needed; no PR opened.

## Pushing the branch and opening the PR

This machine has no `gh`, no Homebrew, and no GitHub credential in the keychain;
`git` uses the `osxkeychain` helper and `user.name` / `user.email` are unset, so
the branch's commits are authored `Makis <makis@Makiss-MacBook-Pro.local>` while
`main`'s are `pchrysostomou <prodromosch@hotmail.co.uk>`.

1. Identity, once: `git config --global user.name pchrysostomou` and
   `git config --global user.email prodromosch@hotmail.co.uk`.
2. A token: GitHub → Settings → Developer settings → Fine-grained tokens, repository
   `pchrysostomou/ananke`, permissions Contents and Pull requests read/write. Store it
   for git without ever pasting it into a chat:
   `printf 'protocol=https\nhost=github.com\nusername=pchrysostomou\npassword=TOKEN\n' | git credential-osxkeychain store`
   (or run any `git push` in a terminal and let the prompt store it).
3. Re-author the unpushed commits to the identity above (optional; not a force-push,
   the branch has never been pushed):
   `FILTER_BRANCH_SQUELCH_WARNING=1 git filter-branch -f --env-filter 'export GIT_AUTHOR_NAME=pchrysostomou GIT_AUTHOR_EMAIL=prodromosch@hotmail.co.uk GIT_COMMITTER_NAME=pchrysostomou GIT_COMMITTER_EMAIL=prodromosch@hotmail.co.uk' main..phase-2-overnight`
4. `git push -u origin phase-2-overnight`, then open
   `https://github.com/pchrysostomou/ananke/compare/main...phase-2-overnight?expand=1`
   and create the PR (a body was drafted in the session's hand-over, beside the
   repo, not in it).
   The nightly workflow runs the ten thousand seeds on the branch once it is
   pushed (`workflow_dispatch`), and nightly on `main` after the merge.

## Operational notes

- Three agent instances died to a 600 s no-output watchdog mid-build and were
  resumed from their on-disk state; no work was lost beyond wall-clock.
- The second 10k run lost most of its wall-clock to the Mac sleeping (battery,
  lid closed: sleep → 2 s DarkWake → sleep, 13:28–14:32). A 10k run here needs
  ~3 h awake, which is why ten thousand moved to GitHub (D-040).
- The nightly's catch rates were invisible until `--nocapture` was added to
  `nightly.yml`'s test command; `scripts/premerge.sh` prints them the same way.
- The `ananke-22/-d/-e/-int` worktrees are left in place beside the repo for
  archaeology; `git worktree remove` them when done.
