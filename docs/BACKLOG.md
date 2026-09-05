# BACKLOG.md — ananke

Deferred ideas live in the issue tracker, one issue per idea, labelled by the phase
they belong to:

- [Every open issue](https://github.com/pchrysostomou/ananke/issues), by phase:
  [`phase:0`](https://github.com/pchrysostomou/ananke/issues?q=is%3Aissue+is%3Aopen+label%3Aphase%3A0),
  [`phase:1`](https://github.com/pchrysostomou/ananke/issues?q=is%3Aissue+is%3Aopen+label%3Aphase%3A1),
  [`phase:2`](https://github.com/pchrysostomou/ananke/issues?q=is%3Aissue+is%3Aopen+label%3Aphase%3A2)
  and so on.
- [`good first issue`](https://github.com/pchrysostomou/ananke/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22):
  the approachable ones, contained in one crate with a test to write.
- [`help wanted`](https://github.com/pchrysostomou/ananke/issues?q=is%3Aissue+is%3Aopen+label%3A%22help+wanted%22):
  the ones that need a design conversation, usually a DECISIONS.md entry, before code.

The rule stays what it was: anything tempting that the current phase does not need
becomes an issue with one line of justification and the phase label, never a change.
Promote an idea by writing its DECISIONS.md entry and moving it into SPEC.md.

Required before Phase 2 starts, demanded by SPEC §1.4 and the D-015 rationale that
protocols are written against loss and reorder from the start:
[message duplication (#1)](https://github.com/pchrysostomou/ananke/issues/1).
