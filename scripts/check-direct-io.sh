#!/usr/bin/env bash
# Textual companion to clippy's `disallowed-methods` / `disallowed-types` (clippy.toml).
# DECISIONS.md D-003 asks for both: clippy resolves paths precisely, this catches the
# `use` lines and re-exports it can miss. Fails if any Rust source outside
# crates/ananke-env names a banned I/O path on a non-comment line.
set -euo pipefail
cd "$(dirname "$0")/.."

pattern='\b(std::time::(Instant|SystemTime)\b|std::fs::|std::net::(TcpListener|TcpStream|UdpSocket|ToSocketAddrs)\b|std::thread::(spawn|scope|sleep|Builder)\b|std::collections::(HashMap|HashSet)\b|std::hash::RandomState\b|tokio::(net|fs|task)::|tokio::spawn\b|tokio::time::(sleep|sleep_until|timeout|timeout_at|interval|interval_at|Instant|Sleep|Interval|Timeout)\b|rand::|rand_core::|getrandom::|hashbrown::|ahash::|foldhash::)'

hits=$(grep -rnE --include='*.rs' "$pattern" crates sim \
    | grep -vE '^crates/ananke-env/' \
    | grep -vE '^[^:]+:[0-9]+:[[:space:]]*//' || true)

if [ -n "$hits" ]; then
    echo "Direct I/O outside crates/ananke-env (BOOTSTRAP_PROMPT.md principle 1):" >&2
    echo "$hits" >&2
    exit 1
fi

# Only three files may switch the lints off: the `real` module, the SystemTime edge
# conversions in time.rs, and the DetHashMap aliases in collections.rs.
allows=$(grep -rnE --include='*.rs' 'clippy::disallowed_(methods|types)' crates sim \
    | grep -vE '^crates/ananke-env/src/(real/mod\.rs|time\.rs|collections\.rs):' || true)

if [ -n "$allows" ]; then
    echo "allow(clippy::disallowed_*) outside the sanctioned files (see clippy.toml):" >&2
    echo "$allows" >&2
    exit 1
fi
echo "check-direct-io: clean"
