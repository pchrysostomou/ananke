# The v0.3.0 store fixture

`store/` is a Raft store written by ananke-raft 0.3.0's own code, the `v0.3.0` tag
(0d30df5), and nothing else. It is kept so that D-059 is tested against a store 0.3.0
really wrote: a later build opens it and must refuse it at open with an error naming its
format version, 1, and the one the build expects, without reading any key of it as the
build's own (D-059).

Never regenerate it with a later tree, and never edit its bytes: a store a later build
made would test nothing about 0.3.0's.

## How it was made

`generate.rs` beside this file is the program. It is not compiled here; it was copied into
the tag's tree as an example and run there, with the tag's `Cargo.lock` and toolchain (Rust
1.98.1). From this repository's root, with `<scratch>` any scratch directory:

```
git worktree add --detach <scratch>/v030 v0.3.0
mkdir -p <scratch>/v030/crates/ananke-raft/examples
cp crates/ananke-raft/tests/fixtures/v0.3.0-store/generate.rs \
   <scratch>/v030/crates/ananke-raft/examples/v030_store_fixture.rs
(cd <scratch>/v030 && CARGO_TARGET_DIR=<scratch>/target-v030 \
   cargo run -p ananke-raft --example v030_store_fixture -- <scratch>/fixture)
cp -R <scratch>/fixture crates/ananke-raft/tests/fixtures/v0.3.0-store/store
git worktree remove --force <scratch>/v030
```

`--force` is needed only because the example is an untracked file; no tracked file of the
tag's tree is changed. The program writes the store under the simulator, which makes it
the same bytes on every run, and copies the files out through `RealEnv`. Four runs, the
last of `generate.rs` as it is here, were compared with `diff -r` and are identical.

## What it holds

The program does what a server does, in its order: the engine opened as `node.rs` opens it
(a 512-byte memtable and 4 KiB log segments, so some state is in tables and some only in
the log), the store opened, which writes incarnation 1, the `RAFT-STORE` marker, one persist
of term 2 with a vote for server 1, a configuration entry of voters 1, 2 and 3 at index 1
and six commands at 2 to 7 with the configuration key, applies of 1 to 4, a snapshot taken at
index 4 into `/raft/snap-4-1`, the log compacted to it, the apply of 5, and a persist of term
3 with a vote for server 2 and one more entry at 8. Under 0.3.0's layout (RAFT.md §3 as the
tag has it):

| Key | Value |
|---|---|
| `0 / 0 / hard` | term 3, vote 2 |
| `0 / 0 / applied` | 5 |
| `0 / 0 / incarnation` | 1 |
| `0 / 1 / <index>` | entries 5 to 8 (1 to 4 deleted by the compaction) |
| `0 / 2 / config` | index 1, voters 1, 2, 3 |
| `0 / 3 / snapshot` | index 4, term 2, taken, take 1, `/raft/snap-4-1` |
| `1 / 0 / a` | `3` (put 1, swapped to 3) |
| `1 / 0 / b` | deleted (put 2, then deleted) |

and no format version, since 0.3.0 records none. `tests/v030_store.rs` asserts each of
these at the engine, under the keys spelled out byte by byte.

## Files

| SHA-256 | Bytes | File |
|---|---:|---|
| `b84f988c760e8d56d5de4b72823d908812d0a4476f89d381606da0a8f5fe7f13` | 651 | `000001.sst` |
| `a73f7e1dc28145f919943b9ff4dd6641e51a0e691b4026f8def5c77e48a3551e` | 1434 | `000001.wal` |
| `d53aa45cebe1a7fe9b973d361b7e3b6b245c4848a98747bde5b3dec886eec4d9` | 323 | `000002.sst` |
| `9f2ad7ad00ed6f93450a3f892b8a1fa45988351dc8aae9ea9b1a00f336f11a73` | 437 | `000003.sst` |
| `ef4fb60a929b99e2f5a6beae2f32dc9cbd0e62d0e52a6db6c505d173f1b5f8f1` | 25 | `CURRENT` |
| `a7f9876bab5715f08e6217b948aba459b3079ed444e06fe214dd64603bda0420` | 44 | `MANIFEST-000001` |
| `ef2438205be9729c7e6934d4ea8b272f5ac1850b2f8b1b34361cb7032b3204bb` | 131 | `MANIFEST-000002` |
| `c523dda4e49e28d3b042faba98474b8db72f044bb44d65060686c78edde7ca3c` | 216 | `MANIFEST-000003` |
| `2bee6f6b2e4912fa47cc2dcd222522dfc0323bcc70842875d317da28774139ee` | 301 | `MANIFEST-000004` |
| `e3c5d35c75b57192475cb354c7f8306f2b79f7380f50a0506f99cc9712323064` | 18 | `RAFT-STORE` |
| `b84f988c760e8d56d5de4b72823d908812d0a4476f89d381606da0a8f5fe7f13` | 651 | `snap-4-1/000001.sst` |
| `d53aa45cebe1a7fe9b973d361b7e3b6b245c4848a98747bde5b3dec886eec4d9` | 323 | `snap-4-1/000002.sst` |
| `c85133ae4cbb3990b99b86ab789c0f6b25e6a093c9d5a4257a69ec97e3f1a013` | 274 | `snap-4-1/000003.sst` |
| `25848d898fb4b9ae7d9640994affa38948e5292d31e8168b8bfdcb8fa8abbef5` | 25 | `snap-4-1/CURRENT` |
| `a71194a78fe6557abd1357d5816f81cf62229757d4bd950cd3f62705b7a32f5e` | 309 | `snap-4-1/MANIFEST-000001` |

5 162 bytes in fifteen files. `shasum -a 256` over `store/` reproduces the column.
