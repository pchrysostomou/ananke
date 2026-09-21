# ananke-shard

The range layer of [ananke](https://github.com/pchrysostomou/ananke): the layer that
makes a *node*, not a Raft group, the unit of the system (`docs/SHARD.md` §4). Many
ranges share one socket, one ticker and one engine, so everything a node sends and
receives is tagged with the range it belongs to.

What is here today is the wire: the batch frame, which carries several messages each
tagged with its 8-byte range id and wraps `ananke-raft`'s message codec without
changing it; the per-peer outbox, which cuts a flush's sends into frames under the
socket's `MAX_FRAME_LEN`; the node's inbox, one per node, bounded in bytes and admitting
in constant time; and the studio decoder, so a frame of six messages reads in the moirae
studio as six messages and not as one.

`ananke-raft` does not depend on this crate and names no range: the core, the codec, the
store, snapshots, refusal and adoption stay there (`docs/SHARD.md` §13, Q40). Licensed
under MIT or Apache-2.0, at your option.
