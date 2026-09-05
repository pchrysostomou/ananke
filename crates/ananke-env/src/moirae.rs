//! The moirae bridge (SPEC §1.5): a simulation's trace as a moirae trace, format v2,
//! written through `moirae-trace` so it opens in the moirae studio and hashes like any
//! moirae trace.
//!
//! The mapping, record by record:
//!
//! | ananke                                   | moirae line                                   |
//! |------------------------------------------|-----------------------------------------------|
//! | `MessageSent`                            | `send` with `msgId` and the decoded payload   |
//! | `MessageDelivered`                       | `deliver`                                     |
//! | `MessageDropped`                         | `drop` with `loss`, `partition`, `crashed` or `queue-full` |
//! | `NodeCrashed` / `NodeRestarted`          | `fault` `crash` (no field lists) / `restart`  |
//! | `PartitionStarted` / `PartitionHealed`   | `fault` `partition` / `heal`                  |
//! | `LinkBlocked` / `LinkUnblocked`          | `log` `ananke.link.blocked` / `.unblocked`    |
//! | `TaskSpawned` / `TaskPolled` / `TaskCompleted` | `log` `ananke.task.*`; polls are optional |
//! | `PollBudgetExceeded`                     | `log` `ananke.task.budget-exceeded`           |
//! | `FsyncLost` / `WriteTorn`                | `log` `ananke.fs.fsync-lost` / `.write-torn`  |
//! | `BlockRotted`                            | `log` `ananke.fs.bit-rot`                     |
//! | `DirectoryEntryLost`                     | `log` `ananke.fs.dir-entry-lost`              |
//! | `WalSegmentOpened` / `WalSynced`         | `log` `ananke.wal.segment-opened` / `.synced` |
//! | `WalTruncated` / `WalRecovered`          | `log` `ananke.wal.truncated` / `.recovered`   |
//! | `TimeAdvanced`                           | nothing: every line carries `t`               |
//!
//! `t` is global virtual time in nanoseconds and the header says `unit: "ns"`. Node ids
//! pass through unchanged: ananke numbers nodes from 1 exactly because moirae does
//! (see [`NodeId`]). Addresses become node ids through the table of every address a
//! node ever bound; a message to an address nobody ever bound becomes a
//! `ananke.net.unmapped` log line, since moirae has no lane for it. The header's
//! `ananke` object records the version, the policy, the fault configuration as
//! integers, each node's clock skew and drift, and the address table.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use moirae_trace::{Cause, Collect, Error, Event, Header, Json, Sink, TimeUnit, Verify, Writer};

use crate::sim::{Sim, Snapshot, TraceRecord};
use crate::{DirEntryOp, DropReason, NodeId, TraceEvent, WalStopReason};

/// Turns a message payload into the `msg` object of a `send` line: an object whose
/// `type` is a string, which is what the studio labels and filters by.
pub type Decoder = dyn Fn(&[u8]) -> Json + Send + Sync;

/// The decoder for payloads nobody can read: `{"type":"bytes","len":N}`.
#[must_use]
pub fn bytes_decoder(payload: &[u8]) -> Json {
    Json::obj(vec![
        ("type", Json::str("bytes")),
        (
            "len",
            Json::Int(i64::try_from(payload.len()).unwrap_or(i64::MAX)),
        ),
    ])
}

/// How to export a trace.
#[derive(Clone, Copy)]
pub struct Export<'a> {
    /// Decodes each message payload for the `send` line.
    pub decoder: &'a Decoder,
    /// Whether every `TaskPolled` becomes a `log` line. On, the trace records every
    /// scheduling decision (SPEC §1.1); off, it is a third the size for the studio.
    pub polls: bool,
}

impl<'a> Export<'a> {
    /// Every scheduling decision included.
    #[must_use]
    pub fn new(decoder: &'a Decoder) -> Self {
        Self {
            decoder,
            polls: true,
        }
    }

    /// Without the `ananke.task.polled` lines.
    #[must_use]
    pub fn without_polls(self) -> Self {
        Self {
            polls: false,
            ..self
        }
    }
}

impl std::fmt::Debug for Export<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Export")
            .field("polls", &self.polls)
            .finish_non_exhaustive()
    }
}

impl Sim {
    /// The trace so far as moirae JSONL, format v2. Two runs with the same config
    /// produce identical bytes; CI pins their hash.
    ///
    /// # Errors
    ///
    /// A seed, time or integer beyond what a JavaScript reader keeps exact, or a decoder
    /// that returned something other than an object.
    pub fn to_moirae(&self, export: &Export<'_>) -> Result<String, Error> {
        Ok(write(&self.snapshot(), export, Collect::default())?.jsonl())
    }

    /// Replays the export against a recorded trace and stops at the first line that
    /// differs. This is how a recording proves it still reproduces after a change, and
    /// says where it stopped doing so.
    ///
    /// # Errors
    ///
    /// [`Error::Divergence`] naming the first differing line, or
    /// [`Error::LongerThanRecording`]; a recording longer than this trace is reported as
    /// a divergence at the first missing line.
    pub fn verify_moirae(&self, recorded: &str, export: &Export<'_>) -> Result<(), Error> {
        let sink = write(&self.snapshot(), export, Verify::against(recorded))?;
        if sink.complete() {
            Ok(())
        } else {
            Err(Error::Divergence {
                line: sink.matched() + 1,
                expected: "<a further recorded line>".to_owned(),
                actual: "<end of this trace>".to_owned(),
            })
        }
    }
}

fn write<S: Sink>(snapshot: &Snapshot, export: &Export<'_>, sink: S) -> Result<S, Error> {
    let mut w = Writer::new(sink);
    w.header(&header(snapshot))?;
    for (node, _, _) in &snapshot.clocks {
        w.emit(&Event::Init {
            t: 0,
            node: node.get(),
        })?;
    }
    let addrs: BTreeMap<SocketAddr, NodeId> = snapshot.addrs.iter().copied().collect();
    for record in &snapshot.records {
        if let Some(event) = convert(record, &addrs, export) {
            w.emit(&event)?;
        }
    }
    Ok(w.into_sink())
}

fn header(snapshot: &Snapshot) -> Header {
    let c = &snapshot.config;
    let ppm = |p: f64| Json::Int((p * 1_000_000.0).round() as i64);
    let ns = |d: std::time::Duration| Json::Int(i64::try_from(d.as_nanos()).unwrap_or(i64::MAX));
    let config = Json::obj(vec![
        ("pDropPpm", ppm(c.net.p_drop)),
        ("delayMinNs", ns(c.net.delay_min)),
        ("delayMaxNs", ns(c.net.delay_max)),
        ("pDurablePpm", ppm(c.fs.p_durable)),
        ("maxSkewNs", ns(c.clock.max_skew)),
        ("maxDriftPpm", Json::Int(i64::from(c.clock.max_drift_ppm))),
        (
            "runLengthHint",
            Json::Int(i64::try_from(c.run_length_hint).unwrap_or(i64::MAX)),
        ),
    ]);
    let clocks = snapshot
        .clocks
        .iter()
        .map(|(node, skew, drift)| {
            Json::obj(vec![
                ("node", node_json(*node)),
                ("skewNs", Json::Int(*skew)),
                ("driftPpm", Json::Int(*drift)),
            ])
        })
        .collect();
    let addrs = snapshot
        .addrs
        .iter()
        .map(|(addr, node)| {
            Json::obj(vec![
                ("node", node_json(*node)),
                ("addr", Json::str(&addr.to_string())),
            ])
        })
        .collect();
    Header {
        seed: c.seed,
        nodes: u32::try_from(snapshot.clocks.len()).expect("node count fits u32"),
        unit: TimeUnit::Ns,
        network: None,
        extra: vec![(
            "ananke".to_owned(),
            Json::obj(vec![
                ("version", Json::str(env!("CARGO_PKG_VERSION"))),
                ("policy", Json::str(&snapshot.policy.name())),
                ("config", config),
                ("clocks", Json::Array(clocks)),
                ("addrs", Json::Array(addrs)),
            ]),
        )],
    }
}

fn node_json(node: NodeId) -> Json {
    Json::Int(i64::from(node.get()))
}

/// A number, or its decimal string when it does not fit a JavaScript integer: a
/// rotted sequence number is still data, and the studio must still open the trace.
fn int(v: u64) -> Json {
    if v <= moirae_trace::MAX_SAFE_INTEGER {
        Json::Int(i64::try_from(v).expect("below 2^53"))
    } else {
        Json::Str(v.to_string())
    }
}

fn convert(
    record: &TraceRecord,
    addrs: &BTreeMap<SocketAddr, NodeId>,
    export: &Export<'_>,
) -> Option<Event> {
    let t = record.at.as_nanos();
    // Every record below except the partition ones is recorded with its node; 0 never
    // occurs and would be visible in the studio as an unknown lane if it did.
    let node = record.node.map_or(0, NodeId::get);
    let log = |event: &str, data: Option<Json>| {
        Some(Event::Log {
            t,
            node,
            event: event.to_owned(),
            data,
        })
    };
    let path_data =
        |path: &std::path::Path| Json::obj(vec![("path", Json::str(&path.display().to_string()))]);
    match &record.event {
        TraceEvent::TaskSpawned { task, name } => log(
            "ananke.task.spawned",
            Some(Json::obj(vec![
                ("task", int(task.get())),
                ("name", Json::str(name)),
            ])),
        ),
        TraceEvent::TaskPolled { task } => {
            if export.polls {
                log(
                    "ananke.task.polled",
                    Some(Json::obj(vec![("task", int(task.get()))])),
                )
            } else {
                None
            }
        }
        TraceEvent::TaskCompleted { task } => log(
            "ananke.task.completed",
            Some(Json::obj(vec![("task", int(task.get()))])),
        ),
        TraceEvent::PollBudgetExceeded { task, polls } => log(
            "ananke.task.budget-exceeded",
            Some(Json::obj(vec![
                ("task", int(task.get())),
                ("polls", int(*polls)),
            ])),
        ),
        TraceEvent::TimeAdvanced { .. } => None,
        TraceEvent::MessageSent {
            id,
            from,
            to,
            payload,
        } => match (addrs.get(from), addrs.get(to)) {
            (Some(f), Some(to_node)) => Some(Event::Send {
                t,
                from: f.get(),
                to: to_node.get(),
                msg_id: id.get(),
                msg: (export.decoder)(payload),
            }),
            _ => log(
                "ananke.net.unmapped",
                Some(Json::obj(vec![
                    ("msgId", int(id.get())),
                    ("from", Json::str(&from.to_string())),
                    ("to", Json::str(&to.to_string())),
                ])),
            ),
        },
        TraceEvent::MessageDelivered { id, .. } => Some(Event::Deliver {
            t,
            msg_id: id.get(),
            dup: false,
        }),
        TraceEvent::MessageDropped { id, reason, .. } => Some(Event::Drop {
            t,
            msg_id: id.get(),
            reason: match reason {
                DropReason::Injected => "loss",
                DropReason::Partitioned => "partition",
                DropReason::Unreachable => "crashed",
                DropReason::QueueFull => "queue-full",
            }
            .to_owned(),
        }),
        TraceEvent::FsyncLost { path } => log("ananke.fs.fsync-lost", Some(path_data(path))),
        TraceEvent::BlockRotted {
            path,
            block,
            offset,
            bit,
        } => log(
            "ananke.fs.bit-rot",
            Some(Json::obj(vec![
                ("path", Json::str(&path.display().to_string())),
                ("block", int(*block)),
                ("offset", int(*offset)),
                ("bit", Json::Int(i64::from(*bit))),
            ])),
        ),
        TraceEvent::WriteTorn {
            path,
            offset,
            written,
            kept,
        } => log(
            "ananke.fs.write-torn",
            Some(Json::obj(vec![
                ("path", Json::str(&path.display().to_string())),
                ("offset", int(*offset)),
                ("written", int(*written as u64)),
                ("kept", int(*kept as u64)),
            ])),
        ),
        TraceEvent::DirectoryEntryLost { dir, entry, op } => log(
            "ananke.fs.dir-entry-lost",
            Some(Json::obj(vec![
                ("dir", Json::str(&dir.display().to_string())),
                ("entry", Json::str(&entry.display().to_string())),
                (
                    "op",
                    Json::str(match op {
                        DirEntryOp::Link => "link",
                        DirEntryOp::Unlink => "unlink",
                        DirEntryOp::Rename => "rename",
                    }),
                ),
            ])),
        ),
        TraceEvent::WalSegmentOpened { segment, first } => log(
            "ananke.wal.segment-opened",
            Some(Json::obj(vec![
                ("segment", int(*segment)),
                ("first", int(*first)),
            ])),
        ),
        TraceEvent::WalSynced {
            segment,
            first,
            up_to,
        } => log(
            "ananke.wal.synced",
            Some(Json::obj(vec![
                ("segment", int(*segment)),
                ("first", int(*first)),
                ("upTo", int(*up_to)),
            ])),
        ),
        TraceEvent::WalTruncated { segment, len } => log(
            "ananke.wal.truncated",
            Some(Json::obj(vec![
                ("segment", int(*segment)),
                ("len", int(*len)),
            ])),
        ),
        TraceEvent::WalRecovered {
            records,
            stop,
            discarded,
        } => log(
            "ananke.wal.recovered",
            Some(Json::obj(vec![
                ("records", int(*records)),
                (
                    "stop",
                    stop.map_or(Json::Null, |stop| {
                        let mut fields = vec![
                            ("segment", int(stop.segment)),
                            ("offset", int(stop.offset)),
                            ("reason", Json::str(stop.reason.as_str())),
                        ];
                        if let WalStopReason::Gap { expected, found } = stop.reason {
                            fields.push(("expected", int(expected)));
                            fields.push(("found", int(found)));
                        }
                        Json::obj(fields)
                    }),
                ),
                ("discarded", int(*discarded)),
            ])),
        ),
        TraceEvent::NodeCrashed { node } => Some(Event::Crash {
            t,
            node: node.get(),
            cause: Cause::Schedule,
            fields: None,
        }),
        TraceEvent::NodeRestarted { node } => Some(Event::Restart {
            t,
            node: node.get(),
        }),
        TraceEvent::PartitionStarted { groups } => Some(Event::Partition {
            t,
            groups: groups_of(groups),
        }),
        TraceEvent::PartitionHealed { groups } => Some(Event::Heal {
            t,
            groups: groups_of(groups),
        }),
        TraceEvent::LinkBlocked { from, to } => Some(Event::Log {
            t,
            node: from.get(),
            event: "ananke.link.blocked".to_owned(),
            data: Some(Json::obj(vec![
                ("from", node_json(*from)),
                ("to", node_json(*to)),
            ])),
        }),
        TraceEvent::LinkUnblocked { from, to } => Some(Event::Log {
            t,
            node: from.get(),
            event: "ananke.link.unblocked".to_owned(),
            data: Some(Json::obj(vec![
                ("from", node_json(*from)),
                ("to", node_json(*to)),
            ])),
        }),
    }
}

fn groups_of(groups: &[Vec<NodeId>]) -> Vec<Vec<u32>> {
    groups
        .iter()
        .map(|g| g.iter().map(|n| n.get()).collect())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::path::Path;
    use std::time::Duration;

    use bytes::Bytes;

    use super::*;
    use crate::sim::{Sim, SimConfig};
    use crate::{Clock, Environment, File, FileSystem, Instant, Network, OpenOptions, Socket};

    fn addr(n: u16) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, 1], n))
    }

    /// Two nodes: a delivered message, one to nobody, one into a partition, a lost
    /// fsync, a crash and a restart.
    fn scenario() -> Sim {
        let mut config = SimConfig::new(11);
        config.fs.p_durable = 0.0;
        config.net.delay_min = Duration::from_millis(1);
        config.net.delay_max = Duration::from_millis(1);
        let mut sim = Sim::new(config);
        let (a, b) = (sim.add_node(), sim.add_node());
        let env_b = sim.env(b);
        env_b.clone().spawn("receiver", async move {
            let sock = env_b.net().bind(addr(2)).await.unwrap();
            loop {
                sock.recv().await.unwrap();
            }
        });
        let env_a = sim.env(a);
        env_a.clone().spawn("sender", async move {
            let sock = env_a.net().bind(addr(1)).await.unwrap();
            let f = env_a
                .fs()
                .open(
                    Path::new("/journal"),
                    OpenOptions::new().write(true).create(true),
                )
                .await
                .unwrap();
            f.write_at(0, Bytes::from_static(b"entry")).await.unwrap();
            f.sync().await.unwrap();
            sock.send(addr(2), Bytes::from_static(b"hi")).await.unwrap();
            sock.send(addr(9), Bytes::from_static(b"void"))
                .await
                .unwrap();
            env_a.clock().sleep(Duration::from_millis(10)).await;
            sock.send(addr(2), Bytes::from_static(b"walled"))
                .await
                .unwrap();
        });
        sim.run_until(Instant::from_nanos(5_000_000));
        sim.partition(&[a], &[b]);
        sim.run_until(Instant::from_nanos(20_000_000));
        sim.heal();
        sim.crash(b);
        sim.restart(b);
        sim.run_until(Instant::from_nanos(30_000_000));
        sim
    }

    #[test]
    fn exports_every_kind_in_moirae_v2() {
        let sim = scenario();
        let jsonl = sim.to_moirae(&Export::new(&bytes_decoder)).unwrap();
        let lines: Vec<&str> = jsonl.lines().collect();
        assert!(lines[0].starts_with("{\"kind\":\"header\",\"v\":2,\"seed\":11,\"nodes\":2,\"unit\":\"ns\",\"ananke\":{\"version\":\""), "{}", lines[0]);
        assert!(
            lines[0].contains("\"policy\":\"")
                && lines[0].contains("\"clocks\":[{\"node\":1,")
                && lines[0].contains("\"addrs\":[{\"node\":1,\"addr\":\"10.0.0.1:1\"}")
        );
        assert_eq!(lines[1], "{\"t\":0,\"seq\":0,\"kind\":\"init\",\"node\":1}");
        assert_eq!(lines[2], "{\"t\":0,\"seq\":1,\"kind\":\"init\",\"node\":2}");
        let has = |needle: &str| jsonl.contains(needle);
        assert!(has(
            "\"kind\":\"send\",\"from\":1,\"to\":2,\"msgId\":0,\"msg\":{\"type\":\"bytes\",\"len\":2}}"
        ));
        assert!(has("\"kind\":\"deliver\",\"msgId\":0}"));
        assert!(has(
            "\"event\":\"ananke.net.unmapped\",\"data\":{\"msgId\":1,\"from\":\"10.0.0.1:1\",\"to\":\"10.0.0.1:9\"}"
        ));
        assert!(has("\"kind\":\"drop\",\"msgId\":1,\"reason\":\"crashed\"}"));
        assert!(has(
            "\"kind\":\"drop\",\"msgId\":2,\"reason\":\"partition\"}"
        ));
        assert!(has(
            "\"kind\":\"fault\",\"fault\":\"partition\",\"groups\":[[1],[2]]}"
        ));
        assert!(has(
            "\"kind\":\"fault\",\"fault\":\"heal\",\"groups\":[[1],[2]]}"
        ));
        assert!(has(
            "\"kind\":\"fault\",\"fault\":\"crash\",\"node\":2,\"cause\":\"schedule\"}"
        ));
        assert!(has("\"kind\":\"fault\",\"fault\":\"restart\",\"node\":2}"));
        assert!(has(
            "\"event\":\"ananke.fs.fsync-lost\",\"data\":{\"path\":\"/journal\"}"
        ));
        assert!(has(
            "\"event\":\"ananke.task.spawned\",\"data\":{\"task\":1,\"name\":\"receiver\"}"
        ));
        assert!(has("\"event\":\"ananke.task.polled\""));
        assert!(has("\"event\":\"ananke.task.completed\""));
        assert!(
            !has("TimeAdvanced") && !has("ananke.time"),
            "time advances are not lines"
        );
        assert!(
            has("\"unit\":\"ns\"") && has("\"t\":20000000,"),
            "t is in nanoseconds"
        );
    }

    #[test]
    fn polls_can_be_left_out_and_the_decoder_is_used() {
        let sim = scenario();
        let full = sim.to_moirae(&Export::new(&bytes_decoder)).unwrap();
        let slim = sim
            .to_moirae(&Export::new(&bytes_decoder).without_polls())
            .unwrap();
        assert!(full.lines().count() > slim.lines().count());
        assert!(!slim.contains("ananke.task.polled"));
        let custom = |payload: &[u8]| {
            Json::obj(vec![(
                "type",
                Json::str(std::str::from_utf8(payload).unwrap_or("?")),
            )])
        };
        let decoded = sim.to_moirae(&Export::new(&custom)).unwrap();
        assert!(decoded.contains("\"msg\":{\"type\":\"hi\"}"));
    }

    #[test]
    fn verify_accepts_its_own_export_and_names_the_first_divergent_line() {
        let sim = scenario();
        let export = Export::new(&bytes_decoder);
        let jsonl = sim.to_moirae(&export).unwrap();
        sim.verify_moirae(&jsonl, &export).unwrap();
        let mut lines: Vec<String> = jsonl.lines().map(str::to_owned).collect();
        lines[4] = lines[4].replace("\"t\":", "\"t\":1");
        let tampered = lines.join("\n") + "\n";
        match sim.verify_moirae(&tampered, &export) {
            Err(Error::Divergence { line: 5, .. }) => {}
            other => panic!("expected a divergence at line 5, got {other:?}"),
        }
        let truncated = jsonl.lines().take(10).collect::<Vec<_>>().join("\n") + "\n";
        assert!(matches!(
            sim.verify_moirae(&truncated, &export),
            Err(Error::LongerThanRecording { line: 11 })
        ));
        let extended = jsonl.clone() + "{\"t\":99,\"seq\":999,\"kind\":\"init\",\"node\":1}\n";
        assert!(matches!(
            sim.verify_moirae(&extended, &export),
            Err(Error::Divergence { .. })
        ));
    }

    #[test]
    fn same_seed_same_bytes() {
        let a = scenario().to_moirae(&Export::new(&bytes_decoder)).unwrap();
        let b = scenario().to_moirae(&Export::new(&bytes_decoder)).unwrap();
        assert_eq!(a, b);
        assert_eq!(moirae_trace::trace_hash(&a), moirae_trace::trace_hash(&b));
    }
}
