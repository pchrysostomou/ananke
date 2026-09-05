//! The in-memory filesystem with the SPEC.md §1.3 fault model.
//!
//! Every inode keeps what the running node sees (`visible`), what would survive a
//! crash (`durable`) and the writes issued since the last successful `sync`. A sync
//! makes everything durable with probability `p_durable`, otherwise returns Ok while
//! persisting nothing (lost fsync). At a crash, a random prefix of the pending writes
//! survives and the next write may be torn to a random prefix of itself. Directory
//! entries follow the same rule: creating, removing or renaming a file is visible at
//! once but durable only after [`FileSystem::sync_dir`] on the directory, and at a crash
//! a random prefix of a directory's pending operations survives. A rename is recorded
//! against its destination's directory. Directories themselves are immediately durable.
//! Every operation takes a delay drawn from `FsFaults::latency_min..=latency_max` on
//! the `fs` stream, applied when the delay ends; with the default of zero it applies
//! at the instant it was issued and nothing is drawn.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;

use super::clock::SimSleep;
use super::state::{Shared, State};
use crate::{DirEntryOp, File, FileSystem, NodeId, OpenOptions, TraceEvent};

pub(super) type InodeId = u64;

pub(super) struct Inode {
    /// Last known name, for trace events; the entry may since have been renamed away.
    path: PathBuf,
    visible: Vec<u8>,
    durable: Vec<u8>,
    pending: Vec<PendingOp>,
}

enum PendingOp {
    Write { offset: u64, data: Bytes },
    Truncate(u64),
}

fn apply(target: &mut Vec<u8>, op: &PendingOp, limit: Option<usize>) {
    match op {
        PendingOp::Write { offset, data } => {
            let data = &data[..limit.unwrap_or(data.len())];
            let start = usize::try_from(*offset).expect("offset exceeds usize");
            if target.len() < start + data.len() {
                target.resize(start + data.len(), 0);
            }
            target[start..start + data.len()].copy_from_slice(data);
        }
        PendingOp::Truncate(size) => {
            target.resize(usize::try_from(*size).expect("size exceeds usize"), 0)
        }
    }
}

/// A directory operation not yet made durable by `sync_dir`.
enum DirOp {
    Link {
        path: PathBuf,
        inode: InodeId,
    },
    Unlink {
        path: PathBuf,
    },
    Rename {
        from: PathBuf,
        to: PathBuf,
        inode: InodeId,
    },
}

impl DirOp {
    fn entry(&self) -> &Path {
        match self {
            DirOp::Link { path, .. } | DirOp::Unlink { path } => path,
            DirOp::Rename { to, .. } => to,
        }
    }

    fn kind(&self) -> DirEntryOp {
        match self {
            DirOp::Link { .. } => DirEntryOp::Link,
            DirOp::Unlink { .. } => DirEntryOp::Unlink,
            DirOp::Rename { .. } => DirEntryOp::Rename,
        }
    }

    fn apply(&self, entries: &mut BTreeMap<PathBuf, InodeId>) {
        match self {
            DirOp::Link { path, inode } => {
                entries.insert(path.clone(), *inode);
            }
            DirOp::Unlink { path } => {
                entries.remove(path);
            }
            DirOp::Rename { from, to, inode } => {
                entries.remove(from);
                entries.insert(to.clone(), *inode);
            }
        }
    }
}

fn parent_of(path: &Path) -> PathBuf {
    path.parent().unwrap_or_else(|| Path::new("")).to_path_buf()
}

/// One node's disk.
pub(super) struct NodeFs {
    dirs: BTreeSet<PathBuf>,
    /// The namespace the running node sees.
    entries: BTreeMap<PathBuf, InodeId>,
    /// The namespace that survives a crash.
    durable_entries: BTreeMap<PathBuf, InodeId>,
    /// Per directory, the operations since its last `sync_dir`.
    pending_dirs: BTreeMap<PathBuf, Vec<DirOp>>,
    inodes: BTreeMap<InodeId, Inode>,
    next_inode: InodeId,
}

fn not_found() -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, "no such file or directory")
}

impl NodeFs {
    pub(super) fn new() -> Self {
        let mut dirs = BTreeSet::new();
        dirs.insert(PathBuf::new());
        dirs.insert(PathBuf::from("/"));
        Self {
            dirs,
            entries: BTreeMap::new(),
            durable_entries: BTreeMap::new(),
            pending_dirs: BTreeMap::new(),
            inodes: BTreeMap::new(),
            next_inode: 1,
        }
    }

    fn parent_exists(&self, path: &Path) -> bool {
        self.dirs
            .contains(path.parent().unwrap_or_else(|| Path::new("")))
    }

    fn open(&mut self, path: &Path, options: OpenOptions) -> io::Result<InodeId> {
        let wants_create = options.is_create() || options.is_create_new();
        if (wants_create || options.is_truncate()) && !options.is_write() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "create/truncate require write",
            ));
        }
        if self.dirs.contains(path) {
            return Err(io::Error::new(
                io::ErrorKind::IsADirectory,
                "is a directory",
            ));
        }
        if let Some(&id) = self.entries.get(path) {
            if options.is_create_new() {
                return Err(io::Error::new(io::ErrorKind::AlreadyExists, "file exists"));
            }
            if options.is_truncate() {
                let inode = self.inodes.get_mut(&id).expect("entry without inode");
                inode.visible.clear();
                inode.pending.push(PendingOp::Truncate(0));
            }
            return Ok(id);
        }
        if !wants_create {
            return Err(not_found());
        }
        if !self.parent_exists(path) {
            return Err(not_found());
        }
        let id = self.next_inode;
        self.next_inode += 1;
        self.inodes.insert(
            id,
            Inode {
                path: path.to_path_buf(),
                visible: Vec::new(),
                durable: Vec::new(),
                pending: Vec::new(),
            },
        );
        self.entries.insert(path.to_path_buf(), id);
        self.pending_dirs
            .entry(parent_of(path))
            .or_default()
            .push(DirOp::Link {
                path: path.to_path_buf(),
                inode: id,
            });
        Ok(id)
    }

    fn create_dir_all(&mut self, path: &Path) -> io::Result<()> {
        if self.entries.contains_key(path) {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, "file exists"));
        }
        for ancestor in path.ancestors() {
            self.dirs.insert(ancestor.to_path_buf());
        }
        Ok(())
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        if !self.dirs.contains(path) {
            return Err(not_found());
        }
        let children = self.entries.keys().chain(self.dirs.iter());
        let mut names: Vec<PathBuf> = children
            .filter(|p| p.parent() == Some(path) && p.as_path() != path)
            .filter_map(|p| p.file_name().map(PathBuf::from))
            .collect();
        names.sort();
        names.dedup();
        Ok(names)
    }

    fn rename(&mut self, from: &Path, to: &Path) -> io::Result<()> {
        if self.dirs.contains(from) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "directory rename is not modelled",
            ));
        }
        let id = *self.entries.get(from).ok_or_else(not_found)?;
        if !self.parent_exists(to) || self.dirs.contains(to) {
            return Err(not_found());
        }
        self.entries.remove(from);
        self.entries.insert(to.to_path_buf(), id);
        if let Some(inode) = self.inodes.get_mut(&id) {
            inode.path = to.to_path_buf();
        }
        self.pending_dirs
            .entry(parent_of(to))
            .or_default()
            .push(DirOp::Rename {
                from: from.to_path_buf(),
                to: to.to_path_buf(),
                inode: id,
            });
        Ok(())
    }

    fn remove_file(&mut self, path: &Path) -> io::Result<()> {
        if self.dirs.contains(path) {
            return Err(io::Error::new(
                io::ErrorKind::IsADirectory,
                "is a directory",
            ));
        }
        self.entries.remove(path).ok_or_else(not_found)?;
        self.pending_dirs
            .entry(parent_of(path))
            .or_default()
            .push(DirOp::Unlink {
                path: path.to_path_buf(),
            });
        Ok(())
    }

    /// Makes every pending operation on `path`'s entries durable.
    fn sync_dir(&mut self, path: &Path) -> io::Result<()> {
        if !self.dirs.contains(path) {
            return Err(not_found());
        }
        for op in self.pending_dirs.remove(path).unwrap_or_default() {
            op.apply(&mut self.durable_entries);
        }
        Ok(())
    }

    fn inode(&self, id: InodeId) -> io::Result<&Inode> {
        self.inodes.get(&id).ok_or_else(not_found)
    }

    fn inode_mut(&mut self, id: InodeId) -> io::Result<&mut Inode> {
        self.inodes.get_mut(&id).ok_or_else(not_found)
    }
}

impl State {
    fn node_fs(&mut self, node: NodeId) -> &mut NodeFs {
        self.fs.entry(node).or_insert_with(NodeFs::new)
    }

    /// The timer an operation issued now waits on, if operations take time.
    fn io_delay(&mut self, shared: &Arc<Shared>) -> Option<SimSleep> {
        let (min, max) = (self.config.fs.latency_min, self.config.fs.latency_max);
        if max.is_zero() {
            return None;
        }
        let span = max.saturating_sub(min).as_nanos() as u64;
        let extra = self.fs_stream.below(span + 1);
        let at = self.now + min + std::time::Duration::from_nanos(extra);
        Some(SimSleep::new(shared.clone(), at))
    }

    fn fs_sync(&mut self, node: NodeId, inode: InodeId) -> io::Result<()> {
        let p_durable = self.config.fs.p_durable;
        let durable = self.fs_stream.chance(p_durable);
        let ino = self.node_fs(node).inode_mut(inode)?;
        if durable {
            ino.durable.clone_from(&ino.visible);
            ino.pending.clear();
            Ok(())
        } else {
            let path = ino.path.clone();
            self.record(Some(node), TraceEvent::FsyncLost { path });
            Ok(())
        }
    }

    /// The §1.3 crash model: for every inode a random prefix of its pending writes
    /// survives in full, the next one may survive as a torn prefix, and the rest are
    /// gone. Then, with `p_bitrot` per block, one bit of the block flips on disk. What
    /// the node sees afterwards is exactly what is durable.
    pub(super) fn apply_crash_faults(&mut self, node: NodeId) {
        let mut events = Vec::new();
        let (p_bitrot, block_size) = (self.config.fs.p_bitrot, self.config.fs.block_size);
        let rng = &mut self.fs_stream;
        if let Some(fs) = self.fs.get_mut(&node) {
            for inode in fs.inodes.values_mut() {
                let pending = std::mem::take(&mut inode.pending);
                let keep = usize::try_from(rng.below(pending.len() as u64 + 1)).unwrap_or(0);
                for op in &pending[..keep] {
                    apply(&mut inode.durable, op, None);
                }
                if let Some(PendingOp::Write { offset, data }) = pending.get(keep) {
                    let kept = usize::try_from(rng.below(data.len() as u64 + 1)).unwrap_or(0);
                    if kept > 0 {
                        apply(&mut inode.durable, &pending[keep], Some(kept));
                    }
                    if kept > 0 && kept < data.len() {
                        events.push(TraceEvent::WriteTorn {
                            path: inode.path.clone(),
                            offset: *offset,
                            written: data.len(),
                            kept,
                        });
                    }
                }
                // Bit rot, block by block. `chance(0.0)` draws nothing, so runs without
                // bit rot configured keep their traces.
                let len = inode.durable.len() as u64;
                let blocks = len.div_ceil(block_size);
                for block in 0..blocks {
                    if !rng.chance(p_bitrot) {
                        continue;
                    }
                    let start = block * block_size;
                    let span = (len - start).min(block_size);
                    let offset = start + rng.below(span);
                    let bit = u8::try_from(rng.below(8)).expect("0..8 fits u8");
                    let index = usize::try_from(offset).expect("offset fits usize");
                    inode.durable[index] ^= 1 << bit;
                    events.push(TraceEvent::BlockRotted {
                        path: inode.path.clone(),
                        block,
                        offset,
                        bit,
                    });
                }
                inode.visible.clone_from(&inode.durable);
            }
            fs.apply_crash_to_directories(rng, &mut events);
        }
        for event in events {
            self.record(Some(node), event);
        }
    }
}

/// One node's view of the simulated disk.
#[derive(Clone)]
pub struct SimFs {
    pub(super) shared: Arc<Shared>,
    pub(super) node: NodeId,
}

impl std::fmt::Debug for SimFs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimFs")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}

impl SimFs {
    /// Runs `f` on the node's disk after the operation's delay, if any.
    fn with<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut NodeFs) -> T + Send + 'static,
    ) -> impl Future<Output = T> + Send {
        let delay = self.shared.lock().io_delay(&self.shared);
        let (shared, node) = (self.shared.clone(), self.node);
        async move {
            if let Some(sleep) = delay {
                sleep.await;
            }
            let mut st = shared.lock();
            f(st.node_fs(node))
        }
    }
}

impl FileSystem for SimFs {
    type File = SimFile;

    fn open(
        &self,
        path: &Path,
        options: OpenOptions,
    ) -> impl Future<Output = io::Result<SimFile>> + Send {
        let path = path.to_path_buf();
        let (shared, node) = (self.shared.clone(), self.node);
        let opened = self.with(move |fs| fs.open(&path, options));
        async move {
            opened.await.map(|inode| SimFile {
                shared,
                node,
                inode,
                readable: options.is_read(),
                writable: options.is_write(),
            })
        }
    }

    fn create_dir_all(&self, path: &Path) -> impl Future<Output = io::Result<()>> + Send {
        let path = path.to_path_buf();
        self.with(move |fs| fs.create_dir_all(&path))
    }

    fn read_dir(&self, path: &Path) -> impl Future<Output = io::Result<Vec<PathBuf>>> + Send {
        let path = path.to_path_buf();
        self.with(move |fs| fs.read_dir(&path))
    }

    fn rename(&self, from: &Path, to: &Path) -> impl Future<Output = io::Result<()>> + Send {
        let (from, to) = (from.to_path_buf(), to.to_path_buf());
        self.with(move |fs| fs.rename(&from, &to))
    }

    fn remove_file(&self, path: &Path) -> impl Future<Output = io::Result<()>> + Send {
        let path = path.to_path_buf();
        self.with(move |fs| fs.remove_file(&path))
    }

    fn sync_dir(&self, path: &Path) -> impl Future<Output = io::Result<()>> + Send {
        let path = path.to_path_buf();
        self.with(move |fs| fs.sync_dir(&path))
    }
}

/// An open file on the simulated disk. Clones share the inode.
#[derive(Clone)]
pub struct SimFile {
    shared: Arc<Shared>,
    node: NodeId,
    inode: InodeId,
    readable: bool,
    writable: bool,
}

impl std::fmt::Debug for SimFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimFile")
            .field("node", &self.node)
            .field("inode", &self.inode)
            .finish_non_exhaustive()
    }
}

fn denied(what: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        format!("file not opened for {what}"),
    )
}

impl SimFile {
    /// Runs `f` on the inode after the operation's delay, if any.
    fn with<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Inode) -> T + Send + 'static,
    ) -> impl Future<Output = io::Result<T>> + Send {
        let delay = self.shared.lock().io_delay(&self.shared);
        let (shared, node, inode) = (self.shared.clone(), self.node, self.inode);
        async move {
            if let Some(sleep) = delay {
                sleep.await;
            }
            let mut st = shared.lock();
            st.node_fs(node).inode_mut(inode).map(f)
        }
    }
}

impl File for SimFile {
    fn read_at(&self, offset: u64, len: usize) -> impl Future<Output = io::Result<Bytes>> + Send {
        let readable = self.readable;
        let read = self.with(move |inode| {
            let start = usize::try_from(offset)
                .unwrap_or(usize::MAX)
                .min(inode.visible.len());
            let end = start.saturating_add(len).min(inode.visible.len());
            Bytes::copy_from_slice(&inode.visible[start..end])
        });
        async move {
            if !readable {
                return Err(denied("reading"));
            }
            read.await
        }
    }

    fn write_at(&self, offset: u64, data: Bytes) -> impl Future<Output = io::Result<()>> + Send {
        let writable = self.writable;
        let write = self.with(move |inode| {
            let op = PendingOp::Write { offset, data };
            apply(&mut inode.visible, &op, None);
            inode.pending.push(op);
        });
        async move {
            if !writable {
                return Err(denied("writing"));
            }
            write.await
        }
    }

    fn size(&self) -> impl Future<Output = io::Result<u64>> + Send {
        self.with(|inode| inode.visible.len() as u64)
    }

    fn set_size(&self, size: u64) -> impl Future<Output = io::Result<()>> + Send {
        let writable = self.writable;
        let truncate = self.with(move |inode| {
            let op = PendingOp::Truncate(size);
            apply(&mut inode.visible, &op, None);
            inode.pending.push(op);
        });
        async move {
            if !writable {
                return Err(denied("writing"));
            }
            truncate.await
        }
    }

    fn sync(&self) -> impl Future<Output = io::Result<()>> + Send {
        let delay = self.shared.lock().io_delay(&self.shared);
        let (shared, node, inode) = (self.shared.clone(), self.node, self.inode);
        async move {
            if let Some(sleep) = delay {
                sleep.await;
            }
            shared.lock().fs_sync(node, inode)
        }
    }
}

impl NodeFs {
    /// For tests and the harness: what is on disk for `path` right now, in the durable
    /// namespace.
    pub(super) fn durable_contents(&self, path: &Path) -> Option<&[u8]> {
        let id = self.durable_entries.get(path)?;
        self.inode(*id).ok().map(|inode| inode.durable.as_slice())
    }

    /// The §1.3 directory-entry loss model: per directory, a random prefix of the
    /// operations since its last `sync_dir` survives; the rest are reported and gone.
    /// Afterwards the node sees exactly the durable namespace.
    fn apply_crash_to_directories(
        &mut self,
        rng: &mut moirae_sched::Pcg32,
        events: &mut Vec<TraceEvent>,
    ) {
        let pending = std::mem::take(&mut self.pending_dirs);
        for (dir, ops) in pending {
            let keep = usize::try_from(rng.below(ops.len() as u64 + 1)).unwrap_or(0);
            for (i, op) in ops.iter().enumerate() {
                if i < keep {
                    op.apply(&mut self.durable_entries);
                } else {
                    events.push(TraceEvent::DirectoryEntryLost {
                        dir: dir.clone(),
                        entry: op.entry().to_path_buf(),
                        op: op.kind(),
                    });
                }
            }
        }
        self.entries.clone_from(&self.durable_entries);
    }
}
