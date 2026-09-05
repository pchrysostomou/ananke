//! The in-memory filesystem with the SPEC.md §1.3 fault model.
//!
//! Every inode keeps what the running node sees (`visible`), what would survive a
//! crash (`durable`) and the writes issued since the last successful `sync`. A sync
//! makes everything durable with probability `p_durable`, otherwise returns Ok while
//! persisting nothing (lost fsync). At a crash, a random prefix of the pending writes
//! survives and the next write may be torn to a random prefix of itself. Directory
//! entries are immediately durable in this version (see BACKLOG.md).

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;

use super::state::{Shared, State};
use crate::{File, FileSystem, NodeId, OpenOptions, TraceEvent};

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

/// One node's disk.
pub(super) struct NodeFs {
    dirs: BTreeSet<PathBuf>,
    entries: BTreeMap<PathBuf, InodeId>,
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
        Ok(())
    }

    fn remove_file(&mut self, path: &Path) -> io::Result<()> {
        if self.dirs.contains(path) {
            return Err(io::Error::new(
                io::ErrorKind::IsADirectory,
                "is a directory",
            ));
        }
        self.entries.remove(path).map(|_| ()).ok_or_else(not_found)
    }

    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        if self.dirs.contains(path) {
            Ok(())
        } else {
            Err(not_found())
        }
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

    fn fs_sync(&mut self, node: NodeId, inode: InodeId) -> io::Result<()> {
        let p_durable = self.config.fs.p_durable;
        let durable = self.rng.chance(p_durable);
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
    /// gone. What the node sees afterwards is exactly what is durable.
    pub(super) fn apply_crash_faults(&mut self, node: NodeId) {
        let mut events = Vec::new();
        let rng = &mut self.rng;
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
                inode.visible.clone_from(&inode.durable);
            }
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
    fn with<T>(&self, f: impl FnOnce(&mut NodeFs) -> T) -> T {
        let mut st = self.shared.lock();
        f(st.node_fs(self.node))
    }
}

impl FileSystem for SimFs {
    type File = SimFile;

    fn open(
        &self,
        path: &Path,
        options: OpenOptions,
    ) -> impl Future<Output = io::Result<SimFile>> + Send {
        let result = self.with(|fs| fs.open(path, options)).map(|inode| SimFile {
            shared: self.shared.clone(),
            node: self.node,
            inode,
            readable: options.is_read(),
            writable: options.is_write(),
        });
        std::future::ready(result)
    }

    fn create_dir_all(&self, path: &Path) -> impl Future<Output = io::Result<()>> + Send {
        std::future::ready(self.with(|fs| fs.create_dir_all(path)))
    }

    fn read_dir(&self, path: &Path) -> impl Future<Output = io::Result<Vec<PathBuf>>> + Send {
        std::future::ready(self.with(|fs| fs.read_dir(path)))
    }

    fn rename(&self, from: &Path, to: &Path) -> impl Future<Output = io::Result<()>> + Send {
        std::future::ready(self.with(|fs| fs.rename(from, to)))
    }

    fn remove_file(&self, path: &Path) -> impl Future<Output = io::Result<()>> + Send {
        std::future::ready(self.with(|fs| fs.remove_file(path)))
    }

    fn sync_dir(&self, path: &Path) -> impl Future<Output = io::Result<()>> + Send {
        std::future::ready(self.with(|fs| fs.sync_dir(path)))
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
    fn with<T>(&self, f: impl FnOnce(&mut Inode) -> T) -> io::Result<T> {
        let mut st = self.shared.lock();
        st.node_fs(self.node).inode_mut(self.inode).map(f)
    }
}

impl File for SimFile {
    fn read_at(&self, offset: u64, len: usize) -> impl Future<Output = io::Result<Bytes>> + Send {
        let result = if self.readable {
            self.with(|inode| {
                let start = usize::try_from(offset)
                    .unwrap_or(usize::MAX)
                    .min(inode.visible.len());
                let end = start.saturating_add(len).min(inode.visible.len());
                Bytes::copy_from_slice(&inode.visible[start..end])
            })
        } else {
            Err(denied("reading"))
        };
        std::future::ready(result)
    }

    fn write_at(&self, offset: u64, data: Bytes) -> impl Future<Output = io::Result<()>> + Send {
        let result = if self.writable {
            self.with(|inode| {
                let op = PendingOp::Write { offset, data };
                apply(&mut inode.visible, &op, None);
                inode.pending.push(op);
            })
        } else {
            Err(denied("writing"))
        };
        std::future::ready(result)
    }

    fn size(&self) -> impl Future<Output = io::Result<u64>> + Send {
        std::future::ready(self.with(|inode| inode.visible.len() as u64))
    }

    fn set_size(&self, size: u64) -> impl Future<Output = io::Result<()>> + Send {
        let result = if self.writable {
            self.with(|inode| {
                let op = PendingOp::Truncate(size);
                apply(&mut inode.visible, &op, None);
                inode.pending.push(op);
            })
        } else {
            Err(denied("writing"))
        };
        std::future::ready(result)
    }

    fn sync(&self) -> impl Future<Output = io::Result<()>> + Send {
        let result = self.shared.lock().fs_sync(self.node, self.inode);
        std::future::ready(result)
    }
}

impl NodeFs {
    /// For tests and the harness: what is on disk for `path` right now.
    pub(super) fn durable_contents(&self, path: &Path) -> Option<&[u8]> {
        let id = self.entries.get(path)?;
        self.inode(*id).ok().map(|inode| inode.durable.as_slice())
    }
}
