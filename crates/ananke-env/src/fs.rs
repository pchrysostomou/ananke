//! The [`FileSystem`] and [`File`] traits (SPEC.md §1.1).
//!
//! The API is positional (`read_at` / `write_at`) rather than cursor-based: it is what a
//! log-structured engine needs, it needs no lock around a shared cursor, and it maps
//! directly onto `pread` / `pwrite`. Durability is explicit: nothing is durable until
//! [`File::sync`] returns, and a rename is not durable until [`FileSystem::sync_dir`] on
//! the parent directory returns (SPEC.md §1.3).

use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};

use bytes::Bytes;

/// How to open a file. The subset of `std::fs::OpenOptions` ananke uses.
///
/// `create`, `create_new` and `truncate` require `write`, as in `std`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OpenOptions {
    read: bool,
    write: bool,
    create: bool,
    create_new: bool,
    truncate: bool,
}

impl OpenOptions {
    /// No access requested; chain the setters before opening.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Open for reading.
    #[must_use]
    pub fn read(mut self, read: bool) -> Self {
        self.read = read;
        self
    }

    /// Open for writing.
    #[must_use]
    pub fn write(mut self, write: bool) -> Self {
        self.write = write;
        self
    }

    /// Create the file if it does not exist.
    #[must_use]
    pub fn create(mut self, create: bool) -> Self {
        self.create = create;
        self
    }

    /// Create the file, failing if it already exists.
    #[must_use]
    pub fn create_new(mut self, create_new: bool) -> Self {
        self.create_new = create_new;
        self
    }

    /// Truncate the file to zero length on open.
    #[must_use]
    pub fn truncate(mut self, truncate: bool) -> Self {
        self.truncate = truncate;
        self
    }

    /// Whether reading was requested.
    #[must_use]
    pub fn is_read(&self) -> bool {
        self.read
    }

    /// Whether writing was requested.
    #[must_use]
    pub fn is_write(&self) -> bool {
        self.write
    }

    /// Whether the file may be created.
    #[must_use]
    pub fn is_create(&self) -> bool {
        self.create
    }

    /// Whether the file must be created.
    #[must_use]
    pub fn is_create_new(&self) -> bool {
        self.create_new
    }

    /// Whether the file is truncated on open.
    #[must_use]
    pub fn is_truncate(&self) -> bool {
        self.truncate
    }
}

/// A filesystem namespace: directories, renames, and opening [`File`]s.
pub trait FileSystem: Send + Sync + 'static {
    /// The open-file handle type.
    type File: File;

    /// Opens, or creates according to `options`, the file at `path`.
    fn open(
        &self,
        path: &Path,
        options: OpenOptions,
    ) -> impl Future<Output = io::Result<Self::File>> + Send;

    /// Creates `path` and any missing parents. Succeeds if it already exists.
    fn create_dir_all(&self, path: &Path) -> impl Future<Output = io::Result<()>> + Send;

    /// The names of the entries directly inside `path`, sorted so that iteration order
    /// is deterministic.
    fn read_dir(&self, path: &Path) -> impl Future<Output = io::Result<Vec<PathBuf>>> + Send;

    /// Atomically replaces `to` with `from`. Durable only once
    /// [`sync_dir`](Self::sync_dir) on the parent directory returns.
    fn rename(&self, from: &Path, to: &Path) -> impl Future<Output = io::Result<()>> + Send;

    /// Removes the file at `path`.
    fn remove_file(&self, path: &Path) -> impl Future<Output = io::Result<()>> + Send;

    /// Flushes the directory entries of `path` to stable storage.
    fn sync_dir(&self, path: &Path) -> impl Future<Output = io::Result<()>> + Send;
}

/// An open file with positional I/O.
pub trait File: Send + Sync + 'static {
    /// Reads up to `len` bytes starting at `offset`. Returns fewer only at end of file.
    fn read_at(&self, offset: u64, len: usize) -> impl Future<Output = io::Result<Bytes>> + Send;

    /// Writes all of `data` starting at `offset`, extending the file if needed. Not
    /// durable until [`sync`](Self::sync) returns.
    fn write_at(&self, offset: u64, data: Bytes) -> impl Future<Output = io::Result<()>> + Send;

    /// The current size in bytes.
    fn size(&self) -> impl Future<Output = io::Result<u64>> + Send;

    /// Truncates, or extends with zeros, to `size` bytes.
    fn set_size(&self, size: u64) -> impl Future<Output = io::Result<()>> + Send;

    /// Flushes data and metadata to stable storage (`fsync`).
    fn sync(&self) -> impl Future<Output = io::Result<()>> + Send;
}
