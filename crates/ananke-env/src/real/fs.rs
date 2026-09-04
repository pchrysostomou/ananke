use std::future::Future;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use tokio::runtime::Handle;

use crate::{File, FileSystem, OpenOptions};

/// The real filesystem. Every operation runs on tokio's blocking pool so it never
/// stalls the async workers.
#[derive(Clone, Debug)]
pub struct RealFs {
    handle: Handle,
}

impl RealFs {
    pub(super) fn new(handle: Handle) -> Self {
        Self { handle }
    }
}

/// Runs `f` on the blocking pool. A panic inside `f` surfaces as an `io::Error`.
async fn blocking<T, F>(handle: &Handle, f: F) -> io::Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> io::Result<T> + Send + 'static,
{
    handle.spawn_blocking(f).await.map_err(io::Error::other)?
}

fn std_options(options: OpenOptions) -> std::fs::OpenOptions {
    let mut std = std::fs::OpenOptions::new();
    std.read(options.is_read())
        .write(options.is_write())
        .create(options.is_create())
        .create_new(options.is_create_new())
        .truncate(options.is_truncate());
    std
}

impl FileSystem for RealFs {
    type File = RealFile;

    fn open(
        &self,
        path: &Path,
        options: OpenOptions,
    ) -> impl Future<Output = io::Result<RealFile>> + Send {
        let path = path.to_path_buf();
        let handle = self.handle.clone();
        async move {
            let file = blocking(&handle, move || std_options(options).open(path)).await?;
            Ok(RealFile {
                file: Arc::new(file),
                handle,
            })
        }
    }

    fn create_dir_all(&self, path: &Path) -> impl Future<Output = io::Result<()>> + Send {
        let path = path.to_path_buf();
        let handle = self.handle.clone();
        async move { blocking(&handle, move || std::fs::create_dir_all(path)).await }
    }

    fn read_dir(&self, path: &Path) -> impl Future<Output = io::Result<Vec<PathBuf>>> + Send {
        let path = path.to_path_buf();
        let handle = self.handle.clone();
        async move {
            blocking(&handle, move || {
                let mut names = std::fs::read_dir(path)?
                    .map(|entry| entry.map(|e| PathBuf::from(e.file_name())))
                    .collect::<io::Result<Vec<_>>>()?;
                names.sort();
                Ok(names)
            })
            .await
        }
    }

    fn rename(&self, from: &Path, to: &Path) -> impl Future<Output = io::Result<()>> + Send {
        let (from, to) = (from.to_path_buf(), to.to_path_buf());
        let handle = self.handle.clone();
        async move { blocking(&handle, move || std::fs::rename(from, to)).await }
    }

    fn remove_file(&self, path: &Path) -> impl Future<Output = io::Result<()>> + Send {
        let path = path.to_path_buf();
        let handle = self.handle.clone();
        async move { blocking(&handle, move || std::fs::remove_file(path)).await }
    }

    fn sync_dir(&self, path: &Path) -> impl Future<Output = io::Result<()>> + Send {
        let path = path.to_path_buf();
        let handle = self.handle.clone();
        async move { blocking(&handle, move || std::fs::File::open(path)?.sync_all()).await }
    }
}

/// An open file on the real filesystem. Clones share the same descriptor.
#[derive(Clone, Debug)]
pub struct RealFile {
    file: Arc<std::fs::File>,
    handle: Handle,
}

impl File for RealFile {
    fn read_at(&self, offset: u64, len: usize) -> impl Future<Output = io::Result<Bytes>> + Send {
        let file = self.file.clone();
        let handle = self.handle.clone();
        async move {
            blocking(&handle, move || {
                let mut buf = vec![0u8; len];
                let mut filled = 0;
                while filled < len {
                    match file.read_at(&mut buf[filled..], offset + filled as u64) {
                        Ok(0) => break,
                        Ok(n) => filled += n,
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                        Err(e) => return Err(e),
                    }
                }
                buf.truncate(filled);
                Ok(Bytes::from(buf))
            })
            .await
        }
    }

    fn write_at(&self, offset: u64, data: Bytes) -> impl Future<Output = io::Result<()>> + Send {
        let file = self.file.clone();
        let handle = self.handle.clone();
        async move { blocking(&handle, move || file.write_all_at(&data, offset)).await }
    }

    fn size(&self) -> impl Future<Output = io::Result<u64>> + Send {
        let file = self.file.clone();
        let handle = self.handle.clone();
        async move { blocking(&handle, move || file.metadata().map(|m| m.len())).await }
    }

    fn set_size(&self, size: u64) -> impl Future<Output = io::Result<()>> + Send {
        let file = self.file.clone();
        let handle = self.handle.clone();
        async move { blocking(&handle, move || file.set_len(size)).await }
    }

    fn sync(&self) -> impl Future<Output = io::Result<()>> + Send {
        let file = self.file.clone();
        let handle = self.handle.clone();
        async move { blocking(&handle, move || file.sync_all()).await }
    }
}
