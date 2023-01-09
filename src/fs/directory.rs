use crate::fs::OpenOptions;
use crate::fs::file::{Metadata, FileType};
use crate::io::{Getdents, SharedFd};
use crate::runtime::driver::op::Op;
use std::ffi::{CStr, OsStr, OsString};
use std::io;
use std::mem::size_of;
use std::ops::DerefMut;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use futures_core::{Future, Stream};

/// # Examples
///
/// ```no_run
/// use tokio_uring::fs::create_dir;
///
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     tokio_uring::start(async {
///         create_dir("/some/dir").await?;
///         Ok::<(), std::io::Error>(())
///     })?;
///     Ok(())
/// }
/// ```
pub async fn create_dir<P: AsRef<Path>>(path: P) -> io::Result<()> {
    Op::make_dir(path.as_ref())?.await
}

/// Removes an empty directory.
///
/// # Examples
///
/// ```no_run
/// use tokio_uring::fs::remove_dir;
///
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     tokio_uring::start(async {
///         remove_dir("/some/dir").await?;
///         Ok::<(), std::io::Error>(())
///     })?;
///     Ok(())
/// }
/// ```
pub async fn remove_dir<P: AsRef<Path>>(path: P) -> io::Result<()> {
    Op::unlink_dir(path.as_ref())?.await
}

enum DirEntriesBuffer {
    Ready(Option<Vec<u8>>),
    InFlight(Op<Getdents<Vec<u8>>>)
}

/// A reference to an open directory on the filesystem.
///
/// It implements the `Stream` trait, yielding instances of `io::Result<DirEntry>`.
/// Entries for the current and parent directories (typically . and ..) are skipped.
///
/// While directories are automatically closed when they go out of scope, the
/// operation happens asynchronously in the background. It is recommended to
/// call the `close()` function in order to guarantee that the directory is successfully
/// closed before exiting the scope.
///
/// # Examples
///
/// Open a directory and read its entries:
///
/// ```no_run
/// use tokio_uring::fs::Dir;
///
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     tokio_uring::start(async {
///         // Open a directory
///         let mut dir = Dir::open(".").await?;
///
///         // Read directory entries
///         while let Some(result) = dir.next().await {
///             let entry = result?;
///             println!("entry: {}", entry.file_name())
///         }
///
///         // Close the directory
///         dir.close().await?;
///
///         Ok(())
///     })
/// }
/// ```
pub struct Dir {
    fd: SharedFd,
    buf: DirEntriesBuffer,
    read: usize,
}

/// An instance of DirEntry represents an entry inside of a directory on the filesystem.
///
/// Each entry can be inspected via methods to learn about the name or type of directory entry.
pub struct DirEntry {
    file_type: FileType,
    name: OsString,
}

impl Dir {
    /// Attempts to open a directory.
    ///
    /// # Errors
    ///
    /// This function will return an error if `path` does not already exist or the path does not refer to a directory.
    /// Other errors may also be returned according to [`OpenOptions::open`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tokio_uring::fs::Dir;
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     tokio_uring::start(async {
    ///         let dir = Dir::open(".").await?;
    ///
    ///         // Close the directory
    ///         dir.close().await?;
    ///         Ok(())
    ///     })
    /// }
    /// ```
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Dir> {
        Ok(Dir::from_shared_fd(
            Op::open(
                path.as_ref(),
                &OpenOptions::new().read(true).custom_flags(libc::O_DIRECTORY),
            )?.await?
        ))
    }

    /// Closes the directory.
    ///
    /// The method completes once the close operation has completed,
    /// guaranteeing that resources associated with the directory have been released.
    ///
    /// If `close` is not called before dropping the file, the directory is closed in
    /// the background, but there is no guarantee as to **when** the close
    /// operation will complete.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tokio_uring::fs::Dir;
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     tokio_uring::start(async {
    ///         // Open the directory
    ///         let dir = Dir::open(".").await?;
    ///         // Close the file
    ///         dir.close().await?;
    ///
    ///         Ok(())
    ///     })
    /// }
    /// ```
    pub async fn close(self) -> io::Result<()> {
        self.fd.close().await;
        Ok(())
    }

    pub(crate) fn from_shared_fd(fd: SharedFd) -> Dir {
        Dir { fd, buf: DirEntriesBuffer::Ready(Some(Vec::with_capacity(32768))), read: 0 }
    }
}

impl Stream for Dir {
    type Item = io::Result<DirEntry>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut dir = self.deref_mut();
        loop {
            if let DirEntriesBuffer::Ready(opt) = &mut dir.buf {
                let mut buf = match opt.take() {
                    Some(buf) => buf,
                    None => return Poll::Ready(None),
                };
                if dir.read >= buf.len() {
                    buf.clear();
                    match Op::getdents(&dir.fd, buf) {
                        Err(err) => return Poll::Ready(Some(Err(err))),
                        Ok(op) => {
                            dir.buf = DirEntriesBuffer::InFlight(op);
                        },
                    }
                } else {
                    dir.buf = DirEntriesBuffer::Ready(Some(buf));
                }
            }

            let buf = match &mut dir.buf {
                DirEntriesBuffer::InFlight(op) => {
                    let (res, buffer) = ready!(Pin::new(op).poll(cx));
                    match res {
                        Err(err) => return Poll::Ready(Some(Err(err))),
                        Ok(_) => {
                            dir.read = 0;
                            buffer
                        },
                    }
                },
                DirEntriesBuffer::Ready(opt) => {
                    match opt.take() {
                        Some(buf) => buf,
                        None => return Poll::Ready(None),
                    }
                },
            };

            if buf.len() == 0 {
                dir.buf = DirEntriesBuffer::Ready(None);
                return Poll::Ready(None);
            }

            unsafe {
                let dirent = buf[dir.read..].as_ptr() as *const libc::dirent64;
                let name_offset = ((*dirent).d_name.as_ptr() as *const u8).offset_from(dirent as *const u8) as usize;
                assert!(buf.len() >= dir.read + name_offset);
                let rec_len = (*dirent).d_reclen as usize;
                let file_type = (*dirent).d_type;
                assert!(buf.len() >= dir.read + rec_len);
                let entry = DirEntry {
                    file_type: match file_type {
                        libc::DT_FIFO => FileType::Fifo,
                        libc::DT_CHR => FileType::CharDevice,
                        libc::DT_DIR => FileType::Dir,
                        libc::DT_BLK => FileType::BlockDevice,
                        libc::DT_REG => FileType::File,
                        libc::DT_LNK => FileType::Symlink,
                        libc::DT_SOCK => FileType::Socket,
                        _ => FileType::Unknown,
                    },
                    name: OsStr::from_bytes(CStr::from_ptr((*dirent).d_name.as_ptr()).to_bytes()).to_os_string(),
                };

                dir.read += rec_len;
                dir.buf = DirEntriesBuffer::Ready(Some(buf));

                if entry.name != "." && entry.name != ".." {
                    return Poll::Ready(Some(Ok(entry)))
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (match &self.buf {
            DirEntriesBuffer::Ready(Some(buf)) => (buf.len() - self.read) / size_of::<libc::dirent64>(),
            _ => 0,
        },
        None)
    }
}

impl DirEntry {
    /// Returns the file type for the file that this entry points at, if available.
    ///
    /// Most file systems can directly provide the file type for a directory entry.
    ///
    /// If it is not available, the file type will be [`FileType::Unknown`]. In this case,
    /// you can use [`Dir::metadata`] to retrieve the file type of the entry.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tokio_uring::fs::{Dir, FileType};
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     tokio_uring::start(async {
    ///         // Open a directory
    ///         let mut dir = Dir::open(".").await?;
    ///
    ///         // Read directory entries
    ///         while let Some(result) = dir.next().await {
    ///             let entry = result?;
    ///             let mut file_type = entry.file_type();
    ///             if file_type == FileType::Unknown {
    ///                 // Retrieve metadata for the entry
    ///                 let metadata = dir.metadata(&entry).await?;
    ///                 file_type = metadata.file_type();
    ///             }
    ///             println!("entry: {}, is dir: {}", entry.file_name(), file_type == FileType::Dir)
    ///         }
    ///
    ///         // Close the directory
    ///         dir.close().await?;
    ///
    ///         Ok(())
    ///     })
    /// }
    /// ```
    pub fn file_type(&self) -> FileType {
        self.file_type
    }
    /// Returns the bare file name of this directory entry without any other leading path component.
    /// # Examples
    ///
    /// ```no_run
    /// use tokio_uring::fs::Dir;
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     tokio_uring::start(async {
    ///         // Open a directory
    ///         let mut dir = Dir::open(".").await?;
    ///
    ///         // Read directory entries
    ///         while let Some(result) = dir.next().await {
    ///             let entry = result?;
    ///             println!("{}", entry.file_name())
    ///         }
    ///
    ///         // Close the directory
    ///         dir.close().await?;
    ///
    ///         Ok(())
    ///     })
    /// }
    /// ```
    pub fn file_name(&self) -> &OsStr {
        self.name.as_os_str()
    }
}
