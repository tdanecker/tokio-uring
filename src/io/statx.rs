use crate::io::SharedFd;
use crate::runtime::CONTEXT;
use crate::runtime::driver::op::{Completable, CqeResult, Op};

use std::{io, mem};
use std::ffi::CString;
use std::ops::BitOr;
use std::path::Path;
use libc::AT_SYMLINK_NOFOLLOW;

pub(crate) struct Statx {
    dirfd: Option<SharedFd>,
    path: CString,
    metadata: Box<libc::statx>,
}

pub(crate) struct StatxFlags(i32);
impl StatxFlags {
    pub(crate) const NONE: StatxFlags = StatxFlags(0);
    pub(crate) const SYMLINK_NOFOLLOW: StatxFlags = StatxFlags(AT_SYMLINK_NOFOLLOW);
}
impl BitOr for StatxFlags {
    type Output = StatxFlags;

    fn bitor(self, rhs: Self) -> Self::Output {
        StatxFlags(self.0 | rhs.0)
    }
}

impl Op<Statx> {
    pub(crate) fn statx(dirfd: Option<&SharedFd>, path: &Path, flags: StatxFlags) -> io::Result<Op<Statx>> {
        use io_uring::{opcode, types};

        let path = super::util::cstr(path)?;

        CONTEXT.with(|x| {
            x.handle().expect("Not in a runtime context").submit_op(
                Statx {
                    dirfd: dirfd.map(|fd| fd.clone()),
                    path,
                    metadata: Box::new(unsafe { mem::zeroed() })
                },
                |statx| {
                    let dirfd = types::Fd(statx.dirfd.as_ref().map_or_else(|| libc::AT_FDCWD, |fd| fd.raw_fd()));
                    let mut flags = flags.0;
                    if statx.path.as_bytes().is_empty() {
                        flags |= libc::AT_EMPTY_PATH;
                    }
                    opcode::Statx::new(dirfd, statx.path.as_ptr(), statx.metadata.as_mut() as *mut _ as *mut _)
                        .flags(flags)
                        .mask(libc::STATX_ALL)
                        .build()
                },
            )
        })
    }
}

impl Completable for Statx {
    type Output = io::Result<Box<libc::statx>>;

    fn complete(self, cqe: CqeResult) -> Self::Output {
        let _ = cqe.result?;

        Ok(self.metadata)
    }
}
