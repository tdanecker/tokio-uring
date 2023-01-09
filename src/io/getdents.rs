use crate::buf::IoBufMut;
use crate::BufResult;
use crate::io::SharedFd;
use crate::runtime::CONTEXT;
use crate::runtime::driver::op::{Completable, CqeResult, Op};

use std::io;

pub(crate) struct Getdents<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight.
    #[allow(dead_code)]
    fd: SharedFd,

    /// Reference to the in-flight buffer.
    pub(crate) buf: T,
}

impl<T: IoBufMut> Op<Getdents<T>> {
    pub(crate) fn getdents(fd: &SharedFd, buf: T) -> io::Result<Op<Getdents<T>>> {
        use io_uring::{opcode, types};

        CONTEXT.with(|x| {
            x.handle().expect("Not in a runtime context").submit_op(
                Getdents {
                    fd: fd.clone(),
                    buf,
                },
                |getdents| {
                    // Get raw buffer info
                    let ptr = getdents.buf.stable_mut_ptr();
                    let len = getdents.buf.bytes_total();
                    opcode::Getdents::new(types::Fd(fd.raw_fd()), ptr, len as _)
                        .build()
                },
            )
        })
    }
}

impl<T> Completable for Getdents<T>
    where
        T: IoBufMut,
{
    type Output = BufResult<usize, T>;

    fn complete(self, cqe: CqeResult) -> Self::Output {
        // Convert the operation result to `usize`
        let res = cqe.result.map(|v| v as usize);
        // Recover the buffer
        let mut buf = self.buf;

        // If the operation was successful, advance the initialized cursor.
        if let Ok(n) = res {
            // Safety: the kernel wrote `n` bytes to the buffer.
            unsafe {
                buf.set_init(n);
            }
        }

        (res, buf)
    }
}
