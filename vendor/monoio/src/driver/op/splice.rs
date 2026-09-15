//! This module works only on linux.

use std::io;

#[cfg(all(target_os = "linux", feature = "iouring"))]
use io_uring::{opcode, types};
#[cfg(all(unix, feature = "legacy"))]
use {
    crate::{driver::ready::Direction, syscall_u32},
    std::os::unix::prelude::AsRawFd,
};

use super::{super::shared_fd::SharedFd, Op, OpAble};

// Currently our Splice does not support setting offset.
pub(crate) struct Splice {
    fd_in: SharedFd,
    fd_out: SharedFd,
    len: u32,
    direction: SpliceDirection,
}
enum SpliceDirection {
    FromPipe,
    ToPipe,
}

impl Op<Splice> {
    pub(crate) fn splice_to_pipe(
        fd_in: &SharedFd,
        fd_out: &SharedFd,
        len: u32,
    ) -> io::Result<Op<Splice>> {
        Op::submit_with(Splice {
            fd_in: fd_in.clone(),
            fd_out: fd_out.clone(),
            len,
            direction: SpliceDirection::ToPipe,
        })
    }

    pub(crate) fn splice_from_pipe(
        fd_in: &SharedFd,
        fd_out: &SharedFd,
        len: u32,
    ) -> io::Result<Op<Splice>> {
        Op::submit_with(Splice {
            fd_in: fd_in.clone(),
            fd_out: fd_out.clone(),
            len,
            direction: SpliceDirection::FromPipe,
        })
    }

    pub(crate) async fn splice(self) -> io::Result<u32> {
        let complete = self.await;
        complete.meta.result
    }
}

impl OpAble for Splice {
    #[cfg(all(target_os = "linux", feature = "iouring"))]
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        const FLAG: u32 = libc::SPLICE_F_MOVE;
        opcode::Splice::new(
            types::Fd(self.fd_in.raw_fd()),
            -1,
            types::Fd(self.fd_out.raw_fd()),
            -1,
            self.len,
        )
        .flags(FLAG)
        .build()
    }

    #[cfg(all(unix, feature = "legacy"))]
    #[inline]
    fn legacy_interest(&self) -> Option<(Direction, usize)> {
        match self.direction {
            SpliceDirection::FromPipe => self
                .fd_out
                .registered_index()
                .map(|idx| (Direction::Write, idx)),
            SpliceDirection::ToPipe => self
                .fd_in
                .registered_index()
                .map(|idx| (Direction::Read, idx)),
        }
    }

    #[cfg(all(unix, feature = "legacy"))]
    fn legacy_call(&mut self) -> io::Result<u32> {
        const FLAG: u32 = libc::SPLICE_F_MOVE | libc::SPLICE_F_NONBLOCK;
        let fd_in = self.fd_in.as_raw_fd();
        let fd_out = self.fd_out.as_raw_fd();
        let off_in = std::ptr::null_mut::<libc::loff_t>();
        let off_out = std::ptr::null_mut::<libc::loff_t>();
        syscall_u32!(splice(
            fd_in,
            off_in,
            fd_out,
            off_out,
            self.len as usize,
            FLAG
        ))
    }
}

/// File input retains the shared cache descriptor until CQE completion, even
/// when its awaiting future is dropped. An explicit offset never changes the
/// open file description's shared position.
pub(crate) struct SpliceFile {
    file: std::sync::Arc<std::os::fd::OwnedFd>,
    pipe: SharedFd,
    offset: i64,
    len: u32,
}

impl Op<SpliceFile> {
    pub(crate) fn splice_file_to_pipe(
        file: std::sync::Arc<std::os::fd::OwnedFd>,
        offset: u64,
        pipe: &SharedFd,
        len: u32,
    ) -> io::Result<Self> {
        let offset = i64::try_from(offset).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "splice offset exceeds i64")
        })?;
        Op::submit_with(SpliceFile {
            file,
            pipe: pipe.clone(),
            offset,
            len,
        })
    }

    pub(crate) async fn splice(self) -> io::Result<u32> {
        self.await.meta.result
    }
}

impl OpAble for SpliceFile {
    #[cfg(all(target_os = "linux", feature = "iouring"))]
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        use std::os::fd::AsRawFd;
        opcode::Splice::new(
            types::Fd(self.file.as_raw_fd()),
            self.offset,
            types::Fd(self.pipe.raw_fd()),
            -1,
            self.len,
        )
        .flags(libc::SPLICE_F_MOVE)
        .build()
    }

    #[cfg(any(feature = "legacy", feature = "poll-io"))]
    fn legacy_interest(&self) -> Option<(super::super::ready::Direction, usize)> {
        None
    }

    #[cfg(any(feature = "legacy", feature = "poll-io"))]
    fn legacy_call(&mut self) -> io::Result<u32> {
        // A buffered file miss may block. Never execute it in the legacy
        // readiness reactor as a supposedly nonblocking operation.
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "file splice requires io_uring",
        ))
    }
}
