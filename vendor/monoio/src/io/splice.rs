//! Splice related trait and default impl.

use std::future::Future;

use super::as_fd::{AsReadFd, AsWriteFd};
use crate::{driver::op::Op, net::Pipe};

/// Splice data from self to pipe.
pub trait SpliceSource {
    /// Splice data from self to pipe.
    fn splice_to_pipe<'a>(
        &'a mut self,
        pipe: &'a mut Pipe,
        len: u32,
    ) -> impl Future<Output = std::io::Result<u32>>;
}

/// Splice data from self from pipe.
pub trait SpliceDestination {
    /// Splice data from self from pipe.
    fn splice_from_pipe<'a>(
        &'a mut self,
        pipe: &'a mut Pipe,
        len: u32,
    ) -> impl Future<Output = std::io::Result<u32>>;
}

impl<T: AsReadFd> SpliceSource for T {
    #[inline]
    async fn splice_to_pipe<'a>(
        &'a mut self,
        pipe: &'a mut Pipe,
        len: u32,
    ) -> std::io::Result<u32> {
        Op::splice_to_pipe(self.as_reader_fd().as_ref(), &pipe.fd, len)?
            .splice()
            .await
    }
}

impl<T: AsWriteFd> SpliceDestination for T {
    #[inline]
    async fn splice_from_pipe<'a>(
        &'a mut self,
        pipe: &'a mut Pipe,
        len: u32,
    ) -> std::io::Result<u32> {
        Op::splice_from_pipe(&pipe.fd, self.as_writer_fd().as_ref(), len)?
            .splice()
            .await
    }
}

/// Splice an immutable shared file range into a pipe on the current io_uring
/// driver. The descriptor and pipe remain alive until completion, including
/// after cancellation. Does not advance the file's shared seek position.
/// Returns `Unsupported` on a legacy driver and `InvalidInput` for offsets
/// outside the signed 64-bit kernel range. Short completions are permitted.
pub async fn splice_file_to_pipe(
    file: std::sync::Arc<std::os::fd::OwnedFd>,
    offset: u64,
    pipe: &mut Pipe,
    len: u32,
) -> std::io::Result<u32> {
    Op::splice_file_to_pipe(file, offset, &pipe.fd, len)?
        .splice()
        .await
}
