use std::{
    collections::VecDeque,
    ffi::CStr,
    io::{self, Read},
    mem,
    os::{fd::RawFd, unix::ffi::OsStrExt as _},
    path::{Path, PathBuf},
    ptr,
    time::Duration,
};

use io_uring::{opcode, squeue, types, IoUring};
use libc::{O_CREAT, O_NOATIME, O_NOFOLLOW, O_TRUNC, O_WRONLY, S_IRGRP, S_IROTH, S_IRUSR, S_IWUSR};
use slab::Slab;
use tar::{Unpacked, Unpacker};

use crate::ring::{Ring, RingCtx, RingOp};

pub struct RingUnpacker<'a, F: FnMut(PathBuf)> {
    ring: Ring<IoUringUnpackerState<'a, F>, UnpackerOp<'a, F>>,
}

struct IoUringUnpackerState<'a, F: FnMut(PathBuf)> {
    files: Slab<UnpackerFile<'a>>,
    buffers: VecDeque<WriteBuf<'a>>,
    open_fds: usize,
    entry_callback: F,
}

impl<'a, F: FnMut(PathBuf)> RingUnpacker<'a, F> {
    pub fn new(buffer: &'a mut [u8], write_capacity: usize, entry_callback: F) -> io::Result<Self> {
        let ring = IoUring::builder().build(512)?;
        ring.submitter().register_iowq_max_workers(&mut [4, 0])?;
        Self::with_ring(ring, buffer, write_capacity, entry_callback)
    }

    pub fn with_ring(
        ring: IoUring,
        buffer: &'a mut [u8],
        write_capacity: usize,
        entry_callback: F,
    ) -> io::Result<Self> {
        assert!(buffer.len() % write_capacity == 0);

        // We register fixed buffers in chunks of up to 1GB as this is faster than registering many
        // `read_capacity` buffers. Registering fixed buffers saves the kernel some work in
        // checking/mapping/unmapping buffers for each read operation.
        const FIXED_BUFFER_LEN: usize = 1024 * 1024 * 1024;
        let iovecs = buffer
            .chunks(FIXED_BUFFER_LEN)
            .map(|buf| libc::iovec {
                iov_base: buf.as_ptr() as _,
                iov_len: buf.len(),
            })
            .collect::<Vec<_>>();

        // Split the buffer into `read_capacity` sized chunks.
        let buf_start = buffer.as_ptr() as usize;
        let buffers = buffer
            .chunks_exact_mut(write_capacity)
            .map(|buf| {
                let io_buf_index = (buf.as_ptr() as usize - buf_start) / FIXED_BUFFER_LEN;
                WriteBuf { buf, io_buf_index }
            })
            .collect::<VecDeque<_>>();

        let ring = Ring::new(
            ring,
            IoUringUnpackerState {
                // FIXME: see how many files we have in a snapshot today and
                // initialize with that capacity
                files: Slab::new(),
                buffers,
                open_fds: 0,
                entry_callback,
            },
        );

        // Safety:
        // The iovecs point to a buffer which is guaranteed to be valid for the
        // lifetime of the reader
        unsafe { ring.register_buffers(&iovecs)? };

        Ok(Self { ring })
    }

    fn wait_free_buf(&mut self) -> io::Result<WriteBuf<'a>> {
        let mut i = 0;
        loop {
            self.ring.process_completions()?;
            let buf = self.ring.ctx_mut().buffers.pop_front();
            match buf {
                Some(buf) => return Ok(buf),
                None => {
                    eprintln!("unpacker waiting {i}");
                    i += 1;
                    self.ring
                        .submit_and_wait(1, Some(Duration::from_millis(10)))?;
                }
            }
        }
    }

    pub fn drain(mut self) -> io::Result<()> {
        self.ring.drain()
    }
}

impl<'a, F: FnMut(PathBuf)> Unpacker for RingUnpacker<'a, F> {
    type File = usize;

    fn open(&mut self, dst: &Path, _overwrite: bool) -> io::Result<Self::File> {
        while self.ring.ctx().files.len() >= 1000 {
            eprintln!("too many open files");
            self.ring.process_completions()?;
            self.ring
                .submit_and_wait(1, Some(Duration::from_millis(10)))?;
        }

        // FIXME: pre-open accounts/, change accounts/bla to bla so we don't
        // keep re-walking the path and locking etc

        let file = UnpackerFile {
            path: dst.to_owned(),
            fd: None,
            backlog: Vec::new(),
            writes_started: 0,
            writes_completed: 0,
            eof: false,
        };
        let file_key = self.ring.ctx_mut().files.insert(file);

        let mut op = OpenOp {
            path: Vec::with_capacity(4096),
            file_key,
            _ctx: std::marker::PhantomData,
            _f: std::marker::PhantomData,
        };

        let buf_ptr = op.path.as_mut_ptr() as *mut u8;
        let bytes = dst.as_os_str().as_bytes();
        assert!(bytes.len() <= op.path.capacity() - 1);
        // Safety:
        // We know that the buffer is large enough to hold the copy and the
        // pointers don't overlap.
        unsafe {
            ptr::copy_nonoverlapping(bytes.as_ptr(), buf_ptr, bytes.len());
            buf_ptr.add(bytes.len()).write(0);
            op.path.set_len(bytes.len() + 1);
        }
        unsafe {
            self.ring.push(UnpackerOp::Open(op))?;
        }

        Ok(file_key)
    }

    fn copy(&mut self, src: &mut dyn Read, dest: &mut usize, size: u64) -> io::Result<u64> {
        let file_key = *dest;

        let mut offset = 0;
        let mut remaining = size as usize;
        while remaining > 0 {
            // eprintln!("waiting buf");
            let buf = self.wait_free_buf()?;
            // eprintln!("have buf");
            let state = self.ring.ctx_mut();
            let file = &mut state.files[file_key];

            let len = src.read(buf.buf)?;
            remaining -= len;
            if len == 0 || remaining == 0 {
                file.eof = true;

                if len == 0 {
                    state.buffers.push_front(buf);
                    if file.complete() {
                        let path = mem::replace(&mut file.path, PathBuf::new());
                        (state.entry_callback)(path);
                        let fd = file.fd.take().unwrap();
                        unsafe {
                            self.ring
                                .push(UnpackerOp::Close(CloseOp::new(fd, file_key)))?;
                        }
                    }
                    break;
                }
            }

            file.writes_started += 1;
            if let Some(fd) = &file.fd {
                let op = WriteOp {
                    file_key,
                    fd: *fd,
                    offset,
                    buf,
                    write_len: len,
                    _f: std::marker::PhantomData,
                };

                unsafe {
                    self.ring.push(UnpackerOp::Write(op))?;
                }
            } else {
                file.backlog.push((buf, offset, len));
            }

            offset += len;
        }

        Ok(size - remaining as u64)
    }

    fn pad(&mut self, _dst: &mut usize, _amount: usize) -> io::Result<()> {
        unimplemented!()
    }

    /// Set the ownerships of a file.
    fn set_ownerships(
        &mut self,
        _dst: &Path,
        _f: &Option<&mut Self::File>,
        _uid: u64,
        _gid: u64,
    ) -> io::Result<()> {
        Ok(())
    }

    /// Set the permissions of a file.
    fn set_perms(
        &mut self,
        _dst: &Path,
        _f: Option<&mut Self::File>,
        _mode: u32,
        _mask: u32,
        _preserve: bool,
    ) -> io::Result<()> {
        Ok(())
    }

    /// Set the access and modification times of a file.
    fn set_file_handle_times(
        &mut self,
        _f: &mut Self::File,
        _atime: Option<u64>,
        _mtime: Option<u64>,
    ) -> io::Result<()> {
        Ok(())
    }

    fn result(&mut self, _file: usize) -> io::Result<Unpacked> {
        Ok(Unpacked::__Nonexhaustive)
    }
}

struct OpenOp<'a, F> {
    // FIXME: pin this
    path: Vec<u8>,
    file_key: usize,
    _ctx: std::marker::PhantomData<&'a ()>,
    _f: std::marker::PhantomData<F>,
}

impl<'a, F> std::fmt::Debug for OpenOp<'a, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenOp")
            .field("path", &unsafe { CStr::from_ptr(self.path.as_ptr() as _) })
            .field("file_key", &self.file_key)
            .field("_ctx", &self._ctx)
            .finish()
    }
}

impl<'a, F: FnMut(PathBuf)> OpenOp<'a, F> {
    fn entry(&mut self) -> squeue::Entry {
        opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), self.path.as_ptr() as _)
            .flags(O_CREAT | O_TRUNC | O_NOFOLLOW | O_WRONLY | O_NOATIME)
            .mode(S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)
            .build()
    }

    fn complete(
        self,
        res: io::Result<i32>,
        ring: &mut RingCtx<IoUringUnpackerState<'a, F>, UnpackerOp<'a, F>>,
    ) -> io::Result<()>
    where
        Self: Sized,
    {
        let fd = match res {
            Ok(fd) => fd,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                // Safety:
                // Self::path is guaranteed to be valid while it's referenced by pointer by the
                // corresponding squeue::Entry.
                eprintln!(
                    "retrying {}",
                    CStr::from_bytes_until_nul(&self.path)
                        .unwrap()
                        .to_string_lossy()
                );
                unsafe { ring.push(UnpackerOp::Open(self))? };
                return Ok(());
            }
            Err(e) => return Err(e),
        };

        let state = ring.ctx_mut();
        let file = &mut state.files[self.file_key];
        // Safety: the fd is valid, having just been returned by the ring
        file.fd = Some(fd);
        state.open_fds += 1;

        let fd = file.fd.clone();
        let mut backlog = mem::replace(&mut file.backlog, Vec::new());
        for (buf, offset, len) in backlog.drain(..) {
            let op = WriteOp {
                file_key: self.file_key,
                fd: fd.clone().unwrap(),
                offset,
                buf,
                write_len: len,
                _f: std::marker::PhantomData,
            };
            unsafe {
                ring.push(UnpackerOp::Write(op))?;
            };
        }

        Ok(())
    }
}

#[derive(Debug)]
struct CloseOp<'a, F> {
    fd: Option<RawFd>,
    file_key: usize,
    _ctx: std::marker::PhantomData<&'a ()>,
    _f: std::marker::PhantomData<F>,
}

impl<'a, F: FnMut(PathBuf)> CloseOp<'a, F> {
    fn new(fd: RawFd, file_key: usize) -> Self {
        Self {
            fd: Some(fd),
            file_key,
            _ctx: std::marker::PhantomData,
            _f: std::marker::PhantomData,
        }
    }

    fn entry(&mut self) -> squeue::Entry {
        opcode::Close::new(types::Fd(self.fd.take().unwrap())).build()
    }

    fn complete(
        self,
        res: io::Result<i32>,
        ring: &mut RingCtx<IoUringUnpackerState<'a, F>, UnpackerOp<'a, F>>,
    ) -> io::Result<()>
    where
        Self: Sized,
    {
        let _ = res?;

        let state = ring.ctx_mut();
        let _ = state.files.remove(self.file_key);
        state.open_fds -= 1;

        Ok(())
    }
}

#[derive(Debug)]
struct WriteOp<'a, F> {
    file_key: usize,
    fd: RawFd,
    offset: usize,
    buf: WriteBuf<'a>,
    write_len: usize,
    _f: std::marker::PhantomData<F>,
}

impl<'a, F: FnMut(PathBuf)> WriteOp<'a, F> {
    fn entry(&mut self) -> squeue::Entry {
        let WriteOp {
            file_key: _,
            fd,
            offset,
            buf,
            write_len,
            _f: _,
        } = self;

        let prio =
            ioprio::Priority::new(ioprio::Class::BestEffort(ioprio::BePriorityLevel::highest()));
        opcode::WriteFixed::new(
            types::Fd(*fd),
            buf.buf.as_mut_ptr(),
            *write_len as u32,
            buf.io_buf_index as u16,
        )
        .offset(*offset as u64)
        .ioprio(prio.inner())
        .build()
        .flags(squeue::Flags::ASYNC)
    }

    fn complete(
        self,
        res: io::Result<i32>,
        ring: &mut RingCtx<IoUringUnpackerState<'a, F>, UnpackerOp<'a, F>>,
    ) -> io::Result<()>
    where
        Self: Sized,
    {
        let written = res? as usize;

        let WriteOp {
            file_key,
            fd,
            offset: _,
            buf,
            write_len,
            _f: _,
        } = self;

        assert_eq!(written, write_len, "short write");

        let state = ring.ctx_mut();
        state.buffers.push_front(buf);

        let file = &mut state.files[file_key];
        file.writes_completed += 1;
        if file.complete() {
            let path = mem::replace(&mut file.path, PathBuf::new());
            (state.entry_callback)(path);
            unsafe {
                ring.push(UnpackerOp::Close(CloseOp::new(fd, file_key)))?;
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
enum UnpackerOp<'a, F> {
    Open(OpenOp<'a, F>),
    Close(CloseOp<'a, F>),
    Write(WriteOp<'a, F>),
}

impl<'a, F: FnMut(PathBuf)> RingOp<IoUringUnpackerState<'a, F>> for UnpackerOp<'a, F> {
    fn entry(&mut self) -> squeue::Entry {
        match self {
            UnpackerOp::Open(op) => op.entry(),
            UnpackerOp::Close(op) => op.entry(),
            UnpackerOp::Write(op) => op.entry(),
        }
    }

    fn complete(
        self,
        res: io::Result<i32>,
        ring: &mut RingCtx<IoUringUnpackerState<'a, F>, UnpackerOp<'a, F>>,
    ) -> io::Result<()>
    where
        Self: Sized,
    {
        match self {
            UnpackerOp::Open(op) => op.complete(res, ring),
            UnpackerOp::Close(op) => op.complete(res, ring),
            UnpackerOp::Write(op) => op.complete(res, ring),
        }
    }
}

struct UnpackerFile<'a> {
    path: PathBuf,
    fd: Option<RawFd>,
    // FIXME: make this a smallvec
    backlog: Vec<(WriteBuf<'a>, usize, usize)>,
    eof: bool,
    writes_started: usize,
    writes_completed: usize,
}

impl UnpackerFile<'_> {
    fn complete(&self) -> bool {
        self.eof && self.writes_started == self.writes_completed
    }
}

impl<'a> std::fmt::Debug for UnpackerFile<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnpackerFile")
            .field("fd", &self.fd)
            .field("complete", &self.eof)
            .field("writes_started", &self.writes_started)
            .field("writes_completed", &self.writes_completed)
            .field("backlog", &self.backlog)
            .finish()
    }
}

struct WriteBuf<'a> {
    buf: &'a mut [u8],
    io_buf_index: usize,
}

impl<'a> std::fmt::Debug for WriteBuf<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteBuf")
            .field("io_buf_index", &self.io_buf_index)
            .finish()
    }
}
