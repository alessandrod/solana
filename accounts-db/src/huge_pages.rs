use std::{
    io,
    ops::{Deref, DerefMut},
};

pub struct HugePagesBuffer {
    mem: *mut u8,
    cap: usize,
}

impl HugePagesBuffer {
    pub fn new(cap: usize) -> io::Result<Self> {
        let mem = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                cap,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_PRIVATE | libc::MAP_HUGETLB | libc::MAP_HUGE_2MB,
                -1,
                0,
            )
        };
        if mem == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { mem: mem as _, cap })
    }
}

impl Drop for HugePagesBuffer {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.mem as _, self.cap);
        }
    }
}

impl Deref for HugePagesBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.mem, self.cap) }
    }
}

impl DerefMut for HugePagesBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.mem, self.cap) }
    }
}
