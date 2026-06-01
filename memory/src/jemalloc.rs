use {
    std::{ffi::c_char, fmt, marker::PhantomData, os::raw::c_uint, ptr, rc::Rc},
    thiserror::Error,
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("jemalloc mallctl {name} failed: {error}")]
    Mallctl {
        name: String,
        error: jemalloc_ctl::Error,
    },

    #[error("jemalloc mallctl {name} failed with errno code {code}")]
    MallctlCommand { name: String, code: i32 },

    #[error("jemalloc mallctl name is too long: {name}")]
    MallctlNameTooLong { name: String },

    #[error("jemalloc decay duration {millis} ms does not fit ssize_t")]
    DecayMillisOutOfRange { millis: u64 },

    #[error("jemalloc returned invalid decay duration {millis} ms")]
    InvalidDecayMillis { millis: isize },

    #[error("jemalloc page byte count overflows usize: {pages} * {page_size}")]
    PageByteCountOverflow { pages: usize, page_size: usize },
}

const MALLCTL_ARENAS_ALL: usize = 4096;

#[derive(Debug, Clone, Copy, Default)]
pub struct Jemalloc;

impl Jemalloc {
    pub fn create_arena() -> Result<Arena, Error> {
        read_raw("arenas.create").map(|id| Arena {
            id: ArenaId::from_raw(id),
        })
    }

    pub fn current_thread_arena() -> Result<ArenaId, Error> {
        read_raw("thread.arena").map(ArenaId::from_raw)
    }

    pub fn bind_current_thread(arena: ArenaId) -> Result<ThreadArenaGuard, Error> {
        Self::bind_current_thread_permanently(arena).map(ThreadArenaGuard::new)
    }

    pub fn bind_current_thread_permanently(arena: ArenaId) -> Result<ArenaId, Error> {
        update_raw("thread.arena", arena.as_raw()).map(ArenaId::from_raw)
    }

    pub fn advance_epoch() -> Result<(), Error> {
        jemalloc_ctl::epoch::advance()
            .map(|_| ())
            .map_err(|error| Error::Mallctl {
                name: "epoch".to_string(),
                error,
            })
    }

    pub fn stats() -> Result<Stats, Error> {
        Ok(Stats {
            allocated: read_stats_ctl("stats.allocated", jemalloc_ctl::stats::allocated::read)?,
            active: read_stats_ctl("stats.active", jemalloc_ctl::stats::active::read)?,
            metadata: read_stats_ctl("stats.metadata", jemalloc_ctl::stats::metadata::read)?,
            resident: read_stats_ctl("stats.resident", jemalloc_ctl::stats::resident::read)?,
            mapped: read_stats_ctl("stats.mapped", jemalloc_ctl::stats::mapped::read)?,
            retained: read_stats_ctl("stats.retained", jemalloc_ctl::stats::retained::read)?,
        })
    }

    pub fn merged_arena_stats() -> Result<ArenaStats, Error> {
        arena_stats(StatsArena::Merged)
    }

    pub fn page_size() -> Result<usize, Error> {
        read_raw("arenas.page")
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArenaId(c_uint);

impl ArenaId {
    pub const fn from_raw(raw: c_uint) -> Self {
        Self(raw)
    }

    pub const fn as_raw(self) -> c_uint {
        self.0
    }
}

impl fmt::Display for ArenaId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Arena {
    id: ArenaId,
}

impl Arena {
    pub const fn id(self) -> ArenaId {
        self.id
    }

    pub fn bind_current_thread(self) -> Result<ThreadArenaGuard, Error> {
        Jemalloc::bind_current_thread(self.id)
    }

    pub fn bind_current_thread_permanently(self) -> Result<ArenaId, Error> {
        Jemalloc::bind_current_thread_permanently(self.id)
    }

    pub fn set_dirty_decay(self, decay: Decay) -> Result<(), Error> {
        write_raw(&self.mallctl_name("dirty_decay_ms"), decay.to_mallctl()?)
    }

    pub fn dirty_decay(self) -> Result<Decay, Error> {
        Decay::from_mallctl(read_raw(&self.mallctl_name("dirty_decay_ms"))?)
    }

    pub fn set_muzzy_decay(self, decay: Decay) -> Result<(), Error> {
        write_raw(&self.mallctl_name("muzzy_decay_ms"), decay.to_mallctl()?)
    }

    pub fn muzzy_decay(self) -> Result<Decay, Error> {
        Decay::from_mallctl(read_raw(&self.mallctl_name("muzzy_decay_ms"))?)
    }

    pub fn set_oversize_threshold(self, bytes: usize) -> Result<(), Error> {
        write_raw(&self.mallctl_name("oversize_threshold"), bytes)
    }

    pub fn oversize_threshold(self) -> Result<usize, Error> {
        read_raw(&self.mallctl_name("oversize_threshold"))
    }

    pub fn set_retain_grow_limit(self, bytes: usize) -> Result<(), Error> {
        write_raw(&self.mallctl_name("retain_grow_limit"), bytes)
    }

    pub fn retain_grow_limit(self) -> Result<usize, Error> {
        read_raw(&self.mallctl_name("retain_grow_limit"))
    }

    pub fn decay(self) -> Result<(), Error> {
        command_raw(&self.mallctl_name("decay"))
    }

    pub fn purge(self) -> Result<(), Error> {
        command_raw(&self.mallctl_name("purge"))
    }

    pub fn stats(self) -> Result<ArenaStats, Error> {
        arena_stats(StatsArena::Arena(self.id))
    }

    /// Destroys this jemalloc arena.
    ///
    /// # Safety
    ///
    /// Jemalloc only checks that the arena was explicitly created and that no
    /// threads are associated with it. It cannot prove that thread caches are
    /// flushed, that all objects allocated from the arena are dead, or that no
    /// copied `ArenaId` will be reused after destruction. The caller must
    /// guarantee that:
    ///
    /// - this arena was explicitly created by `arenas.create`;
    /// - no thread is currently associated with this arena via `thread.arena`;
    /// - all thread caches that allocated or deallocated through this arena
    ///   have been flushed;
    /// - no live allocation from this arena will ever be accessed again;
    /// - no stale `Arena` or `ArenaId` handle will be used after this call.
    pub unsafe fn destroy(self) -> Result<(), Error> {
        command_raw(&self.mallctl_name("destroy"))
    }

    fn mallctl_name(self, name: &str) -> String {
        format!("arena.{}.{}", self.id.as_raw(), name)
    }
}

#[derive(Debug)]
pub struct ThreadArenaGuard {
    previous_arena: ArenaId,
    _not_send_or_sync: PhantomData<Rc<()>>,
}

impl ThreadArenaGuard {
    const fn new(previous_arena: ArenaId) -> Self {
        Self {
            previous_arena,
            _not_send_or_sync: PhantomData,
        }
    }

    pub const fn previous_arena(&self) -> ArenaId {
        self.previous_arena
    }
}

impl Drop for ThreadArenaGuard {
    fn drop(&mut self) {
        let _ = Jemalloc::bind_current_thread_permanently(self.previous_arena);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decay {
    Never,
    Immediate,
    Millis(u64),
}

impl Decay {
    fn to_mallctl(self) -> Result<isize, Error> {
        match self {
            Self::Never => Ok(-1),
            Self::Immediate => Ok(0),
            Self::Millis(millis) => {
                isize::try_from(millis).map_err(|_| Error::DecayMillisOutOfRange { millis })
            }
        }
    }

    fn from_mallctl(millis: isize) -> Result<Self, Error> {
        match millis {
            -1 => Ok(Self::Never),
            0 => Ok(Self::Immediate),
            millis if millis > 0 => u64::try_from(millis)
                .map(Self::Millis)
                .map_err(|_| Error::InvalidDecayMillis { millis }),
            millis => Err(Error::InvalidDecayMillis { millis }),
        }
    }
}

impl fmt::Display for Decay {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Never => formatter.write_str("never"),
            Self::Immediate => formatter.write_str("immediate"),
            Self::Millis(millis) => write!(formatter, "{millis} ms"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    pub allocated: usize,
    pub active: usize,
    pub metadata: usize,
    pub resident: usize,
    pub mapped: usize,
    pub retained: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArenaStats {
    pub mapped: usize,
    pub retained: usize,
    pub active: usize,
    pub dirty: usize,
    pub muzzy: usize,
    pub active_pages: usize,
    pub dirty_pages: usize,
    pub muzzy_pages: usize,
    pub dirty_decay: Decay,
    pub muzzy_decay: Decay,
    pub dirty_purges: PurgeStats,
    pub muzzy_purges: PurgeStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PurgeStats {
    pub sweeps: u64,
    pub madvise: u64,
    pub purged_pages: u64,
}

#[derive(Debug, Clone, Copy)]
enum StatsArena {
    Merged,
    Arena(ArenaId),
}

impl StatsArena {
    fn stat_name(self, stat_name: &str) -> String {
        match self {
            Self::Merged => format!("stats.arenas.{MALLCTL_ARENAS_ALL}.{stat_name}"),
            Self::Arena(arena_id) => format!("stats.arenas.{}.{stat_name}", arena_id.as_raw()),
        }
    }

    fn dirty_decay_name(self) -> String {
        self.decay_name("dirty_decay_ms")
    }

    fn muzzy_decay_name(self) -> String {
        self.decay_name("muzzy_decay_ms")
    }

    fn decay_name(self, decay_name: &str) -> String {
        match self {
            Self::Merged => format!("arenas.{decay_name}"),
            Self::Arena(arena_id) => format!("arena.{}.{decay_name}", arena_id.as_raw()),
        }
    }
}

fn arena_stats(arena: StatsArena) -> Result<ArenaStats, Error> {
    let page_size = Jemalloc::page_size()?;
    let active_pages = read_arena_stat(arena, "pactive")?;
    let dirty_pages = read_arena_stat(arena, "pdirty")?;
    let muzzy_pages = read_arena_stat(arena, "pmuzzy")?;

    Ok(ArenaStats {
        mapped: read_arena_stat(arena, "mapped")?,
        retained: read_arena_stat(arena, "retained")?,
        active: pages_to_bytes(active_pages, page_size)?,
        dirty: pages_to_bytes(dirty_pages, page_size)?,
        muzzy: pages_to_bytes(muzzy_pages, page_size)?,
        active_pages,
        dirty_pages,
        muzzy_pages,
        dirty_decay: Decay::from_mallctl(read_raw(&arena.dirty_decay_name())?)?,
        muzzy_decay: Decay::from_mallctl(read_raw(&arena.muzzy_decay_name())?)?,
        dirty_purges: PurgeStats {
            sweeps: read_arena_stat(arena, "dirty_npurge")?,
            madvise: read_arena_stat(arena, "dirty_nmadvise")?,
            purged_pages: read_arena_stat(arena, "dirty_purged")?,
        },
        muzzy_purges: PurgeStats {
            sweeps: read_arena_stat(arena, "muzzy_npurge")?,
            madvise: read_arena_stat(arena, "muzzy_nmadvise")?,
            purged_pages: read_arena_stat(arena, "muzzy_purged")?,
        },
    })
}

fn pages_to_bytes(pages: usize, page_size: usize) -> Result<usize, Error> {
    pages
        .checked_mul(page_size)
        .ok_or(Error::PageByteCountOverflow { pages, page_size })
}

fn read_arena_stat<T: Copy>(arena: StatsArena, stat_name: &str) -> Result<T, Error> {
    read_raw(&arena.stat_name(stat_name))
}

fn read_stats_ctl<T>(
    name: &str,
    read: impl FnOnce() -> std::result::Result<T, jemalloc_ctl::Error>,
) -> Result<T, Error> {
    read().map_err(|error| Error::Mallctl {
        name: name.to_string(),
        error,
    })
}

fn read_raw<T: Copy>(name: &str) -> Result<T, Error> {
    let mallctl_name = mallctl_name(name)?;

    // SAFETY: callers choose T according to jemalloc's documented mallctl type for `name`.
    unsafe { jemalloc_ctl::raw::read::<T>(&mallctl_name) }.map_err(|error| Error::Mallctl {
        name: name.to_string(),
        error,
    })
}

fn write_raw<T: Copy>(name: &str, value: T) -> Result<(), Error> {
    let mallctl_name = mallctl_name(name)?;

    // SAFETY: callers choose T according to jemalloc's documented mallctl type for `name`.
    unsafe { jemalloc_ctl::raw::write::<T>(&mallctl_name, value) }.map_err(|error| Error::Mallctl {
        name: name.to_string(),
        error,
    })
}

fn update_raw<T: Copy>(name: &str, value: T) -> Result<T, Error> {
    let mallctl_name = mallctl_name(name)?;

    // SAFETY: callers choose T according to jemalloc's documented mallctl type for `name`.
    unsafe { jemalloc_ctl::raw::update::<T>(&mallctl_name, value) }.map_err(|error| {
        Error::Mallctl {
            name: name.to_string(),
            error,
        }
    })
}

fn command_raw(name: &str) -> Result<(), Error> {
    let mallctl_name = mallctl_name(name)?;
    // SAFETY: `mallctl_name` is nul-terminated, and this command form does not read or write
    // any old or new values.
    let ret = unsafe {
        jemalloc_sys::mallctl(
            mallctl_name.as_ptr().cast::<c_char>(),
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            0,
        )
    };

    if ret == 0 {
        Ok(())
    } else {
        Err(Error::MallctlCommand {
            name: name.to_string(),
            code: ret,
        })
    }
}

fn mallctl_name(name: &str) -> Result<Vec<u8>, Error> {
    let capacity = name
        .len()
        .checked_add(1)
        .ok_or_else(|| Error::MallctlNameTooLong {
            name: name.to_string(),
        })?;
    let mut mallctl_name = Vec::with_capacity(capacity);
    mallctl_name.extend_from_slice(name.as_bytes());
    mallctl_name.push(0);
    Ok(mallctl_name)
}

#[cfg(test)]
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[cfg(test)]
mod tests {
    use crate::jemalloc::{ArenaId, Decay, Jemalloc};

    #[test]
    fn binds_current_thread_until_guard_drop() {
        let arena = Jemalloc::create_arena().unwrap();
        let original_arena = Jemalloc::current_thread_arena().unwrap();

        {
            let guard = arena.bind_current_thread().unwrap();
            assert_eq!(guard.previous_arena(), original_arena);
            assert_eq!(Jemalloc::current_thread_arena().unwrap(), arena.id());
        }

        assert_eq!(Jemalloc::current_thread_arena().unwrap(), original_arena);
    }

    #[test]
    fn changes_arena_decay_settings() {
        let arena = Jemalloc::create_arena().unwrap();

        arena.set_dirty_decay(Decay::Never).unwrap();
        arena.set_muzzy_decay(Decay::Immediate).unwrap();
        assert_eq!(arena.dirty_decay().unwrap(), Decay::Never);
        assert_eq!(arena.muzzy_decay().unwrap(), Decay::Immediate);
    }

    #[test]
    fn reads_global_and_arena_stats() {
        let arena = Jemalloc::create_arena().unwrap();

        arena.set_dirty_decay(Decay::Never).unwrap();
        arena.set_muzzy_decay(Decay::Immediate).unwrap();
        Jemalloc::advance_epoch().unwrap();

        let stats = Jemalloc::stats().unwrap();
        assert!(stats.active >= stats.allocated);
        assert!(stats.mapped >= stats.active);

        let merged_arena_stats = Jemalloc::merged_arena_stats().unwrap();
        assert!(merged_arena_stats.mapped >= merged_arena_stats.active);

        let arena_stats = arena.stats().unwrap();
        assert_eq!(arena_stats.dirty_decay, Decay::Never);
        assert_eq!(arena_stats.muzzy_decay, Decay::Immediate);
    }

    #[test]
    fn changes_arena_oversize_threshold() {
        const OVERSIZE_THRESHOLD: usize = 16_777_216;
        let arena = Jemalloc::create_arena().unwrap();

        arena.set_oversize_threshold(OVERSIZE_THRESHOLD).unwrap();

        assert_eq!(arena.oversize_threshold().unwrap(), OVERSIZE_THRESHOLD);
    }

    #[test]
    fn runs_arena_decay_and_purge_commands() {
        let arena = Jemalloc::create_arena().unwrap();

        arena.decay().unwrap();
        arena.purge().unwrap();
    }

    #[test]
    fn destroys_unused_arena() {
        let arena = Jemalloc::create_arena().unwrap();

        // SAFETY: this fresh arena has no associated threads, no allocations,
        // and no tcache activity.
        unsafe {
            arena.destroy().unwrap();
        }
    }

    #[test]
    fn preserves_raw_arena_id() {
        let arena_id = ArenaId::from_raw(42);

        assert_eq!(arena_id.as_raw(), 42);
        assert_eq!(arena_id.to_string(), "42");
    }
}
