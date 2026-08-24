#[cfg(not(windows))]
use std::fs::File;
use std::{io, path::Path};

/// Persist directory-entry changes where the host filesystem supports it.
///
/// Unix filesystems expose directories as synchronizable file descriptors.
/// Windows does not allow a directory to be opened through `File::open`, so
/// file contents are flushed by the callers while directory-entry durability
/// is left to the Windows filesystem semantics.
pub(crate) fn sync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(not(windows))]
    {
        File::open(dir)?.sync_all()
    }

    #[cfg(windows)]
    {
        let _ = dir;
        Ok(())
    }
}
