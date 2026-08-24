#[cfg(not(windows))]
use std::fs::File;
use std::{io, path::Path};

/// Flush directory-entry changes where the host filesystem exposes that
/// operation. Windows flushes the files themselves, but does not permit a
/// directory to be opened as a regular file for this purpose.
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
