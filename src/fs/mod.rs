//! Filesystem manipulation operations.

mod directory;
pub use directory::create_dir;
pub use directory::remove_dir;
pub use directory::Dir;
pub use directory::DirEntry;

mod file;
pub use file::remove_file;
pub use file::rename;
pub use file::metadata;
pub use file::symlink_metadata;
pub use file::File;
pub use file::Metadata;
pub use file::FileType;

mod open_options;
pub use open_options::OpenOptions;
