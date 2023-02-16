use crate::constants::{INTERVAL_NS, START_TIME_NS};
use std::path::{Path, PathBuf};

pub mod book;
pub mod constants;
pub mod data;
pub mod replay;
pub mod stat;
pub mod summary;

pub(crate) fn interval_loc(timestamp: u64) -> usize {
    ((timestamp - START_TIME_NS) / INTERVAL_NS) as usize
}

pub(crate) fn create_folder(path: &Path, out_dir: &Path) -> PathBuf {
    let wo_extension = path.with_extension("").with_extension("");
    let file_name = wo_extension.file_name().unwrap();
    let out_dir = out_dir.join(file_name.to_str().unwrap());
    let _ = std::fs::create_dir_all(&out_dir);
    out_dir
}
