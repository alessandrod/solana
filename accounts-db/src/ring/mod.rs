#![allow(clippy::module_inception)]

#[cfg(target_os = "linux")]
mod ring;
#[cfg(target_os = "linux")]
pub mod ring_dir_remover;

#[cfg(target_os = "linux")]
pub use ring::*;
