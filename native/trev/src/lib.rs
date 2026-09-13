//! Exact, single-stream TREV calculation. Python owns replay and persistence.
#[cfg(feature = "python")]
mod bindings;
pub mod engine;
pub mod prepared;
pub mod stream;

#[derive(Debug)]
pub enum Error {
    Overflow,
    Expansion,
    Density,
    Data(&'static str),
    UnknownSpec(u64),
    PreparedCursors,
}
pub type Result<T> = std::result::Result<T, Error>;
