pub mod api;
pub mod capture;
pub mod config;
pub mod db;
pub mod engine;
pub mod health;
pub mod journal;
pub mod model;
pub mod ops;
pub mod prepared;
pub mod recovery;
pub mod service;
pub mod storage;
pub mod venues;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("CHART_VALUE_OVERFLOW")]
    Overflow,
    #[error("TREV_EXPANSION_LIMIT")]
    Expansion,
    #[error("CHART_BAR_TOO_DENSE")]
    Density,
    #[error("{0}")]
    Data(&'static str),
}
pub type Result<T> = std::result::Result<T, Error>;
