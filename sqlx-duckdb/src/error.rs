use duckdb::{arrow::util::display, Error};
use futures_channel::oneshot::Canceled;
pub(crate) use sqlx_core::error::*;
use derive_more::{derive::{Display, Error}, AsMut, AsRef, Constructor, Debug, Deref, DerefMut, From, Into};
use std::sync::Arc;

use crate::DuckDB;

// #[derive(Debug, Display, From, Into, AsRef, AsMut, Deref, DerefMut, Constructor, Error)]
#[derive(Debug, Display)]
#[display("{}", message)]
pub struct DuckDBError
{
    pub error: duckdb::Error,
    pub message: String,
}

impl std::error::Error for DuckDBError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

impl From<duckdb::Error> for DuckDBError {
    fn from(error: duckdb::Error) -> Self {
        Self::new(error)
    }
}

impl DuckDBError {
    pub fn new(error: duckdb::Error) -> Self {
        let message = error.to_string().to_owned();
        Self {
            error,
            message
        }
    }
}

impl DatabaseError for DuckDBError {
    fn message(&self) -> &str {
        &self.message.as_str()
    }

    fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        self
    }

    fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
        self
    }

    fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
        self
    }

    fn kind(&self) -> ErrorKind {
        match self.error {
            _ => ErrorKind::Other
        }
    }
}

pub(crate) fn convert_received_error<R>(e: Result<Result<R, DuckDBError>, Canceled>) -> Result<R, sqlx_core::Error> {
    match e {
        Err(_) => Err(sqlx_core::Error::WorkerCrashed),
        Ok(Err(e)) => Err(sqlx_core::Error::from(e)),
        Ok(Ok(r)) => Ok(r),
    }
}