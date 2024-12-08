use crate::{connection::{ConnectionContext, DuckDBTransactionContext}, DuckDB, DuckDBConnection, DuckDBError};
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use sqlx_core::{connection::Connection, transaction::TransactionManager};

pub struct DuckDBTransactionManager;

trait IsDuckDBConnection {
    fn as_duckdb(&self) -> &duckdb::Connection;
    fn as_duckdb_mut(&mut self) -> &mut duckdb::Connection;
}

impl IsDuckDBConnection for duckdb::Connection {
    fn as_duckdb(&self) -> &duckdb::Connection {
        self
    }

    fn as_duckdb_mut(&mut self) -> &mut duckdb::Connection {
        self
    }
}

impl TransactionManager for DuckDBTransactionManager {
    type Database = DuckDB;

    fn begin(conn: &mut DuckDBConnection) -> BoxFuture<'_, Result<(), sqlx_core::Error>> {
        unimplemented!()
    }

    fn commit(conn: &mut DuckDBConnection) -> BoxFuture<'_, Result<(), sqlx_core::Error>> {
        unimplemented!()
    }

    fn rollback(conn: &mut DuckDBConnection) -> BoxFuture<'_, Result<(), sqlx_core::Error>> {
        unimplemented!()
    }

    fn start_rollback(conn: &mut DuckDBConnection) {
        unimplemented!()
    }
}