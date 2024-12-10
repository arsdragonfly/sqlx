use crate::{connection::{ConnectionContext, ConnectionMessage, DuckDBTransactionContext}, error::convert_received_error, DuckDB, DuckDBConnection, DuckDBError};
use futures_channel::oneshot;
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use sqlx_core::{connection::Connection, transaction::TransactionManager};

pub struct DuckDBTransactionManager;

impl TransactionManager for DuckDBTransactionManager {
    type Database = DuckDB;

    fn begin(conn: &mut DuckDBConnection) -> BoxFuture<'_, Result<(), sqlx_core::Error>> {
        Box::pin(async move {
            let (sender, receiver) = oneshot::channel::<Result<(), DuckDBError>>();
            conn.sender.send(
                ConnectionMessage::Begin(sender)).map_err(|_| sqlx_core::Error::WorkerCrashed)?;
            convert_received_error(receiver.await)
        })
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