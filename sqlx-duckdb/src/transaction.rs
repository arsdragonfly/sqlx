use crate::{connection::{ConnectionMessage, DuckDBTransactionContext}, error::convert_received_error, DuckDB, DuckDBConnection, DuckDBError};
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use sqlx_core::{connection::Connection, transaction::TransactionManager};

pub struct DuckDBTransactionManager;

impl TransactionManager for DuckDBTransactionManager {
    type Database = DuckDB;

    // begin, commit and rollback need to use rendezvous channels to avoid transaction/savepoint leak

    fn begin(conn: &mut DuckDBConnection) -> BoxFuture<'_, Result<(), sqlx_core::Error>> {
        let (sender, receiver) = flume::bounded::<Result<(), DuckDBError>>(0);
        let send_result = conn.sender.send(
            ConnectionMessage::Begin(sender)).map_err(|_| sqlx_core::Error::WorkerCrashed);
        match send_result {
            Err(_) => Box::pin(async { Err(sqlx_core::Error::WorkerCrashed) }),
            Ok(_) => Box::pin(receiver.into_recv_async().map(|result| convert_received_error(result)))
        }
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