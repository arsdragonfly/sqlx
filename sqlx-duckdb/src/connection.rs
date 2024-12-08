use std::cell::RefCell;
use std::result::Result;
use std::sync::{Arc, Mutex};

use derive_more::derive;
use duckdb::types::ToSqlOutput;
use futures_channel::oneshot;
use futures_core::future::BoxFuture;
pub(crate) use sqlx_core::connection::*;

use crate::database::DuckDB;
use crate::options::DuckDBConnectOptions;
use crate::DuckDBError;

trait FirstArgStaticLifetimeUnspecifiedReturnFn<A1, A2>: FnOnce(&mut A1, &mut A2) + Send + 'static where
    A1: 'static,
{
}

impl<F, A1, A2> FirstArgStaticLifetimeUnspecifiedReturnFn<A1, A2> for F where
    F: FnOnce(&mut A1, &mut A2) + Send + 'static,
    A1: 'static,
{
}

trait FirstArgStaticLifetimeFn<A1, A2, A3>: FnOnce(&mut A1, &mut A2) -> A3 + Send + 'static where
    A1: 'static,
{
}

impl<F, A1, A2, R> FirstArgStaticLifetimeFn<A1, A2, R> for F where
    F: FnOnce(&mut A1, &mut A2) -> R + Send + 'static,
    A1: 'static,
{
}

trait RawCallFnTrait<R>: for<'a> FirstArgStaticLifetimeFn<duckdb::Connection, ConnectionContext<'a>, Result<R, duckdb::Error>> {}

impl<T, R> RawCallFnTrait<R> for T where T: for<'a> FirstArgStaticLifetimeFn<duckdb::Connection, ConnectionContext<'a>, Result<R, duckdb::Error>> {}

type RawCallFn<R> = dyn RawCallFnTrait<R>;

trait CallFnTrait: for<'a> FirstArgStaticLifetimeUnspecifiedReturnFn<duckdb::Connection, ConnectionContext<'a>> {}

impl<T> CallFnTrait for T where T: for<'a> FirstArgStaticLifetimeUnspecifiedReturnFn<duckdb::Connection, ConnectionContext<'a>> {}

type CallFn = dyn CallFnTrait;

pub type ConnectionCallFn = Box<CallFn>;

pub enum ConnectionMessage {
    Execute(ConnectionCallFn),
    Close(oneshot::Sender<Result<(), DuckDBError>>),
}

pub(crate) struct ConnectionContext<'a> {
    pub(crate) transaction_context: Option<DuckDBTransactionContext<'a>>,
}

pub(crate) struct DuckDBTransactionContext<'a> {
    pub(crate) transaction: duckdb::Transaction<'a>,
    pub(crate) savepoints: Vec<duckdb::Savepoint<'a>>,
}

pub struct DuckDBConnection {
    pub sender: flume::Sender<ConnectionMessage>,
}

impl DuckDBConnection {
    pub async fn call<F, R>(&self, function: F) -> Result<R, sqlx_core::Error>
    where
        F: RawCallFnTrait<R>,
        R: Send + 'static,
    {
        let (sender, receiver) = oneshot::channel::<Result<R, duckdb::Error>>();

        self.sender
            .send(ConnectionMessage::Execute(Box::new(move |conn, ctx| {
                let value = function(conn, ctx);
                let _ = sender.send(value);
            })))
            .map_err(|_| sqlx_core::Error::WorkerCrashed)?;

        match receiver.await {
            Err(_) => Err(sqlx_core::Error::WorkerCrashed),
            Ok(Err(e)) => Err(sqlx_core::Error::from(DuckDBError::new(e))),
            Ok(Ok(value)) => Ok(value),
        }
    }
}

impl Connection for DuckDBConnection {
    type Database = DuckDB;

    type Options = DuckDBConnectOptions;

    fn close(self) -> BoxFuture<'static, Result<(), sqlx_core::Error>> {
        Box::pin(async move {
            let (result_sender, result_receiver) = oneshot::channel();
            self.sender.send(ConnectionMessage::Close(result_sender));
            let result = result_receiver.await;
            match result {
                Err(e) => Err(sqlx_core::Error::WorkerCrashed),
                Ok(Err(e)) => Err(sqlx_core::Error::from(e)),
                Ok(Ok(_)) => Ok(()),
            }
        })
    }

    fn close_hard(self) -> BoxFuture<'static, Result<(), sqlx_core::Error>> {
        // TODO: duckdb-rs doesn't really provide hard close semantics
        Box::pin(async move {
            let (result_sender, result_receiver) = oneshot::channel();
            self.sender.send(ConnectionMessage::Close(result_sender));
            let result = result_receiver.await;
            match result {
                Err(e) => Err(sqlx_core::Error::WorkerCrashed),
                Ok(Err(e)) => Err(sqlx_core::Error::from(e)),
                Ok(Ok(_)) => Ok(()),
            }
        })
    }

    fn ping(&mut self) -> BoxFuture<'_, Result<(), sqlx_core::Error>> {
        Box::pin(async move {
            self.call(|conn, _| conn.execute("SELECT 1", []).map(|_| ()))
                .await
        })
    }

    #[inline]
    fn shrink_buffers(&mut self) {
        // No-op.
    }

    #[doc(hidden)]
    fn flush(&mut self) -> BoxFuture<'_, Result<(), sqlx_core::Error>> {
        Box::pin(futures_util::future::ok(()))
    }

    #[doc(hidden)]
    fn should_flush(&self) -> bool {
        false
    }

    fn begin(&mut self) -> BoxFuture<'_, Result<sqlx_core::transaction::Transaction<'_, Self::Database>, sqlx_core::Error>>
        where
            Self: Sized {
        unimplemented!()
    }
}
