use std::cell::RefCell;
use std::rc::Rc;
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

trait UnspecifiedReturnFn<A1, A2>:
    FnOnce(&mut A1, &A2) + Send + 'static
{
}

impl<F, A1, A2> UnspecifiedReturnFn<A1, A2> for F
where
    F: FnOnce(&mut A1, &A2) + Send + 'static
{
}

trait FirstArgMutableRefFn<A1, A2, A3>:
    FnOnce(&mut A1, &A2) -> A3 + Send + 'static
{
}

impl<F, A1, A2, R> FirstArgMutableRefFn<A1, A2, R> for F
where
    F: FnOnce(&mut A1, & A2) -> R + Send + 'static,
{
}

trait RawCallFnTrait<'a, R>:
    FirstArgMutableRefFn<
    duckdb::Connection,
    RefCell<ConnectionContext<'a>>,
    Result<R, duckdb::Error>,
>
where
    R: Send + 'static,
{
}

impl<'a, T, R> RawCallFnTrait<'a, R> for T where
    T: FirstArgMutableRefFn<
        duckdb::Connection,
        RefCell<ConnectionContext<'a>>,
        Result<R, duckdb::Error>,
    >,
    R: Send + 'static,
{
}

type RawCallFn<R> = dyn for<'a> RawCallFnTrait<'a, R>;

pub(crate) trait CallFnTrait<'a>:
    UnspecifiedReturnFn<duckdb::Connection, RefCell<ConnectionContext<'a>>>
{
}

impl<'a, T> CallFnTrait<'a> for T where
    T: UnspecifiedReturnFn<
        duckdb::Connection,
        RefCell<ConnectionContext<'a>>,
    >
{
}

trait LifetimeAOutlivesLifetimeB<'a, 'b> where 'a: 'b {}

impl<'a, 'b, T> LifetimeAOutlivesLifetimeB<'a, 'b> for T where 'a: 'b {}

pub(crate) type CallFn = dyn for<'a> CallFnTrait<'a>;

pub type ConnectionCallFn = Box<CallFn>;

pub enum ConnectionMessage {
    Execute(ConnectionCallFn),
    Close(oneshot::Sender<Result<(), DuckDBError>>),
    Begin(oneshot::Sender<Result<(), DuckDBError>>),
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
        F: for<'a> RawCallFnTrait<'a, R>,
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

    fn begin(
        &mut self,
    ) -> BoxFuture<
        '_,
        Result<sqlx_core::transaction::Transaction<'_, Self::Database>, sqlx_core::Error>,
    >
    where
        Self: Sized,
    {
        unimplemented!()
    }
}
