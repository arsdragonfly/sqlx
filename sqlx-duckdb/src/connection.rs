use std::cell::Cell;
use std::result::Result;

use futures_channel::oneshot;
use futures_core::future::BoxFuture;
pub(crate) use sqlx_core::connection::*;

use crate::database::DuckDB;
use crate::error::convert_received_error;
use crate::options::DuckDBConnectOptions;
use crate::DuckDBError;

pub(crate) type CallFn = dyn FnOnce(&duckdb::Connection) + Send + 'static;

pub type ConnectionCallFn = Box<CallFn>;

pub enum ConnectionMessage {
    Execute(ConnectionCallFn),
    Close(oneshot::Sender<Result<(), DuckDBError>>),
    Begin(oneshot::Sender<Result<(), DuckDBError>>),
}

pub(crate) enum DuckDBTransactionContext<'a> {
    None,
    Transaction(duckdb::Transaction<'a>),
    Savepoint(duckdb::Savepoint<'a>),
}

pub(crate) enum DuckDBEventLoopReturnReason {
    Commit,
    ConnectionClose,
}

pub struct DuckDBConnection {
    pub sender: flume::Sender<ConnectionMessage>,
}

impl DuckDBConnection {
    pub async fn call<F, R>(&self, function: F) -> Result<R, sqlx_core::Error>
    where
        F: FnOnce(&duckdb::Connection) -> Result<R, duckdb::Error> + Send + 'static,
        R: Send + 'static,
    {
        let (sender, receiver) = oneshot::channel::<Result<R, duckdb::Error>>();

        self.sender
            .send(ConnectionMessage::Execute(Box::new(move |conn| {
                let value = function(conn);
                let _ = sender.send(value);
            })))
            .map_err(|_| sqlx_core::Error::WorkerCrashed)?;

        convert_received_error(receiver.await.map(|r| r.map_err(DuckDBError::new)))
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
            convert_received_error(result)
        })
    }

    fn close_hard(self) -> BoxFuture<'static, Result<(), sqlx_core::Error>> {
        // TODO: duckdb-rs doesn't really provide hard close semantics
        Box::pin(async move {
            let (result_sender, result_receiver) = oneshot::channel();
            self.sender.send(ConnectionMessage::Close(result_sender));
            let result = result_receiver.await;
            convert_received_error(result)
        })
    }

    fn ping(&mut self) -> BoxFuture<'_, Result<(), sqlx_core::Error>> {
        Box::pin(async move {
            self.call(|conn| conn.execute("SELECT 1", []).map(|_| ()))
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

pub(crate) fn connect(
    options: &DuckDBConnectOptions,
) -> futures_core::future::BoxFuture<'_, Result<DuckDBConnection, sqlx_core::Error>> {
    Box::pin(async move {
        let (sender, receiver) = flume::unbounded::<ConnectionMessage>();
        let (result_sender, result_receiver) =
            oneshot::channel::<Result<DuckDBConnection, sqlx_core::Error>>();
        let url_clone = options.url.clone();
        let config_clone = options.config.clone();
        std::thread::spawn(move || {
            let mut config = duckdb::Config::default();
            for (key, value) in config_clone.config {
                match config.with(key.as_ref(), value.as_ref()) {
                    Err(e) => {
                        result_sender.send(Err(sqlx_core::Error::from(DuckDBError::new(e))));
                        return;
                    }
                    Ok(a) => config = a,
                }
            }
            let conn = duckdb::Connection::open_with_flags(url_clone.as_str(), config);
            if let Err(e) = conn {
                result_sender.send(Err(sqlx_core::Error::from(DuckDBError::new(e))));
                return;
            }
            result_sender.send(Ok(DuckDBConnection { sender }));
            let conn_cell: Cell<Option<duckdb::Connection>> = Cell::new(Some(conn.unwrap()));
            loop {
                let conn_raw = conn_cell.take().unwrap();
                let mut initial_context = DuckDBTransactionContext::None;
                if let (DuckDBEventLoopReturnReason::ConnectionClose, sender) = event_loop(&conn_raw, &mut initial_context, &receiver) {
                    match conn_raw.close() {
                        Ok(_) => {
                            sender.map(|s| s.send(Ok(())));
                            break;
                        }
                        Err((conn_new, e)) => {
                            sender.map(|s| s.send(Err(DuckDBError::new(e))));
                            conn_cell.set(Some(conn_new));
                        }
                    }
                }
            }
        });
        let result = result_receiver.await;
        match result {
            Err(_) => Err(sqlx_core::Error::WorkerCrashed),
            Ok(res) => res,
        }
    })
}

fn event_loop(
    conn: &duckdb::Connection,
    ctx: &mut DuckDBTransactionContext,
    receiver: &flume::Receiver<ConnectionMessage>,
) -> (
    DuckDBEventLoopReturnReason,
    Option<oneshot::Sender<Result<(), DuckDBError>>>,
) {
    // Every time we create a new transaction/savepoint, we call event_loop again so that previous tx/sp stays on the stack.
    // Committing the current transaction/releasing the topmost savepoint returns from the topmost event_loop.
    // This plays nicely with the lifetime bounds that duckdb-rs gives us.
    while let Ok(message) = receiver.recv() {
        match message {
            ConnectionMessage::Execute(f) => {
                f(&conn);
            }
            ConnectionMessage::Begin(s) => {
                let new_txn_context = match ctx {
                    DuckDBTransactionContext::None => conn
                        .unchecked_transaction()
                        .map(|txn| DuckDBTransactionContext::Transaction(txn)),
                    DuckDBTransactionContext::Transaction(ref mut txn) => txn
                        .savepoint()
                        .map(|sp| DuckDBTransactionContext::Savepoint(sp)),
                    DuckDBTransactionContext::Savepoint(ref mut sp) => sp
                        .savepoint()
                        .map(|sp| DuckDBTransactionContext::Savepoint(sp)),
                };
                match new_txn_context {
                    Err(e) => {
                        s.send(Err(DuckDBError::new(e)));
                    }
                    Ok(mut ctx_) => {
                        s.send(Ok(()));
                        let (reason, sender) = event_loop(conn, &mut ctx_, receiver);
                        match reason {
                            DuckDBEventLoopReturnReason::ConnectionClose => {
                                return (DuckDBEventLoopReturnReason::ConnectionClose, sender);
                            }
                            DuckDBEventLoopReturnReason::Commit => {
                                let commit_result = match ctx_ {
                                    DuckDBTransactionContext::Transaction(txn) => txn.commit(),
                                    DuckDBTransactionContext::Savepoint(sp) => sp.commit(),
                                    _ => Ok(()),
                                }
                                .map_err(|e| DuckDBError::new(e));
                                sender.map(|s| s.send(commit_result));
                            }
                        }
                    }
                }
            }
            ConnectionMessage::Close(s) => {
                // close is handled outside the bottommost event_loop because it consumes the connection,
                // which, by the lifetime bounds of duckdb-rs, invalidates all transactions/savepoints built on top of it.
                // It is not clear if the connection returned from a failed close() could still have tx/sp on it.
                // If so, fixing the situation would involve fixing upstream duckdb-rs and turning tx/sp APIs into
                // accessors on the connection object.
                return (DuckDBEventLoopReturnReason::ConnectionClose, Some(s));
            }
        }
    }
    return (DuckDBEventLoopReturnReason::ConnectionClose, None);
}
