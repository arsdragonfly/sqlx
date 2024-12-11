use std::cell::Cell;
use std::result::Result;

use duckdb::DropBehavior;
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
pub(crate) use sqlx_core::connection::*;

use crate::database::DuckDB;
use crate::error::convert_received_error;
use crate::options::DuckDBConnectOptions;
use crate::DuckDBError;

pub(crate) type CallFn = dyn FnOnce(&duckdb::Connection) + Send + 'static;

pub type ConnectionCallFn = Box<CallFn>;

pub enum ConnectionMessage {
    Execute(ConnectionCallFn),
    Begin(flume::Sender<Result<(), DuckDBError>>),
    Commit(flume::Sender<Result<(), DuckDBError>>),
    Rollback(Option<flume::Sender<Result<(), DuckDBError>>>),
    Close(flume::Sender<Result<(), DuckDBError>>),
}

pub(crate) enum DuckDBTransactionContext<'a> {
    None,
    Transaction(duckdb::Transaction<'a>),
    Savepoint(duckdb::Savepoint<'a>),
}

pub(crate) enum DuckDBEventLoopReturnReason {
    Commit,
    TransactionRollback,
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
        let (sender, receiver) = flume::bounded::<Result<R, duckdb::Error>>(1);

        self.sender
            .send(ConnectionMessage::Execute(Box::new(move |conn| {
                let value = function(conn);
                let _ = sender.send(value);
            })))
            .map_err(|_| sqlx_core::Error::WorkerCrashed)?;

        convert_received_error(
            receiver
                .into_recv_async()
                .await
                .map(|r| r.map_err(DuckDBError::new)),
        )
    }
}

impl Connection for DuckDBConnection {
    type Database = DuckDB;

    type Options = DuckDBConnectOptions;

    // close needs to use rendezvous channels to avoid connection/thread leak

    fn close(self) -> BoxFuture<'static, Result<(), sqlx_core::Error>> {
        let (sender, receiver) = flume::bounded(0);
        let send_result = self.sender.send(ConnectionMessage::Close(sender));
        match send_result {
            Err(_) => Box::pin(async { Err(sqlx_core::Error::WorkerCrashed) }),
            Ok(_) => Box::pin(
                receiver
                    .into_recv_async()
                    .map(|result| convert_received_error(result)),
            ),
        }
    }

    fn close_hard(self) -> BoxFuture<'static, Result<(), sqlx_core::Error>> {
        // TODO: duckdb-rs doesn't really provide hard close semantics
        let (sender, receiver) = flume::bounded(0);
        let send_result = self.sender.send(ConnectionMessage::Close(sender));
        match send_result {
            Err(_) => Box::pin(async { Err(sqlx_core::Error::WorkerCrashed) }),
            Ok(_) => Box::pin(
                receiver
                    .into_recv_async()
                    .map(|result| convert_received_error(result)),
            ),
        }
    }

    fn ping(&mut self) -> BoxFuture<'_, Result<(), sqlx_core::Error>> {
        Box::pin(self.call(|conn| conn.execute("SELECT 1", []).map(|_| ())))
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
    let (sender, receiver) = flume::unbounded::<ConnectionMessage>();
    let (result_sender, result_receiver) =
        flume::bounded::<Result<DuckDBConnection, sqlx_core::Error>>(0);
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
        if let Err(_) = result_sender.send(Ok(DuckDBConnection { sender })) {
            // receiver dropped, clean up connection and thread
            conn.unwrap().close();
            return;
        }
        let conn_cell: Cell<Option<duckdb::Connection>> = Cell::new(Some(conn.unwrap()));
        loop {
            let conn_raw = conn_cell.take().unwrap();
            let mut initial_context = DuckDBTransactionContext::None;
            if let (DuckDBEventLoopReturnReason::ConnectionClose, sender) =
                event_loop(&conn_raw, &mut initial_context, &receiver)
            {
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
    Box::pin(
        result_receiver
            .into_recv_async()
            .map(|result| match result {
                Err(_) => Err(sqlx_core::Error::WorkerCrashed),
                Ok(res) => res,
            }),
    )
}

fn event_loop(
    conn: &duckdb::Connection,
    ctx: &mut DuckDBTransactionContext,
    receiver: &flume::Receiver<ConnectionMessage>,
) -> (
    DuckDBEventLoopReturnReason,
    Option<flume::Sender<Result<(), DuckDBError>>>,
) {
    // Every time we create a new transaction/savepoint, we call event_loop again so that previous tx/sp stays on the stack.
    // Committing or rolling back the current transaction/releasing the topmost savepoint returns from the topmost event_loop.
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
                            DuckDBEventLoopReturnReason::TransactionRollback => {
                                let rollback_result = match ctx_ {
                                    DuckDBTransactionContext::Transaction(txn) => txn.rollback(),
                                    DuckDBTransactionContext::Savepoint(mut sp) => sp.rollback(),
                                    _ => Ok(()),
                                }
                                .map_err(|e| DuckDBError::new(e));
                                sender.map(|s| s.send(rollback_result));
                            }
                        }
                    }
                }
            }
            ConnectionMessage::Commit(s) => {
                return (DuckDBEventLoopReturnReason::Commit, Some(s));
            }
            ConnectionMessage::Rollback(s) => match ctx {
                DuckDBTransactionContext::None => {
                    s.map(|s| s.send(Ok(())));
                }
                DuckDBTransactionContext::Transaction(_) => {
                    return (DuckDBEventLoopReturnReason::TransactionRollback, s);
                }
                DuckDBTransactionContext::Savepoint(sp) => {
                    return (DuckDBEventLoopReturnReason::TransactionRollback, s);
                }
            },
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
