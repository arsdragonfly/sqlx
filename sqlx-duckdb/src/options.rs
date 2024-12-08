use crate::connection::{
    CallFn, CallFnTrait, ConnectionContext, ConnectionMessage, DuckDBConnection, DuckDBTransactionContext
};
use crate::{DuckDB, DuckDBError};
use futures_channel::oneshot;
pub(crate) use sqlx_core::connection::*;
use sqlx_core::executor::Execute;
use sqlx_core::IndexMap;
use std::borrow::BorrowMut;
use std::cell::{Cell, RefCell, RefMut};
use std::rc::Rc;
use std::str::FromStr;
use std::{borrow::Cow, thread};

#[derive(Clone, Debug)]
pub struct DuckDBConnectConfig {
    pub config: IndexMap<Cow<'static, str>, Cow<'static, str>>,
}

#[derive(Clone, Debug)]
pub struct DuckDBConnectOptions {
    pub url: url::Url,
    pub config: DuckDBConnectConfig,
}

impl FromStr for DuckDBConnectOptions {
    type Err = sqlx_core::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let url = url::Url::parse(s).map_err(|e| sqlx_core::Error::Configuration(Box::new(e)))?;
        Ok(Self {
            url,
            config: DuckDBConnectConfig {
                config: IndexMap::new(),
            },
        })
    }
}

impl ConnectOptions for DuckDBConnectOptions {
    type Connection = DuckDBConnection;

    fn from_url(url: &url::Url) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            url: url.clone(),
            config: DuckDBConnectConfig {
                config: IndexMap::new(),
            },
        })
    }

    fn to_url_lossy(&self) -> url::Url {
        self.url.clone()
    }

    fn connect(
        &self,
    ) -> futures_core::future::BoxFuture<'_, Result<Self::Connection, sqlx_core::Error>>
    where
        Self::Connection: Sized,
    {
        Box::pin(async move {
            let (sender, receiver) = flume::unbounded::<ConnectionMessage>();
            let (result_sender, result_receiver) =
                oneshot::channel::<Result<Self::Connection, sqlx_core::Error>>();
            let url_clone = self.url.clone();
            let config_clone = self.config.clone();
            thread::spawn(move || {
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
                let conn_cell: Cell<Option<duckdb::Connection>> = Cell::new(Some(conn.unwrap()));
                // let conn_: &'static mut _ = Box::leak(Box::new(conn.unwrap()));
                // let conn_ = conn.unwrap();
                loop {
                    {
                        let mut conn_raw = conn_cell.take().unwrap();
                        if let Some(sender) = event_loop(&mut conn_raw, &receiver) {
                            // we received a signal to close the connection
                            match conn_raw.close() {
                                Ok(_) => {
                                    sender.send(Ok(()));
                                    break;
                                }
                                Err((c, e)) => {
                                    sender.send(Err(DuckDBError::new(e)));
                                    conn_cell.set(Some(c));
                                }
                            }
                        } else {
                            break;
                        }
                    }
                }
            });
            let result = result_receiver.await;
            match result {
                Err(e) => Err(sqlx_core::Error::WorkerCrashed),
                Ok(res) => res,
            }
        })
    }

    fn log_statements(self, level: log::LevelFilter) -> Self {
        // TODO: implement this
        self
    }

    fn log_slow_statements(self, level: log::LevelFilter, duration: std::time::Duration) -> Self {
        // TODO: implement this
        self
    }

    fn disable_statement_logging(self) -> Self {
        // TODO: implement this
        self
    }
}

fn event_loop(
    // conn: &mut duckdb::Connection,
    conn: &mut duckdb::Connection,
    receiver: &flume::Receiver<ConnectionMessage>,
) -> Option<oneshot::Sender<Result<(), DuckDBError>>> {
    let mut ctx = RefCell::new(ConnectionContext {
        transaction_context: None,
    });
    let ctx_ = &ctx;
    while let Ok(message) = receiver.recv() {
        match message {
            ConnectionMessage::Execute(f) => f(conn, &ctx),
            ConnectionMessage::Close(s) => {
                return Some(s);
            }
            _ => {
                // let txn = conn.borrow_mut().transaction().unwrap();
                begin_txn(conn, ctx_);
                todo!();
            }
        }
    }
    return None;
}

fn begin_txn<'b, 'a: 'b>(
    conn: &'a mut duckdb::Connection,
    ctx: &RefCell<ConnectionContext<'b>>,
) {
    // let txn = conn.transaction().unwrap();
    let txn = conn.transaction().unwrap();
    let txn_ctx = DuckDBTransactionContext {
        transaction: txn,
        savepoints: Vec::new(),
    };
    let new_ctx = RefCell::new(ConnectionContext {
        transaction_context: Some(txn_ctx),
    });
    new_ctx.swap(ctx);
}

fn foo() -> Box<CallFn> {
    Box::new(&begin_txn)
    // Box::new(|conn, ctx| -> () {
    //     begin_txn(conn, ctx);
    // })
}