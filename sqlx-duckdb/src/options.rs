use crate::connection::{
    connect, CallFn, ConnectionMessage, DuckDBConnection, DuckDBTransactionContext
};
use crate::{DuckDB, DuckDBError};
use duckdb::Savepoint;
pub(crate) use sqlx_core::connection::*;
use sqlx_core::executor::Execute;
use sqlx_core::IndexMap;
use std::borrow::BorrowMut;
use std::cell::{Cell, RefCell, RefMut};
use std::rc::Rc;
use std::str::FromStr;
use std::{borrow::Cow, thread};
use tracing::event;

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
        connect(self)
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
