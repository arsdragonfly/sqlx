pub(crate) use sqlx_core::database::{Database, HasStatementCache};

/*
use crate::{
    SqliteArgumentValue, SqliteArguments, SqliteColumn, SqliteConnection, SqliteQueryResult,
    SqliteRow, SqliteStatement, SqliteTransactionManager, SqliteTypeInfo, SqliteValue,
    SqliteValueRef,
};
*/



/// DuckDB database driver.
#[derive(Debug)]
pub struct DuckDB;

/*
impl Database for DuckDB {
    type Connection = DuckDBConnection;

    type TransactionManager = DuckDBTransactionManager;

    type Row = DuckDBRow;

    type QueryResult = DuckDBQueryResult;

    type Column = DuckDBColumn;

    type TypeInfo = DuckDBTypeInfo;

    type Value = DuckDBValue;
    type ValueRef<'r> = DuckDBValueRef<'r>;

    type Arguments<'q> = DuckDBArguments<'q>;
    type ArgumentBuffer<'q> = Vec<DuckDBArgumentValue<'q>>;

    type Statement<'q> = DuckDBStatement<'q>;

    const NAME: &'static str = "DuckDB";

    const URL_SCHEMES: &'static [&'static str] = &["duckdb"];
}
*/

/*
impl Database for Sqlite {
    type Connection = SqliteConnection;

    type TransactionManager = SqliteTransactionManager;

    type Row = SqliteRow;

    type QueryResult = SqliteQueryResult;

    type Column = SqliteColumn;

    type TypeInfo = SqliteTypeInfo;

    type Value = SqliteValue;
    type ValueRef<'r> = SqliteValueRef<'r>;

    type Arguments<'q> = SqliteArguments<'q>;
    type ArgumentBuffer<'q> = Vec<SqliteArgumentValue<'q>>;

    type Statement<'q> = SqliteStatement<'q>;

    const NAME: &'static str = "SQLite";

    const URL_SCHEMES: &'static [&'static str] = &["sqlite"];
}
*/

impl HasStatementCache for DuckDB {}
