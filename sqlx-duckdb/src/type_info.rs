use derive_more::derive::{Display, From, Into};
use duckdb::types::Type;
use sqlx_core::type_info::TypeInfo;

#[derive(Debug, Clone, Display, From, Into, PartialEq, Eq)]
pub struct DuckDBTypeInfo(pub(crate) duckdb::types::Type);

impl TypeInfo for DuckDBTypeInfo {
    fn name(&self) -> &str {
        match self.0 {
            Type::Null => "NULL",
            Type::Boolean => "BOOLEAN",
            Type::TinyInt => "TINYINT",
            Type::SmallInt => "SMALLINT",
            Type::Int => "INTEGER",
            Type::BigInt => "BIGINT",
            Type::HugeInt => "HUGEINT",
            Type::UTinyInt => "UTINYINT",
            Type::USmallInt => "USMALLINT",
            Type::UInt => "UINTEGER",
            Type::UBigInt => "UBIGINT",
            Type::Float => "FLOAT",
            Type::Double => "DOUBLE",
            Type::Decimal => "DECIMAL",
            Type::Timestamp => "TIMESTAMP",
            Type::Text => "VARCHAR",
            Type::Blob => "BLOB",
            Type::Date32 => "DATE",
            Type::Time64 => "TIME",
            Type::Interval => "INTERVAL",
            Type::List(_) => "LIST",
            Type::Enum => "ENUM",
            Type::Struct(_) => "STRUCT",
            Type::Map(_, _) => "MAP",
            Type::Array(_, _) => "ARRAY",
            Type::Union => "UNION",
            Type::Any => "ANY",
        }
    }

    fn is_null(&self) -> bool {
        matches!(self.0, Type::Null)
    }

    fn is_void(&self) -> bool {
        false
    }

    fn type_compatible(&self, other: &Self) -> bool
        where
            Self: Sized, {
        self.0 == other.0
    }
}
