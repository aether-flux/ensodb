use std::string::FromUtf8Error;

#[derive(Debug)]
pub enum DbError {
    DatabaseExists,
    DatabaseNotFound,
    NoDatabaseSelected,

    InvalidPrimaryKey,
    InvalidPrimaryKeyType,
    TableNotFound,
    NoTableSelected,
    ColumnCountMismatch,
    TypeMismatch { column: String },

    UnsupportedExpression,
    UnsupportedStatement,
    UnsupportedFilter,
    ParseError(String),
    UnexpectedToken { expected: String, found: String },

    Io(std::io::Error),
    SerdeJsonError(serde_json::Error),
    Utf8(FromUtf8Error),
}

impl From<std::io::Error> for DbError {
    fn from(e: std::io::Error) -> Self {
        DbError::Io(e)
    }
}

impl From<serde_json::Error> for DbError {
    fn from(e: serde_json::Error) -> Self {
        DbError::SerdeJsonError(e)
    }
}

impl From<FromUtf8Error> for DbError {
    fn from(e: FromUtf8Error) -> Self {
        DbError::Utf8(e)
    }
}
