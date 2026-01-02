use crate::{error::DbError, types::Value};

pub enum Expr {
    Literal(Value),
    Eq {
        column: String,
        value: Box<Expr>,
    }
}

impl Expr {
    pub fn eval(&self) -> Result<Value, DbError> {
        match self {
            Expr::Literal(v) => Ok(v.clone()),
            _ => Err(DbError::UnsupportedExpression),
        }
    }
}

pub enum Stmt {
    Insert {
        table: String,
        values: Vec<Expr>,
    },
    Select {
        table: String,
        filter: Option<Expr>,
    },
    Delete {
        table: String,
        filter: Expr,
    }
}

pub enum QueryResult {
    Rows(Option<Vec<Vec<Value>>>),
    Affected(u64),
}
