use crate::{error::DbError, types::{Value, Column}};

use super::lexer::Token;

#[derive(Debug)]
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

impl From<Token> for Expr {
    fn from(tok: Token) -> Self {
        match tok {
            Token::Int(v) => Expr::Literal(Value::Int(v)),
            Token::Float(v) => Expr::Literal(Value::Float(v)),
            Token::String(v) => Expr::Literal(Value::String(v)),
            _ => unreachable!(),
        }
    }
}

#[derive(Debug)]
pub enum Stmt {
    CreateTable {
        table: String,
        columns: Vec<Column>,
        primary_key: usize,
    },
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

#[derive(Debug)]
pub struct Rowset {
    pub table: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

#[derive(Debug)]
pub enum QueryResult {
    Affected(u64),

    // Rows(Rowset),
    Rows {
        table: String,
        rows: Option<Vec<Vec<Value>>>,
    }
}
