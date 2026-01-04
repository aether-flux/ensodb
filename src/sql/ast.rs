use crate::{error::DbError, types::Value};

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
