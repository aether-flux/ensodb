use crate::{error::DbError, types::{Column, DataType}};

use super::{ast::{Expr, Stmt}, lexer::{Lexer, Token}};

pub struct Parser {
    lexer: Lexer,
    current: Token,
}

impl Parser {
    pub fn new(mut lexer: Lexer) -> Result<Self, DbError> {
        let current = lexer.next_token().map_err(|e| DbError::ParseError(e))?;

        Ok(Self { lexer, current })
    }

    fn advance(&mut self) -> Result<(), DbError> {
        self.current = self.lexer
            .next_token()
            .map_err(|e| DbError::ParseError(e))?;
        Ok(())
    }

    fn expect(&mut self, expected: Token) -> Result<(), DbError> {
        if self.current == expected {
            self.advance()
        } else {
            Err(DbError::UnexpectedToken {
                expected: format!("{:?}", expected),
                found: format!("{:?}", self.current),
            })
        }
    }

    fn expect_ident(&mut self) -> Result<String, DbError> {
        if let Token::Ident(name) = &self.current {
            let out = name.clone();
            self.advance()?;
            Ok(out)
        } else {
            Err(DbError::UnexpectedToken {
                expected: "identifier".into(),
                found: format!("{:?}", self.current),
            })
        }
    }

    // Parsing
    fn parse_create(&mut self) -> Result<Stmt, DbError> {
        self.expect(Token::Create)?;
        self.expect(Token::Table)?;

        let table = self.expect_ident()?;

        self.expect(Token::LParen)?;

        let mut columns = Vec::new();
        let mut primary_key = None;

        loop {
            // column name
            let col_name = self.expect_ident()?;

            // column type
            // self.advance()?;
            // let dtype = match self.current.clone() {
            //     Token::Int(v) => DataType::Int,
            //     Token::String(v) => DataType::String,
            //     Token::Float(v) => DataType::Float,
            //     _ => return Err(DbError::InvalidDataType),
            // };

            let dtype = match &self.current {
                Token::Ident(t) => match t.to_uppercase().as_str() {
                    "INT" => DataType::Int,
                    "STRING" => DataType::String,
                    "FLOAT" => DataType::Float,
                    "BOOL" => DataType::Bool,
                    _ => return Err(DbError::InvalidDataType),
                },
                _ => return Err(DbError::InvalidDataType),
            };
            self.advance()?;

            let col_idx = columns.len();
            columns.push(Column::new(&col_name, dtype));

            if self.current == Token::Primary {
                self.advance()?;
                self.expect(Token::Key)?;

                if primary_key.is_some() {
                    return Err(DbError::DuplicatePrimaryKey);
                }

                primary_key = Some(col_idx);
            }

            match self.current {
                Token::Comma => {
                    self.advance()?;
                    continue;
                }
                Token::RParen => {
                    self.advance()?;
                    break;
                }
                _ => {
                    return Err(DbError::UnexpectedToken {
                        expected: "comma or ')'".into(),
                        found: format!("{:?}", self.current),
                    });
                }
            }
        }

        let primary_key = primary_key.ok_or(DbError::PrimaryKeyMissing)?;

        Ok(Stmt::CreateTable {
            table,
            columns,
            primary_key,
        })
    }

    fn parse_insert(&mut self) -> Result<Stmt, DbError> {
        self.expect(Token::Insert)?;
        self.expect(Token::Into)?;

        let table = self.expect_ident()?;

        self.expect(Token::Values)?;
        self.expect(Token::LParen)?;

        let mut values = Vec::new();

        loop {
            values.push(self.parse_expr()?);

            match self.current {
                Token::Comma => {
                    self.advance()?;
                }
                Token::RParen => break,
                _ => {
                    return Err(DbError::ParseError("Expected ',' or ')' in VALUES".into()))
                }
            }
        }

        self.expect(Token::RParen)?;
        self.expect(Token::Semicolon)?;

        Ok(Stmt::Insert { table, values })
    }

    fn parse_select(&mut self) -> Result<Stmt, DbError> {
        self.expect(Token::Select)?;
        self.expect(Token::Star)?;
        self.expect(Token::From)?;

        let table = self.expect_ident()?;

        let filter = if self.current == Token::Where {
            Some(self.parse_where()?)
        } else {
            None
        };

        self.expect(Token::Semicolon)?;

        Ok(Stmt::Select { table, filter })
    }

    fn parse_delete(&mut self) -> Result<Stmt, DbError> {
        self.expect(Token::Delete)?;
        self.expect(Token::From)?;

        let table = self.expect_ident()?;

        let filter = if self.current == Token::Where {
            self.parse_where()?
        } else {
            return Err(DbError::ParseError("DELETE requires WHERE clause".into()));
        };

        self.expect(Token::Semicolon)?;

        Ok(Stmt::Delete { table, filter })
    }

    fn parse_where(&mut self) -> Result<Expr, DbError> {
        self.expect(Token::Where)?;

        let column = self.expect_ident()?;
        self.expect(Token::Eq)?;

        let value = self.parse_expr()?;

        Ok(Expr::Eq {
            column,
            value: Box::new(value),
        })
    }

    fn parse_expr(&mut self) -> Result<Expr, DbError> {
        match &self.current {
            Token::Int(_) |
            Token::Float(_) |
            Token::String(_) => {
                let token = self.current.clone();
                self.advance()?;
                Ok(Expr::from(token))
            }

            _ => Err(DbError::UnsupportedExpression),
        }
    }

    pub fn parse_stmt(&mut self) -> Result<Stmt, DbError> {
        match self.current {
            Token::Create => self.parse_create(),
            Token::Insert => self.parse_insert(),
            Token::Select => self.parse_select(),
            Token::Delete => self.parse_delete(),
            _ => Err(DbError::UnsupportedStatement),
        }
    }
}
