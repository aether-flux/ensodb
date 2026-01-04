use std::time;

use api::Enso;
use engine::Engine;
use sql::{lexer::{Lexer, Token}, parser::Parser};
use types::{Column, Value};

mod record;
mod storage;
mod utils;
mod engine;
mod types;
mod api;
mod schema;
mod error;
mod codec;
mod sql;

fn main() {
    // let mut db = Enso::open("test_db").unwrap();
    //
    // db.create_table(
    //     "users",
    //     schema! {
    //         id: Int => pk,
    //         name: String,
    //         age: Int,
    //     }
    // ).unwrap();
    //
    // db.insert(row![1, "amartya", 24]).unwrap();
    //
    // let row = db.select_by_pk(1).unwrap().unwrap();
    // println!("Row: {:?}", row);
    //
    // db.delete_by_pk(1).unwrap();
    // println!("Row after deletion: {:?}", db.select_by_pk(1).unwrap());

    let mut lexer = Lexer::new(r#"INSERT INTO users VALUES (1, "amartya");"#);

    // loop {
    //     let tok = lexer.next_token().unwrap();
    //     println!("{:?}", tok);
    //     if tok == Token::EOF {
    //         break;
    //     }
    // }

    let mut parser = Parser::new(lexer).unwrap();
    let stmt = parser.parse_stmt().unwrap();

    println!("{:#?}", stmt);
}
