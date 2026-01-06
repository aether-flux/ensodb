use std::{sync::{Arc, Mutex}, time};

use api::Enso;
use engine::Engine;
use rustyline::{error::ReadlineError, DefaultEditor, Editor};
use sql::{lexer::{Lexer, Token}, parser::Parser};
use tcp::start_tcp;
use types::{Column, Value};
use repl::{start_repl};

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
mod repl;
mod client;
mod tcp;
mod pretty;

fn main() -> Result<(), Box<dyn std::error::Error>> {
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

    // TESTING parser ///////////////////////////////////////////////////////

    // let mut lexer = Lexer::new(r#"INSERT INTO users VALUES (1, "amartya");"#);
    //
    // let mut parser = Parser::new(lexer).unwrap();
    // let stmt = parser.parse_stmt().unwrap();
    //
    // println!("{:#?}", stmt);

    // CREATING interactive repl ///////////////////////////////////////////

    let db = Enso::open("test_db").unwrap();
    let db = Arc::new(Mutex::new(db));

    start_tcp(db.clone());
    start_repl(db);

    Ok(())
}
