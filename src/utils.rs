use serde::{de::DeserializeOwned, Serialize};

use crate::{api::Enso, error::DbError, schema, sql::{ast::QueryResult, lexer::Lexer, parser::Parser}};
use crate::{types::Column};

// SERIALIZATION AND STORAGE

pub fn encode_u32(x: u32) -> [u8; 4] {
    x.to_be_bytes()
}

pub fn decode_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes(bytes.try_into().unwrap())
}

pub fn to_bytes<T: Serialize>(value: &T) -> Vec<u8> {
    bincode::serialize(value).unwrap()
}

pub fn from_bytes<T: DeserializeOwned>(bytes: &[u8]) -> T {
    bincode::deserialize(bytes).unwrap()
}

// REPL CLI

// pub fn handle_meta_commands(line: &str, db: &mut Enso) -> bool {
//     if !line.starts_with('.') {
//         return false;
//     }
//
//     let parts: Vec<&str> = line.split_whitespace().collect();
//
//     match parts.as_slice() {
//         [".exit"] => {
//             println!("Bye");
//             std::process::exit(0);
//         }
//
//         [".open", name] => {
//             match Enso::open(name) {
//                 Ok(new_db) => {
//                     *db = new_db;
//                     db.create_table(
//                         "users",
//                         schema! {
//                             id: Int => pk,
//                             name: String,
//                         }
//                     ).unwrap();
//
//                     println!("Opened database '{}'", name);
//                 }
//                 Err(e) => eprintln!("Error: {:?}", e),
//             }
//         }
//
//         [".help"] => print_help(),
//
//         _ => println!("Unknown command. Type '.help'"),
//     }
//
//     true
// }
//
// pub fn run_query(line: &str, db: &mut Enso) -> Result<(), DbError> {
//     let lexer = Lexer::new(line);
//     let mut parser = Parser::new(lexer)?;
//
//     let stmt = parser.parse_stmt()?;
//     let result = db.execute(stmt)?;
//
//     print_result(result);
//
//     Ok(())
// }
//
// // Commands ideas:
// // .tables           List tables in current database
// // .schema <table>   Show table schema
// fn print_help() {
//     println!(
// r#"
// EnsoDB Help
// ===========
//
// Meta Commands:
//   .open <db>        Open or create a database
//   .help             Show this help
//   .exit             Exit EnsoDB
//
// SQL Statements:
//   INSERT INTO <table> VALUES (...)
//   SELECT * FROM <table>
//   SELECT * FROM <table> WHERE <column(primary_key)> = <value>
//   DELETE FROM <table> WHERE <column(primary_key)> = <value>
// "#
//     );
// }
//
// fn print_result(result: QueryResult) {
//     match result {
//         QueryResult::Affected(n) => {
//             println!("{} row(s) affected", n);
//         }
//
//         QueryResult::Rows(Some(rows)) => {
//             for row in rows {
//                 println!("{:?}", row);
//             }
//         }
//
//         QueryResult::Rows(None) => {
//             println!("Empty set");
//         }
//     }
// }
