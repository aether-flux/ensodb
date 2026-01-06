use std::{sync::{Arc, Mutex}, time::Duration};

use rustyline::{error::ReadlineError, DefaultEditor};

use crate::{api::Enso, error::DbError, pretty::pretty_rows, schema, sql::{ast::QueryResult, lexer::Lexer, parser::Parser}, types::{TableSchema, Value}};

pub fn start_repl(db: Arc<Mutex<Enso>>) {
    println!("EnsoDB v0.1");
    println!("Type '.help' for commands.\n");

    std::thread::sleep(Duration::from_millis(500));
    let mut rl = DefaultEditor::new().unwrap();

    loop {
        let prompt = {
            let db = db.lock().unwrap();
            match db.db.as_deref() {
                Some(name) => format!("[{}] >", name),
                None => "[no-db] >".to_string(),
            }
        };

        match rl.readline(&prompt) {
            Ok(line) => {
                let line = line.trim();

                if line.is_empty() {
                    continue;
                }

                rl.add_history_entry(line);

                let meta_cmd = {
                    let mut db = db.lock().unwrap();
                    handle_meta_commands(line, &mut db)
                };
                // if handle_meta_commands(line, &mut db) {
                //     continue;
                // }
                if meta_cmd {
                    continue;
                }

                let res = {
                    let mut db = db.lock().unwrap();
                    match run_query(line, &mut db) {
                        Ok(_) => {},
                        Err(e) => eprintln!("Error: {:?}", e),
                    }
                };
                // match run_query(line, &mut db) {
                //     Ok(_) => {},
                //     Err(e) => eprintln!("Error: {:?}", e),
                // }
            },

            Err(ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            },

            Err(ReadlineError::Eof) => {
                println!("Bye");
                break;
            },

            Err(e) => {
                eprintln!("REPL Error: {:?}", e);
                break;
            }
        }

        println!("");
    }
}

fn handle_meta_commands(line: &str, db: &mut Enso) -> bool {
    if !line.starts_with('.') {
        return false;
    }

    let line = &normalize_input(line);
    let parts: Vec<&str> = line.split_whitespace().collect();

    match parts.as_slice() {
        [".exit"] => {
            println!("Bye");
            std::process::exit(0);
        }

        [".open", name] => {
            match Enso::open(name) {
                Ok(new_db) => {
                    *db = new_db;
                    db.create_table(
                        "users",
                        schema! {
                            id: Int => pk,
                            name: String,
                        }
                    ).unwrap();

                    println!("Opened database '{}'", name);
                }
                Err(e) => eprintln!("Error: {:?}", e),
            }
        }

        [".help"] => print_help(),

        _ => println!("Unknown command. Type '.help'"),
    }

    true
}

fn run_query(line: &str, db: &mut Enso) -> Result<(), DbError> {
    let lexer = Lexer::new(line);
    let mut parser = Parser::new(lexer)?;

    let stmt = parser.parse_stmt()?;
    let result = db.execute(stmt)?;

    print_result(db, result);

    Ok(())
}

// Commands ideas:
// .tables           List tables in current database
// .schema <table>   Show table schema
fn print_help() {
    println!(
r#"
EnsoDB Help
===========

Meta Commands:
  .open <db>        Open or create a database
  .help             Show this help
  .exit             Exit EnsoDB

SQL Statements:
  CREATE TABLE IDENT (COLNAME TYPE [PRIMARY KEY] [, ...]);
  INSERT INTO <table> VALUES (...);
  SELECT * FROM <table>;
  SELECT * FROM <table> WHERE <column(primary_key)> = <value>;
  DELETE FROM <table> WHERE <column(primary_key)> = <value>;
"#
    );
}

pub fn print_result(db: &mut Enso, result: QueryResult) {
    let out = format_response(db, result).unwrap();
    println!("{}", out);
}

// pub fn format_response(db: &Enso, res: QueryResult) -> Result<String, DbError> {
//     match res {
//         QueryResult::Affected(n) => {
//             Ok(format!("{} row(s) affected\n", n))
//         }
//
//         QueryResult::Rows(Some(rows)) => {
//             // let mut res = String::new();
//             // for row in rows {
//             //     res.push_str(format!("{:?}\n", row).as_str());
//             // }
//             let schema = db.schema.get(db.current_db(), table)?;
//             res
//         }
//
//         QueryResult::Rows(None) => {
//             Ok(format!("Empty set\n"))
//         }
//     }
// }

pub fn format_response(db: &mut Enso, res: QueryResult) -> Result<String, DbError> {
    match res {
        QueryResult::Affected(n) => {
            Ok(format!("{} row(s) affected\n", n))
        }

        QueryResult::Rows { table, rows } => {
            if let None = rows {
                return Ok("Empty set\n".to_string());
            }

            let rows = rows.unwrap();

            if rows.is_empty() {
                return Ok("Empty set\n".to_string());
            }

            let current_db = db.current_db().to_string();
            let schema = db.schema.get(&current_db, &table)?;

            let out = pretty_rows(&schema, &rows);

            Ok(out)
        }
    }
}

fn normalize_input(input: &str) -> String {
    input
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_string()
}
