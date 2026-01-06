use std::{sync::{Arc, Mutex}, time::Duration};

use rustyline::{error::ReadlineError, DefaultEditor};

use crate::{api::Enso, error::DbError, schema, sql::{ast::QueryResult, lexer::Lexer, parser::Parser}};

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

    print_result(result);

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
  INSERT INTO <table> VALUES (...)
  SELECT * FROM <table>
  SELECT * FROM <table> WHERE <column(primary_key)> = <value>
  DELETE FROM <table> WHERE <column(primary_key)> = <value>
"#
    );
}

pub fn print_result(result: QueryResult) {
    match result {
        QueryResult::Affected(n) => {
            println!("{} row(s) affected", n);
        }

        QueryResult::Rows(Some(rows)) => {
            for row in rows {
                println!("{:?}", row);
            }
        }

        QueryResult::Rows(None) => {
            println!("Empty set");
        }
    }
}

pub fn format_response(res: QueryResult) -> String {
    match res {
        QueryResult::Affected(n) => {
            format!("{} row(s) affected\n", n)
        }

        QueryResult::Rows(Some(rows)) => {
            let mut res = String::new();
            for row in rows {
                res.push_str(format!("{:?}\n", row).as_str());
            }
            res
        }

        QueryResult::Rows(None) => {
            format!("Empty set\n")
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
