use std::{io::{BufRead, BufReader, Write}, net::{TcpListener, TcpStream}, sync::{Arc, Mutex}};

use crate::{api::Enso, repl::format_response};

pub const EOF_MARKER: &str = "<ENSO_EOF>";

pub fn start_tcp(db: Arc<Mutex<Enso>>) {
    std::thread::spawn(move || {
        let addr = "127.0.0.1:5432";
        let listener = TcpListener::bind(addr).expect("Failed to bind server");
        println!("[enso] TCP server listening on {}\n", addr);

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let db = db.clone();
                    std::thread::spawn(|| { handle_client(stream, db); });
                }
                Err(e) => {
                    eprintln!("[enso] TCP error: {:?}", e);
                }
            }
        }
    });
}

fn handle_client(stream: TcpStream, db: Arc<Mutex<Enso>>) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut writer = stream;

    loop {
        let mut query = String::new();

        if reader.read_line(&mut query).is_err() {
            break;
        }

        let query = query.trim();
        if query.is_empty() {
            continue;
        }

        let mut response = {
            let mut db = db.lock().unwrap();
            match db.query(query) {
                Ok(res) => format_response(&mut db, res),
                Err(e) => Ok(format!("ERROR: {:?}\n", e)),
            }
        };

        if let Err(e) = response {
            eprintln!("[enso] Error: {:?}", e);
            continue;
        }

        let mut response = response.unwrap();

        response.push('\n');
        response.push_str(EOF_MARKER);
        response.push('\n');

        // let _ = writer.write_all(response.as_bytes());
        writer.write_all(response.as_bytes()).ok();
        writer.flush().ok();
    }
}
