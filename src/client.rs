use std::{io::{BufRead, BufReader, Write}, net::TcpStream};

use crate::error::DbError;

pub struct EnsoDB {
    stream: TcpStream,
}

impl EnsoDB {
    pub fn connect(addr: &str) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        Ok(Self { stream })
    }

    pub fn execute(&mut self, query: &str) -> Result<String, DbError> {
        self.stream.write_all(query.as_bytes())?;
        self.stream.write_all(b"\n")?;

        let mut response = String::new();
        BufReader::new(&self.stream).read_line(&mut response)?;
        Ok(response)
    }
}
