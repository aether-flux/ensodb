use std::{collections::HashMap, fs::OpenOptions, io::{Read, Seek, SeekFrom, Write}};

use crate::{record::Record, utils::decode_u32};

pub struct Storage {
    file: std::fs::File,
}

impl Storage {
    pub fn new() -> Self {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open("data/enso.log")
            .unwrap();

        Self { file }
    }

    pub fn rebuild_index(&mut self) -> std::io::Result<HashMap<String, u64>> {
        let mut index = HashMap::new();
        self.file.seek(SeekFrom::Start(0))?;

        loop {
            // Read header
            let mut header = [0u8; 16];
            match self.file.read_exact(&mut header) {
                Ok(_) => {},
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let key_len = decode_u32(&header[0..4]) as usize;
            let val_len = decode_u32(&header[4..8]) as usize;
            let record_len = 16 + key_len + val_len;

            // Read the rest of the record
            let mut buf = vec![0u8; record_len-16];
            self.file.read_exact(&mut buf)?;

            // Reconstruct the record bytes
            let mut full = Vec::new();
            full.extend_from_slice(&header);
            full.extend_from_slice(&buf);
            let record = Record::deserialize(&full);

            // Read offset (current positon - length of record)
            let offset = self.file.stream_position()? - (record_len as u64);

            // Insert in index
            index.insert(record.key, offset);
        }

        Ok(index)
    }

    // -> Append data (record) to end of log file, returns offset
    pub fn append(&mut self, record: &Record) -> std::io::Result<u64> {
        // Calculate offset (current length of file)
        let offset = self.file.seek(SeekFrom::End(0))?;

        // Serialize record and append to the file
        let encoded_data = record.serialize();
        self.file.write_all(&encoded_data).unwrap();
        self.file.flush().unwrap();

        Ok(offset as u64)
    }

    // -> Read data (key-value pair) at given offset
    pub fn read_at(&mut self, offset: u64) -> std::io::Result<Record> {
        self.file.seek(SeekFrom::Start(offset))?;
        
        // first read header (16 bytes)
        let mut header = [0u8; 16];
        self.file.read_exact(&mut header)?;
        
        let key_len = decode_u32(&header[0..4]) as usize;
        let val_len = decode_u32(&header[4..8]) as usize;
        let record_len = 16 + key_len + val_len;
        
        // now read the rest
        let mut buf = vec![0u8; record_len];
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(&mut buf)?;
        
        Ok(Record::deserialize(&buf))
    }
}
