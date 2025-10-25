use std::{fs::OpenOptions, io::{Read, Seek, SeekFrom, Write}};

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

    // -> Append data (record) to end of log file, returns offset
    pub fn append(&mut self, record: &Record) -> std::io::Result<u64> {
        // Calculate offset (current length of file)
        let mut file_content = String::new();
        self.file.read_to_string(&mut file_content);

        // Serialize record and append to the file
        let encoded_data = record.serialize();
        self.file.write_all(&encoded_data).unwrap();
        self.file.flush().unwrap();

        Ok(file_content.len() as u64)
    }

    // -> Read data (key-value pair) at given offset
    // pub fn read_at(&mut self, offset: u64) -> std::io::Result<Record> {
    //     self.file.seek(SeekFrom::Start(offset)).unwrap();
    //     let mut metadata = vec![0u8, 16];
    //     self.file.read_exact(&mut metadata).unwrap();
    //
    //     let key_len = decode_u32(&metadata[0..4]) as usize;
    //     let val_len = decode_u32(&metadata[4..8]) as usize;
    //     let record_len = key_len + val_len;
    //
    //     self.file.seek(SeekFrom::Start(offset+16)).unwrap();
    //     let mut buffer = vec![0u8; record_len];
    //     self.file.read_exact(&mut buffer).unwrap();
    //     let record = Record::deserialize(&buffer);
    //
    //     Ok(record)
    // }
    pub fn read_at(&mut self, offset: u64) -> std::io::Result<Record> {
        use std::io::{Read, Seek, SeekFrom};

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
