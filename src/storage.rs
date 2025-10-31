use std::{collections::HashMap, fs::{File, OpenOptions}, io::{Read, Seek, SeekFrom, Write}};
use std::path::Path;
use chrono::Utc;
use crate::{record::Record, types::Manifest, utils::decode_u32};

// const MAX_FILE_SIZE: u64 = 10 * 1000 * 1000;
const MAX_FILE_SIZE: u64 = 400;

pub struct Storage {
    file: std::fs::File,
    manifest: Manifest,
}

impl Storage {
    pub fn new() -> Self {
        let manifest_path = "data/manifest.json";

        let manifest = if Path::new(manifest_path).exists() {
            let data = std::fs::read_to_string(manifest_path).unwrap();
            serde_json::from_str::<Manifest>(&data).unwrap()
        } else {
            let manifest = Manifest {
                active_segment: "enso-0001.log".to_string(),
                segments: vec!["enso-0001.log".to_string()],
                last_compaction: None,
            };

            std::fs::create_dir_all("data/segments").unwrap();
            std::fs::write(manifest_path, serde_json::to_string_pretty(&manifest).unwrap()).unwrap();

            manifest
        };

        let active_path = format!("data/segments/{}", manifest.active_segment);
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(active_path)
            .unwrap();

        Self { file, manifest }
    }

    pub fn save_manifest(&self) {
        let data = serde_json::to_string_pretty(&self.manifest).unwrap();
        std::fs::write("data/manifest.json", data).unwrap();
    }

    pub fn rebuild_index(&mut self) -> std::io::Result<HashMap<String, u64>> {
        let mut index = HashMap::new();
        self.file.seek(SeekFrom::Start(0))?;

        loop {
            // Read header
            let mut header = [0u8; 17];
            match self.file.read_exact(&mut header) {
                Ok(_) => {},
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let key_len = decode_u32(&header[0..4]) as usize;
            let val_len = decode_u32(&header[4..8]) as usize;
            let record_len = 17 + key_len + val_len;

            // Read the rest of the record
            let mut buf = vec![0u8; record_len-17];
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

    // -> File compaction if size exceeds threshold
    pub fn compact(&mut self) -> std::io::Result<()> {
        let mut records = HashMap::new();

        self.file.seek(SeekFrom::Start(0))?;
        loop {
            // first read header (16 bytes)
            let mut header = [0u8; 17];
            if self.file.read_exact(&mut header).is_err() { break; }
            
            let key_len = decode_u32(&header[0..4]) as usize;
            let val_len = decode_u32(&header[4..8]) as usize;
            let record_len = 17 + key_len + val_len;
            
            // now read the rest
            let mut buf = vec![0u8; record_len-17];
            self.file.read_exact(&mut buf)?;

            // reconstruct full buffer
            let mut full = Vec::new();
            full.extend_from_slice(&header);
            full.extend_from_slice(&buf);

            // deserialize and insert into records
            let record = Record::deserialize(&full);
            if record.deleted == false {
                records.insert(record.key.clone(), record);
            }
        }

        // create temporary file
        let active_segment = &self.manifest.active_segment;
        let active_segment = &active_segment[..active_segment.rfind('.').unwrap()];
        let tmp_path = format!("data/segments/{}.tmp", active_segment);
        let mut tmp_file = File::create(tmp_path.clone())?;

        // write 
        for record in records.values() {
            tmp_file.write_all(&record.serialize())?;
        }
        tmp_file.flush()?;

        std::fs::rename(tmp_path, format!("data/segments/{}.log", active_segment))?;

        self.manifest.last_compaction = Some(Utc::now().to_rfc3339());
        self.save_manifest();

        self.file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(format!("data/segments/{}.log", active_segment))?;

        Ok(())
    }

    // -> Append data (record) to end of log file, returns offset
    pub fn append(&mut self, record: &Record) -> std::io::Result<u64> {
        // Calculate offset (current length of file)
        let offset = self.file.seek(SeekFrom::End(0))?;

        // Serialize record and append to the file
        let encoded_data = record.serialize();
        self.file.write_all(&encoded_data).unwrap();
        self.file.flush().unwrap();

        // Check file size
        let size = std::fs::metadata(format!("data/segments/{}", &self.manifest.active_segment))?.len();
        if size > MAX_FILE_SIZE {
            self.compact()?;
        }

        Ok(offset as u64)
    }

    // -> Read data (key-value pair) at given offset
    pub fn read_at(&mut self, offset: u64) -> std::io::Result<Record> {
        self.file.seek(SeekFrom::Start(offset))?;
        
        // first read header (16 bytes)
        let mut header = [0u8; 17];
        self.file.read_exact(&mut header)?;
        
        let key_len = decode_u32(&header[0..4]) as usize;
        let val_len = decode_u32(&header[4..8]) as usize;
        let record_len = 17 + key_len + val_len;
        
        // now read the rest
        let mut buf = vec![0u8; record_len];
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(&mut buf)?;
        
        Ok(Record::deserialize(&buf))
    }

    // -> Delete data by adding a record with deleted:true
    // pub fn delete_key(&mut self, key: String) -> std::io::Result<u64> {
    //
    // }
}
