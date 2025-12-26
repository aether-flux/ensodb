use std::{collections::HashMap, fs::{rename, File, OpenOptions}, io::{Read, Seek, SeekFrom, Write}, num::NonZeroUsize};
use std::path::Path;
use chrono::Utc;
use lru::LruCache;
use crate::{record::Record, types::{Manifest, SegIndex}, utils::decode_u32};

// const MAX_FILE_SIZE: u64 = 10 * 1000 * 1000;
const MAX_FILE_SIZE: u64 = 111;

pub struct Storage {
    file: std::fs::File,
    pub manifest: Manifest,
}

impl Storage {
    pub fn new() -> Self {
        let manifest_path = "data/manifest.json";
        // std::fs::create_dir_all("data/index").unwrap();

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
            // std::fs::create_dir_all("data/index").unwrap();
            std::fs::write(manifest_path, serde_json::to_string_pretty(&manifest).unwrap()).unwrap();

            manifest
        };

        // Create idx file(s)
        std::fs::create_dir_all("data/index").unwrap();
        let idx_path = format!("data/index/{}.idx", manifest.active_segment.trim_end_matches(".log"));
        if !Path::new(&idx_path).exists() {
            File::create(idx_path).unwrap();
        }

        // Create segment file
        let active_path = format!("data/segments/{}", manifest.active_segment);
        let file = OpenOptions::new()
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

    pub fn append_idx_entry(idx_path: &str, key: &str, offset: u64) -> std::io::Result<()> {
        let mut f = OpenOptions::new().create(true).append(true).open(idx_path)?;
        f.write_all(&(key.len() as u32).to_be_bytes())?;
        f.write_all(key.as_bytes())?;
        f.write_all(&offset.to_be_bytes())?;
        f.flush()?;
        Ok(())
    }

    pub fn load_idx(&self, idx_path: &str) -> std::io::Result<SegIndex> {
        let mut map = HashMap::new();
        let mut f = File::open(idx_path)?;

        loop {
            let mut len_b = [0u8; 4];
            if let Err(e) = f.read_exact(&mut len_b) {
                if e.kind() == std::io::ErrorKind::UnexpectedEof { break; }
                else { return Err(e); }
            }
            let key_len = u32::from_be_bytes(len_b) as usize;

            let mut key_b = vec![0u8; key_len];
            f.read_exact(&mut key_b)?;
            let mut off_b = [0u8; 8];
            f.read_exact(&mut off_b)?;

            let key = String::from_utf8(key_b).expect("index key not utf8");
            let off = u64::from_be_bytes(off_b);

            map.insert(key, off);
        }

        Ok(map)
    }

    pub fn rebuild_index(&mut self) -> std::io::Result<LruCache<String, SegIndex>> {
        let mut cache: LruCache<String, SegIndex> = LruCache::new(NonZeroUsize::new(4).unwrap());
        let segments = &self.manifest.segments;
        let segments: Vec<String> = segments.iter().take(4).cloned().collect();

        for s in segments {
            let seg_name = &s[..s.rfind('.').unwrap()];
            let idx_path = format!("data/index/{}.idx", seg_name);
            let seg_index = match self.load_idx(idx_path.as_str()) {
                Ok(map) => map,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // First run: no index files created
                    HashMap::new()
                }
                Err(e) => return Err(e),
            };
            cache.put(s.to_string(), seg_index);
        }

        Ok(cache)
    }

    fn active_idx_path(&self) -> String {
        let seg = &self.manifest.active_segment;
        let name = &seg[..seg.rfind('.').unwrap()];
        format!("data/index/{}.idx", name)
    }

    fn next_segment_name(&self) -> String {
        let last = self.manifest.segments.last().unwrap();
        let num: u32 = last
            .trim_start_matches("enso-")
            .trim_end_matches(".log")
            .parse()
            .unwrap();

        format!("enso-{:04}.log", num+1)
    }

    // pub fn rebuild_index(&mut self) -> std::io::Result<HashMap<String, u64>> {
    //     let mut index = HashMap::new();
    //     self.file.seek(SeekFrom::Start(0))?;
    //
    //     loop {
    //         // Read header
    //         let mut header = [0u8; 17];
    //         match self.file.read_exact(&mut header) {
    //             Ok(_) => {},
    //             Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
    //             Err(e) => return Err(e),
    //         }
    //
    //         let key_len = decode_u32(&header[0..4]) as usize;
    //         let val_len = decode_u32(&header[4..8]) as usize;
    //         let record_len = 17 + key_len + val_len;
    //
    //         // Read the rest of the record
    //         let mut buf = vec![0u8; record_len-17];
    //         self.file.read_exact(&mut buf)?;
    //
    //         // Reconstruct the record bytes
    //         let mut full = Vec::new();
    //         full.extend_from_slice(&header);
    //         full.extend_from_slice(&buf);
    //         let record = Record::deserialize(&full);
    //
    //         // Read offset (current positon - length of record)
    //         let offset = self.file.stream_position()? - (record_len as u64);
    //
    //         // Insert in index
    //         index.insert(record.key, offset);
    //     }
    //
    //     Ok(index)
    // }

    fn rotate_segment(&mut self) -> std::io::Result<()> {
        let new_seg = self.next_segment_name();

        // create new segment file
        let seg_path = format!("data/segments/{}", new_seg);
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&seg_path)?;

        // create index file
        let idx_path = format!("data/index/{}.idx", new_seg.trim_end_matches(".log"));
        File::create(idx_path)?;

        // update manifest
        self.manifest.segments.push(new_seg.clone());
        self.manifest.active_segment = new_seg;
        self.save_manifest();

        // switch active file
        self.file = file;

        Ok(())
    }

    // -> File compaction if size exceeds threshold
    fn read_seg_into_map(seg: &str, mut records: HashMap<String, Record>) -> std::io::Result<HashMap<String, Record>> {
        let mut file = OpenOptions::new()
            .read(true)
            .open(format!("data/segments/{}", seg))?;
        // let mut records = HashMap::new();

        file.seek(SeekFrom::Start(0))?;
        loop {
            // first read header (16 bytes)
            let mut header = [0u8; 17];
            if file.read_exact(&mut header).is_err() { break; }
            
            let key_len = decode_u32(&header[0..4]) as usize;
            let val_len = decode_u32(&header[4..8]) as usize;
            let record_len = 17 + key_len + val_len;
            
            // now read the rest
            let mut buf = vec![0u8; record_len-17];
            file.read_exact(&mut buf)?;

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
        
        Ok(records)
    }

    fn write_compacted_records(name: &str, records: HashMap<String, Record>) -> std::io::Result<()> {
        // create temp files
        let mut tmp_log_file = File::create(format!("data/segments/{}.log.tmp", name))?;
        let mut tmp_idx_file = File::create(format!("data/index/{}.idx.tmp", name))?;

        let mut records: Vec<_> = records.into_iter().collect();
        records.sort_by(|a, b| a.0.cmp(&b.0));
        for (key, record) in records.iter() {
            // write record to tmp log
            let record_bytes = &record.serialize();
            let offset = tmp_log_file.seek(SeekFrom::End(0))?;
            tmp_log_file.write_all(&record_bytes)?;
            tmp_log_file.flush()?;

            // write offset to tmp idx
            tmp_idx_file.write_all(&(key.len() as u32).to_be_bytes())?;
            tmp_idx_file.write_all(key.as_bytes())?;
            tmp_idx_file.write_all(&offset.to_be_bytes())?;
            tmp_idx_file.flush()?;
        }

        // atomic swap
        tmp_log_file.sync_all()?;
        tmp_idx_file.sync_all()?;

        rename(format!("data/segments/{}.log.tmp", name), format!("data/segments/{}.log", name))?;
        rename(format!("data/index/{}.idx.tmp", name), format!("data/index/{}.idx", name))?;

        // Ok(format!("data/segments/{}.log", name))
        Ok(())
    }

    pub fn compact_segments(&mut self) -> std::io::Result<(Vec<String>, String)> {
        // println!("Compacting start...");
        let segments = &self.manifest.segments;
        let segments: Vec<String> = segments.iter().filter(|&s| s.to_string() != self.manifest.active_segment).cloned().collect();
        if segments.len() < 2 { return Ok((vec![], String::new())); }

        // compress all segments into one map
        let mut records: HashMap<String, Record> = HashMap::new();
        for seg in segments.iter().rev().clone() {
            records = Self::read_seg_into_map(&seg, records)?;
        }

        // write to new segment
        // let name = &segments[0][..segments[0].rfind('.').unwrap()];
        // let name = format!("{}-compacted", name);
        let name = self.next_segment_name();
        Self::write_compacted_records(&name[..name.rfind('.').unwrap()], records)?;
        // println!("New segment created");

        let timestamp = Utc::now();
        self.manifest.segments.retain(|s| !segments.contains(s));
        self.manifest.segments.push(name.clone());
        self.manifest.last_compaction = Some(timestamp.to_string());
        self.save_manifest();
        // println!("Manifest saved");

        // delete old segments
        for seg in segments.clone() {
            let seg_name = &seg[..seg.rfind('.').unwrap()];
            let _ = std::fs::remove_file(format!("data/segments/{}.log", seg_name));
            let _ = std::fs::remove_file(format!("data/index/{}.idx", seg_name));

            // cleanup LRU cache/index
            // self.index.pop(&seg_name);
        }

        // println!("Compacting end...");

        Ok((segments, name))
    }

    // -> Append data (record) to end of log file, returns offset
    pub fn append(&mut self, record: &Record) -> std::io::Result<u64> {
        let record_bytes = record.serialize();
        let cur_size = self.file.metadata()?.len();

        // Check if appending would overflow file size threshold
        if cur_size + record_bytes.len() as u64 > MAX_FILE_SIZE {
            self.rotate_segment()?;
        }

        // Append to log
        let offset = self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&record_bytes)?;
        self.file.flush()?;

        // Write index entry
        let idx_path = self.active_idx_path();
        Self::append_idx_entry(&idx_path, &record.key, offset)?;

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

    pub fn read_from_segment(&mut self, seg: &str, offset: u64) -> std::io::Result<Record> {
        let seg_path = format!("data/segments/{}", seg);
        let mut seg_file = OpenOptions::new()
            .read(true)
            .open(seg_path)?;

        seg_file.seek(SeekFrom::Start(offset))?;
        
        // first read header (16 bytes)
        let mut header = [0u8; 17];
        seg_file.read_exact(&mut header)?;
        
        let key_len = decode_u32(&header[0..4]) as usize;
        let val_len = decode_u32(&header[4..8]) as usize;
        let record_len = 17 + key_len + val_len;
        
        // now read the rest
        let mut buf = vec![0u8; record_len];
        seg_file.seek(SeekFrom::Start(offset))?;
        seg_file.read_exact(&mut buf)?;
        
        Ok(Record::deserialize(&buf))
    }
}
