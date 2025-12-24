use std::{collections::HashMap, num::NonZeroUsize, time::{SystemTime, UNIX_EPOCH}};
use lru::LruCache;
use serde::{de::DeserializeOwned, Serialize};
use crate::{record::Record, storage::Storage, types::SegIndex, utils::{from_bytes, to_bytes}};

pub struct EnsoDB {
    pub storage: Storage,
    // pub index: HashMap<String, u64>,
    pub index: LruCache<String, SegIndex>,
}

impl EnsoDB {
    pub fn new() -> Self {
        let mut storage = Storage::new();
        match storage.rebuild_index() {
            Ok(index) => Self { storage, index },
            Err(e) => {
                println!("[EnsoDB error] Error rebuilding index: {}", e);
                Self { storage, index: LruCache::new(NonZeroUsize::new(4).unwrap()) }
            },
        }
    }

    fn active_segment(&self) -> String {
        self.storage.manifest.active_segment.clone()
    }

    fn get_or_load_seg_index(&mut self, seg: &str) -> &mut SegIndex {
        if !self.index.contains(seg) {
            let name = &seg[..seg.rfind('.').unwrap()];
            let idx_path = format!("data/index/{}.idx", name);
            let map = self.storage.load_idx(&idx_path).unwrap_or_default();
            self.index.put(seg.to_string(), map);
        }

        self.index.get_mut(seg).unwrap()
    }

    pub fn set<T: Serialize>(&mut self, key: String, value: T) {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs();

        let encoded_value = to_bytes(&value);
        let record = Record::new(key.clone(), encoded_value, now, false);

        match self.storage.append(&record) {
            Ok(offset) => {
                let seg = self.active_segment();
                let seg_idx = self.get_or_load_seg_index(&seg);
                seg_idx.insert(key.clone(), offset);
                // let name = &seg[..seg.rfind('.').unwrap()];
                // let idx_path = format!("data/index/{}.idx", name);
                // let _ = Storage::append_idx_entry(&idx_path, &key, offset);
            },
            Err(e) => println!("[EnsoDB error] Error while appending: {}", e),
        }
    }

    // pub fn get<T: DeserializeOwned>(&mut self, key: String) -> Option<T> {
    //     if let Some(offset) = self.index.get(&key) {
    //         match self.storage.read_at(*offset) {
    //             Ok(record) => {
    //                 if record.deleted {
    //                     println!("[EnsoDB error] Record not found in database");
    //                     return None;
    //                 }
    //
    //                 let value: T = from_bytes(&record.value);
    //                 Some(value)
    //             },
    //             Err(e) => {
    //                 println!("[EnsoDB error] Error while reading: {}", e);
    //                 None
    //             }
    //         }
    //     // } else if let Ok(record, offset) = self.search_in_log(key) { 
    //     //     self.index.insert(key.to_string(), offset);
    //     //     return Some(record.value);
    //     } else {
    //         println!("[EnsoDB error] Record not found in database");
    //         None
    //     }
    // }

    // pub fn get<T: DeserializeOwned>(&mut self, key: String) -> Option<T> {
    //     let seg = self.active_segment();
    //     let seg_idx = self.get_or_load_seg_index(&seg);
    //
    //     let offset = seg_idx.get(&key)?.clone();
    //     let record = self.storage.read_at(offset).ok()?;
    //
    //     if record.deleted { return None; }
    //
    //     Some(from_bytes(&record.value))
    // }

    pub fn get<T: DeserializeOwned>(&mut self, key: String) -> Option<T> {
        let segments: Vec<String> = self.storage.manifest.segments.iter().cloned().collect();
        for seg in segments.into_iter().rev() {
            let seg_idx = self.get_or_load_seg_index(&seg);

            if let Some(offset) = seg_idx.get(&key).copied() {
                match self.storage.read_from_segment(&seg, offset) {
                    Ok(record) => {
                        if record.deleted {
                            return None;
                        }

                        let value: T = from_bytes(&record.value);
                        return Some(value);
                    },
                    Err(_) => return None,
                }
            }
        }

        None
    }

    pub fn delete(&mut self, key: String) {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs();
        let record = Record::new(key.clone(), vec![0; 17], now, true);

        match self.storage.append(&record) {
            Ok(offset) => {
                let seg = self.active_segment();
                let seg_idx = self.get_or_load_seg_index(&seg);
                seg_idx.insert(key.clone(), offset);
                // let name = &seg[..seg.rfind('.').unwrap()];
                // let idx_path = format!("data/index/{}.idx", name);
                // let _ = Storage::append_idx_entry(&idx_path, &key, offset);
            },
            Err(e) => println!("[EnsoDB error] Error while deleting: {}", e),
        }
    }
}
