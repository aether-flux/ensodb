use std::{collections::HashMap, num::NonZeroUsize, sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex}, time::{SystemTime, UNIX_EPOCH}};
use lru::LruCache;
use serde::{de::DeserializeOwned, Serialize};
use crate::{record::Record, storage::Storage, types::SegIndex, utils::{from_bytes, to_bytes}};

// const MAX_SEGMENTS: usize = 50;
const MAX_SEGMENTS: usize = 3;

pub struct EnsoDB {
    pub storage: Arc<Mutex<Storage>>,
    pub index: LruCache<String, SegIndex>,
    compaction_running: Arc<AtomicBool>,
}

impl EnsoDB {
    pub fn new() -> Self {
        let mut storage = Storage::new();
        let index = storage.rebuild_index().unwrap_or_else(|_| LruCache::new(NonZeroUsize::new(4).unwrap()));
        
        Self {
            storage: Arc::new(Mutex::new(storage)),
            index,
            compaction_running: Arc::new(AtomicBool::new(false)),
        }
    }

    fn active_segment(&self) -> String {
        let storage = self.storage.lock().unwrap();
        storage.manifest.active_segment.clone()
    }

    fn get_or_load_seg_index(&mut self, seg: &str) -> &mut SegIndex {
        if !self.index.contains(seg) {
            let name = &seg[..seg.rfind('.').unwrap()];
            let idx_path = format!("data/index/{}.idx", name);
            let mut storage = self.storage.lock().unwrap();
            let map = storage.load_idx(&idx_path).unwrap_or_default();
            self.index.put(seg.to_string(), map);
        }

        self.index.get_mut(seg).unwrap()
    }

    fn maybe_compact(&mut self) {
        // return if compaction already running
        if self.compaction_running.swap(true, Ordering::SeqCst) {
            return;
        }

        let seg_count = {
            let storage = self.storage.lock().unwrap();
            storage.manifest.segments.len()
        };
        if seg_count <= MAX_SEGMENTS {
            return;
        }

        let storage = Arc::clone(&self.storage);
        let compaction_flag = Arc::clone(&self.compaction_running);

        std::thread::spawn(move || {
            let result = {
                let mut storage = storage.lock().unwrap();
                storage.compact_segments()
            };

            if let Err(e) = result {
                eprintln!("[EnsoDB] Compaction failed: {}", e);
            }

            compaction_flag.store(false, Ordering::SeqCst);
        });

        // match self.storage.compact_segments() {
        //     Ok(removed) => {
        //         // clean up LRU index/cache
        //         for seg in removed {
        //             self.index.pop(&seg);
        //         }
        //     }
        //     Err(e) => {
        //         eprintln!("[EnsoDB] Compaction failed: {}", e);
        //     }
        // }
    }

    pub fn set<T: Serialize>(&mut self, key: String, value: T) {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs();

        let encoded_value = to_bytes(&value);
        let record = Record::new(key.clone(), encoded_value, now, false);
        // let mut storage = self.storage.lock().unwrap();

        let offset = {
            let mut storage = self.storage.lock().unwrap();
            storage.append(&record)
        };

        if let Err(e) = offset {
            eprintln!("[EnsoDB error] Error while storing: {}", e);
        } else {
            let seg = self.active_segment();
            let seg_idx = self.get_or_load_seg_index(&seg);
            seg_idx.insert(key.clone(), offset.unwrap());

            // check for compaction
            self.maybe_compact();
        }

        // match storage.append(&record) {
        //     Ok(offset) => {
        //         let seg = self.active_segment();
        //         let seg_idx = self.get_or_load_seg_index(&seg);
        //         seg_idx.insert(key.clone(), offset);
        //
        //         // check for compaction
        //         self.maybe_compact();
        //     },
        //     Err(e) => println!("[EnsoDB error] Error while appending: {}", e),
        // }
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
        // let segments: Vec<String> = storage.manifest.segments.iter().cloned().collect();
        let segments = {
            let storage = self.storage.lock().unwrap();
            storage.manifest.segments.clone()
        };
        for seg in segments.into_iter().rev() {
            let seg_idx = self.get_or_load_seg_index(&seg);

            if let Some(offset) = seg_idx.get(&key).copied() {
                let mut storage = self.storage.lock().unwrap();
                match storage.read_from_segment(&seg, offset) {
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

        let offset = {
            let mut storage = self.storage.lock().unwrap();
            storage.append(&record)
        };

        if let Err(e) = offset {
            eprintln!("[EnsoDB error] Error while deleting: {}", e);
        } else {
            let seg = self.active_segment();
            let seg_idx = self.get_or_load_seg_index(&seg);
            seg_idx.insert(key.clone(), offset.unwrap());

            // check for compaction
            self.maybe_compact();
        }

        // match self.storage.append(&record) {
        //     Ok(offset) => {
        //         let seg = self.active_segment();
        //         let seg_idx = self.get_or_load_seg_index(&seg);
        //         seg_idx.insert(key.clone(), offset);
        //
        //         // check for compaction
        //         self.maybe_compact();
        //     },
        //     Err(e) => println!("[EnsoDB error] Error while deleting: {}", e),
        // }
    }
}
