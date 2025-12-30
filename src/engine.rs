use std::{collections::HashMap, num::NonZeroUsize, sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex, RwLock}, time::{SystemTime, UNIX_EPOCH}};
use lru::LruCache;
use serde::{de::DeserializeOwned, Serialize};
use crate::{record::Record, storage::Storage, types::SegIndex, utils::{from_bytes, to_bytes}};

// const MAX_SEGMENTS: usize = 50;
const MAX_SEGMENTS: usize = 3;

pub struct Engine {
    pub storage: Arc<Mutex<Storage>>,
    pub index: Arc<RwLock<LruCache<String, SegIndex>>>,
    compaction_running: Arc<AtomicBool>,
}

impl Engine {
    pub fn new() -> Self {
        let mut storage = Storage::new();
        let index = Arc::new(RwLock::new(
            storage.rebuild_index().unwrap_or_else(|_| LruCache::new(NonZeroUsize::new(4).unwrap()))
        ));
        
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

    // fn get_or_load_seg_index(&mut self, seg: &str) -> &mut SegIndex {
    //     if !self.index.contains(seg) {
    //         let name = &seg[..seg.rfind('.').unwrap()];
    //         let idx_path = format!("data/index/{}.idx", name);
    //         let mut storage = self.storage.lock().unwrap();
    //         let map = storage.load_idx(&idx_path).unwrap_or_default();
    //         self.index.put(seg.to_string(), map);
    //     }
    //
    //     self.index.get_mut(seg).unwrap()
    // }

    fn ensure_seg_index_loaded(&self, seg: &str) {
        {
            let index = self.index.read().unwrap();
            if index.contains(seg) {
                return;
            }
        }

        // If segment is not present
        let map = {
            let storage = self.storage.lock().unwrap();
            let name = &seg[..seg.rfind('.').unwrap()];
            let idx_path = format!("data/index/{}.idx", name);
            storage.load_idx(&idx_path).unwrap_or_default()
        };

        let mut index = self.index.write().unwrap();
        index.put(seg.to_string(), map);
    }

    fn maybe_compact(&mut self) {
        let seg_count = {
            let storage = self.storage.lock().unwrap();
            storage.manifest.segments.len()
        };

        // check if number of segments exceeds threshold
        if seg_count <= MAX_SEGMENTS {
            return;
        }

        // return if compaction already running
        if self.compaction_running.swap(true, Ordering::SeqCst) {
            return;
        }

        let storage = Arc::clone(&self.storage);
        let index = Arc::clone(&self.index);
        let compaction_flag = Arc::clone(&self.compaction_running);

        std::thread::spawn(move || {
            let result = {
                let mut storage = storage.lock().unwrap();
                storage.compact_segments()
            };

            if let Err(e) = result {
                eprintln!("[EnsoDB] Compaction failed: {}", e);
            } else if let Ok((removed, new_seg)) = result {
                let mut index = index.write().unwrap();

                for seg in removed {
                    index.pop(&seg);
                }

                // load new segment index
                let idx = {
                    let storage = storage.lock().unwrap();
                    storage.load_idx(format!("data/index/{}.idx", &new_seg[..new_seg.rfind('.').unwrap()]).as_str()).unwrap_or_default()
                };
                index.put(new_seg, idx);
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

    // pub fn set<T: Serialize>(&mut self, key: String, value: T) {
    //     let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs();
    //
    //     let encoded_value = to_bytes(&value);
    //     let record = Record::new(key.clone(), encoded_value, now, false);
    //     // let mut storage = self.storage.lock().unwrap();
    //
    //     let offset = {
    //         let mut storage = self.storage.lock().unwrap();
    //         storage.append(&record)
    //     };
    //
    //     if let Err(e) = offset {
    //         eprintln!("[EnsoDB error] Error while storing: {}", e);
    //     } else {
    //         let seg = self.active_segment();
    //         self.ensure_seg_index_loaded(&seg);
    //
    //         {
    //             let mut index = self.index.write().unwrap();
    //             index.get_mut(&seg).unwrap().insert(key.clone(), offset.unwrap());
    //         }
    //
    //         // check for compaction
    //         self.maybe_compact();
    //     }
    // }

    // pub fn get<T: DeserializeOwned>(&mut self, key: String) -> Option<T> {
    //     // let segments: Vec<String> = storage.manifest.segments.iter().cloned().collect();
    //     let segments = {
    //         let storage = self.storage.lock().unwrap();
    //         storage.manifest.segments.clone()
    //     };
    //     for seg in segments.into_iter().rev() {
    //         // let seg_idx = self.get_or_load_seg_index(&seg);
    //         self.ensure_seg_index_loaded(&seg);
    //
    //         let offset = {
    //             let mut index = self.index.write().unwrap();
    //             index.get(&seg)?.get(&key).copied()
    //         };
    //
    //         if let Some(offset) = offset {
    //             let mut storage = self.storage.lock().unwrap();
    //             let record = storage.read_from_segment(&seg, offset).ok()?;
    //             if record.deleted { return None; }
    //             return Some(from_bytes(&record.value));
    //         }
    //
    //     }
    //
    //     None
    // }

    // pub fn delete(&mut self, key: String) {
    //     let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs();
    //     let record = Record::new(key.clone(), vec![0; 17], now, true);
    //
    //     let offset = {
    //         let mut storage = self.storage.lock().unwrap();
    //         storage.append(&record)
    //     };
    //
    //     if let Err(e) = offset {
    //         eprintln!("[EnsoDB error] Error while deleting: {}", e);
    //     } else {
    //         let seg = self.active_segment();
    //         self.ensure_seg_index_loaded(&seg);
    //
    //         {
    //             let mut index = self.index.write().unwrap();
    //             index.get_mut(&seg).unwrap().insert(key.clone(), offset.unwrap());
    //         }
    //
    //         // check for compaction
    //         self.maybe_compact();
    //     }
    // }

    pub fn set_raw(&mut self, key: String, value: Vec<u8>) {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs();

        let record = Record::new(key.clone(), value, now, false);

        let offset = {
            let mut storage = self.storage.lock().unwrap();
            storage.append(&record)
        };

        if let Err(e) = offset {
            eprintln!("[EnsoDB error] Error while storing: {}", e);
            return;
        }

        let seg = self.active_segment();
        self.ensure_seg_index_loaded(&seg);

        {
            let mut index = self.index.write().unwrap();
            index.get_mut(&seg).unwrap().insert(key, offset.unwrap());
        }

        self.maybe_compact();
    }
}
