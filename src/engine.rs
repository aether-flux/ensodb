use std::{collections::HashMap, time::{SystemTime, UNIX_EPOCH}};

use serde::{de::DeserializeOwned, Serialize};

use crate::{record::Record, storage::Storage, utils::{from_bytes, to_bytes}};

pub struct EnsoDB {
    pub storage: Storage,
    pub index: HashMap<String, u64>,
}

impl EnsoDB {
    pub fn new() -> Self {
        let mut storage = Storage::new();
        match storage.rebuild_index() {
            Ok(index) => Self { storage, index },
            Err(e) => {
                println!("[EnsoDB error] Error rebuilding index: {}", e);
                Self { storage, index: HashMap::new() }
            },
        }
    }

    pub fn set<T: Serialize>(&mut self, key: String, value: T) {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs();

        let encoded_value = to_bytes(&value);
        let record = Record::new(key.clone(), encoded_value, now, false);

        match self.storage.append(&record) {
            Ok(offset) => {
                self.index.insert(key, offset);
            },
            Err(e) => println!("[EnsoDB error] Error while appending: {}", e),
        }
    }

    pub fn get<T: DeserializeOwned>(&mut self, key: String) -> Option<T> {
        if let Some(offset) = self.index.get(&key) {
            match self.storage.read_at(*offset) {
                Ok(record) => {
                    if record.deleted {
                        println!("[EnsoDB error] Record not found in database");
                        return None;
                    }

                    let value: T = from_bytes(&record.value);
                    Some(value)
                },
                Err(e) => {
                    println!("[EnsoDB error] Error while reading: {}", e);
                    None
                }
            }
        // } else if let Ok(record, offset) = self.search_in_log(key) { 
        //     self.index.insert(key.to_string(), offset);
        //     return Some(record.value);
        } else {
            println!("[EnsoDB error] Record not found in database");
            None
        }
    }

    pub fn delete(&mut self, key: String) {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs();
        let record = Record::new(key.clone(), vec![0; 17], now, true);

        match self.storage.append(&record) {
            Ok(offset) => {
                self.index.insert(key, offset);
            },
            Err(e) => println!("[EnsoDB error] Error while deleting: {}", e),
        }
    }
}
