use std::{collections::HashMap, time::{SystemTime, UNIX_EPOCH}};

use crate::{record::Record, storage::Storage};

pub struct EnsoDB {
    pub storage: Storage,
    pub index: HashMap<String, u64>,
}

impl EnsoDB {
    pub fn new() -> Self {
        Self {
            storage: Storage::new(),
            index: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: String, value: Vec<u8>) {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs();
        let record = Record::new(key.clone(), value, now);

        match self.storage.append(&record) {
            Ok(offset) => {
                self.index.insert(key, offset);
            },
            Err(e) => println!("[EnsoDB error] Error while appending: {}", e),
        }
    }

    pub fn get(&mut self, key: String) -> Option<Vec<u8>> {
        if let Some(offset) = self.index.get(&key) {
            match self.storage.read_at(*offset) {
                Ok(record) => {
                    Some(record.value)
                },
                Err(e) => {
                    println!("[EnsoDB error] Error while reading: {}", e);
                    None
                }
            }
        } else {
            println!("[EnsoDB error] Record not found in database");
            None
        }
    }
}
