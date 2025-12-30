use std::collections::HashMap;

use crate::{error::DbError, types::TableSchema};

pub struct SchemaManager {
    // HashMap stores db -> (table -> schema)
    schemas: HashMap<String, HashMap<String, TableSchema>>
}

impl SchemaManager {
    pub fn new() -> Self {
        Self { schemas: HashMap::new() }
    }

    pub fn load_db(&mut self, db: &str) -> Result<(), DbError> {
        // path to table schemas
        let path = format!("data/schema/{}", db);
        let mut tables = HashMap::new();

        // read through every table schema
        for entry in std::fs::read_dir(&path)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                let content = std::fs::read_to_string(&path)?;
                let schema: TableSchema = serde_json::from_str(&content)?;

                tables.insert(schema.name.clone(), schema);
            }
        }

        self.schemas.insert(db.to_string(), tables);
        Ok(())
    }

    pub fn insert(&mut self, db: &str, table: &str, schema: TableSchema) -> Result<(), DbError> {
        if self.schemas.contains_key(&db.to_string()) {
            // if 'db' entry exists, add table schema
            self.schemas.get_mut(&db.to_string()).unwrap().insert(table.to_string(), schema);
        } else {
            // if 'db' entry doesn't exist, create a new map
            let mut map = HashMap::new();
            map.insert(table.to_string(), schema);
            self.schemas.insert(db.to_string(), map);
        }

        Ok(())
    }

    pub fn get(&mut self, db: &str, table: &str) -> Result<&TableSchema, DbError> {
        if !self.schemas.contains_key(&db.to_string()) {
            return Err(DbError::NoDatabaseSelected);
        }

        match self.schemas.get(&db.to_string()).unwrap().get(&table.to_string()) {
            Some(schema) => Ok(schema),
            None => Err(DbError::TableNotFound),
        }

        // Ok(())
    }
}
