use crate::{codec::RowCodec, engine::Engine, error::DbError, schema::SchemaManager, types::{Column, TableSchema, Value}};

struct Enso {
    engine: Engine,
    pub db: Option<String>,
    pub table: Option<String>,
    pub schema: SchemaManager,
}

impl Enso {
    pub fn new() -> Self {
        let engine = Engine::new();
        let schema = SchemaManager::new();
        return Self { engine, db: None, table: None, schema };
    }

    // -> Create new or use existing database
    pub fn open(db: &str) -> Result<Self, DbError> {
        let engine = Engine::new();

        // create db path if it doesn't exist
        let path = format!("data/schema/{}", db);
        if !std::path::Path::new(&path).exists() {
            std::fs::create_dir_all(&path)?;
        }

        let mut schema = SchemaManager::new();
        schema.load_db(&db)?;
        let db = Some(db.to_string());
        return Ok(Self { engine, db, table: None, schema });
    }

    // -> Create new table with schema
    pub fn create_table(&mut self, table: &str, columns: Vec<Column>, primary_key: &str) -> Result<(), DbError> {
        let db = self.db.as_ref().ok_or(DbError::NoDatabaseSelected)?;

        // validate primary key
        let primary_key = columns.iter().position(|c| c.name == primary_key).ok_or(DbError::InvalidPrimaryKey)?;
        let schema = TableSchema { name: table.to_string(), columns, primary_key };

        // store schema in disk
        let path = format!("data/schema/{}/{}.json", db, table);
        let json = serde_json::to_string_pretty(&schema)?;
        std::fs::write(&path, json)?;

        // cache the schema
        self.schema.insert(&db, &table, schema)?;

        self.table = Some(table.to_string());
        Ok(())
    }

    // -> Set current/active table
    pub fn use_table(&mut self, table: &str) -> Result<(), DbError> {
        let db = self.db.as_ref().ok_or(DbError::NoDatabaseSelected)?;

        // check if table schema exists
        let path = format!("data/schema/{}/{}.json", db, table);
        if !std::path::Path::new(&path).exists() {
            return Err(DbError::TableNotFound);
        }

        self.table = Some(table.to_string());
        Ok(())
    }

    // -> Insert into active/current table (set using use_table())
    pub fn insert(&mut self, row: Vec<Value>) -> Result<(), DbError> {
        let table = self.table.as_ref().ok_or(DbError::NoTableSelected)?;
        let db = self.db.as_ref().ok_or(DbError::NoDatabaseSelected)?;

        // get schema
        let schema = self.schema.get(db, table)?;
        
        if row.len() != schema.columns.len() {
            return Err(DbError::ColumnCountMismatch);
        }

        // type checking
        for (value, column) in row.iter().zip(schema.columns.iter()) {
            if !value.matches(&column.dtype) {
                return Err(DbError::TypeMismatch {
                    column: column.name.clone(),
                });
            }
        }

        // primary key
        let pk_idx = schema.primary_key;
        let pk_value = &row[pk_idx];

        let key = format!("{}:{}:{}", db, table, pk_value);
        let val = RowCodec::encode(&row)?;

        self.engine.set_raw(key, val);

        Ok(())
    }

    // -> Insert into specified table
    pub fn insert_into(&mut self, table: &str, row: Vec<Value>) -> Result<(), DbError> {
        let db = self.db.as_ref().ok_or(DbError::NoDatabaseSelected)?;

        // get schema
        let schema = self.schema.get(db, table)?;
        
        if row.len() != schema.columns.len() {
            return Err(DbError::ColumnCountMismatch);
        }

        // type checking
        for (value, column) in row.iter().zip(schema.columns.iter()) {
            if !value.matches(&column.dtype) {
                return Err(DbError::TypeMismatch {
                    column: column.name.clone(),
                });
            }
        }

        // primary key
        let pk_idx = schema.primary_key;
        let pk_value = &row[pk_idx];

        let key = format!("{}:{}:{}", db, table, pk_value);
        let val = RowCodec::encode(&row)?;

        self.engine.set_raw(key, val);

        Ok(())
    }

    // -> Select/fetch all rows
    pub fn select_all() {}

    // -> Select all rows from specified table
    pub fn select_all_from() {}
}
