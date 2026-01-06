use crate::{codec::RowCodec, engine::Engine, error::DbError, schema::SchemaManager, sql::{ast::{Expr, QueryResult, Stmt}, lexer::Lexer, parser::Parser}, types::{Column, TableSchema, Value}};

pub struct Enso {
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

    // -> Get selected DB name
    pub fn current_db(&self) -> &str {
        self.db.as_deref().unwrap_or("no-db")
    }

    // -> Create new table with schema
    pub fn create_table(&mut self, table: &str, schema: (Vec<Column>, usize)) -> Result<(), DbError> {
        let db = self.db.as_ref().ok_or(DbError::NoDatabaseSelected)?;
        let (columns, primary_key) = schema;

        // validate primary key
        // let primary_key = columns.iter().position(|c| c.name == primary_key).ok_or(DbError::InvalidPrimaryKey)?;
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
    pub fn insert<I, V>(&mut self, row: I) -> Result<(), DbError>
    where I: IntoIterator<Item = V>, V: Into<Value> {
        let table = self.table.as_ref().ok_or(DbError::NoTableSelected)?;
        let db = self.db.as_ref().ok_or(DbError::NoDatabaseSelected)?;
        let row: Vec<Value> = row.into_iter().map(Into::into).collect();

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
    pub fn insert_into<I, V>(&mut self, table: &str, row: I) -> Result<(), DbError>
    where I: IntoIterator<Item = V>, V: Into<Value> {
        let db = self.db.as_ref().ok_or(DbError::NoDatabaseSelected)?;
        let row: Vec<Value> = row.into_iter().map(Into::into).collect();

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
    pub fn select_all(&mut self) -> Result<Vec<Vec<Value>>, DbError> {
        let db = self.db.as_ref().ok_or(DbError::NoDatabaseSelected)?;
        let table = self.table.as_ref().ok_or(DbError::NoTableSelected)?;
        let schema = self.schema.get(&db, &table)?;

        let prefix = format!("{}:{}:", db, table);
        let mut rows = Vec::new();

        for (key, value) in self.engine.scan_prefix(&prefix) {
            let row = RowCodec::decode(&value, schema)?;
            rows.push(row);
        }

        Ok(rows)
    }

    // -> Select all rows from specified table
    pub fn select_all_from(&mut self, table: &str) -> Result<Vec<Vec<Value>>, DbError> {
        let db = self.db.as_ref().ok_or(DbError::NoDatabaseSelected)?;
        let schema = self.schema.get(&db, &table)?;

        let prefix = format!("{}:{}:", db, table);
        let mut rows = Vec::new();

        for (key, value) in self.engine.scan_prefix(&prefix) {
            let row = RowCodec::decode(&value, schema)?;
            rows.push(row);
        }

        Ok(rows)
    }

    // -> Select row by primary key 
    pub fn select_by_pk<V>(&mut self, pk: V) -> Result<Option<Vec<Value>>, DbError>
    where V: Into<Value> {
        let db = self.db.as_ref().ok_or(DbError::NoDatabaseSelected)?;
        let table = self.table.as_ref().ok_or(DbError::NoTableSelected)?;
        let schema = self.schema.get(&db, &table)?;
        let pk = pk.into();

        let key = format!("{}:{}:{}", db, table, pk);

        match self.engine.get_raw(&key) {
            Some(bytes) => {
                let row = RowCodec::decode(&bytes, schema)?;
                Ok(Some(row))
            },
            None => Ok(None),
        }
    }

    // -> Select row by primary key from specified table
    pub fn select_by_pk_from<V>(&mut self, table: &str, pk: V) -> Result<Option<Vec<Value>>, DbError>
    where V: Into<Value> {
        let db = self.db.as_ref().ok_or(DbError::NoDatabaseSelected)?;
        let schema = self.schema.get(&db, &table)?;
        let pk = pk.into();

        let key = format!("{}:{}:{}", db, table, pk);

        match self.engine.get_raw(&key) {
            Some(bytes) => {
                let row = RowCodec::decode(&bytes, schema)?;
                Ok(Some(row))
            },
            None => Ok(None),
        }
    }

    pub fn select_where(&mut self, table: &str, filter: Option<Expr>) -> Result<Option<Vec<Vec<Value>>>, DbError> {
        if let Some(filter) = filter {
            match filter {
                Expr::Eq { column, value } => {
                    let v = value.eval()?;
                    let row = self.select_by_pk_from(table, v)?;
                    if let Some(row) = row {
                        Ok(Some(vec![row]))
                    } else {
                        Ok(None)
                    }
                },
                _ => Err(DbError::UnsupportedFilter),
            }
        } else {
            let rows = self.select_all_from(table)?;
            Ok(Some(rows))
        }
    }

    // -> Delete row by primary key
    pub fn delete_by_pk<V>(&mut self, pk: V) -> Result<(), DbError>
    where V: Into<Value> {
        let db = self.db.as_ref().ok_or(DbError::NoDatabaseSelected)?;
        let table = self.table.as_ref().ok_or(DbError::NoTableSelected)?;
        let pk = pk.into();

        let key = format!("{}:{}:{}", db, table, pk);
        self.engine.delete_raw(key);

        Ok(())
    }

    // -> Delete row by primary key from specified table
    pub fn delete_by_pk_from<V>(&mut self, table: &str, pk: V) -> Result<(), DbError>
    where V: Into<Value> {
        let db = self.db.as_ref().ok_or(DbError::NoDatabaseSelected)?;
        let pk = pk.into();

        let key = format!("{}:{}:{}", db, table, pk);
        self.engine.delete_raw(key);

        Ok(())
    }

    pub fn delete_where(&mut self, table: &str, filter: Expr) -> Result<u64, DbError> {
        match filter {
            Expr::Eq { column, value } => {
                let v = value.eval()?;
                self.delete_by_pk_from(table, v)?;
                Ok(1)
            },
            _ => Err(DbError::UnsupportedFilter),
        }
    }

    pub fn query(&mut self, input: &str) -> Result<QueryResult, DbError> {
        let lexer = Lexer::new(input);
        let mut parser = Parser::new(lexer)?;

        let stmt = parser.parse_stmt()?;
        let result = self.execute(stmt)?;

        Ok(result)
    }

    pub fn execute(&mut self, stmt: Stmt) -> Result<QueryResult, DbError> {
        match stmt {
            Stmt::Insert { table, values } => {
                let row: Vec<Value> = values
                    .into_iter()
                    .map(|expr| expr.eval())
                    .collect::<Result<_, _>>()?;

                self.insert_into(&table, row)?;
                Ok(QueryResult::Affected(1))
            }

            Stmt::Select { table, filter } => {
                let rows = self.select_where(&table, filter)?;
                Ok(QueryResult::Rows(rows))
            }

            Stmt::Delete { table, filter } => {
                let deleted = self.delete_where(&table, filter)?;
                Ok(QueryResult::Affected(deleted))
            }

            _ => Err(DbError::UnsupportedStatement),
        }
    }
}

#[macro_export]
macro_rules! row {
    ($($val:expr),* $(,)?) => {
        vec![$(Value::from($val)),*]
    };
}

#[macro_export]
macro_rules! col {
    ($name:ident : $dtype:ident) => {
        Column::new(
        stringify!($name),
        types::DataType::$dtype
        )
    };
}

#[macro_export]
macro_rules! cols {
    ($($name:ident : $dtype:ident),* $(,)?) => {
        vec![
            $(col!($name : $dtype)),*
        ]
    };
}

#[macro_export]
macro_rules! schema {
    ($($name:ident : $dtype:ident $(=> $pk:ident)?),* $(,)?) => {{
        let mut columns = Vec::new();
        let mut primary_key = None;

        $(
            let idx = columns.len();
            columns.push(crate::types::Column::new(
                stringify!($name),
                crate::types::DataType::$dtype
            ));

            $(
                {
                    if stringify!($pk) != "pk" {
                        panic!("Unknown schema attribute: {}", stringify!($pk));
                    }
                    primary_key = Some(idx);
                }
            )?
        )*

        let pk = primary_key.expect("Primary key not specified");
        (columns, pk)
    }};
}
