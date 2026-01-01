use std::collections::HashMap;

use serde::{Serialize, Deserialize};

use crate::error::DbError;

// Storage engine

pub type SegIndex = HashMap<String, u64>;

#[derive(Serialize, Deserialize, Debug)]
pub struct Manifest {
    pub active_segment: String,
    pub segments: Vec<String>,
    pub last_compaction: Option<String>,
}


// User API
#[derive(Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Column {
    pub name: String,
    pub dtype: DataType,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum DataType {
    Int,
    Float,
    Bool,
    String,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Value {
    Int(i64),
    Float(f64),
    Bool(bool),
    String(String),
    Null,
}

impl Column {
    pub fn new(name: &str, dtype: DataType) -> Self {
        Self { name: name.to_string(), dtype }
    }
}

impl Value {
    pub fn matches(&self, dtype: &DataType) -> bool {
        match (self, dtype) {
            (Value::Int(_), DataType::Int) => true,
            (Value::Float(_), DataType::Float) => true,
            (Value::Bool(_), DataType::Bool) => true,
            (Value::String(_), DataType::String) => true,
            (Value::Null, _) => true,
            _ => false,
        }
    }

    pub fn to_key_bytes(&self) -> Result<Vec<u8>, DbError> {
        match self {
            Value::Int(v) => Ok(v.to_be_bytes().to_vec()),
            Value::String(s) => Ok(s.as_bytes().to_vec()),
            _ => Err(DbError::InvalidPrimaryKeyType),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int(v) => write!(f, "i:{}", v),
            Value::Float(v) => write!(f, "f:{}", v),
            Value::Bool(v) => write!(f, "b:{}", v),
            Value::String(v) => write!(f, "s:{}", v),
            Value::Null => write!(f, "")
        }
    }
}

// Automatic type conversions

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Int(v as i64)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}

impl<T> From<Option<T>> for Value
where
    T: Into<Value>
{
    fn from(v: Option<T>) -> Self {
        match v {
            Some(x) => x.into(),
            None => Value::Null,
        }
    }
}
