use crate::{error::DbError, types::{DataType, TableSchema, Value}};

pub struct RowCodec;

impl RowCodec {
    pub fn encode(row: &[Value]) -> Result<Vec<u8>, DbError> {
        let mut buf = Vec::new();

        for value in row {
            match value {
                Value::Null => {
                    buf.push(0);
                }

                Value::Int(v) => {
                    buf.push(1);
                    buf.extend(&v.to_be_bytes());
                }

                Value::Float(v) => {
                    buf.push(1);
                    buf.extend(&v.to_be_bytes());
                }

                Value::Bool(v) => {
                    buf.push(1);
                    buf.push(*v as u8);
                }

                Value::String(s) => {
                    buf.push(1);
                    let bytes = s.as_bytes();
                    let len = bytes.len() as u32;
                    buf.extend(&len.to_be_bytes());
                    buf.extend(bytes);
                }
            }
        }
        
        Ok(buf)
    }

    pub fn decode(bytes: &[u8], schema: &TableSchema) -> Result<Vec<Value>, DbError> {
        let mut row = Vec::with_capacity(schema.columns.len());
        let mut cursor = 0;

        for col in &schema.columns {
            let null_flag = bytes[cursor];
            cursor += 1;

            if null_flag == 0 {
                row.push(Value::Null);
                continue;
            }

            match col.dtype {
                DataType::Int => {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(&bytes[cursor..cursor+8]);
                    cursor += 8;
                    row.push(Value::Int(i64::from_be_bytes(buf)));
                }

                DataType::Float => {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(&bytes[cursor..cursor+8]);
                    cursor += 8;
                    row.push(Value::Float(f64::from_be_bytes(buf)));
                }

                DataType::Bool => {
                    let v = bytes[cursor] != 0;
                    cursor += 1;
                    row.push(Value::Bool(v));
                }

                DataType::String => {
                    let mut len_buf = [0u8; 4];
                    len_buf.copy_from_slice(&bytes[cursor..cursor+4]);
                    cursor += 4;
                    let len = u32::from_be_bytes(len_buf) as usize;

                    let s = String::from_utf8(bytes[cursor..cursor+len].to_vec())?;
                    cursor += len;

                    row.push(Value::String(s));
                }
            }
        }

        Ok(row)
    }
}
