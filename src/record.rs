use crate::utils::{decode_u32, encode_u32};

pub struct Record {
    pub key: String,
    pub value: Vec<u8>,
    pub timestamp: u64,
    pub deleted: bool,
}

impl Record {
    pub fn new(key: String, value: Vec<u8>, timestamp: u64, deleted: bool) -> Self {
        Self { key, value, timestamp, deleted }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(&encode_u32(self.key.len() as u32));
        bytes.extend_from_slice(&encode_u32(self.value.len() as u32));
        bytes.extend_from_slice(&self.timestamp.to_be_bytes());
        bytes.push(self.deleted as u8);
        bytes.extend_from_slice(self.key.as_bytes());
        bytes.extend_from_slice(&self.value);

        bytes
    }

    pub fn deserialize(buf: &[u8]) -> Self {
        let key_len = decode_u32(&buf[0..4]) as usize;
        let val_len = decode_u32(&buf[4..8]) as usize;
        let timestamp = u64::from_be_bytes(buf[8..16].try_into().unwrap());
        let deleted = buf[16] != 0;

        let key_start = 17;
        let key_end = key_start + key_len;
        let val_end = key_end + val_len;

        Record {
            key: String::from_utf8(buf[key_start..key_end].to_vec()).unwrap(),
            value: buf[key_end..val_end].to_vec(),
            timestamp,
            deleted,
        }
    }
}
