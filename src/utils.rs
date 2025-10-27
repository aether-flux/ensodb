use serde::{de::DeserializeOwned, Serialize};

pub fn encode_u32(x: u32) -> [u8; 4] {
    x.to_be_bytes()
}

pub fn decode_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes(bytes.try_into().unwrap())
}

pub fn to_bytes<T: Serialize>(value: &T) -> Vec<u8> {
    bincode::serialize(value).unwrap()
}

pub fn from_bytes<T: DeserializeOwned>(bytes: &[u8]) -> T {
    bincode::deserialize(bytes).unwrap()
}
