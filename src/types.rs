use std::collections::HashMap;

use serde::{Serialize, Deserialize};

// -> Manifest formats

// #[derive(Serialize, Deserialize, Debug)]
// pub struct SegmentInfo {
//     pub name: String,
// }
//
// #[derive(Serialize, Deserialize, Debug)]
// pub enum SegmentState {
//     Active,
//     Stale,
//     Compacted,
// }

pub type SegIndex = HashMap<String, u64>;

#[derive(Serialize, Deserialize, Debug)]
pub struct Manifest {
    pub active_segment: String,
    pub segments: Vec<String>,
    pub last_compaction: Option<String>,
}
