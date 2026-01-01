use std::time;

use api::Enso;
use engine::Engine;
use types::{Column, Value};

mod record;
mod storage;
mod utils;
mod engine;
mod types;
mod api;
mod schema;
mod error;
mod codec;

fn main() {
    // struct
    // #[derive(serde::Serialize, serde::Deserialize, Debug)]
    // struct User { id: u32, name: String }

    let mut db = Enso::open("test_db").unwrap();

    db.create_table(
        "users",
        schema! {
            id: Int => pk,
            name: String,
            age: Int,
        }
    ).unwrap();

    db.insert(row![1, "amartya", 24]).unwrap();

    let row = db.select_by_pk(1).unwrap().unwrap();
    println!("Row: {:?}", row);

    db.delete_by_pk(1).unwrap();
    println!("Row after deletion: {:?}", db.select_by_pk(1).unwrap());
}
