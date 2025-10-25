use engine::EnsoDB;

mod record;
mod storage;
mod utils;
mod engine;

fn main() {
    let mut db = EnsoDB::new();

    db.set("one".to_string(), vec![1, 2, 3]);
    db.set("two".to_string(), vec![4, 5, 6]);

    println!("Index: {:#?}\n", db.index);

    if let Some(d) = db.get("two".to_string()) {
        println!("Key: two, Value: {:?}", d);
    }

    if let Some(d) = db.get("three".to_string()) {
        println!("Key: three, Value: {:?}", d);
    }

    db.set("three".to_string(), vec![7, 8, 9]);

    println!("\nIndex: {:#?}\n", db.index);

    if let Some(d) = db.get("three".to_string()) {
        println!("Key: three, Value: {:?}", d);
    }
}
