use engine::EnsoDB;

mod record;
mod storage;
mod utils;
mod engine;
mod types;
mod tests;

fn main() {
    let mut db = EnsoDB::new();

    // // simple string
    // db.set("greeting".to_string(), "hello world".to_string());
    // 
    // // integer
    // db.set("year".to_string(), 2025u32);
    // 
    // // float
    // db.set("pi".to_string(), 3.14159f64);
    
    // struct
    #[derive(serde::Serialize, serde::Deserialize, Debug)]
    struct User { id: u32, name: String }

    for i in 0..15 {
        db.set("user".to_string(), User { id: i as u32, name: "enso".into() });
    }

    // println!("{:#?}", db.storage.manifest);

    // println!("");
    // for (k, v) in db.index.iter() {
    //     println!("Key: {}, value: {:?}", k, v);
    // }
    // println!("");

    // println!("{:?}", db.get::<User>("user-24".to_string()));
    // println!("{:?}", db.get::<User>("user-29".to_string()));
    // println!("{:?}", db.get::<User>("user-15".to_string()));
    // println!("{:?}", db.get::<User>("user-0".to_string()));
    println!("{:?}", db.get::<User>("user".to_string()));

    // db.delete("pi".to_string());

    // println!("Index: {:#?}", db.index);

    // println!("{:?}", db.get::<String>("greeting".to_string()));
    // println!("{:?}", db.get::<u32>("year".to_string()));
    // println!("{:?}", db.get::<f64>("pi".to_string()));
    // println!("{:?}", db.get::<User>("user".to_string()));
    // println!("{:?}", db.get::<String>("test".to_string()));
}
