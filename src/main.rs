use engine::EnsoDB;

mod record;
mod storage;
mod utils;
mod engine;

fn main() {
    let mut db = EnsoDB::new();

    // simple string
    // db.set("greeting".to_string(), "hello world".to_string());
    
    // integer
    // db.set("year".to_string(), 2025u32);
    
    // float
    // db.set("pi".to_string(), 3.14159f64);
    
    // struct
    #[derive(serde::Serialize, serde::Deserialize, Debug)]
    struct User { id: u32, name: String }
    // db.set("user".to_string(), User { id: 1, name: "enso".into() });

    db.delete("pi".to_string());

    println!("{:?}", db.get::<String>("greeting".to_string()));
    println!("{:?}", db.get::<u32>("year".to_string()));
    println!("{:?}", db.get::<f64>("pi".to_string()));
    println!("{:?}", db.get::<User>("user".to_string()));
}
