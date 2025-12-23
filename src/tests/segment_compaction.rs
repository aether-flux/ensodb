use crate::engine::EnsoDB;

fn segment_compaction() {
    let mut db = EnsoDB::new();

    #[derive(serde::Serialize, serde::Deserialize, Debug)]
    struct User { id: u32, name: String }

    for i in 0..15 {
        db.set("user".to_string(), User { id: 1, name: "enso".into() });
    }
}
