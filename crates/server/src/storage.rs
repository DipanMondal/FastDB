use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::Path,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::index::InMemoryIndex;

pub const WAL_FILE: &str = "data/wal.jsonl";

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WalEntry {
    CreateCollection { name: String, dimension: usize },
    DeleteCollection { name: String },
    UpsertVector {
        collection: String,
        id: String,
        values: Vec<f32>,
        metadata: Option<Value>,
    },
    DeleteVector {
        collection: String,
        id: String,
    },
}

fn ensure_data_dir() -> anyhow::Result<()> {
    let path = Path::new("data");
    if !path.exists() {
        fs::create_dir_all(path)?;
    }
    Ok(())
}

pub fn append_entry(entry: &WalEntry) -> anyhow::Result<()> {
    ensure_data_dir()?;

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(WAL_FILE)?;
    let mut writer = BufWriter::new(file);

    let line = serde_json::to_string(entry)?;
    writer.write_all(line.as_bytes())?;
    writer.write_all(b"\n")?;
    writer.flush()?;

    Ok(())
}

pub fn load_collections_from_wal() -> anyhow::Result<HashMap<String, InMemoryIndex>> {
    ensure_data_dir()?;

    let path = Path::new(WAL_FILE);
    let mut collections: HashMap<String, InMemoryIndex> = HashMap::new();

    if !path.exists() {
        return Ok(collections);
    }

    let file = File::open(path)?;
    let reader = BufReader::new(file);

    for (lineno, line) in reader.lines().enumerate() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                eprintln!("failed to read WAL line {}: {:?}", lineno + 1, e);
                continue;
            }
        };

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let entry: WalEntry = match serde_json::from_str(trimmed) {
            Ok(e) => e,
            Err(e) => {
                eprintln!("failed to parse WAL line {}: {:?} (line: {})", lineno + 1, e, trimmed);
                continue;
            }
        };

        match entry {
            WalEntry::CreateCollection { name, dimension } => {
                collections
                    .entry(name)
                    .or_insert_with(|| InMemoryIndex::new(dimension));
            }
            WalEntry::DeleteCollection { name } => {
                collections.remove(&name);
            }
            WalEntry::UpsertVector {
                collection,
                id,
                values,
                metadata,
            } => {
                let dim = values.len();
                let index = collections
                    .entry(collection)
                    .or_insert_with(|| InMemoryIndex::new(dim));
                let _ = index.upsert(id, values, metadata);
            }
            WalEntry::DeleteVector { collection, id } => {
                if let Some(index) = collections.get_mut(&collection) {
                    index.delete(&id);
                }
            }
        }
    }

    Ok(collections)
}
