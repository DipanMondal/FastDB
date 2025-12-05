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
    CreateCollection {
        tenant: String,
        name: String,
        dimension: usize,
    },
    DeleteCollection {
        tenant: String,
        name: String,
    },
    UpsertVector {
        tenant: String,
        collection: String,
        id: String,
        values: Vec<f32>,
        metadata: Option<Value>,
    },
    DeleteVector {
        tenant: String,
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

// tenant -> { collection_name -> index }
pub fn load_collections_from_wal(
) -> anyhow::Result<HashMap<String, HashMap<String, InMemoryIndex>>> {
    ensure_data_dir()?;

    let path = Path::new(WAL_FILE);
    let mut collections: HashMap<String, HashMap<String, InMemoryIndex>> = HashMap::new();

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
                eprintln!(
                    "failed to parse WAL line {}: {:?} (line: {})",
                    lineno + 1,
                    e,
                    trimmed
                );
                continue;
            }
        };

        match entry {
            WalEntry::CreateCollection {
                tenant,
                name,
                dimension,
            } => {
                let tenant_map = collections.entry(tenant).or_insert_with(HashMap::new);
                tenant_map
                    .entry(name)
                    .or_insert_with(|| InMemoryIndex::new(dimension));
            }
            WalEntry::DeleteCollection { tenant, name } => {
                if let Some(tenant_map) = collections.get_mut(&tenant) {
                    tenant_map.remove(&name);
                    if tenant_map.is_empty() {
                        collections.remove(&tenant);
                    }
                }
            }
            WalEntry::UpsertVector {
                tenant,
                collection,
                id,
                values,
                metadata,
            } => {
                let dim = values.len();
                let tenant_map = collections.entry(tenant).or_insert_with(HashMap::new);
                let index = tenant_map
                    .entry(collection)
                    .or_insert_with(|| InMemoryIndex::new(dim));
                let _ = index.upsert(id, values, metadata);
            }
            WalEntry::DeleteVector {
                tenant,
                collection,
                id,
            } => {
                if let Some(tenant_map) = collections.get_mut(&tenant) {
                    if let Some(index) = tenant_map.get_mut(&collection) {
                        index.delete(&id);
                    }
                    if tenant_map.is_empty() {
                        collections.remove(&tenant);
                    }
                }
            }
        }
    }

    Ok(collections)
}
