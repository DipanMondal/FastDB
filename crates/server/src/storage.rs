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
pub const SNAPSHOT_FILE: &str = "data/snapshot.json";

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

/// Apply all WAL entries onto an existing collections map.
///
/// This is the core replay logic used both when there is no snapshot
/// (start from empty map) and when there *is* a snapshot (start from
/// snapshot state, then apply changes since snapshot).
pub fn replay_wal(
    collections: &mut HashMap<String, HashMap<String, InMemoryIndex>>,
) -> anyhow::Result<()> {
    ensure_data_dir()?;

    let path = Path::new(WAL_FILE);
    if !path.exists() {
        return Ok(());
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

    Ok(())
}

/// Helper: load collections *only* from WAL (no snapshot).
pub fn load_collections_from_wal(
) -> anyhow::Result<HashMap<String, HashMap<String, InMemoryIndex>>> {
    let mut collections: HashMap<String, HashMap<String, InMemoryIndex>> = HashMap::new();
    replay_wal(&mut collections)?;
    Ok(collections)
}

///////////////////////////////////////
// Snapshots
///////////////////////////////////////

#[derive(Serialize, Deserialize)]
struct SnapshotVector {
    id: String,
    values: Vec<f32>,
    metadata: Option<Value>,
}

#[derive(Serialize, Deserialize)]
struct SnapshotCollection {
    dimension: usize,
    vectors: Vec<SnapshotVector>,
}

#[derive(Serialize, Deserialize)]
struct Snapshot {
    tenants: HashMap<String, HashMap<String, SnapshotCollection>>,
}

/// Load collections from snapshot.json if it exists.
/// Returns Ok(Some(map)) if snapshot found, Ok(None) if not present.
pub fn load_collections_from_snapshot(
) -> anyhow::Result<Option<HashMap<String, HashMap<String, InMemoryIndex>>>> {
    ensure_data_dir()?;

    let path = Path::new(SNAPSHOT_FILE);
    if !path.exists() {
        return Ok(None);
    }

    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let snap: Snapshot = serde_json::from_reader(reader)?;

    let mut result: HashMap<String, HashMap<String, InMemoryIndex>> = HashMap::new();

    for (tenant, collections) in snap.tenants {
        let mut tenant_map: HashMap<String, InMemoryIndex> = HashMap::new();

        for (name, sc) in collections {
            let mut index = InMemoryIndex::new(sc.dimension);
            for v in sc.vectors {
                let _ = index.upsert(v.id, v.values, v.metadata);
            }
            tenant_map.insert(name, index);
        }

        result.insert(tenant, tenant_map);
    }

    Ok(Some(result))
}

/// Write a full snapshot of all tenants/collections to snapshot.json
/// and truncate the WAL afterwards.
pub fn write_snapshot_from_state(
    collections: &HashMap<String, HashMap<String, InMemoryIndex>>,
) -> anyhow::Result<()> {
    ensure_data_dir()?;

    // Build snapshot struct
    let mut tenants: HashMap<String, HashMap<String, SnapshotCollection>> = HashMap::new();

    for (tenant, col_map) in collections.iter() {
        let mut col_snap_map = HashMap::new();

        for (name, index) in col_map.iter() {
            let vectors = index
                .export_vectors()
                .into_iter()
                .map(|(id, values, metadata)| SnapshotVector { id, values, metadata })
                .collect();

            let sc = SnapshotCollection {
                dimension: index.dimension(),
                vectors,
            };

            col_snap_map.insert(name.clone(), sc);
        }

        tenants.insert(tenant.clone(), col_snap_map);
    }

    let snap = Snapshot { tenants };

    // Write to temp file first, then atomically rename
    let tmp_path = Path::new("data/snapshot.json.tmp");
    {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(tmp_path)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer(writer, &snap)?;
    }

    fs::rename(tmp_path, SNAPSHOT_FILE)?;

    // Truncate WAL after successful snapshot (simple compaction)
    truncate_wal()?;

    Ok(())
}

fn truncate_wal() -> anyhow::Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(WAL_FILE)?;
    file.sync_all()?;
    Ok(())
}
