use serde_json::Value;
use std::collections::HashMap;

use hnsw_rs::prelude::{DistCosine, Hnsw};

pub struct InMemoryIndex {
    dim: usize,
    // Ground-truth store for vectors + metadata
    vectors: HashMap<String, IndexedVector>,
    // HNSW index over the same vectors
    hnsw: Hnsw<'static, f32, DistCosine>,
    // External string id -> internal numeric id used by HNSW
    id_to_data_id: HashMap<String, usize>,
    // Internal numeric id -> external string id
    data_id_to_id: HashMap<usize, String>,
    // Next internal id to allocate
    next_data_id: usize,
}

struct IndexedVector {
    values: Vec<f32>,
    metadata: Option<Value>,
}

pub struct ScoredPoint {
    pub id: String,
    /// similarity score ~ 1 - cosine_distance (higher is better)
    pub score: f32,
    pub metadata: Option<Value>,
}

impl InMemoryIndex {
    pub fn new(dim: usize) -> Self {
        // Reasonable defaults; we can tune later
        let max_nb_connection = 16;   // M
        let max_elements = 1_000_000; // capacity hint
        let max_layer = 16;
        let ef_construction = 200;

        let hnsw = Hnsw::<f32, DistCosine>::new(
            max_nb_connection,
            max_elements,
            max_layer,
            ef_construction,
            DistCosine {},
        );

        Self {
            dim,
            vectors: HashMap::new(),
            hnsw,
            id_to_data_id: HashMap::new(),
            data_id_to_id: HashMap::new(),
            next_data_id: 0,
        }
    }

    pub fn dimension(&self) -> usize {
        self.dim
    }

    pub fn upsert(
        &mut self,
        id: String,
        values: Vec<f32>,
        metadata: Option<Value>,
    ) -> Result<(), String> {
        if values.len() != self.dim {
            return Err(format!(
                "expected vector of dimension {}, got {}",
                self.dim,
                values.len()
            ));
        }

        // Basic sanity: avoid zero vector, which is degenerate for cosine
        let norm_sq: f32 = values.iter().map(|x| x * x).sum();
        if norm_sq == 0.0 {
            return Err("vector norm must be > 0".into());
        }

        let iv = IndexedVector { values, metadata };

        // Get or assign an internal id for HNSW
        let data_id = if let Some(&existing) = self.id_to_data_id.get(&id) {
            existing
        } else {
            let d = self.next_data_id;
            self.next_data_id += 1;
            self.id_to_data_id.insert(id.clone(), d);
            self.data_id_to_id.insert(d, id.clone());
            d
        };

        // Insert into HNSW: NOTE the tuple argument (&[f32], usize)
        let vec_ref: &[f32] = &iv.values;
        self.hnsw.insert((vec_ref, data_id));

        // Store/overwrite in ground-truth map
        self.vectors.insert(id, iv);

        Ok(())
    }

    pub fn delete(&mut self, id: &str) -> bool {
        let removed = self.vectors.remove(id).is_some();
        if removed {
            if let Some(data_id) = self.id_to_data_id.remove(id) {
                self.data_id_to_id.remove(&data_id);
                // HNSW has no hard delete; we just stop exposing this id.
            }
        }
        removed
    }

    pub fn query(&self, query: &[f32], top_k: usize) -> Result<Vec<ScoredPoint>, String> {
        if query.len() != self.dim {
            return Err(format!(
                "expected query vector of dimension {}, got {}",
                self.dim,
                query.len()
            ));
        }

        if top_k == 0 || self.vectors.is_empty() {
            return Ok(Vec::new());
        }

        let qnorm_sq: f32 = query.iter().map(|x| x * x).sum();
        if qnorm_sq == 0.0 {
            return Err("query vector norm must be > 0".into());
        }

        // ef (search breadth) – can be tuned
        let ef = top_k.max(64);
        let neighbours = self.hnsw.search(query, top_k * 4, ef);

        let mut scored = Vec::new();

        for n in neighbours {
            let data_id = n.d_id;
            let dist = n.distance;

            // Map back to external id; skip IDs we’ve “deleted”
            let Some(external_id) = self.data_id_to_id.get(&data_id) else {
                continue;
            };
            let Some(stored) = self.vectors.get(external_id) else {
                continue;
            };

            // DistCosine returns a distance; convert to similarity-ish score
            let score = 1.0 - dist;

            scored.push(ScoredPoint {
                id: external_id.clone(),
                score,
                metadata: stored.metadata.clone(),
            });

            if scored.len() == top_k {
                break;
            }
        }

        Ok(scored)
    }

    pub fn vector_count(&self) -> usize {
        self.vectors.len()
    }
}
