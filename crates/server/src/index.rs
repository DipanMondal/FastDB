use serde_json::Value;
use std::cmp::Ordering;
use std::collections::HashMap;

pub struct InMemoryIndex {
    dim: usize,
    vectors: HashMap<String, IndexedVector>,
}

struct IndexedVector {
    values: Vec<f32>,
    norm: f32,
    metadata: Option<Value>,
}

pub struct ScoredPoint {
    pub id: String,
    pub score: f32,
    pub metadata: Option<Value>,
}

impl InMemoryIndex {
    pub fn new(dim: usize) -> Self {
        Self {
            dim,
            vectors: HashMap::new(),
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

        let norm_sq: f32 = values.iter().map(|x| x * x).sum();
        if norm_sq == 0.0 {
            return Err("vector norm must be > 0".into());
        }
        let norm = norm_sq.sqrt();

        let iv = IndexedVector {
            values,
            norm,
            metadata,
        };

        self.vectors.insert(id, iv);
        Ok(())
    }

    pub fn delete(&mut self, id: &str) -> bool {
        self.vectors.remove(id).is_some()
    }

    pub fn query(&self, query: &[f32], top_k: usize) -> Result<Vec<ScoredPoint>, String> {
        if query.len() != self.dim {
            return Err(format!(
                "expected query vector of dimension {}, got {}",
                self.dim,
                query.len()
            ));
        }

        if top_k == 0 {
            return Ok(Vec::new());
        }

        let qnorm_sq: f32 = query.iter().map(|x| x * x).sum();
        if qnorm_sq == 0.0 {
            return Err("query vector norm must be > 0".into());
        }
        let qnorm = qnorm_sq.sqrt();

        let mut scored: Vec<ScoredPoint> = Vec::with_capacity(self.vectors.len());

        for (id, v) in &self.vectors {
            let dot: f32 = v.values.iter().zip(query).map(|(a, b)| a * b).sum();
            let score = dot / (v.norm * qnorm); // cosine similarity

            scored.push(ScoredPoint {
                id: id.clone(),
                score,
                metadata: v.metadata.clone(),
            });
        }

        scored.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(Ordering::Equal)
        });

        if scored.len() > top_k {
            scored.truncate(top_k);
        }

        Ok(scored)
    }
	
	pub fn vector_count(&self) -> usize {
        self.vectors.len()
    }

}
