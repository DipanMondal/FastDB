use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
}

// ---------- collections: create ----------

#[derive(Deserialize)]
pub struct CreateCollectionRequest {
    pub name: String,
    pub dimension: usize,
}

#[derive(Serialize)]
pub struct CreateCollectionResponse {
    pub name: String,
    pub dimension: usize,
}

// ---------- vectors: upsert/query ----------

#[derive(Deserialize)]
pub struct UpsertRequest {
    pub vectors: Vec<VectorData>,
}

#[derive(Deserialize)]
pub struct VectorData {
    pub id: String,
    pub values: Vec<f32>,
    #[serde(default)]
    pub metadata: Option<Value>,
}

#[derive(Serialize)]
pub struct UpsertResponse {
    pub upserted: usize,
}

#[derive(Deserialize)]
pub struct QueryRequest {
    pub vector: Vec<f32>,
    pub top_k: usize,
}

#[derive(Serialize)]
pub struct QueryMatch {
    pub id: String,
    pub score: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

#[derive(Serialize)]
pub struct QueryResponse {
    pub matches: Vec<QueryMatch>,
}

// ---------- collections: list/get ----------

#[derive(Serialize)]
pub struct CollectionSummary {
    pub name: String,
    pub dimension: usize,
    pub vectors: usize,
}

#[derive(Serialize)]
pub struct ListCollectionsResponse {
    pub collections: Vec<CollectionSummary>,
}

#[derive(Serialize)]
pub struct GetCollectionResponse {
    pub name: String,
    pub dimension: usize,
    pub vectors: usize,
}

// ---------- collections: stats ----------

#[derive(Serialize)]
pub struct CollectionStatsResponse {
    pub name: String,
    pub dimension: usize,
    pub vectors: usize,
    pub index_type: String,
}

// ---------- delete responses ----------

#[derive(Serialize)]
pub struct DeleteVectorResponse {
    pub deleted: bool,
}

#[derive(Serialize)]
pub struct DeleteCollectionResponse {
    pub deleted: bool,
}
