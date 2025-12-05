use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};

use crate::auth::ApiKey;
use crate::index::InMemoryIndex;
use crate::models::{
    CollectionSummary, CreateCollectionRequest, CreateCollectionResponse,
    DeleteCollectionResponse, DeleteVectorResponse, GetCollectionResponse, HealthResponse,
    ListCollectionsResponse, QueryMatch, QueryRequest, QueryResponse, UpsertRequest,
    UpsertResponse,
};

use crate::state::AppState;
use crate::storage::{append_entry, WalEntry};


// ---------- health ----------

pub async fn health(State(_state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

// ---------- collections ----------

pub async fn create_collection(
    State(state): State<AppState>,
    _api_key: ApiKey,
    Json(payload): Json<CreateCollectionRequest>,
) -> Result<Json<CreateCollectionResponse>, (StatusCode, String)> {
    if payload.dimension == 0 {
        return Err((
            StatusCode::BAD_REQUEST,
            "dimension must be greater than 0".into(),
        ));
    }

    let mut collections = state.collections.write().await;

    if collections.contains_key(&payload.name) {
        return Err((
            StatusCode::CONFLICT,
            format!("collection '{}' already exists", payload.name),
        ));
    }

    let name = payload.name.clone();
    let dimension = payload.dimension;

    collections.insert(name.clone(), InMemoryIndex::new(dimension));

    if let Err(e) =
        append_entry(&WalEntry::CreateCollection { name: name.clone(), dimension })
    {
        tracing::error!("failed to append WAL for create_collection: {:?}", e);
    }

    Ok(Json(CreateCollectionResponse { name, dimension }))
}


pub async fn list_collections(
    State(state): State<AppState>,
    _api_key: ApiKey,
) -> Json<ListCollectionsResponse> {
    let collections = state.collections.read().await;

    let mut items = Vec::with_capacity(collections.len());
    for (name, index) in collections.iter() {
        items.push(CollectionSummary {
            name: name.clone(),
            dimension: index.dimension(),
            vectors: index.vector_count(),
        });
    }

    Json(ListCollectionsResponse { collections: items })
}


pub async fn get_collection(
    State(state): State<AppState>,
    _api_key: ApiKey,
    Path(name): Path<String>,
) -> Result<Json<GetCollectionResponse>, (StatusCode, String)> {
    let collections = state.collections.read().await;

    match collections.get(&name) {
        Some(index) => Ok(Json(GetCollectionResponse {
            name,
            dimension: index.dimension(),
            vectors: index.vector_count(),
        })),
        None => Err((
            StatusCode::NOT_FOUND,
            format!("collection '{}' not found", name),
        )),
    }
}

pub async fn delete_collection(
    State(state): State<AppState>,
    _api_key: ApiKey,
    Path(name): Path<String>,
) -> Result<Json<DeleteCollectionResponse>, (StatusCode, String)> {
    let mut collections = state.collections.write().await;

    let existed = collections.remove(&name).is_some();
    if !existed {
        return Err((
            StatusCode::NOT_FOUND,
            format!("collection '{}' not found", name),
        ));
    }

    if let Err(e) = append_entry(&WalEntry::DeleteCollection { name: name.clone() }) {
        tracing::error!("failed to append WAL for delete_collection: {:?}", e);
    }

    Ok(Json(DeleteCollectionResponse { deleted: true }))
}


// ---------- upsert ----------

pub async fn upsert_vectors(
    State(state): State<AppState>,
    _api_key: ApiKey,
    Path(name): Path<String>,
    Json(payload): Json<UpsertRequest>,
) -> Result<Json<UpsertResponse>, (StatusCode, String)> {
    let mut collections = state.collections.write().await;

    let index = collections.get_mut(&name).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("collection '{}' not found", name),
        )
    })?;

    let mut count = 0usize;
    for v in payload.vectors {
        let id = v.id;
        let values = v.values;
        let metadata = v.metadata;

        index
            .upsert(id.clone(), values.clone(), metadata.clone())
            .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
        count += 1;

        if let Err(e) = append_entry(&WalEntry::UpsertVector {
            collection: name.clone(),
            id,
            values,
            metadata,
        }) {
            tracing::error!("failed to append WAL for upsert_vector: {:?}", e);
        }
    }

    Ok(Json(UpsertResponse { upserted: count }))
}

// ---------- query ----------

pub async fn query_vectors(
    State(state): State<AppState>,
	_api_key: ApiKey,
    Path(name): Path<String>,
    Json(payload): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, (StatusCode, String)> {
    let collections = state.collections.read().await;

    let index = collections.get(&name).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("collection '{}' not found", name),
        )
    })?;

    let scored = index
        .query(&payload.vector, payload.top_k)
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?;

    let matches: Vec<QueryMatch> = scored
        .into_iter()
        .map(|sp| QueryMatch {
            id: sp.id,
            score: sp.score,
            metadata: sp.metadata,
        })
        .collect();

    Ok(Json(QueryResponse { matches }))
}

// ---------- delete vector ----------

pub async fn delete_vector(
    State(state): State<AppState>,
	_api_key: ApiKey,
    Path((name, id)): Path<(String, String)>,
) -> Result<Json<DeleteVectorResponse>, (StatusCode, String)> {
    let mut collections = state.collections.write().await;

    let index = collections.get_mut(&name).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("collection '{}' not found", name),
        )
    })?;

    let deleted = index.delete(&id);

    if deleted {
        if let Err(e) = append_entry(&WalEntry::DeleteVector {
            collection: name.clone(),
            id: id.clone(),
        }) {
            tracing::error!("failed to append WAL for delete_vector: {:?}", e);
        }
    }

    Ok(Json(DeleteVectorResponse { deleted }))
}

