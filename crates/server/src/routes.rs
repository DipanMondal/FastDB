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
    UpsertResponse,CollectionStatsResponse,
};

use crate::state::AppState;
use crate::storage::{append_entry, WalEntry};


// ---------- health ----------

pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

// ---------- collections -----------
pub async fn create_collection(
    State(state): State<AppState>,
    api_key: ApiKey,
    Json(payload): Json<CreateCollectionRequest>,
) -> Result<Json<CreateCollectionResponse>, (StatusCode, String)> {
    if payload.dimension == 0 {
        return Err((
            StatusCode::BAD_REQUEST,
            "dimension must be greater than 0".into(),
        ));
    }

    let tenant = api_key.0;

    let mut collections = state.collections.write().await;
    let tenant_map = collections.entry(tenant.clone()).or_default();

    if tenant_map.contains_key(&payload.name) {
        return Err((
            StatusCode::CONFLICT,
            format!("collection '{}' already exists", payload.name),
        ));
    }

    tenant_map.insert(
        payload.name.clone(),
        InMemoryIndex::new(payload.dimension),
    );

    if let Err(e) = append_entry(&WalEntry::CreateCollection {
        tenant: tenant.clone(),
        name: payload.name.clone(),
        dimension: payload.dimension,
    }) {
        tracing::error!("failed to append WAL for create_collection: {:?}", e);
    }

    Ok(Json(CreateCollectionResponse {
        name: payload.name,
        dimension: payload.dimension,
    }))
}



pub async fn list_collections(
    State(state): State<AppState>,
    api_key: ApiKey,
) -> Json<ListCollectionsResponse> {
    let tenant = api_key.0;
    let collections = state.collections.read().await;

    let mut items = Vec::new();

    if let Some(tenant_map) = collections.get(&tenant) {
        items.reserve(tenant_map.len());
        for (name, index) in tenant_map.iter() {
            items.push(CollectionSummary {
                name: name.clone(),
                dimension: index.dimension(),
                vectors: index.vector_count(),
            });
        }
    }

    Json(ListCollectionsResponse { collections: items })
}



pub async fn get_collection(
    State(state): State<AppState>,
    api_key: ApiKey,
    Path(name): Path<String>,
) -> Result<Json<GetCollectionResponse>, (StatusCode, String)> {
    let tenant = api_key.0;
    let collections = state.collections.read().await;

    let tenant_map = collections.get(&tenant).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("collection '{}' not found", name),
        )
    })?;

    match tenant_map.get(&name) {
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

pub async fn collection_stats(
    State(state): State<AppState>,
    api_key: ApiKey,
    Path(name): Path<String>,
) -> Result<Json<CollectionStatsResponse>, (StatusCode, String)> {
    let tenant = api_key.0;
    let collections = state.collections.read().await;

    let tenant_map = collections.get(&tenant).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("collection '{}' not found", name),
        )
    })?;

    let index = tenant_map.get(&name).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("collection '{}' not found", name),
        )
    })?;

    let resp = CollectionStatsResponse {
        name,
        dimension: index.dimension(),
        vectors: index.vector_count(),
        index_type: "hnsw_cosine".to_string(),
    };

    Ok(Json(resp))
}



pub async fn delete_collection(
    State(state): State<AppState>,
    api_key: ApiKey,
    Path(name): Path<String>,
) -> Result<Json<DeleteCollectionResponse>, (StatusCode, String)> {
    let tenant = api_key.0;
    let mut collections = state.collections.write().await;

    let existed = if let Some(tenant_map) = collections.get_mut(&tenant) {
        let removed = tenant_map.remove(&name).is_some();
        if tenant_map.is_empty() {
            collections.remove(&tenant);
        }
        removed
    } else {
        false
    };

    if !existed {
        return Err((
            StatusCode::NOT_FOUND,
            format!("collection '{}' not found", name),
        ));
    }

    if let Err(e) = append_entry(&WalEntry::DeleteCollection {
        tenant: tenant.clone(),
        name: name.clone(),
    }) {
        tracing::error!("failed to append WAL for delete_collection: {:?}", e);
    }

    Ok(Json(DeleteCollectionResponse { deleted: true }))
}



// ---------- upsert ----------

pub async fn upsert_vectors(
    State(state): State<AppState>,
    api_key: ApiKey,
    Path(name): Path<String>,
    Json(payload): Json<UpsertRequest>,
) -> Result<Json<UpsertResponse>, (StatusCode, String)> {
    let tenant = api_key.0;
    let mut collections = state.collections.write().await;

    let tenant_map = collections.get_mut(&tenant).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("collection '{}' not found", name),
        )
    })?;

    let index = tenant_map.get_mut(&name).ok_or_else(|| {
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
            tenant: tenant.clone(),
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
    api_key: ApiKey,
    Path(name): Path<String>,
    Json(payload): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, (StatusCode, String)> {
    let tenant = api_key.0;
    let collections = state.collections.read().await;

    let tenant_map = collections.get(&tenant).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("collection '{}' not found", name),
        )
    })?;

    let index = tenant_map.get(&name).ok_or_else(|| {
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
    api_key: ApiKey,
    Path((name, id)): Path<(String, String)>,
) -> Result<Json<DeleteVectorResponse>, (StatusCode, String)> {
    let tenant = api_key.0;
    let mut collections = state.collections.write().await;

    let tenant_map = collections.get_mut(&tenant).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("collection '{}' not found", name),
        )
    })?;

    let index = tenant_map.get_mut(&name).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("collection '{}' not found", name),
        )
    })?;

    let deleted = index.delete(&id);

    if deleted {
        if let Err(e) = append_entry(&WalEntry::DeleteVector {
            tenant: tenant.clone(),
            collection: name.clone(),
            id: id.clone(),
        }) {
            tracing::error!("failed to append WAL for delete_vector: {:?}", e);
        }
    }

    Ok(Json(DeleteVectorResponse { deleted }))
}


