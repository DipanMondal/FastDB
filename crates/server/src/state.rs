use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::index::InMemoryIndex;

#[derive(Clone)]
pub struct AppState {
    // tenant_id (api_key) -> { collection_name -> index }
    pub collections: Arc<RwLock<HashMap<String, HashMap<String, InMemoryIndex>>>>,
    pub api_keys: Arc<HashSet<String>>,
}

impl AppState {
    pub fn new() -> Self {
        let api_keys = default_api_keys();
        Self {
            collections: Arc::new(RwLock::new(HashMap::new())),
            api_keys: Arc::new(api_keys),
        }
    }

    pub fn with_collections(
        initial: HashMap<String, HashMap<String, InMemoryIndex>>,
    ) -> Self {
        let api_keys = default_api_keys();
        Self {
            collections: Arc::new(RwLock::new(initial)),
            api_keys: Arc::new(api_keys),
        }
    }
}

fn default_api_keys() -> HashSet<String> {
    if let Ok(val) = std::env::var("OPENVDB_API_KEYS") {
        let keys = val
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<HashSet<_>>();

        tracing::info!("loaded {} API keys from OPENVDB_API_KEYS", keys.len());
        keys
    } else {
        let mut set = HashSet::new();
        set.insert("dev-key".to_string());
        tracing::warn!("OPENVDB_API_KEYS not set, using default dev-key");
        set
    }
}
