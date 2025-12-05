use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::index::InMemoryIndex;

#[derive(Clone)]
pub struct AppState {
    pub collections: Arc<RwLock<HashMap<String, InMemoryIndex>>>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            collections: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn with_collections(initial: HashMap<String, InMemoryIndex>) -> Self {
        Self {
            collections: Arc::new(RwLock::new(initial)),
        }
    }
}
