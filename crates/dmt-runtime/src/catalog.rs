use crate::EngineError;
use dmt_core::{Graph, GraphId};
use dmt_store::Store;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

pub(crate) struct GraphCatalog {
    registered: BTreeMap<GraphId, Arc<Graph>>,
    cache: Mutex<BTreeMap<(GraphId, u32), Arc<Graph>>>,
}
impl GraphCatalog {
    pub(crate) fn new(registered: BTreeMap<GraphId, Arc<Graph>>) -> Self {
        Self {
            registered,
            cache: Mutex::default(),
        }
    }
    pub(crate) fn registered(&self, id: &GraphId) -> Option<&Arc<Graph>> {
        self.registered.get(id)
    }
    pub(crate) fn registered_ids(&self) -> Vec<GraphId> {
        self.registered.keys().cloned().collect()
    }
    pub(crate) fn graphs(&self) -> impl Iterator<Item = &Arc<Graph>> {
        self.registered.values()
    }
    pub(crate) async fn resolve(
        &self,
        store: &dyn Store,
        id: &GraphId,
        version: u32,
    ) -> Result<Arc<Graph>, EngineError> {
        if let Some(graph) = self.registered(id).filter(|g| g.version() == version) {
            return Ok(graph.clone());
        }
        let key = (id.clone(), version);
        if let Some(graph) = self.cache.lock().expect("catalog lock poisoned").get(&key) {
            return Ok(graph.clone());
        }
        let graph = Arc::new(store.load_graph(id, version).await?.ok_or_else(|| {
            EngineError::GraphUnavailable {
                graph_id: id.clone(),
                version,
            }
        })?);
        self.cache
            .lock()
            .expect("catalog lock poisoned")
            .insert(key, graph.clone());
        Ok(graph)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use dmt_core::{EndStatus, GraphBuilder, fixtures};
    use dmt_store::MemoryStore;
    #[tokio::test]
    async fn resolve_prefers_registered_then_store() {
        let registered = fixtures::linear();
        let catalog = GraphCatalog::new(BTreeMap::from([(
            registered.id().clone(),
            Arc::new(registered.clone()),
        )]));
        let store = MemoryStore::new();
        assert_eq!(
            *catalog.resolve(&store, registered.id(), 1).await.unwrap(),
            registered
        );
        let old = GraphBuilder::new("linear", 2)
            .start("a")
            .task("a")
            .end("done", EndStatus::Completed)
            .edge("a", "done")
            .build()
            .unwrap();
        store.register_graph(&old).await.unwrap();
        assert_eq!(*catalog.resolve(&store, old.id(), 2).await.unwrap(), old);
        assert_eq!(
            *catalog
                .resolve(&MemoryStore::new(), old.id(), 2)
                .await
                .unwrap(),
            old
        );
        assert!(matches!(
            catalog.resolve(&store, old.id(), 3).await,
            Err(EngineError::GraphUnavailable { .. })
        ));
    }
}
