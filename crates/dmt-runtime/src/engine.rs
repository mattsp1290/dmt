use crate::{BuildError, EngineConfig, HandlerRegistry, NodeHandler, catalog::GraphCatalog};
use dmt_core::{Graph, NodeId};
use dmt_store::{Clock, Store, SystemClock};
use std::{collections::BTreeMap, sync::Arc};

/// Validated engine, ready to start inside a Tokio runtime.
pub struct Engine {
    pub(crate) store: Arc<dyn Store>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) config: EngineConfig,
    pub(crate) catalog: GraphCatalog,
    pub(crate) handlers: HandlerRegistry,
}
impl Engine {
    /// Construct an engine builder.
    #[must_use]
    pub fn builder() -> EngineBuilder {
        EngineBuilder::default()
    }
    /// Register graphs and start workers inside a Tokio runtime.
    /// # Errors
    /// Returns registration errors or exhausted transient retries.
    pub async fn start(self) -> Result<crate::EngineHandle, crate::EngineError> {
        use crate::{CancellationToken, commit_loop, handle::HandleInner, shared::Shared};
        use dmt_store::StoreError;
        for graph in self.catalog.graphs() {
            let mut last = StoreError::Busy;
            let mut registered = false;
            for retry in 0..=self.config.max_replan_attempts {
                if retry > 0 {
                    tokio::time::sleep(crate::backoff::replan_delay(
                        self.config.replan_backoff,
                        retry - 1,
                    ))
                    .await;
                }
                match self.store.register_graph(graph).await {
                    Ok(()) => {
                        registered = true;
                        break;
                    }
                    Err(e) if commit_loop::transient(&e) => last = e,
                    Err(e) => return Err(e.into()),
                }
            }
            if !registered {
                return Err(crate::EngineError::Contention {
                    attempts: self.config.max_replan_attempts.saturating_add(1),
                    last,
                });
            }
        }
        if self.config.heartbeat_every.saturating_mul(2) > self.config.lease {
            tracing::warn!("heartbeat cadence exceeds half the lease");
        }
        let shared = Arc::new(Shared {
            store: self.store,
            clock: self.clock,
            config: self.config,
            catalog: self.catalog,
            handlers: self.handlers,
            wake: tokio::sync::Notify::new(),
            stop: CancellationToken::new(),
            handler_cancel: CancellationToken::new(),
            kill: CancellationToken::new(),
            tracker: tokio_util::task::TaskTracker::new(),
            in_flight: std::sync::Mutex::default(),
        });
        for _ in 0..shared.config.workers {
            shared.spawn(crate::worker::run(shared.clone()));
        }
        if shared.config.workers > 0 {
            shared.spawn(crate::sweeps::run(shared.clone()));
        }
        Ok(crate::EngineHandle {
            inner: Arc::new(HandleInner { shared }),
        })
    }
}
/// Collects graphs, handlers, persistence, and configuration without doing I/O.
#[derive(Default)]
pub struct EngineBuilder {
    store: Option<Arc<dyn Store>>,
    clock: Option<Arc<dyn Clock>>,
    config: EngineConfig,
    graphs: Vec<Graph>,
    handlers: Vec<(NodeId, Arc<dyn NodeHandler>)>,
}
impl EngineBuilder {
    /// Set the backend.
    #[must_use]
    pub fn store(mut self, store: Arc<dyn Store>) -> Self {
        self.store = Some(store);
        self
    }
    /// Register one graph version; each id may be supplied only once.
    #[must_use]
    pub fn graph(mut self, graph: Graph) -> Self {
        self.graphs.push(graph);
        self
    }
    /// Register an executable node handler; each node id may be supplied only once.
    #[must_use]
    pub fn handler(mut self, node_id: impl Into<NodeId>, handler: Arc<dyn NodeHandler>) -> Self {
        self.handlers.push((node_id.into(), handler));
        self
    }
    /// Set persistence time; cadence and timeout time still come from Tokio.
    #[must_use]
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self
    }
    /// Set configuration.
    #[must_use]
    pub fn config(mut self, config: EngineConfig) -> Self {
        self.config = config;
        self
    }
    /// Validate store, graph ids, handler ids, config, then completeness in that order.
    /// # Errors
    /// Returns a missing store, duplicate registration, invalid config, or missing handler.
    pub fn build(self) -> Result<Engine, BuildError> {
        let store = self.store.ok_or(BuildError::MissingStore)?;
        let mut graphs = BTreeMap::new();
        for graph in self.graphs {
            let graph_id = graph.id().clone();
            if graphs.insert(graph_id.clone(), Arc::new(graph)).is_some() {
                return Err(BuildError::DuplicateGraph { graph_id });
            }
        }
        let mut handlers = HandlerRegistry::new();
        for (node_id, handler) in self.handlers {
            if handlers.register(node_id.clone(), handler).is_some() {
                return Err(BuildError::DuplicateHandler { node_id });
            }
        }
        self.config.validate()?;
        if self.config.workers > 0 {
            for graph in graphs.values() {
                handlers.require_all(graph)?;
            }
        }
        Ok(Engine {
            store,
            clock: self.clock.unwrap_or_else(|| Arc::new(SystemClock)),
            config: self.config,
            catalog: GraphCatalog::new(graphs),
            handlers,
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HandlerError, NodeContext, NodeInput};
    use async_trait::async_trait;
    use dmt_core::{NodeOutcome, Outcome, fixtures};
    use dmt_store::MemoryStore;
    struct Done;
    #[async_trait]
    impl NodeHandler for Done {
        async fn run(&self, _: NodeContext, _: NodeInput) -> Result<NodeOutcome, HandlerError> {
            Ok(NodeOutcome::Done(Outcome::done("ok")))
        }
    }
    #[test]
    fn build_validates_in_order() {
        assert!(matches!(
            Engine::builder().build(),
            Err(BuildError::MissingStore)
        ));
        let store = Arc::new(MemoryStore::new());
        let graph = fixtures::linear();
        let mut value: serde_json::Value = serde_json::from_str(&graph.to_json()).unwrap();
        value["version"] = 2.into();
        let other = Graph::from_json(&value.to_string()).unwrap();
        assert!(matches!(
            Engine::builder()
                .store(store.clone())
                .graph(graph.clone())
                .graph(other)
                .build(),
            Err(BuildError::DuplicateGraph { .. })
        ));
        assert!(matches!(
            Engine::builder()
                .store(store.clone())
                .handler("a", Arc::new(Done))
                .handler("a", Arc::new(Done))
                .build(),
            Err(BuildError::DuplicateHandler { .. })
        ));
        assert!(matches!(
            Engine::builder().store(store.clone()).graph(graph).build(),
            Err(BuildError::MissingHandler { .. })
        ));
        assert!(
            Engine::builder()
                .store(store)
                .config(EngineConfig {
                    workers: 0,
                    ..EngineConfig::default()
                })
                .build()
                .is_ok()
        );
    }
}
