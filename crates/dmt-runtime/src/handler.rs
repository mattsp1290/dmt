use crate::{BuildError, CancellationToken};
use async_trait::async_trait;
use dmt_core::{
    Graph, GraphId, JoinInput, NodeId, NodeKind, NodeOutcome, RunId, StepKey, TaskId, TaskRecord,
};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};

/// At-least-once node execution. Use `NodeContext::step_key` for idempotent side effects.
#[async_trait]
pub trait NodeHandler: Send + Sync {
    /// Execute one claimed attempt. Yield regularly and be cancel-safe at every await.
    /// # Errors
    /// Return a retryable or permanent handler failure.
    async fn run(&self, ctx: NodeContext, input: NodeInput) -> Result<NodeOutcome, HandlerError>;
}
/// Identity and cancellation for a single dispatch.
#[derive(Debug, Clone)]
pub struct NodeContext {
    /// Owning run.
    pub run_id: RunId,
    /// Graph definition id.
    pub graph_id: GraphId,
    /// Graph definition version.
    pub graph_version: u32,
    /// Executed node.
    pub node_id: NodeId,
    /// Persisted task id.
    pub task_id: TaskId,
    /// Stable side-effect idempotency key across retries and reclaims.
    pub step_key: StepKey,
    /// One-based attempt, including reclaims.
    pub attempt: u32,
    /// Cancelled on local run cancellation, lease loss, timeout, or shutdown cancellation.
    pub cancel: CancellationToken,
}
/// Input appropriate to the node kind.
#[derive(Debug, Clone, PartialEq)]
pub enum NodeInput {
    /// Task or fan-out payload.
    Task(Value),
    /// One fan-out branch and its index.
    Branch { index: u32, value: Value },
    /// Branch results sorted by index by the planner.
    Join(JoinInput),
}
/// A handler failure, preserving its message.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HandlerError {
    /// Retry according to the node's retry policy.
    #[error("{0}")]
    Retryable(String),
    /// Exhaust immediately.
    #[error("{0}")]
    Permanent(String),
}
impl From<HandlerError> for NodeOutcome {
    fn from(error: HandlerError) -> Self {
        match error {
            HandlerError::Retryable(message) => Self::Fail {
                message,
                retryable: true,
            },
            HandlerError::Permanent(message) => Self::Fail {
                message,
                retryable: false,
            },
        }
    }
}
/// Node-id keyed handlers shared across the engine's graphs.
#[derive(Default)]
pub struct HandlerRegistry {
    handlers: BTreeMap<NodeId, Arc<dyn NodeHandler>>,
}
impl std::fmt::Debug for HandlerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.handlers.keys()).finish()
    }
}
impl HandlerRegistry {
    /// Empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    /// Insert a handler, returning any previous handler.
    pub fn register(
        &mut self,
        node_id: impl Into<NodeId>,
        handler: Arc<dyn NodeHandler>,
    ) -> Option<Arc<dyn NodeHandler>> {
        self.handlers.insert(node_id.into(), handler)
    }
    /// Look up a node handler.
    #[must_use]
    pub fn get(&self, node_id: &NodeId) -> Option<&Arc<dyn NodeHandler>> {
        self.handlers.get(node_id)
    }
    /// Check every executable node in graph order.
    /// # Errors
    /// Returns the first missing handler.
    pub fn require_all(&self, graph: &Graph) -> Result<(), BuildError> {
        for (node_id, node) in graph.nodes() {
            if !matches!(node.kind, NodeKind::Wait { .. } | NodeKind::End { .. })
                && self.get(node_id).is_none()
            {
                return Err(BuildError::MissingHandler {
                    graph_id: graph.id().clone(),
                    node_id: node_id.clone(),
                });
            }
        }
        Ok(())
    }
}
pub(crate) fn node_input(kind: &NodeKind, task: &TaskRecord) -> Result<NodeInput, String> {
    match kind {
        NodeKind::Task | NodeKind::FanOut { .. } => Ok(NodeInput::Task(task.input.clone())),
        NodeKind::Branch => task
            .branch
            .as_ref()
            .map(|branch| NodeInput::Branch {
                index: branch.index,
                value: task.input.clone(),
            })
            .ok_or_else(|| "branch task has no branch reference".into()),
        NodeKind::Join { .. } => serde_json::from_value(task.input.clone())
            .map(NodeInput::Join)
            .map_err(|e| e.to_string()),
        NodeKind::Wait { .. } | NodeKind::End { .. } => Err("node kind is never dispatched".into()),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use dmt_core::{JoinPolicy, Micros, Outcome, fixtures, plan_start};
    struct Done;
    #[async_trait]
    impl NodeHandler for Done {
        async fn run(&self, _: NodeContext, _: NodeInput) -> Result<NodeOutcome, HandlerError> {
            Ok(NodeOutcome::Done(Outcome::done("ok")))
        }
    }
    #[test]
    fn require_all_reports_first_missing_node() {
        let graph = fixtures::fan_out(JoinPolicy::All);
        let mut registry = HandlerRegistry::new();
        for id in ["a", "fo", "jn"] {
            registry.register(id, Arc::new(Done));
        }
        assert_eq!(
            registry.require_all(&graph),
            Err(BuildError::MissingHandler {
                graph_id: graph.id().clone(),
                node_id: "br".into()
            })
        );
        registry.register("br", Arc::new(Done));
        assert_eq!(registry.require_all(&graph), Ok(()));
        let graph = fixtures::pipeline();
        for (id, node) in graph.nodes() {
            if !matches!(node.kind, NodeKind::Wait { .. } | NodeKind::End { .. }) {
                registry.register(id.clone(), Arc::new(Done));
            }
        }
        assert_eq!(registry.require_all(&graph), Ok(()));
    }
    #[test]
    fn handler_error_maps_to_fail() {
        for (error, retryable) in [
            (HandlerError::Retryable("message".into()), true),
            (HandlerError::Permanent("message".into()), false),
        ] {
            assert_eq!(
                NodeOutcome::from(error),
                NodeOutcome::Fail {
                    message: "message".into(),
                    retryable
                }
            );
        }
    }
    #[tokio::test]
    async fn node_input_maps_each_kind() {
        use dmt_store::{ClaimRequest, MemoryStore, Store};
        let store = MemoryStore::new();
        let graph = fixtures::linear();
        store.register_graph(&graph).await.unwrap();
        store
            .create_run(plan_start(&graph, "r".into(), Value::Bool(true), Micros(0)).unwrap())
            .await
            .unwrap();
        let mut task = store
            .claim_ready(ClaimRequest {
                worker_id: dmt_core::WorkerId::new(),
                now: Micros(0),
                lease_micros: 1,
                limit: 1,
                graphs: vec![graph.id().clone()],
            })
            .await
            .unwrap()
            .remove(0)
            .task;
        for kind in [
            NodeKind::Task,
            NodeKind::FanOut {
                branch: "b".into(),
                join: "j".into(),
            },
        ] {
            assert_eq!(
                node_input(&kind, &task),
                Ok(NodeInput::Task(Value::Bool(true)))
            );
        }
        assert!(node_input(&NodeKind::Branch, &task).is_err());
        task.branch = Some(dmt_core::BranchRef {
            join_id: "j".into(),
            index: 3,
        });
        assert_eq!(
            node_input(&NodeKind::Branch, &task),
            Ok(NodeInput::Branch {
                index: 3,
                value: Value::Bool(true)
            })
        );
        let kind = NodeKind::Join {
            policy: JoinPolicy::All,
        };
        assert!(node_input(&kind, &task).is_err());
        let input = JoinInput {
            results: vec![],
            expected: 0,
            quorum_met: true,
        };
        task.input = serde_json::to_value(&input).unwrap();
        assert_eq!(node_input(&kind, &task), Ok(NodeInput::Join(input)));
        assert!(
            node_input(
                &NodeKind::Wait {
                    signal: "go".into(),
                    deadline_micros: None
                },
                &task
            )
            .is_err()
        );
    }
}
