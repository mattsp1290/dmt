use super::{
    Edge, EndStatus, Graph, GraphError, Guard, JoinPolicy, NodeDef, NodeKind, RetryPolicy,
};
use crate::{GraphId, NodeId};
use std::collections::BTreeMap;
/// Fluent graph builder collecting duplicate definitions and validation errors.
#[derive(Debug)]
pub struct GraphBuilder {
    graph: Graph,
    errors: Vec<GraphError>,
}
impl GraphBuilder {
    #[must_use]
    pub fn new(id: impl Into<GraphId>, version: u32) -> Self {
        Self {
            graph: Graph {
                id: id.into(),
                version,
                start: NodeId::from(""),
                nodes: BTreeMap::new(),
                edges: Vec::new(),
            },
            errors: Vec::new(),
        }
    }
    #[must_use]
    pub fn start(mut self, node: impl Into<NodeId>) -> Self {
        self.graph.start = node.into();
        self
    }
    fn insert(
        mut self,
        id: NodeId,
        kind: NodeKind,
        retry: RetryPolicy,
        timeout_micros: Option<i64>,
    ) -> Self {
        if self
            .graph
            .nodes
            .insert(
                id.clone(),
                NodeDef {
                    kind,
                    retry,
                    timeout_micros,
                },
            )
            .is_some()
        {
            self.errors.push(GraphError::DuplicateNode { node: id });
        }
        self
    }
    #[must_use]
    pub fn task(self, id: impl Into<NodeId>) -> Self {
        self.task_with(id, RetryPolicy::default(), None)
    }
    #[must_use]
    pub fn task_with(
        self,
        id: impl Into<NodeId>,
        retry: RetryPolicy,
        timeout: Option<i64>,
    ) -> Self {
        self.insert(id.into(), NodeKind::Task, retry, timeout)
    }
    #[must_use]
    pub fn fan_out(
        self,
        id: impl Into<NodeId>,
        branch: impl Into<NodeId>,
        join: impl Into<NodeId>,
    ) -> Self {
        self.insert(
            id.into(),
            NodeKind::FanOut {
                branch: branch.into(),
                join: join.into(),
            },
            RetryPolicy::default(),
            None,
        )
    }
    #[must_use]
    pub fn branch(self, id: impl Into<NodeId>) -> Self {
        self.branch_with(id, RetryPolicy::default(), None)
    }
    #[must_use]
    pub fn branch_with(
        self,
        id: impl Into<NodeId>,
        retry: RetryPolicy,
        timeout: Option<i64>,
    ) -> Self {
        self.insert(id.into(), NodeKind::Branch, retry, timeout)
    }
    #[must_use]
    pub fn join(self, id: impl Into<NodeId>, policy: JoinPolicy) -> Self {
        self.join_with(id, policy, RetryPolicy::default(), None)
    }
    #[must_use]
    pub fn join_with(
        self,
        id: impl Into<NodeId>,
        policy: JoinPolicy,
        retry: RetryPolicy,
        timeout: Option<i64>,
    ) -> Self {
        self.insert(id.into(), NodeKind::Join { policy }, retry, timeout)
    }
    #[must_use]
    pub fn wait(
        self,
        id: impl Into<NodeId>,
        signal: impl Into<String>,
        deadline_micros: Option<i64>,
    ) -> Self {
        self.insert(
            id.into(),
            NodeKind::Wait {
                signal: signal.into(),
                deadline_micros,
            },
            RetryPolicy::default(),
            None,
        )
    }
    #[must_use]
    pub fn end(self, id: impl Into<NodeId>, status: EndStatus) -> Self {
        self.insert(
            id.into(),
            NodeKind::End { status },
            RetryPolicy::default(),
            None,
        )
    }
    #[must_use]
    pub fn edge(self, from: impl Into<NodeId>, to: impl Into<NodeId>) -> Self {
        self.add_edge(from.into(), to.into(), Guard::Default)
    }
    #[must_use]
    pub fn edge_on(
        self,
        from: impl Into<NodeId>,
        to: impl Into<NodeId>,
        label: impl Into<String>,
    ) -> Self {
        self.add_edge(from.into(), to.into(), Guard::Label(label.into()))
    }
    fn add_edge(mut self, from: NodeId, to: NodeId, guard: Guard) -> Self {
        self.graph.edges.push(Edge { from, to, guard });
        self
    }
    /// Validate the completed definition.
    /// # Errors
    /// Returns all duplicate-node and graph validation errors.
    pub fn build(mut self) -> Result<Graph, GraphError> {
        self.errors.extend(self.graph.validate());
        if self.errors.is_empty() {
            Ok(self.graph)
        } else {
            Err(GraphError::Invalid(self.errors))
        }
    }
}
