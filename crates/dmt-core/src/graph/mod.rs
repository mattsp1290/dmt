mod builder;
mod hash;
mod validate;
use crate::{GraphId, NodeId};
pub use builder::GraphBuilder;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
/// Validated graph definition. Every deserialization validates its structure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawGraph")]
pub struct Graph {
    id: GraphId,
    version: u32,
    start: NodeId,
    nodes: BTreeMap<NodeId, NodeDef>,
    edges: Vec<Edge>,
}
#[derive(Deserialize)]
struct RawGraph {
    id: GraphId,
    version: u32,
    start: NodeId,
    nodes: BTreeMap<NodeId, NodeDef>,
    edges: Vec<Edge>,
}
impl TryFrom<RawGraph> for Graph {
    type Error = GraphError;
    fn try_from(raw: RawGraph) -> Result<Self, Self::Error> {
        let graph = Self {
            id: raw.id,
            version: raw.version,
            start: raw.start,
            nodes: raw.nodes,
            edges: raw.edges,
        };
        let errors = graph.validate();
        if errors.is_empty() {
            Ok(graph)
        } else {
            Err(GraphError::Invalid(errors))
        }
    }
}
impl Graph {
    #[must_use]
    pub fn id(&self) -> &GraphId {
        &self.id
    }
    #[must_use]
    pub fn version(&self) -> u32 {
        self.version
    }
    #[must_use]
    pub fn start(&self) -> &NodeId {
        &self.start
    }
    #[must_use]
    pub fn node(&self, id: &NodeId) -> Option<&NodeDef> {
        self.nodes.get(id)
    }
    pub fn nodes(&self) -> impl Iterator<Item = (&NodeId, &NodeDef)> {
        self.nodes.iter()
    }
    pub fn edges_from(&self, node: &NodeId) -> impl Iterator<Item = &Edge> {
        let node = node.clone();
        self.edges.iter().filter(move |edge| edge.from == node)
    }
    /// Parse a graph using the derived JSON shape.
    /// # Errors
    /// Returns JSON syntax errors or typed validation errors.
    pub fn from_json(json: &str) -> Result<Self, GraphError> {
        let raw: RawGraph =
            serde_json::from_str(json).map_err(|e| GraphError::Json(e.to_string()))?;
        Self::try_from(raw)
    }
    /// Serialize a graph for authoring or storage.
    /// # Panics
    /// Panics if serialization of these JSON-compatible types fails.
    #[must_use]
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("graph fields serialize to JSON")
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeDef {
    pub kind: NodeKind,
    #[serde(default)]
    pub retry: RetryPolicy,
    #[serde(default)]
    pub timeout_micros: Option<i64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NodeKind {
    Task,
    FanOut {
        branch: NodeId,
        join: NodeId,
    },
    Branch,
    Join {
        policy: JoinPolicy,
    },
    Wait {
        signal: String,
        deadline_micros: Option<i64>,
    },
    End {
        status: EndStatus,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndStatus {
    Completed,
    Failed,
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Edge {
    pub from: NodeId,
    pub to: NodeId,
    pub guard: Guard,
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Guard {
    Default,
    Label(String),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinPolicy {
    All,
    Quorum(u32),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_backoff_micros: i64,
    pub max_backoff_micros: i64,
    pub multiplier_permille: u32,
}
impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff_micros: 1_000_000,
            max_backoff_micros: 60_000_000,
            multiplier_permille: 2000,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GraphError {
    #[error("invalid graph: {0:?}")]
    Invalid(Vec<Self>),
    #[error("invalid graph JSON: {0}")]
    Json(String),
    #[error("duplicate node: {node}")]
    DuplicateNode { node: NodeId },
    #[error("empty graph")]
    EmptyGraph,
    #[error("missing start: {start}")]
    StartMissing { start: NodeId },
    #[error("invalid start {start}: {kind:?}")]
    StartKind { start: NodeId, kind: NodeKind },
    #[error("edge {from} -> {to} missing {which}")]
    EdgeEndpointMissing {
        from: NodeId,
        to: NodeId,
        which: String,
    },
    #[error("fan-out {fan_out} has invalid {field}: {target}")]
    FanOutTargetKind {
        fan_out: NodeId,
        field: String,
        target: NodeId,
    },
    #[error("branch {branch} has {owners} owners")]
    BranchOwnership { branch: NodeId, owners: usize },
    #[error("join {join} has {owners} owners")]
    JoinOwnership { join: NodeId, owners: usize },
    #[error("explicit edge conflicts with implicit successor: {from} -> {to}")]
    ImplicitSuccessorEdge { from: NodeId, to: NodeId },
    #[error("node {node} has no outgoing edge")]
    NoOutgoingEdge { node: NodeId },
    #[error("multiple defaults on {node}")]
    MultipleDefaultGuards { node: NodeId },
    #[error("duplicate guard {label} on {node}")]
    DuplicateGuard { node: NodeId, label: String },
    #[error("end {node} has edges")]
    EndHasEdges { node: NodeId },
    #[error("wait {node} has no timeout edge")]
    WaitDeadlineWithoutTimeoutEdge { node: NodeId },
    #[error("invalid deadline on {node}")]
    InvalidDeadline { node: NodeId },
    #[error("duplicate signal {signal}: {nodes:?}")]
    DuplicateSignalName { signal: String, nodes: Vec<NodeId> },
    #[error("unreachable node {node}")]
    Unreachable { node: NodeId },
    #[error("invalid identifier {value:?}: {reason}")]
    InvalidIdentifier { value: String, reason: String },
    #[error("invalid retry on {node}: {reason}")]
    InvalidRetryPolicy { node: NodeId, reason: String },
    #[error("reserved label {label} on {node}")]
    ReservedLabel { node: NodeId, label: String },
    #[error("invalid quorum on {node}")]
    InvalidQuorum { node: NodeId },
    #[error("invalid timeout on {node}")]
    InvalidTimeout { node: NodeId },
}

impl RetryPolicy {
    /// Delay after the given failing attempt (one-based), capped by the policy.
    #[must_use]
    pub fn backoff(&self, attempt: u32) -> i64 {
        let mut backoff = self.initial_backoff_micros;
        for _ in 1..attempt.min(1000) {
            if backoff >= self.max_backoff_micros {
                break;
            }
            // Scale before clamping: saturating the product before division can shrink the delay.
            let scaled = i128::from(backoff) * i128::from(self.multiplier_permille) / 1000;
            let capped = scaled.min(i128::from(self.max_backoff_micros));
            backoff = i64::try_from(capped).unwrap_or(i64::MIN);
        }
        backoff.min(self.max_backoff_micros)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn backoff_scales_before_clamping() {
        let constant = RetryPolicy {
            initial_backoff_micros: 10_000_000_000_000_000,
            max_backoff_micros: 20_000_000_000_000_000,
            multiplier_permille: 1000,
            ..RetryPolicy::default()
        };
        for attempt in 1..=1000 {
            assert_eq!(constant.backoff(attempt), constant.initial_backoff_micros);
        }
        let growing = RetryPolicy {
            multiplier_permille: 4000,
            ..constant
        };
        assert_eq!(growing.backoff(2), growing.max_backoff_micros);
        let unvalidated = RetryPolicy {
            initial_backoff_micros: i64::MIN,
            max_backoff_micros: i64::MAX,
            multiplier_permille: u32::MAX,
            ..RetryPolicy::default()
        };
        assert_eq!(unvalidated.backoff(2), i64::MIN);
    }
    #[test]
    fn default_backoff_and_cap() {
        let retry = RetryPolicy::default();
        assert_eq!(
            (1..=8).map(|a| retry.backoff(a)).collect::<Vec<_>>(),
            vec![
                1_000_000, 2_000_000, 4_000_000, 8_000_000, 16_000_000, 32_000_000, 60_000_000,
                60_000_000
            ]
        );
        assert_eq!(retry.backoff(1000), 60_000_000);
    }
}
