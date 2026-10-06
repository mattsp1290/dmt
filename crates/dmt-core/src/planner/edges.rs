use crate::{Edge, Graph, Guard, NodeId};
pub(super) fn select_edge<'a>(graph: &'a Graph, from: &NodeId, label: &str) -> Option<&'a Edge> {
    graph
        .edges_from(from)
        .find(|e| matches!(&e.guard, Guard::Label(value) if value == label))
        .or_else(|| graph.edges_from(from).find(|e| e.guard == Guard::Default))
}
pub(super) fn select_failed_edge<'a>(graph: &'a Graph, from: &NodeId) -> Option<&'a Edge> {
    graph
        .edges_from(from)
        .find(|e| matches!(&e.guard, Guard::Label(label) if label == "failed"))
}
