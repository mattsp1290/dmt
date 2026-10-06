use crate::{Edge, Graph, Guard, NodeId};
pub(super) fn select_edge<'a>(graph: &'a Graph, from: &NodeId, label: &str) -> Option<&'a Edge> {
    // Collect the source's edges without tying the returned reference to `from`.
    graph
        .nodes()
        .find(|(id, _)| *id == from)
        .and_then(|(id, _)| {
            graph
                .edges_from(id)
                .find(|e| matches!(&e.guard, Guard::Label(value) if value == label))
                .or_else(|| graph.edges_from(id).find(|e| e.guard == Guard::Default))
        })
}
pub(super) fn select_failed_edge<'a>(graph: &'a Graph, from: &NodeId) -> Option<&'a Edge> {
    graph
        .nodes()
        .find(|(id, _)| *id == from)
        .and_then(|(id, _)| {
            graph
                .edges_from(id)
                .find(|e| e.guard == Guard::Label("failed".into()))
        })
}
