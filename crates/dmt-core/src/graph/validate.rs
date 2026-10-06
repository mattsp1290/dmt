use super::{Edge, Graph, GraphError, Guard, JoinPolicy, NodeKind};
use crate::NodeId;
use std::collections::{BTreeMap, BTreeSet};
fn valid(value: &str) -> bool {
    !value.is_empty() && !value.contains('/')
}
fn identifier(value: &str, errors: &mut Vec<GraphError>) {
    if !valid(value) {
        errors.push(GraphError::InvalidIdentifier {
            value: value.into(),
            reason: "must be non-empty and contain no slash".into(),
        });
    }
}
impl Graph {
    /// Collect every validation error; cycles are permitted.
    #[must_use]
    pub fn validate(&self) -> Vec<GraphError> {
        let mut errors = Vec::new();
        if self.nodes.is_empty() {
            errors.push(GraphError::EmptyGraph);
        }
        if !self.nodes.contains_key(&self.start) {
            errors.push(GraphError::StartMissing {
                start: self.start.clone(),
            });
        }
        identifier(self.start.as_str(), &mut errors);
        for (id, node) in &self.nodes {
            identifier(id.as_str(), &mut errors);
            if let NodeKind::Wait { signal, .. } = &node.kind {
                identifier(signal, &mut errors);
            }
            if let NodeKind::FanOut { branch, join } = &node.kind {
                identifier(branch.as_str(), &mut errors);
                identifier(join.as_str(), &mut errors);
            }
            let retry = &node.retry;
            if !(1..=1000).contains(&retry.max_attempts)
                || retry.initial_backoff_micros < 0
                || retry.max_backoff_micros < retry.initial_backoff_micros
                || retry.multiplier_permille < 1000
            {
                errors.push(GraphError::InvalidRetryPolicy {node: id.clone(), reason: "attempts must be 1..=1000; backoff must be nonnegative, capped above its initial value, and multiplier at least 1000".into()});
            }
            if node.timeout_micros.is_some_and(|t| t <= 0) {
                errors.push(GraphError::InvalidTimeout { node: id.clone() });
            }
            if matches!(
                node.kind,
                NodeKind::Join {
                    policy: JoinPolicy::Quorum(0)
                }
            ) {
                errors.push(GraphError::InvalidQuorum { node: id.clone() });
            }
            if let NodeKind::Wait {
                deadline_micros: Some(d),
                ..
            } = &node.kind
                && *d <= 0
            {
                errors.push(GraphError::InvalidDeadline { node: id.clone() });
            }
        }
        for edge in &self.edges {
            identifier(edge.from.as_str(), &mut errors);
            identifier(edge.to.as_str(), &mut errors);
            if let Guard::Label(label) = &edge.guard {
                identifier(label, &mut errors);
                if label == "cancelled" {
                    errors.push(GraphError::ReservedLabel {
                        node: edge.from.clone(),
                        label: label.clone(),
                    });
                }
            }
        }
        // Missing starts and empty graphs cannot support structural analysis.
        if !self.nodes.is_empty() && self.nodes.contains_key(&self.start) {
            self.validate_structure(&mut errors);
        }
        errors
    }
    fn usable_nodes(&self) -> impl Iterator<Item = (&NodeId, &super::NodeDef)> {
        self.nodes.iter().filter(|(id, node)| {
            valid(id.as_str())
                && match &node.kind {
                    NodeKind::Wait { signal, .. } => valid(signal),
                    NodeKind::FanOut { branch, join } => {
                        valid(branch.as_str()) && valid(join.as_str())
                    }
                    _ => true,
                }
        })
    }
    fn usable_edges(&self) -> impl Iterator<Item = &Edge> {
        self.edges.iter().filter(|edge| {
            valid(edge.from.as_str())
                && valid(edge.to.as_str())
                && match &edge.guard {
                    Guard::Default => true,
                    Guard::Label(label) => valid(label),
                }
        })
    }
    fn validate_structure(&self, errors: &mut Vec<GraphError>) {
        if let Some((_, node)) = self.usable_nodes().find(|(id, _)| *id == &self.start)
            && !matches!(
                node.kind,
                NodeKind::Task | NodeKind::FanOut { .. } | NodeKind::Wait { .. }
            )
        {
            errors.push(GraphError::StartKind {
                start: self.start.clone(),
                kind: node.kind.clone(),
            });
        }
        let mut branch_owners: BTreeMap<NodeId, usize> = BTreeMap::new();
        let mut join_owners: BTreeMap<NodeId, usize> = BTreeMap::new();
        let mut signals: BTreeMap<String, Vec<NodeId>> = BTreeMap::new();
        for (id, node) in self.usable_nodes() {
            if let NodeKind::FanOut { branch, join } = &node.kind {
                *branch_owners.entry(branch.clone()).or_default() += 1;
                *join_owners.entry(join.clone()).or_default() += 1;
                for (field, target, matches_kind) in [
                    (
                        "branch",
                        branch,
                        self.node(branch)
                            .is_some_and(|n| matches!(n.kind, NodeKind::Branch)),
                    ),
                    (
                        "join",
                        join,
                        self.node(join)
                            .is_some_and(|n| matches!(n.kind, NodeKind::Join { .. })),
                    ),
                ] {
                    if !matches_kind {
                        errors.push(GraphError::FanOutTargetKind {
                            fan_out: id.clone(),
                            field: field.into(),
                            target: target.clone(),
                        });
                    }
                }
            }
            if let NodeKind::Wait { signal, .. } = &node.kind {
                signals.entry(signal.clone()).or_default().push(id.clone());
            }
        }
        for (signal, nodes) in signals {
            if nodes.len() > 1 {
                errors.push(GraphError::DuplicateSignalName { signal, nodes });
            }
        }
        self.validate_edges(errors);
        self.validate_nodes(&branch_owners, &join_owners, errors);
        // Reachability depends on the complete topology; malformed identifiers make it unknowable.
        if !errors
            .iter()
            .any(|e| matches!(e, GraphError::InvalidIdentifier { .. }))
        {
            self.validate_reachability(errors);
        }
    }
    fn validate_edges(&self, errors: &mut Vec<GraphError>) {
        for edge in self.usable_edges() {
            for (which, id) in [("from", &edge.from), ("to", &edge.to)] {
                if !self.nodes.contains_key(id) {
                    errors.push(GraphError::EdgeEndpointMissing {
                        from: edge.from.clone(),
                        to: edge.to.clone(),
                        which: which.into(),
                    });
                }
            }
            if self
                .node(&edge.from)
                .is_some_and(|n| matches!(n.kind, NodeKind::FanOut { .. } | NodeKind::Branch))
                || self
                    .node(&edge.to)
                    .is_some_and(|n| matches!(n.kind, NodeKind::Branch | NodeKind::Join { .. }))
            {
                errors.push(GraphError::ImplicitSuccessorEdge {
                    from: edge.from.clone(),
                    to: edge.to.clone(),
                });
            }
        }
    }
    fn validate_nodes(
        &self,
        branch_owners: &BTreeMap<NodeId, usize>,
        join_owners: &BTreeMap<NodeId, usize>,
        errors: &mut Vec<GraphError>,
    ) {
        for (id, node) in self.usable_nodes() {
            if self.edges_from(id).any(|edge| {
                !valid(edge.to.as_str())
                    || matches!(&edge.guard, Guard::Label(label) if !valid(label))
            }) {
                continue;
            }
            let outgoing: Vec<_> = self.edges_from(id).collect();
            match &node.kind {
                NodeKind::Branch => {
                    let owners = branch_owners.get(id).copied().unwrap_or(0);
                    if owners != 1 {
                        errors.push(GraphError::BranchOwnership {
                            branch: id.clone(),
                            owners,
                        });
                    }
                }
                NodeKind::Join { .. } => {
                    let owners = join_owners.get(id).copied().unwrap_or(0);
                    if owners != 1 {
                        errors.push(GraphError::JoinOwnership {
                            join: id.clone(),
                            owners,
                        });
                    }
                }
                NodeKind::End { .. } if !outgoing.is_empty() => {
                    errors.push(GraphError::EndHasEdges { node: id.clone() });
                }
                _ => {}
            }
            if matches!(
                node.kind,
                NodeKind::Task | NodeKind::Join { .. } | NodeKind::Wait { .. }
            ) && outgoing.is_empty()
            {
                errors.push(GraphError::NoOutgoingEdge { node: id.clone() });
            }
            if matches!(
                node.kind,
                NodeKind::Wait {
                    deadline_micros: Some(_),
                    ..
                }
            ) && !outgoing
                .iter()
                .any(|e| e.guard == Guard::Label("timeout".into()))
            {
                errors.push(GraphError::WaitDeadlineWithoutTimeoutEdge { node: id.clone() });
            }
            if outgoing
                .iter()
                .filter(|e| e.guard == Guard::Default)
                .count()
                > 1
            {
                errors.push(GraphError::MultipleDefaultGuards { node: id.clone() });
            }
            let mut labels = BTreeSet::new();
            for edge in outgoing {
                if let Guard::Label(label) = &edge.guard
                    && !labels.insert(label)
                {
                    errors.push(GraphError::DuplicateGuard {
                        node: id.clone(),
                        label: label.clone(),
                    });
                }
            }
        }
    }
    fn validate_reachability(&self, errors: &mut Vec<GraphError>) {
        let mut adjacency: BTreeMap<&NodeId, Vec<&NodeId>> = BTreeMap::new();
        for edge in &self.edges {
            adjacency.entry(&edge.from).or_default().push(&edge.to);
        }
        for (id, node) in &self.nodes {
            if let NodeKind::FanOut { branch, join } = &node.kind {
                adjacency.entry(id).or_default().push(branch);
                adjacency.entry(branch).or_default().push(join);
            }
        }
        let mut seen = BTreeSet::new();
        let mut pending = vec![&self.start];
        while let Some(id) = pending.pop() {
            if seen.insert(id)
                && let Some(successors) = adjacency.get(id)
            {
                pending.extend(successors);
            }
        }
        for node in self.nodes.keys() {
            if !seen.contains(node) {
                errors.push(GraphError::Unreachable { node: node.clone() });
            }
        }
    }
}
