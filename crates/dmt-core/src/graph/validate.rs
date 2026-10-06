use super::{Graph, GraphError, Guard, JoinPolicy, NodeKind};
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
        for (id, node) in &self.nodes {
            identifier(id.as_str(), &mut errors);
            if let NodeKind::Wait { signal, .. } = &node.kind {
                identifier(signal, &mut errors);
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
        // Structural checks are meaningful only after the graph's identifiers and start are usable.
        if self.nodes.is_empty()
            || !self.nodes.contains_key(&self.start)
            || errors
                .iter()
                .any(|e| matches!(e, GraphError::InvalidIdentifier { .. }))
        {
            return errors;
        }
        self.validate_structure(&mut errors);
        errors
    }
    fn validate_structure(&self, errors: &mut Vec<GraphError>) {
        if let Some(node) = self.node(&self.start)
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
        let mut branch_owners: BTreeMap<NodeId, Vec<NodeId>> = BTreeMap::new();
        let mut join_owners: BTreeMap<NodeId, Vec<NodeId>> = BTreeMap::new();
        let mut signals: BTreeMap<String, Vec<NodeId>> = BTreeMap::new();
        for (id, node) in &self.nodes {
            if let NodeKind::FanOut { branch, join } = &node.kind {
                branch_owners
                    .entry(branch.clone())
                    .or_default()
                    .push(id.clone());
                join_owners
                    .entry(join.clone())
                    .or_default()
                    .push(id.clone());
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
        self.validate_reachability(&branch_owners, errors);
    }
    fn validate_edges(&self, errors: &mut Vec<GraphError>) {
        for edge in &self.edges {
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
        branch_owners: &BTreeMap<NodeId, Vec<NodeId>>,
        join_owners: &BTreeMap<NodeId, Vec<NodeId>>,
        errors: &mut Vec<GraphError>,
    ) {
        for (id, node) in &self.nodes {
            let outgoing: Vec<_> = self.edges_from(id).collect();
            match &node.kind {
                NodeKind::Branch => {
                    let owners = branch_owners.get(id).map_or(0, Vec::len);
                    if owners != 1 {
                        errors.push(GraphError::BranchOwnership {
                            branch: id.clone(),
                            owners,
                        });
                    }
                }
                NodeKind::Join { .. } => {
                    let owners = join_owners.get(id).map_or(0, Vec::len);
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
    fn validate_reachability(
        &self,
        branch_owners: &BTreeMap<NodeId, Vec<NodeId>>,
        errors: &mut Vec<GraphError>,
    ) {
        let mut seen = BTreeSet::new();
        let mut pending = vec![self.start.clone()];
        while let Some(id) = pending.pop() {
            if !seen.insert(id.clone()) {
                continue;
            }
            pending.extend(self.edges_from(&id).map(|e| e.to.clone()));
            if let Some(node) = self.node(&id) {
                match &node.kind {
                    NodeKind::FanOut { branch, .. } => pending.push(branch.clone()),
                    NodeKind::Branch => {
                        if let Some(owners) = branch_owners.get(&id) {
                            for owner in owners {
                                if let Some(super::NodeDef {
                                    kind: NodeKind::FanOut { join, .. },
                                    ..
                                }) = self.node(owner)
                                {
                                    pending.push(join.clone());
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        for node in self.nodes.keys() {
            if !seen.contains(node) {
                errors.push(GraphError::Unreachable { node: node.clone() });
            }
        }
    }
}
