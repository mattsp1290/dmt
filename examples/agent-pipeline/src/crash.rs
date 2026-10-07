//! Explicit process-crash switches used only by the example and its tests.
use crate::graph;
use dmt::{NodeContext, NodeId, NodeInput};
pub const ENV: &str = "AGENT_PIPELINE_CRASH_AT";
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CrashAt {
    InHandler { node: NodeId, branch: Option<u32> },
    BeforeApply { node: NodeId },
}
/// Parse a node, node:index, or node:after switch.
/// # Errors
/// Returns a usage message for unknown nodes or malformed suffixes.
pub fn parse(value: &str) -> Result<CrashAt, String> {
    let (node, suffix) = value
        .split_once(':')
        .map_or((value, None), |(n, s)| (n, Some(s)));
    if !graph::executable_nodes().contains(&node) {
        return Err(format!("unknown crash node {node}"));
    }
    match suffix {
        Some("after") => Ok(CrashAt::BeforeApply { node: node.into() }),
        Some(index) => Ok(CrashAt::InHandler {
            node: node.into(),
            branch: Some(
                index
                    .parse()
                    .map_err(|_| format!("invalid crash branch {index}"))?,
            ),
        }),
        None => Ok(CrashAt::InHandler {
            node: node.into(),
            branch: None,
        }),
    }
}
/// Read the crash switch once at startup.
/// # Errors
/// Returns a usage message for a non-Unicode or malformed value.
pub fn from_env() -> Result<Option<CrashAt>, String> {
    match std::env::var(ENV) {
        Ok(value) if value.is_empty() => Ok(None),
        Ok(value) => parse(&value).map(Some),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(e) => Err(e.to_string()),
    }
}
impl CrashAt {
    #[must_use]
    pub fn matches(&self, ctx: &NodeContext, input: &NodeInput) -> bool {
        match self {
            Self::InHandler { node, branch } if node == &ctx.node_id => {
                branch.is_none()
                    || matches!(input, NodeInput::Branch { index, .. } if Some(*index) == *branch)
            }
            _ => false,
        }
    }
    #[must_use]
    pub fn fault_point(&self) -> Option<dmt::runtime::FaultPoint> {
        match self {
            Self::BeforeApply { node } => Some(dmt::runtime::FaultPoint::BeforeApply {
                node_id: node.clone(),
                attempt: 1,
            }),
            Self::InHandler { .. } => None,
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parse_forms_and_errors() {
        assert_eq!(
            parse("plan").unwrap(),
            CrashAt::InHandler {
                node: "plan".into(),
                branch: None
            }
        );
        assert_eq!(
            parse("review:1").unwrap(),
            CrashAt::InHandler {
                node: "review".into(),
                branch: Some(1)
            }
        );
        assert_eq!(
            parse("open-pr:after").unwrap(),
            CrashAt::BeforeApply {
                node: "open-pr".into()
            }
        );
        for value in ["", "nope", "review:bad", "review:", "review:1:2"] {
            assert!(parse(value).is_err(), "{value}");
        }
    }
}
