use dmt_core::NodeId;
/// Test-only process crash before applying a matching dispatched outcome.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultPoint {
    /// Abort before apply for this node and one-based attempt.
    BeforeApply { node_id: NodeId, attempt: u32 },
}
pub(crate) fn before_apply(fault: Option<&FaultPoint>, node: &NodeId, attempt: u32) {
    if matches!(fault, Some(FaultPoint::BeforeApply { node_id, attempt: wanted }) if node_id == node && *wanted == attempt)
    {
        std::process::abort();
    }
}
