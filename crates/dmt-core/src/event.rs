use crate::GraphId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
/// Durable lifecycle events emitted by the run machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RunEvent {
    RunStarted {
        graph_id: GraphId,
        graph_version: u32,
        definition_hash: String,
        input: Value,
    },
    RunParked,
    RunResumed,
    RunCompleted {
        output: Value,
    },
    RunFailed {
        message: String,
    },
    RunCancelled {
        reason: String,
    },
}
