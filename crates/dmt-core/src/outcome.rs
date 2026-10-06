use serde::{Deserialize, Serialize};
use serde_json::Value;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Outcome {
    pub label: String,
    pub payload: Value,
}
impl Outcome {
    #[must_use]
    pub fn done(label: impl Into<String>) -> Self {
        Self::with_payload(label, Value::Null)
    }
    #[must_use]
    pub fn with_payload(label: impl Into<String>, payload: Value) -> Self {
        Self {
            label: label.into(),
            payload,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeOutcome {
    Done(Outcome),
    FanOut(Vec<Value>),
    Fail { message: String, retryable: bool },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalPayload {
    pub label: String,
    pub payload: Value,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BranchResult {
    Done { index: u32, outcome: Outcome },
    Failed { index: u32, message: String },
}
impl BranchResult {
    #[must_use]
    pub fn index(&self) -> u32 {
        match self {
            Self::Done { index, .. } | Self::Failed { index, .. } => *index,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinInput {
    pub results: Vec<BranchResult>,
    pub expected: u32,
    pub quorum_met: bool,
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn join_round_trip() {
        let input = JoinInput {
            results: vec![
                BranchResult::Done {
                    index: 0,
                    outcome: Outcome::done("ok"),
                },
                BranchResult::Failed {
                    index: 1,
                    message: "error".into(),
                },
            ],
            expected: 2,
            quorum_met: false,
        };
        assert_eq!(input.results[0].index(), 0);
        assert_eq!(input.results[1].index(), 1);
        assert_eq!(
            serde_json::from_value::<JoinInput>(serde_json::to_value(&input).unwrap()).unwrap(),
            input
        );
    }
}
