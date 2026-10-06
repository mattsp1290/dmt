use serde::{Deserialize, Serialize};
macro_rules! id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);
        impl $name {
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.into())
            }
        }
        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }
        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}
id!(GraphId);
id!(NodeId);
id!(RunId);
id!(TaskId);
id!(JoinId);
id!(SignalId);
id!(WorkerId);
macro_rules! minted {
    ($name:ident) => {
        impl $name {
            #[must_use]
            pub fn new() -> Self {
                Self(uuid::Uuid::new_v4().to_string())
            }
        }
        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
    };
}
minted!(RunId);
minted!(WorkerId);
impl RunId {
    #[must_use]
    pub fn is_valid(&self) -> bool {
        !self.0.is_empty() && !self.0.contains('/')
    }
}
impl SignalId {
    #[must_use]
    pub fn for_run(run: &RunId, key: &SignalKey) -> Self {
        Self(format!("{run}/{key}"))
    }
}
/// Globally unique task or join key, including the run identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StepKey(String);
impl StepKey {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
    #[must_use]
    pub fn task(run: &RunId, node: &NodeId, occurrence: u32) -> Self {
        Self(format!("{run}/{node}/{occurrence}"))
    }
    #[must_use]
    pub fn branch(run: &RunId, node: &NodeId, occurrence: u32, index: u32) -> Self {
        Self(format!("{run}/{node}/{occurrence}/{index}"))
    }
}
impl std::fmt::Display for StepKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
/// Signal key unique within a run; stores index it together with the run id.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SignalKey(String);
impl SignalKey {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
    #[must_use]
    pub fn new(signal: &str, occurrence: u32) -> Self {
        Self(format!("{signal}/{occurrence}"))
    }
}
impl std::fmt::Display for SignalKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn key_formats_and_ids() {
        let run = RunId::from("run");
        let node = NodeId::from("task");
        assert_eq!(StepKey::task(&run, &node, 2).as_str(), "run/task/2");
        assert_eq!(
            StepKey::branch(&run, &node, 2, 3).to_string(),
            "run/task/2/3"
        );
        let signal = SignalKey::new("approve", 2);
        assert_eq!(signal.as_str(), "approve/2");
        assert_eq!(
            SignalId::for_run(&run, &signal).to_string(),
            "run/approve/2"
        );
        assert_eq!(NodeId::from(node.as_str()), node);
        assert!(run.is_valid());
        assert!(!RunId::from("bad/id").is_valid());
    }
}
