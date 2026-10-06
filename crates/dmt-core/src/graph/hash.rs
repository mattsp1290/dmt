use super::Graph;
use sha2::{Digest, Sha256};
impl Graph {
    /// Hash the compact derived JSON with canonically sorted edges.
    /// # Panics
    /// Panics if serialization of these JSON-compatible types fails.
    #[must_use]
    pub fn definition_hash(&self) -> String {
        let mut canonical = self.clone();
        canonical.edges.sort();
        format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&canonical).expect("graph fields serialize to JSON"))
        )
    }
}
