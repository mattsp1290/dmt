use dmt_store::StoreError;
use std::sync::atomic::{AtomicBool, Ordering};

/// One-shot fault at a 1-based statement index in a write transaction.
/// BEGIN, COMMIT, and reader-pool statements are excluded. Zero never fires.
/// A short transaction leaves the fault armed; clones share its fired state.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultPoint {
    /// Return a backend error after statement N succeeds.
    AfterStatement(usize),
    /// Remain pending after statement N until the caller drops its future.
    StallAfterStatement(usize),
}
#[derive(Debug)]
pub(crate) struct FaultState {
    point: Option<FaultPoint>,
    fired: AtomicBool,
}
impl FaultState {
    pub(crate) fn new(point: Option<FaultPoint>) -> Self {
        Self {
            point,
            fired: AtomicBool::new(false),
        }
    }
    pub(crate) fn fired(&self) -> bool {
        self.fired.load(Ordering::Acquire)
    }
    pub(crate) async fn after_statement(&self, index: usize) -> Result<(), StoreError> {
        let Some(point) = self.point else {
            return Ok(());
        };
        let target = match point {
            FaultPoint::AfterStatement(n) | FaultPoint::StallAfterStatement(n) => n,
        };
        if target != index || self.fired.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        match point {
            FaultPoint::AfterStatement(_) => Err(StoreError::Backend(format!(
                "injected fault after statement {index}"
            ))),
            FaultPoint::StallAfterStatement(_) => {
                std::future::pending::<()>().await;
                Ok(())
            }
        }
    }
}
