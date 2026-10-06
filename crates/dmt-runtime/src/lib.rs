//! Backend-neutral asynchronous execution of dmt graphs.

mod backoff;
mod catalog;
mod config;
mod engine;
mod error;
mod handler;
pub use config::EngineConfig;
pub use engine::{Engine, EngineBuilder};
pub use error::{BuildError, EngineError};
pub use handler::{HandlerError, HandlerRegistry, NodeContext, NodeHandler, NodeInput};
pub use tokio_util::sync::CancellationToken;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    fn assert_send_sync<T: Send + Sync>() {}
    #[test]
    fn public_types_are_send_sync() {
        assert_send_sync::<Arc<dyn NodeHandler>>();
        assert_send_sync::<HandlerRegistry>();
        assert_send_sync::<EngineConfig>();
        assert_send_sync::<Engine>();
    }
}

mod commit_loop;
mod dispatch;
mod handle;
mod shared;
mod shutdown;
mod worker;
pub use handle::{EngineHandle, Quiescent};

pub(crate) type NodeOutcomeResult = Result<dmt_core::NodeOutcome, HandlerError>;
