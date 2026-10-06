//! Pure graph model, lifecycle, and commit planner for dmt, without I/O.
pub mod event;
pub mod lifecycle;
pub use event::RunEvent;
pub use lifecycle::{
    Effects, LifecycleError, LifecycleEvent, LifecycleMachine, RejectedTransition, RunStatus,
    new_machine, restore,
};
