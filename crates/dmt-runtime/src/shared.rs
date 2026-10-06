use crate::{CancellationToken, EngineConfig, HandlerRegistry, catalog::GraphCatalog};
use dmt_core::{RunId, TaskId};
use dmt_store::{ClaimedTask, Clock, Store};
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex},
};
use tokio::{sync::Notify, task::JoinHandle};
use tokio_util::task::TaskTracker;

pub(crate) struct Shared {
    pub store: Arc<dyn Store>,
    pub clock: Arc<dyn Clock>,
    pub config: EngineConfig,
    pub catalog: GraphCatalog,
    pub handlers: HandlerRegistry,
    pub wake: Notify,
    pub stop: CancellationToken,
    pub handler_cancel: CancellationToken,
    pub kill: CancellationToken,
    pub tracker: TaskTracker,
    pub in_flight: Mutex<BTreeMap<(TaskId, u32), InFlight>>,
}
pub(crate) struct InFlight {
    pub run_id: RunId,
    pub cancel: CancellationToken,
}
impl Shared {
    pub fn spawn<F>(&self, future: F) -> JoinHandle<Option<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.tracker
            .spawn(self.kill.clone().run_until_cancelled_owned(future))
    }
    pub fn register(self: &Arc<Self>, claimed: &ClaimedTask) -> (CancellationToken, InFlightGuard) {
        let cancel = self.handler_cancel.child_token();
        let key = (claimed.task.task_id.clone(), claimed.task.attempt);
        self.in_flight
            .lock()
            .expect("in-flight lock poisoned")
            .insert(
                key.clone(),
                InFlight {
                    run_id: claimed.task.run_id.clone(),
                    cancel: cancel.clone(),
                },
            );
        (
            cancel,
            InFlightGuard {
                shared: self.clone(),
                key,
            },
        )
    }
}
pub(crate) struct InFlightGuard {
    shared: Arc<Shared>,
    key: (TaskId, u32),
}
impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.shared
            .in_flight
            .lock()
            .expect("in-flight lock poisoned")
            .remove(&self.key);
        self.shared.wake.notify_waiters();
    }
}
