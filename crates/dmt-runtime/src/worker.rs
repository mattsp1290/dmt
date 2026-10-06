use crate::{dispatch, shared::Shared};
use dmt_store::ClaimRequest;
use std::sync::Arc;
use tokio::{sync::Semaphore, time::sleep};

pub(crate) async fn run(shared: Arc<Shared>) {
    let slots = Arc::new(Semaphore::new(shared.config.claim_limit));
    loop {
        if shared.stop.is_cancelled() {
            break;
        }
        let notified = shared.wake.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let free = slots.available_permits();
        let tasks = if free == 0 {
            vec![]
        } else {
            let request = ClaimRequest {
                worker_id: shared.config.worker_id.clone(),
                now: shared.clock.now(),
                lease_micros: shared.config.lease_micros(),
                limit: free,
                graphs: shared.catalog.registered_ids(),
            };
            match shared.store.claim_ready(request).await {
                Ok(tasks) => tasks,
                Err(error) => {
                    tracing::warn!(%error, "claim_ready failed");
                    vec![]
                }
            }
        };
        let progressed = !tasks.is_empty();
        for claimed in tasks {
            let permit = slots
                .clone()
                .try_acquire_owned()
                .expect("store honors claim limit; worker owns slots");
            let (cancel, guard) = shared.register(&claimed);
            let state = shared.clone();
            shared.spawn(async move {
                dispatch::run(state.clone(), claimed, cancel, guard).await;
                drop(permit);
                state.wake.notify_waiters();
            });
        }
        if progressed {
            continue;
        }
        tokio::select! {
            () = shared.stop.cancelled() => break,
            () = notified => {},
            () = sleep(shared.config.poll_interval) => {},
        }
    }
}
