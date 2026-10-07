use crate::shared::Shared;
pub(crate) async fn abort(shared: &Shared) {
    shared.kill.cancel();
    shared.tracker.close();
    shared.tracker.wait().await;
}
pub(crate) async fn graceful(
    shared: &Shared,
    drain: std::time::Duration,
) -> Result<(), crate::EngineError> {
    shared.stop.cancel();
    shared.tracker.close();
    if tokio::time::timeout(drain, shared.tracker.wait())
        .await
        .is_ok()
    {
        return Ok(());
    }
    shared.handler_cancel.cancel();
    if tokio::time::timeout(shared.config.cancel_grace, shared.tracker.wait())
        .await
        .is_ok()
    {
        return Ok(());
    }
    let abandoned = shared
        .in_flight
        .lock()
        .expect("in-flight lock poisoned")
        .len();
    abort(shared).await;
    Err(crate::EngineError::ShutdownTimedOut { abandoned })
}
