use crate::shared::Shared;
pub(crate) async fn abort(shared: &Shared) {
    shared.kill.cancel();
    shared.tracker.close();
    shared.tracker.wait().await;
}
