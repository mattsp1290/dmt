mod common;
#[tokio::test]
async fn quick_start_completes_on_memory_store() {
    common::quick_start(std::sync::Arc::new(dmt::MemoryStore::new())).await;
}
#[test]
fn module_paths_resolve() {
    #[cfg(feature = "telemetry")]
    {
        let _: Option<dmt::telemetry::TelemetryConfig> = None;
        let _ = dmt::runtime::telemetry::run_trace_id;
    }
    let _ = dmt::core::plan_start;
    let _: Option<dmt::core::Commit> = None;
    let _: Option<dmt::store::LeaseProof> = None;
    let _: Option<dmt::runtime::EngineConfig> = None;
    let _: Option<&dyn dmt::Store> = None;
    #[cfg(feature = "test-faults")]
    let _ = dmt::runtime::FaultPoint::BeforeApply {
        node_id: "work".into(),
        attempt: 1,
    };
}
