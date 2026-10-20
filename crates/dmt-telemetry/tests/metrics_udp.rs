mod common;
use dmt_telemetry::{TelemetryConfig, init};
use std::{net::UdpSocket, time::Duration};
#[test]
fn shutdown_delivers_pending_metrics() {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let config = TelemetryConfig {
        traces: false,
        metrics: true,
        dogstatsd_addr: socket.local_addr().unwrap().to_string(),
        metrics_flush_interval: Duration::from_millis(200),
        ..common::config()
    };
    let telemetry = init(config).unwrap();
    metrics::counter!("dmt.test.counter", "k" => "v").increment(3);
    telemetry.shutdown(Duration::from_secs(1)).unwrap();
    let started = std::time::Instant::now();
    let mut buffer = vec![0; 65536];
    loop {
        let n = socket
            .recv(&mut buffer)
            .expect("pending counter forwarded before shutdown returns");
        let packet = std::str::from_utf8(&buffer[..n]).unwrap();
        if packet.contains("dmt.test.counter:3|c|#") && packet.contains("service:dmt") {
            break;
        }
        assert!(started.elapsed() < Duration::from_secs(2));
    }
}

#[test]
fn directly_configured_zero_interval_is_normalized() {
    if common::isolated_datadog_test("directly_configured_zero_interval_is_normalized") {
        return;
    }
    assert_direct_interval(Duration::ZERO);
}

#[test]
fn directly_configured_subminimum_interval_is_normalized() {
    if common::isolated_datadog_test("directly_configured_subminimum_interval_is_normalized") {
        return;
    }
    assert_direct_interval(Duration::from_millis(1));
}

fn assert_direct_interval(requested: Duration) {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let config = TelemetryConfig {
        traces: false,
        metrics: true,
        dogstatsd_addr: socket.local_addr().unwrap().to_string(),
        metrics_flush_interval: requested,
        ..common::config()
    };
    let interval = dmt_telemetry::Builder::install_metrics(&config)
        .unwrap()
        .unwrap();
    assert_eq!(interval, Duration::from_millis(100));
    metrics::counter!("dmt.test.direct_interval").increment(1);
    std::thread::sleep(interval.saturating_mul(2) + Duration::from_millis(50));
    let mut buffer = vec![0; 65536];
    let n = socket
        .recv(&mut buffer)
        .expect("normalized recorder forwards to its test socket");
    let packet = std::str::from_utf8(&buffer[..n]).unwrap();
    assert!(packet.contains("dmt.test.direct_interval:1|c"));
}
