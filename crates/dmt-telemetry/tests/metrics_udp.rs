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
