//! Integration tests for the `[options_response]` configuration: the OPTIONS
//! keep-alive ACL (static allow / auto-learned call-traffic peers) and the
//! extra headers appended to 200 OK responses.
//!
//! Strict mode: a probe is silently dropped until its source has carried call
//! traffic (inbound INVITE) or matches the static ACL.

use active_call::{
    app::{AppState, AppStateBuilder},
    config::{Config, OptionsResponseConfig},
};
use std::time::Duration;
use tokio::net::{TcpListener, UdpSocket};
use tokio::time::timeout;
use tracing::info;

struct SipNode {
    sip_port: u16,
}

async fn spawn_node(mutate_config: impl FnOnce(&mut Config)) -> SipNode {
    // Reserve an ephemeral UDP port for the SIP transport.
    let probe_socket = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    let sip_port = probe_socket.local_addr().unwrap().port();
    drop(probe_socket);

    // Reserve an HTTP port so the router can be served (not used by the tests
    // themselves but keeps the node close to production shape).
    let http_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let http_port = http_listener.local_addr().unwrap().port();

    let mut config = Config {
        addr: "127.0.0.1".to_string(),
        udp_port: sip_port,
        http_addr: format!("127.0.0.1:{http_port}"),
        log_level: Some("info".to_string()),
        useragent: Some("ActiveCallTest".to_string()),
        media_cache_path: "./target/tmp_media".to_string(),
        ..Default::default()
    };
    mutate_config(&mut config);

    let app: AppState = AppStateBuilder::new()
        .with_config(config)
        .build()
        .await
        .expect("failed to build app state");

    let sip_app = app.clone();
    tokio::spawn(async move {
        let _ = sip_app.serve().await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    SipNode { sip_port }
}

/// Minimal raw-SIP client that sends OPTIONS probes and INVITEs.
struct Probe {
    socket: UdpSocket,
    server: std::net::SocketAddr,
}

impl Probe {
    async fn new(server: std::net::SocketAddr) -> Self {
        let socket = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        Self { socket, server }
    }

    fn options(&self, call_id: &str) -> String {
        format!(
            "OPTIONS sip:bot@{server} SIP/2.0\r\n\
             Via: SIP/2.0/UDP 127.0.0.1:{port};branch=z9hG4bKopt{call_id};rport\r\n\
             From: <sip:prober@127.0.0.1>;tag=probetag{call_id}\r\n\
             To: <sip:bot@{server}>\r\n\
             Call-ID: options-{call_id}@127.0.0.1\r\n\
             CSeq: 1 OPTIONS\r\n\
             Max-Forwards: 70\r\n\
             Content-Length: 0\r\n\
             \r\n",
            server = self.server,
            port = self.socket.local_addr().unwrap().port(),
        )
    }

    fn invite(&self, call_id: &str) -> String {
        let sdp = "v=0\r\n\
                   o=- 123456 1 IN IP4 127.0.0.1\r\n\
                   s=-\r\n\
                   c=IN IP4 127.0.0.1\r\n\
                   t=0 0\r\n\
                   m=audio 40000 RTP/AVP 0 101\r\n\
                   a=rtpmap:0 PCMU/8000\r\n\
                   a=rtpmap:101 telephone-event/8000\r\n\
                   a=sendrecv\r\n";
        format!(
            "INVITE sip:bot@{server} SIP/2.0\r\n\
             Via: SIP/2.0/UDP 127.0.0.1:{port};branch=z9hG4bKinv{call_id};rport\r\n\
             From: <sip:caller@127.0.0.1>;tag=invttag{call_id}\r\n\
             To: <sip:bot@{server}>\r\n\
             Call-ID: invite-{call_id}@127.0.0.1\r\n\
             CSeq: 1 INVITE\r\n\
             Contact: <sip:caller@127.0.0.1:{port}>\r\n\
             Max-Forwards: 70\r\n\
             Content-Type: application/sdp\r\n\
             Content-Length: {len}\r\n\
             \r\n\
             {sdp}",
            server = self.server,
            port = self.socket.local_addr().unwrap().port(),
            len = sdp.len(),
        )
    }

    async fn send(&self, msg: String) {
        self.socket
            .send_to(msg.as_bytes(), self.server)
            .await
            .unwrap();
    }

    /// Collects responses for up to `wait` and returns every message received.
    async fn collect(&self, wait: Duration) -> Vec<String> {
        let mut messages = Vec::new();
        let deadline = tokio::time::Instant::now() + wait;
        while let Some(remaining) = deadline.checked_duration_since(tokio::time::Instant::now()) {
            let mut buf = [0u8; 4096];
            match timeout(remaining, self.socket.recv_from(&mut buf)).await {
                Ok(Ok((n, _))) => {
                    messages.push(String::from_utf8_lossy(&buf[..n]).to_string());
                }
                _ => break,
            }
        }
        messages
    }

    /// Whether any received message carries a 200 OK status line.
    async fn received_ok(&self, wait: Duration) -> Option<String> {
        for msg in self.collect(wait).await {
            if msg.starts_with("SIP/2.0 200") {
                return Some(msg);
            }
            info!(status = %msg.lines().next().unwrap_or(""), "probe received non-200 response");
        }
        None
    }
}

/// With auto-learn and no static ACL, a probe from a source that never carried
/// call traffic must be dropped; after one inbound INVITE it must be answered.
#[tokio::test]
async fn options_auto_learn_answers_after_call_traffic() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init()
        .ok();

    let node = spawn_node(|config| {
        config.options_response = Some(OptionsResponseConfig {
            enabled: Some(true),
            auto_learn: Some(true),
            allow: vec![],
            allow_registered_servers: Some(false),
            ..Default::default()
        });
    })
    .await;
    let probe = Probe::new(format!("127.0.0.1:{}", node.sip_port).parse().unwrap()).await;

    // Strict mode: no call traffic learned yet -> the probe is dropped.
    probe.send(probe.options("before-call")).await;
    assert!(
        probe.received_ok(Duration::from_secs(1)).await.is_none(),
        "OPTIONS must be dropped before any call traffic is learned"
    );

    // One inbound INVITE teaches the peer address (the node has no invite
    // handler and rejects the call, but the transport-level learning already
    // happened — that is exactly what keeps the ACL fresh).
    probe.send(probe.invite("learn-1")).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    probe.send(probe.options("after-call")).await;
    let msg = probe
        .received_ok(Duration::from_secs(3))
        .await
        .expect("OPTIONS must be answered after call traffic was learned");
    assert!(msg.contains("Allow:"), "200 OK must carry Allow: {msg}");
}

/// A static ACL entry answers probes immediately and `extra_headers` are
/// appended to the 200 OK.
#[tokio::test]
async fn options_static_allow_and_extra_headers() {
    let node = spawn_node(|config| {
        config.options_response = Some(OptionsResponseConfig {
            enabled: Some(true),
            auto_learn: Some(false),
            allow: vec!["127.0.0.1".to_string()],
            allow_registered_servers: Some(false),
            extra_headers: vec![("X-Node-Id".to_string(), "test-node".to_string())],
            ..Default::default()
        });
    })
    .await;
    let probe = Probe::new(format!("127.0.0.1:{}", node.sip_port).parse().unwrap()).await;

    probe.send(probe.options("static-1")).await;
    let msg = probe
        .received_ok(Duration::from_secs(3))
        .await
        .expect("OPTIONS from an ACL-allowed source must be answered");
    assert!(
        msg.contains("X-Node-Id: test-node"),
        "200 OK must carry the configured extra header: {msg}"
    );
    assert!(msg.contains("Allow:"), "200 OK must carry Allow: {msg}");
    assert!(
        msg.contains("Accept: application/sdp"),
        "200 OK must carry Accept: {msg}"
    );
}

/// `enabled = false` keeps the node from answering probes at all.
#[tokio::test]
async fn options_disabled_stays_silent() {
    let node = spawn_node(|config| {
        config.options_response = Some(OptionsResponseConfig {
            enabled: Some(false),
            ..Default::default()
        });
    })
    .await;
    let probe = Probe::new(format!("127.0.0.1:{}", node.sip_port).parse().unwrap()).await;

    probe.send(probe.options("disabled-1")).await;
    assert!(
        probe.collect(Duration::from_secs(1)).await.is_empty(),
        "OPTIONS must not be answered when options_response is disabled"
    );
}
