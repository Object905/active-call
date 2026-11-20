//! End-to-end test for the sipflow in-process signaling capture:
//!
//! 1. A customer (raw SIP UAC) calls in with a known Call-ID; a WS bot
//!    accepts, then hangs up.
//! 2. Every SIP message of the call (both directions) must be captured and
//!    the session associated with the customer's SIP Call-ID.
//! 3. At hangup the callrecord hook must upload one merged, time-sorted
//!    JSONL (with the customer Call-ID on every line) to the configured
//!    HTTP endpoint and evict the flows from memory.
//! 4. The persisted CDR must carry the session's `sipCallIds`.

use active_call::{
    app::{AppState, AppStateBuilder},
    config::{CallRecordConfig, Config, InviteHandlerConfig},
    sipflow::{SipFlowConfig, SipFlowUploadConfig},
};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, Level};

#[derive(Debug, Deserialize)]
struct WebhookPayload {
    #[serde(rename = "dialogId")]
    dialog_id: String,
    event: String,
}

const CUSTOMER_CALL_ID: &str = "sipflow-e2e-customer-1";

struct TestNode {
    http_port: u16,
    sip_port: u16,
    webhook_rx: mpsc::Receiver<WebhookPayload>,
    app: AppState,
}

async fn spawn_node(sip_port: u16, upload_url: String, cdr_root: String) -> TestNode {
    let (webhook_tx, webhook_rx) = mpsc::channel::<WebhookPayload>(1);
    let webhook = axum::Router::new().route(
        "/mock-handler",
        axum::routing::post(
            move |axum::Json(payload): axum::Json<WebhookPayload>| async move {
                if payload.event == "invite" {
                    let _ = webhook_tx.try_send(payload);
                }
                axum::Json(serde_json::json!({"status": "ok"}))
            },
        ),
    );
    let webhook_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let webhook_port = webhook_listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(webhook_listener, webhook).await.unwrap();
    });

    let port_key = (sip_port % 1000) as u16;
    let config = Config {
        addr: "127.0.0.1".to_string(),
        udp_port: sip_port,
        log_level: Some("debug".to_string()),
        useragent: Some("ActiveCallTest".to_string()),
        handler: Some(InviteHandlerConfig::Webhook {
            url: Some(format!("http://127.0.0.1:{webhook_port}/mock-handler")),
            method: Some("POST".to_string()),
            headers: None,
            urls: None,
        }),
        accept_timeout: Some("10s".to_string()),
        rtp_start_port: Some(33000 + port_key * 20),
        rtp_end_port: Some(33020 + port_key * 20),
        media_cache_path: "./target/tmp_media".to_string(),
        callrecord: Some(CallRecordConfig::Local { root: cdr_root }),
        sipflow: Some(SipFlowConfig {
            upload: Some(SipFlowUploadConfig::Http {
                url: upload_url,
                headers: None,
                file_field: None,
                file_name: None,
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    let app: AppState = AppStateBuilder::new()
        .with_config(config)
        .build()
        .await
        .expect("failed to build app state");
    assert!(
        app.sip_flow.is_some(),
        "sipflow capture should be active when [sipflow] is configured"
    );
    let sip_app = app.clone();
    let http_app = app.clone();
    tokio::spawn(async move {
        let _ = sip_app.serve().await;
    });
    let http_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let http_port = http_listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(
            http_listener,
            active_call::handler::call_router().with_state(http_app),
        )
        .await
        .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    TestNode {
        http_port,
        sip_port,
        webhook_rx,
        app,
    }
}

type WsStream = tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
>;

async fn attach_and_accept(
    node: &mut TestNode,
) -> (
    futures::stream::SplitSink<WsStream, Message>,
    mpsc::UnboundedReceiver<String>,
    String,
) {
    let payload = tokio::time::timeout(Duration::from_secs(8), node.webhook_rx.recv())
        .await
        .expect("webhook not called")
        .expect("webhook channel closed");
    let session_id = payload.dialog_id;

    let (ws, _) = connect_async(format!(
        "ws://127.0.0.1:{}/call?id={session_id}",
        node.http_port
    ))
    .await
    .expect("failed to attach websocket to the ringing call");
    let (mut sink, mut stream) = ws.split();

    let _ = stream.next().await; // early attach event
    sink.send(Message::text(r#"{"command":"accept","option":{}}"#.to_string()))
        .await
        .expect("failed to send accept");

    let (event_tx, event_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(Ok(msg)) = stream.next().await {
            if let Message::Text(t) = msg {
                let _ = event_tx.send(t.to_string());
            }
        }
    });
    (sink, event_rx, session_id)
}

const PCMU_OFFER: &str = "v=0\r\n\
o=- 123456 1 IN IP4 127.0.0.1\r\n\
s=-\r\n\
c=IN IP4 127.0.0.1\r\n\
t=0 0\r\n\
m=audio 40000 RTP/AVP 0 101\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=rtpmap:101 telephone-event/8000\r\n\
a=sendrecv\r\n";

fn is_rtp(msg: &[u8]) -> bool {
    msg.len() >= 12 && (msg[0] & 0xC0) == 0x80
}

struct SipSock {
    socket: UdpSocket,
}

impl SipSock {
    async fn bind() -> Self {
        Self {
            socket: UdpSocket::bind(("127.0.0.1", 0)).await.unwrap(),
        }
    }

    fn port(&self) -> u16 {
        self.socket.local_addr().unwrap().port()
    }

    async fn send_raw(&self, msg: &str, dst: std::net::SocketAddr) {
        self.socket.send_to(msg.as_bytes(), dst).await.unwrap();
    }

    async fn recv_sip(&self, timeout: Duration) -> Option<String> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut buf = [0u8; 8192];
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return None;
            }
            match tokio::time::timeout(remaining, self.socket.recv_from(&mut buf)).await {
                Ok(Ok((n, _src))) => {
                    if is_rtp(&buf[..n]) {
                        continue;
                    }
                    return Some(String::from_utf8_lossy(&buf[..n]).to_string());
                }
                _ => return None,
            }
        }
    }

    fn hdr<'a>(&self, msg: &'a str, name: &str) -> Option<String> {
        msg.lines().find_map(|l| {
            let (k, v) = l.split_once(':')?;
            if k.trim().eq_ignore_ascii_case(name) {
                Some(v.trim().to_string())
            } else {
                None
            }
        })
    }

    fn dst(&self, msg: &str) -> std::net::SocketAddr {
        let via = self.hdr(msg, "Via").unwrap_or_default();
        let port = via
            .split("rport=")
            .nth(1)
            .and_then(|p| p.split(';').next())
            .and_then(|p| p.parse().ok())
            .or_else(|| {
                via.rsplit(':')
                    .next()
                    .and_then(|p| p.split(';').next())
                    .and_then(|p| p.parse().ok())
            })
            .unwrap_or(5060);
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    fn reply(&self, req: &str, code_line: &str) {
        let via = self.hdr(req, "Via").unwrap_or_default();
        let from = self.hdr(req, "From").unwrap_or_default();
        let to = self.hdr(req, "To").unwrap_or_default();
        let call_id = self.hdr(req, "Call-ID").unwrap_or_default();
        let cseq = self.hdr(req, "CSeq").unwrap_or_default();
        let resp = format!(
            "{code_line}\r\n\
             Via: {via}\r\n\
             From: {from}\r\n\
             To: {to}\r\n\
             Call-ID: {call_id}\r\n\
             CSeq: {cseq}\r\n\
             Content-Length: 0\r\n\
             \r\n"
        );
        let dst = self.dst(req);
        futures::executor::block_on(self.send_raw(&resp, dst));
    }
}

/// Customer: INVITE with a fixed Call-ID, ACK the answer, then wait for the
/// bot's BYE and finish.
async fn run_customer(sock: SipSock, pbx: std::net::SocketAddr, events: mpsc::UnboundedSender<String>) {
    let port = sock.port();
    let invite = format!(
        "INVITE sip:100@{pbx} SIP/2.0\r\n\
         Via: SIP/2.0/UDP 127.0.0.1:{port};branch=z9hG4bKsipflow1;rport\r\n\
         From: <sip:1001@127.0.0.1>;tag=customer-tag\r\n\
         To: <sip:100@{pbx}>\r\n\
         Call-ID: {CUSTOMER_CALL_ID}\r\n\
         CSeq: 1 INVITE\r\n\
         Contact: <sip:1001@127.0.0.1:{port}>\r\n\
         Max-Forwards: 70\r\n\
         Content-Type: application/sdp\r\n\
         Content-Length: {len}\r\n\
         \r\n\
         {PCMU_OFFER}",
        len = PCMU_OFFER.len(),
    );
    sock.send_raw(&invite, pbx).await;

    let ok = loop {
        match sock.recv_sip(Duration::from_secs(10)).await {
            Some(msg) if msg.starts_with("SIP/2.0 200") => break msg,
            Some(msg) if msg.starts_with("SIP/2.0") => {
                sock.reply(&msg, "SIP/2.0 100 Trying");
                continue;
            }
            Some(_) => continue,
            None => {
                let _ = events.send("TIMEOUT: customer never answered".into());
                return;
            }
        }
    };
    let via = sock.hdr(&ok, "Via").unwrap_or_default();
    let to = sock.hdr(&ok, "To").unwrap_or_default();
    let ack = format!(
        "ACK sip:100@{pbx} SIP/2.0\r\n\
         Via: {via}\r\n\
         From: <sip:1001@127.0.0.1>;tag=customer-tag\r\n\
         To: {to}\r\n\
         Call-ID: {CUSTOMER_CALL_ID}\r\n\
         CSeq: 1 ACK\r\n\
         Max-Forwards: 70\r\n\
         Content-Length: 0\r\n\r\n"
    );
    sock.send_raw(&ack, pbx).await;
    let _ = events.send("customer_answered".into());

    loop {
        match sock.recv_sip(Duration::from_secs(15)).await {
            Some(msg) if msg.starts_with("BYE") => {
                sock.reply(&msg, "SIP/2.0 200 OK");
                let _ = events.send("customer_got_bye".into());
                return;
            }
            Some(_) => {}
            None => {
                let _ = events.send("TIMEOUT: customer never got BYE".into());
                return;
            }
        }
    }
}

async fn wait_event(
    rx: &mut mpsc::UnboundedReceiver<String>,
    needle: &str,
    timeout: Duration,
) -> Option<String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return None;
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(ev)) => {
                if ev.contains(needle) {
                    return Some(ev);
                }
            }
            _ => return None,
        }
    }
}

#[tokio::test]
async fn sipflow_captures_and_uploads_session_signaling() {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_test_writer()
        .try_init()
        .ok();

    // HTTP receiver collecting the uploaded JSONL bodies (multipart form).
    let uploads: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let uploads_for_handler = uploads.clone();
    let upload_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let upload_port = upload_listener.local_addr().unwrap().port();
    let upload_app = axum::Router::new().route(
        "/sipflow",
        axum::routing::post(
            move |mut form: axum::extract::Multipart| async move {
                while let Ok(Some(field)) = form.next_field().await {
                    if let Ok(text) = field.text().await {
                        uploads_for_handler.lock().unwrap().push(text);
                    }
                }
                axum::Json(serde_json::json!({"status": "ok"}))
            },
        ),
    );
    tokio::spawn(async move {
        axum::serve(upload_listener, upload_app).await.unwrap();
    });

    let cdr_dir = tempfile::tempdir().unwrap();
    let mut node = spawn_node(
        35290,
        format!("http://127.0.0.1:{upload_port}/sipflow"),
        cdr_dir.path().to_string_lossy().to_string(),
    )
    .await;
    let pbx = format!("127.0.0.1:{}", node.sip_port).parse().unwrap();

    let customer_sock = SipSock::bind().await;
    let customer_port = customer_sock.port();
    let (customer_tx, mut customer_rx) = mpsc::unbounded_channel();
    tokio::spawn(run_customer(customer_sock, pbx, customer_tx));

    // Attach the WS bot and accept.
    let (mut ws_sink, mut ws_events, session_id) = attach_and_accept(&mut node).await;
    wait_event(
        &mut customer_rx,
        "customer_answered",
        Duration::from_secs(10),
    )
    .await
    .expect("customer call was never answered");

    // While the call is up, the flows must be live in memory.
    let flow = node
        .app
        .sip_flow
        .as_ref()
        .expect("sipflow capture is configured")
        .clone();
    let live_items = flow.query_flow(CUSTOMER_CALL_ID, 0, u64::MAX);
    assert!(
        live_items.len() >= 2,
        "expected INVITE + responses captured while the call is up, got {}",
        live_items.len()
    );

    // The session must be associated with the customer's SIP Call-ID.
    let associated = flow.get_session_call_ids(&session_id);
    assert!(
        associated.iter().any(|id| id == CUSTOMER_CALL_ID),
        "session {session_id} must be associated with {CUSTOMER_CALL_ID}, got {associated:?}"
    );

    // Bot hangs up -> BYE towards the customer -> session tears down.
    // (The server closes the WS on hangup; the SIP BYE is the reliable
    // teardown signal here.)
    ws_sink
        .send(Message::text(r#"{"command":"hangup"}"#.to_string()))
        .await
        .expect("failed to send hangup");
    wait_event(&mut customer_rx, "customer_got_bye", Duration::from_secs(15))
        .await
        .expect("customer never received the BYE");

    // Wait for the JSONL upload.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let body = loop {
        {
            let uploads = uploads.lock().unwrap();
            if let Some(body) = uploads.iter().find(|b| b.contains(CUSTOMER_CALL_ID)) {
                break body.clone();
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "sipflow JSONL was never uploaded"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    let lines: Vec<serde_json::Value> = body
        .lines()
        .map(|l| serde_json::from_str(l).expect("jsonl line"))
        .collect();
    assert!(lines.len() >= 6, "expected a full call trace, got {lines:?}");

    // Every line carries the customer Call-ID.
    assert!(lines
        .iter()
        .all(|l| l["call_id"] == CUSTOMER_CALL_ID));

    // First message is the inbound INVITE from the customer's socket.
    let first = &lines[0];
    assert!(
        first["payload"].as_str().unwrap().starts_with("INVITE"),
        "first captured message should be the INVITE: {}",
        first["payload"]
    );
    assert_eq!(first["src_addr"], format!("127.0.0.1:{customer_port}"));

    // The BYE towards the customer appears with swapped addresses.
    assert!(lines.iter().any(|l| {
        l["payload"].as_str().unwrap().starts_with("BYE")
            && l["dst_addr"] == format!("127.0.0.1:{customer_port}")
    }));

    // Time-ordered.
    let timestamps: Vec<u64> = lines
        .iter()
        .map(|l| l["timestamp"].as_u64().unwrap())
        .collect();
    let mut sorted = timestamps.clone();
    sorted.sort();
    assert_eq!(timestamps, sorted, "JSONL must be time-sorted");

    // Flows are evicted after the upload.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        flow.query_flow(CUSTOMER_CALL_ID, 0, u64::MAX).len(),
        0,
        "flows must be evicted at hangup"
    );

    // The persisted CDR carries the session's SIP Call-IDs.
    let mut cdr_with_call_ids = false;
    for entry in std::fs::read_dir(cdr_dir.path()).unwrap().flatten() {
        let path = entry.path();
        if !path.is_file() {
            // CDRs live in per-day subdirectories.
            if let Ok(entries) = std::fs::read_dir(&path) {
                for file in entries.flatten() {
                    if let Ok(content) = std::fs::read_to_string(file.path()) {
                        if content.contains("sipCallIds") && content.contains(CUSTOMER_CALL_ID) {
                            cdr_with_call_ids = true;
                            break;
                        }
                    }
                }
            }
        }
        if cdr_with_call_ids {
            break;
        }
    }
    assert!(
        cdr_with_call_ids,
        "CDR must contain sipCallIds including the customer Call-ID"
    );

    info!("sipflow e2e test completed successfully");
}
