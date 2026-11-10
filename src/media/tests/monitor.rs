//! Side-path audio monitor tests: hub/processor filtering and the outbound
//! websocket forwarder (chunking, pacing, handshake metadata, give-up
//! semantics). Reuses the local warp websocket server pattern from
//! `media_pass.rs`.
use crate::media::AudioFrame;
use crate::media::Samples;
use crate::media::monitor::{
    LEG_AGENT, LEG_CALLER, MonitorHub, MonitorOption, MonitorProcessor,
    monitor_start_metadata, run_monitor_forwarder,
};
use crate::media::processor::Processor;
use anyhow::Result;
use bytes::Bytes;
use futures::StreamExt;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use warp::Filter;
use warp::ws::Ws;

/// Everything the mock monitor server observed for one connection.
#[derive(Clone, Default)]
struct Captured {
    raw_query: Arc<tokio::sync::Mutex<Option<String>>>,
    header: Arc<tokio::sync::Mutex<Option<String>>>,
    text: Arc<tokio::sync::Mutex<Vec<String>>>,
    binary: Arc<tokio::sync::Mutex<Vec<Vec<u8>>>>,
}

impl Captured {
    async fn binary_len(&self) -> usize {
        self.binary.lock().await.len()
    }
}

async fn start_capture_server(cancel_token: CancellationToken) -> Result<(SocketAddr, Captured)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let captured = Captured::default();
    let filter_captured = captured.clone();

    let routes = warp::any()
        .and(warp::ws())
        .and(warp::header::optional::<String>("x-monitor"))
        .and(warp::query::raw())
        .map(move |ws: Ws, header: Option<String>, raw_query: String| {
            let captured = filter_captured.clone();
            ws.on_upgrade(move |websocket| async move {
                *captured.raw_query.lock().await = Some(raw_query);
                *captured.header.lock().await = header;
                let (_, mut rx) = websocket.split();
                while let Some(Ok(msg)) = rx.next().await {
                    if msg.is_binary() {
                        captured.binary.lock().await.push(msg.as_bytes().to_vec());
                    } else if msg.is_text() {
                        captured.text.lock().await.push(msg.to_str().unwrap_or("").to_string());
                    }
                }
            })
        });

    tokio::spawn(async move {
        warp::serve(routes)
            .incoming(listener)
            .graceful(async move {
                cancel_token.cancelled().await;
            })
            .run()
            .await;
    });
    Ok((addr, captured))
}

/// Wait until `probe` turns true, polling every 20 ms.
async fn wait_for(probe: impl Fn() -> futures::future::BoxFuture<'static, bool>) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if probe().await {
            return true;
        }
        if Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Wait until the mock server accepts TCP connections. The forwarder dials
/// out exactly once, so tests must not race the server startup.
async fn wait_server(addr: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "mock server never came up at {addr}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn pcm_frame(track_id: &str, samples: usize) -> AudioFrame {
    AudioFrame {
        track_id: track_id.to_string(),
        samples: Samples::PCM {
            samples: vec![100i16; samples],
        },
        sample_rate: 16000,
        channels: 1,
        ..Default::default()
    }
}

/// Build a hub payload exactly like `MonitorProcessor` does: leg byte + pcm.
fn hub_frame(leg: u8, pcm_len: usize) -> Bytes {
    let mut data = Vec::with_capacity(pcm_len + 1);
    data.push(leg);
    data.extend_from_slice(&vec![0xABu8; pcm_len]);
    Bytes::from(data)
}

#[tokio::test]
async fn monitor_processor_filters_by_owner_and_prefixes_leg() -> Result<()> {
    let hub = MonitorHub::new();
    let mut rx = hub.subscribe();
    let mut agent = MonitorProcessor::new("server-side-track".to_string(), LEG_AGENT, hub.clone());
    let mut caller = MonitorProcessor::new("caller-track".to_string(), LEG_CALLER, hub);

    // The media-pass chain also processes the *relayed* caller audio (tagged
    // with the caller track id): the owner filter must drop it, otherwise the
    // caller leg would be published twice.
    let mut relayed = pcm_frame("caller-track", 4);
    agent.process_frame(&mut relayed)?;
    assert!(rx.try_recv().is_err(), "relayed frame must be filtered");

    // Own downlink frame -> published with the agent leg tag.
    let mut own = pcm_frame("server-side-track", 4);
    agent.process_frame(&mut own)?;
    let frame = rx.try_recv().expect("agent frame");
    assert_eq!(frame[0], LEG_AGENT);
    assert_eq!(frame.len(), 1 + 8); // leg byte + 4 samples * 2 bytes

    // Caller uplink frame -> published with the caller leg tag.
    let mut uplink = pcm_frame("caller-track", 4);
    caller.process_frame(&mut uplink)?;
    let frame = rx.try_recv().expect("caller frame");
    assert_eq!(frame[0], LEG_CALLER);

    // Non-PCM frames are ignored.
    let mut empty = AudioFrame {
        track_id: "caller-track".to_string(),
        samples: Samples::Empty,
        sample_rate: 16000,
        channels: 1,
        ..Default::default()
    };
    caller.process_frame(&mut empty)?;
    assert!(rx.try_recv().is_err(), "empty frame must be ignored");
    Ok(())
}

#[tokio::test]
async fn forwarder_chunks_by_buffer_size() -> Result<()> {
    let cancel = CancellationToken::new();
    let (addr, captured) = start_capture_server(cancel.clone()).await?;
    wait_server(addr).await;

    // 640-byte chunks, no pacing.
    let option = MonitorOption::new(format!("ws://{addr}"), Some(640), None, None);
    let hub = MonitorHub::new();
    let metadata = serde_json::json!({ "event": "monitor_start" });
    crate::spawn(run_monitor_forwarder(
        "s.chunk".to_string(),
        option,
        metadata,
        cancel.child_token(),
        hub.subscribe(),
    ));

    // Publish 4 frames x 320 bytes on the caller leg -> 2 exact 640-byte chunks.
    for _ in 0..4 {
        hub.publish(hub_frame(LEG_CALLER, 320));
    }

    assert!(
        wait_for(|| {
            let captured = captured.clone();
            Box::pin(async move { captured.binary_len().await >= 2 })
        })
        .await,
        "expected 2 chunks"
    );
    let binary = captured.binary.lock().await;
    assert_eq!(binary[0].len(), 1 + 640, "chunk = leg byte + 640 pcm bytes");
    assert_eq!(binary[0][0], LEG_CALLER);
    assert_eq!(binary[1].len(), 1 + 640);
    cancel.cancel();
    Ok(())
}

#[tokio::test]
async fn forwarder_flushes_partial_buffer_on_ptime() -> Result<()> {
    let cancel = CancellationToken::new();
    let (addr, captured) = start_capture_server(cancel.clone()).await?;
    wait_server(addr).await;

    // No buffer_size, ptime pacing only: partial buffers are flushed, never padded.
    let option = MonitorOption::new(format!("ws://{addr}"), None, Some(50), None);
    let hub = MonitorHub::new();
    let metadata = serde_json::json!({ "event": "monitor_start" });
    crate::spawn(run_monitor_forwarder(
        "s.ptime".to_string(),
        option,
        metadata,
        cancel.child_token(),
        hub.subscribe(),
    ));

    hub.publish(hub_frame(LEG_AGENT, 100));

    assert!(
        wait_for(|| {
            let captured = captured.clone();
            Box::pin(async move { captured.binary_len().await >= 1 })
        })
        .await,
        "expected the partial buffer to be flushed by the ptime timer"
    );
    let binary = captured.binary.lock().await;
    assert_eq!(binary[0].len(), 1 + 100);
    assert_eq!(binary[0][0], LEG_AGENT);
    cancel.cancel();
    Ok(())
}

#[tokio::test]
async fn forwarder_passes_frames_through_without_buffering() -> Result<()> {
    let cancel = CancellationToken::new();
    let (addr, captured) = start_capture_server(cancel.clone()).await?;
    wait_server(addr).await;

    // Neither buffer_size nor ptime: one websocket frame per tapped frame.
    let option = MonitorOption::new(format!("ws://{addr}"), None, None, None);
    let hub = MonitorHub::new();
    let metadata = serde_json::json!({ "event": "monitor_start" });
    crate::spawn(run_monitor_forwarder(
        "s.passthrough".to_string(),
        option,
        metadata,
        cancel.child_token(),
        hub.subscribe(),
    ));

    hub.publish(hub_frame(LEG_CALLER, 64));
    hub.publish(hub_frame(LEG_CALLER, 80));

    assert!(
        wait_for(|| {
            let captured = captured.clone();
            Box::pin(async move { captured.binary_len().await >= 2 })
        })
        .await,
        "expected both frames to be forwarded as-is"
    );
    let binary = captured.binary.lock().await;
    assert_eq!(binary[0].len(), 1 + 64);
    assert_eq!(binary[1].len(), 1 + 80);
    cancel.cancel();
    Ok(())
}

#[tokio::test]
async fn forwarder_sends_metadata_query_and_headers() -> Result<()> {
    let cancel = CancellationToken::new();
    let (addr, captured) = start_capture_server(cancel.clone()).await?;
    wait_server(addr).await;

    let mut headers = HashMap::new();
    headers.insert("x-monitor".to_string(), "h1".to_string());
    let option = MonitorOption::new(format!("ws://{addr}"), None, None, Some(headers));
    let hub = MonitorHub::new();
    let metadata = monitor_start_metadata(
        "s.meta",
        serde_json::json!("sip"),
        "2026-10-27T00:00:00Z".to_string(),
        serde_json::json!({ "tenant": "t1" }),
    );
    crate::spawn(run_monitor_forwarder(
        "s.meta".to_string(),
        option,
        metadata,
        cancel.child_token(),
        hub.subscribe(),
    ));

    // Handshake carries the session id as a query parameter and the custom header.
    assert!(
        wait_for(|| {
            let captured = captured.clone();
            Box::pin(async move { captured.raw_query.lock().await.is_some() })
        })
        .await,
        "expected the websocket handshake"
    );
    assert_eq!(
        captured.raw_query.lock().await.as_deref(),
        Some("session_id=s.meta")
    );
    assert_eq!(
        captured.header.lock().await.as_deref(),
        Some("h1"),
        "custom handshake header must reach the server"
    );

    // First message is the JSON metadata text frame.
    assert!(
        wait_for(|| {
            let captured = captured.clone();
            Box::pin(async move { !captured.text.lock().await.is_empty() })
        })
        .await,
        "expected the metadata text frame"
    );
    let metadata: serde_json::Value =
        serde_json::from_str(&captured.text.lock().await[0])?;
    assert_eq!(metadata["event"], "monitor_start");
    assert_eq!(metadata["sessionId"], "s.meta");
    assert_eq!(metadata["extras"]["tenant"], "t1");
    cancel.cancel();
    Ok(())
}

#[tokio::test]
async fn forwarder_gives_up_when_server_unreachable() -> Result<()> {
    // Bind then drop a listener: connecting to that port is deterministically refused.
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    drop(listener);

    let cancel = CancellationToken::new();
    let option = MonitorOption::new(format!("ws://{addr}"), None, None, None);
    let hub = MonitorHub::new();
    let metadata = serde_json::json!({ "event": "monitor_start" });
    let handle = crate::spawn(run_monitor_forwarder(
        "s.dead".to_string(),
        option,
        metadata,
        cancel.child_token(),
        hub.subscribe(),
    ));

    // Single attempt: the task must return promptly, not retry.
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("forwarder should give up quickly")
        .expect("forwarder task should not panic");
    cancel.cancel();
    Ok(())
}
