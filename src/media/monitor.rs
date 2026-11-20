//! Side-path (侧路) audio distribution: mirrors both call legs' audio to an
//! external websocket server.
//!
//! Unlike [`crate::media::track::media_pass::MediaPassTrack`] — which replaces
//! the media path with a bidirectional bridge — the monitor is a passive,
//! one-way tap: `MonitorProcessor` instances are attached to the caller track
//! (uplink, leg 1) and the server-side track (downlink, leg 0) processor
//! chains, publish `[1-byte leg][pcm16]` frames into a [`MonitorHub`], and a
//! forwarder task dials out to the configured websocket and pushes the frames.
//! The main media path is never modified or blocked.
//!
//! Wire protocol:
//! - the dial-out URL gets a `session_id` query parameter appended;
//! - the first message is a JSON text frame (see [`monitor_start_metadata`])
//!   carrying session info;
//! - every following message is a binary frame `[1-byte leg][pcm16 mono]`
//!   (leg `0` = agent/server side, `1` = caller), PCM at
//!   [`INTERNAL_SAMPLERATE`].
//!
//! Failure semantics: the forwarder tries **once**. A failed handshake, a
//! rejected upgrade or a dropped connection logs a warning and gives up for
//! the rest of the call — audio is simply not published, the call itself is
//! unaffected.
use crate::media::{AudioFrame, INTERNAL_SAMPLERATE, Samples, TrackId};
use anyhow::Result;
use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_with::skip_serializing_none;
use std::{collections::HashMap, time::Duration};
use tokio::sync::broadcast;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::{HeaderName, HeaderValue},
    },
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Leg tag byte prepended to every published frame: server-side (agent) audio
/// played towards the caller (TTS, media-pass downlink, ambiance).
pub const LEG_AGENT: u8 = 0;
/// Leg tag byte prepended to every published frame: caller uplink audio.
pub const LEG_CALLER: u8 = 1;

/// Broadcast channel capacity, in tagged frames. A slow consumer lags behind
/// and silently drops its own frames instead of back-pressuring the media
/// path (200 frames ≈ 4s of 20ms audio).
pub const MONITOR_CHANNEL_CAPACITY: usize = 200;

#[skip_serializing_none]
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct MonitorOption {
    /// Websocket url to dial out to, e.g. `ws://asr-server:9000/audio`.
    url: String,
    /// Target size in bytes of each binary frame pushed to the remote (PCM16
    /// payload, excluding the leg prefix byte). `None`/0 disables chunking:
    /// tapped frames are forwarded as they arrive (about 20 ms each).
    buffer_size: Option<u32>,
    /// Interval in milliseconds at which partially filled buffers are flushed
    /// to the remote. `None`/0 disables the flush timer.
    ptime: Option<u32>,
    /// Extra HTTP headers for the websocket handshake (e.g. `Authorization`).
    headers: Option<HashMap<String, String>>,
}

impl MonitorOption {
    pub fn new(
        url: String,
        buffer_size: Option<u32>,
        ptime: Option<u32>,
        headers: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            url,
            buffer_size,
            ptime,
            headers,
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn buffer_size(&self) -> Option<u32> {
        self.buffer_size
    }

    pub fn ptime(&self) -> Option<u32> {
        self.ptime
    }

    pub fn headers(&self) -> Option<&HashMap<String, String>> {
        self.headers.as_ref()
    }
}

/// Per-session fan-out point between the media-path taps and consumers (the
/// outbound websocket forwarder today, possible local sinks later). With no
/// subscriber a publish is a near no-op.
#[derive(Clone)]
pub struct MonitorHub {
    sender: broadcast::Sender<Bytes>,
}

impl Default for MonitorHub {
    fn default() -> Self {
        Self::new()
    }
}

impl MonitorHub {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(MONITOR_CHANNEL_CAPACITY);
        Self { sender }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Bytes> {
        self.sender.subscribe()
    }

    pub(crate) fn publish(&self, data: Bytes) {
        // No subscriber / lagging consumer: drop instead of back-pressuring.
        let _ = self.sender.send(data);
    }
}

/// Mirrors a track's own PCM frames into a [`MonitorHub`], tagged with a leg
/// byte. Frames merely relayed for another leg (e.g. the caller's audio
/// forwarded through the media-pass track's `send_packet`) are filtered out by
/// the `owner` check — the originating track publishes its own copy.
pub struct MonitorProcessor {
    owner: TrackId,
    leg: u8,
    hub: MonitorHub,
}

impl MonitorProcessor {
    pub fn new(owner: TrackId, leg: u8, hub: MonitorHub) -> Self {
        Self { owner, leg, hub }
    }
}

impl super::processor::Processor for MonitorProcessor {
    fn process_frame(&mut self, frame: &mut AudioFrame) -> Result<()> {
        if frame.track_id != self.owner {
            return Ok(());
        }
        if let Samples::PCM { samples } = &frame.samples
            && !samples.is_empty()
        {
            let pcm = audio_codec::samples_to_bytes(samples);
            let mut data = Vec::with_capacity(pcm.len() + 1);
            data.push(self.leg);
            data.extend_from_slice(&pcm);
            self.hub.publish(Bytes::from(data));
        }
        Ok(())
    }
}

/// Build the JSON metadata frame sent as the first (text) message after the
/// websocket is established. `extras` carries the call's custom variables
/// (SIP extracted headers, playbook vars, …).
pub fn monitor_start_metadata(
    session_id: &str,
    call_type: serde_json::Value,
    started_at: String,
    extras: serde_json::Value,
) -> serde_json::Value {
    json!({
        "event": "monitor_start",
        "sessionId": session_id,
        "callType": call_type,
        "startedAt": started_at,
        "format": "pcm16/mono",
        "sampleRate": INTERNAL_SAMPLERATE,
        "legs": [
            { "leg": LEG_AGENT, "direction": "agent", "sampleRate": INTERNAL_SAMPLERATE },
            { "leg": LEG_CALLER, "direction": "caller", "sampleRate": INTERNAL_SAMPLERATE },
        ],
        "extras": extras,
    })
}

/// Dial out to `option.url()` and push both legs' audio until the call ends or
/// the connection breaks. Single attempt: any failure logs a warning and gives
/// up for the rest of the call.
pub async fn run_monitor_forwarder(
    session_id: String,
    option: MonitorOption,
    metadata: serde_json::Value,
    cancel_token: CancellationToken,
    mut receiver: broadcast::Receiver<Bytes>,
) {
    let mut url = match url::Url::parse(option.url()) {
        Ok(url) => url,
        Err(e) => {
            warn!(session_id, url = %option.url(), error = %e, "monitor: invalid url, giving up");
            return;
        }
    };
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("session_id", &session_id);
    }

    // Start from the standard &str conversion so the required websocket
    // handshake headers (Host/Connection/Upgrade/Sec-WebSocket-*) are
    // generated, then layer the custom headers on top. A bare
    // `Request::builder().uri(..)` would pass through `IntoClientRequest`
    // untouched (the `Request` impl is a no-op) and fail the handshake.
    let mut request = match url.as_str().into_client_request() {
        Ok(request) => request,
        Err(e) => {
            warn!(session_id, url = %url, error = %e, "monitor: invalid request, giving up");
            return;
        }
    };
    if let Some(headers) = option.headers() {
        for (name, value) in headers {
            let (name, value) = match (
                HeaderName::from_bytes(name.as_bytes()),
                HeaderValue::from_str(value),
            ) {
                (Ok(name), Ok(value)) => (name, value),
                (name_err, value_err) => {
                    warn!(
                        session_id,
                        header = %name,
                        name_error = ?name_err.err(),
                        value_error = ?value_err.err(),
                        "monitor: invalid header, giving up"
                    );
                    return;
                }
            };
            request.headers_mut().insert(name, value);
        }
    }

    info!(session_id, url = %url, "monitor: connecting");
    let (ws, _) = match connect_async(request).await {
        Ok(ok) => ok,
        Err(e) => {
            warn!(
                session_id,
                url = %url,
                error = %e,
                "monitor: connect failed, giving up for this call"
            );
            return;
        }
    };
    let (mut ws_sink, mut ws_source) = ws.split();
    // Keep draining the read side: tungstenite answers protocol pings while
    // reading, and this observes a remote close instead of hanging the write
    // half forever.
    crate::spawn(async move {
        while let Some(msg) = ws_source.next().await {
            if msg.is_err() {
                break;
            }
        }
    });

    if let Err(e) = ws_sink.send(Message::Text(metadata.to_string().into())).await {
        warn!(
            session_id,
            error = %e,
            "monitor: failed to send metadata, giving up for this call"
        );
        return;
    }
    info!(session_id, url = %url, "monitor: connected");

    let buffer_size = option.buffer_size().filter(|size| *size > 0).unwrap_or(0) as usize;
    let ptime_ms = option.ptime().filter(|p| *p > 0).unwrap_or(0);
    let mut ticker = tokio::time::interval(Duration::from_millis(ptime_ms.max(1) as u64));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // One buffer per leg: legs interleave on the hub and must not mix inside
    // a chunk (and may even differ in format later).
    let mut leg_buffers: HashMap<u8, BytesMut> = HashMap::new();

    loop {
        let mut outgoing: Vec<Bytes> = Vec::new();
        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => break,
            _ = ticker.tick(), if ptime_ms > 0 => {
                // Flush whatever accumulated, even a partial chunk — never
                // pad with silence, a monitor must not fabricate audio.
                for (leg, buffer) in leg_buffers.iter_mut() {
                    if !buffer.is_empty() {
                        outgoing.push(tag_frame(*leg, std::mem::take(buffer)));
                    }
                }
            }
            msg = receiver.recv() => match msg {
                Ok(data) => {
                    if data.is_empty() {
                        continue;
                    }
                    let leg = data[0];
                    let buffer = leg_buffers.entry(leg).or_default();
                    buffer.extend_from_slice(&data[1..]);
                    if buffer_size > 0 {
                        // Emit exact-size chunks as soon as they fill.
                        while buffer.len() >= buffer_size {
                            outgoing.push(tag_frame(leg, buffer.split_to(buffer_size)));
                        }
                    } else if ptime_ms == 0 {
                        // No buffering configured: pass frames straight through.
                        outgoing.push(tag_frame(leg, std::mem::take(buffer)));
                    }
                }
                Err(broadcast::error::RecvError::Lagged(dropped)) => {
                    warn!(session_id, dropped, "monitor: lagged, dropping frames");
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }

        let mut failed = false;
        for chunk in outgoing {
            if let Err(e) = ws_sink.send(Message::Binary(chunk)).await {
                warn!(
                    session_id,
                    error = %e,
                    "monitor: websocket send failed, giving up for this call"
                );
                failed = true;
                break;
            }
        }
        if failed {
            break;
        }
    }

    let _ = ws_sink.close().await;
    info!(session_id, url = %url, "monitor forwarder stopped");
}

fn tag_frame(leg: u8, pcm: BytesMut) -> Bytes {
    let mut out = BytesMut::with_capacity(pcm.len() + 1);
    out.extend_from_slice(&[leg]);
    out.extend_from_slice(&pcm);
    out.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monitor_option_parses_camel_case() {
        let option: MonitorOption = serde_json::from_value(json!({
            "url": "ws://server:9000/audio",
            "bufferSize": 640,
            "ptime": 100,
            "headers": { "Authorization": "Bearer token", "X-Tenant-ID": "t1" }
        }))
        .unwrap();
        assert_eq!(option.url(), "ws://server:9000/audio");
        assert_eq!(option.buffer_size(), Some(640));
        assert_eq!(option.ptime(), Some(100));
        let headers = option.headers().unwrap();
        assert_eq!(headers.get("Authorization").map(String::as_str), Some("Bearer token"));
        assert_eq!(headers.get("X-Tenant-ID").map(String::as_str), Some("t1"));
    }

    #[test]
    fn monitor_option_defaults_are_passthrough() {
        let option: MonitorOption = serde_json::from_value(json!({ "url": "ws://x" })).unwrap();
        assert_eq!(option.buffer_size(), None);
        assert_eq!(option.ptime(), None);
        assert_eq!(option.headers(), None);
    }

    #[test]
    fn metadata_contains_session_and_legs() {
        let metadata = monitor_start_metadata(
            "s.1",
            json!("sip"),
            "2026-10-27T00:00:00Z".to_string(),
            json!({ "k": "v" }),
        );
        assert_eq!(metadata["event"], "monitor_start");
        assert_eq!(metadata["sessionId"], "s.1");
        assert_eq!(metadata["callType"], "sip");
        assert_eq!(metadata["sampleRate"], INTERNAL_SAMPLERATE);
        assert_eq!(metadata["legs"][0]["leg"], LEG_AGENT);
        assert_eq!(metadata["legs"][0]["direction"], "agent");
        assert_eq!(metadata["legs"][1]["leg"], LEG_CALLER);
        assert_eq!(metadata["legs"][1]["direction"], "caller");
        assert_eq!(metadata["extras"]["k"], "v");
    }
}
