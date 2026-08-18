//! Peer websocket forwarding.
//!
//! When a client connects to `/call` (or `/call/sip`, `/call/webrtc`) with a
//! session id that is not hosted on this node, the node polls every configured
//! peer ([`crate::config::Config::peers`]) and tunnels the websocket to the
//! first peer that accepts the call. Forwarding requests carry `forward=1` and
//! a `visited` list so that a peer never silently creates a new call for a
//! find-phase request and so that A→B→A loops cannot occur.

use crate::app::AppState;
use crate::call::active_call::CallParams;
use crate::config::Config;
use axum::extract::ws::{Message as AxumMessage, WebSocket};
use futures::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tracing::{debug, info, warn};

type PeerStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Maximum forwarding depth. Guards against misconfigured peer loops.
const MAX_FORWARD_HOPS: usize = 16;

/// Strip any scheme and trailing slash so peer addresses compare consistently.
fn normalize_endpoint(addr: &str) -> String {
    let mut s = addr.trim();
    for scheme in ["wss://", "ws://", "https://", "http://"] {
        if let Some(rest) = s.strip_prefix(scheme) {
            s = rest;
            break;
        }
    }
    s.trim_end_matches('/').to_string()
}

fn addr_already_visited(peer: &str, visited: &[String], self_addr: &str) -> bool {
    let normalized = normalize_endpoint(peer);
    if !self_addr.is_empty() && normalize_endpoint(self_addr) == normalized {
        return true;
    }
    visited.iter().any(|v| normalize_endpoint(v) == normalized)
}

/// Attempt to forward a websocket to the first peer that hosts `session_id`.
///
/// Returns the connected websocket stream on success, `None` when no peer
/// accepts the call (or peers are not configured).
pub async fn try_forward(
    app_state: &AppState,
    session_id: &str,
    params: &CallParams,
) -> Option<PeerStream> {
    if app_state.config.peers.is_empty() {
        return None;
    }

    let self_addr = app_state.config.http_addr.trim().to_string();
    let mut visited = params.visited_list();
    if visited.len() >= MAX_FORWARD_HOPS {
        warn!(
            session_id,
            visited = ?visited,
            "peer forward exceeded max hops, giving up"
        );
        return None;
    }
    if !self_addr.is_empty() {
        visited.push(self_addr.clone());
    }
    let visited_str = visited.join(",");
    let query = params.to_forward_query(&visited_str);

    for peer in &app_state.config.peers {
        let Some(base) = Config::peer_ws_endpoint(peer) else {
            warn!(peer, "skipping invalid peer address");
            continue;
        };
        if addr_already_visited(peer, &visited, &self_addr) {
            debug!(session_id, peer, "skipping already-visited peer");
            continue;
        }
        let url = format!("{}/call?{}", base.trim_end_matches('/'), query);
        debug!(session_id, %url, "attempting peer forward");
        match tokio::time::timeout(
            Duration::from_secs(3),
            tokio_tungstenite::connect_async(&url),
        )
        .await
        {
            Ok(Ok((ws, _resp))) => {
                info!(session_id, %url, "peer accepted forwarded websocket");
                return Some(ws);
            }
            Ok(Err(e)) => {
                warn!(session_id, %url, "peer forward connection failed: {}", e);
            }
            Err(_) => {
                warn!(session_id, %url, "peer forward timed out");
            }
        }
    }
    None
}

/// Bidirectionally relay frames between the client websocket and the peer
/// websocket until either side closes.
pub async fn tunnel(client: WebSocket, peer: PeerStream) {
    let (mut client_sink, mut client_stream) = client.split();
    let (mut peer_sink, mut peer_stream) = peer.split();

    let reason = loop {
        tokio::select! {
            msg = client_stream.next() => {
                match msg {
                    Some(Ok(m)) => {
                        if let Some(t) = axum_to_tungstenite(m)
                            && let Err(e) = peer_sink.send(t).await
                        {
                            break format!("forward to peer failed: {}", e);
                        }
                    }
                    Some(Err(e)) => {
                        break format!("client websocket error: {}", e);
                    }
                    None => {
                        break "client websocket closed".to_string();
                    }
                }
            }
            msg = peer_stream.next() => {
                match msg {
                    Some(Ok(m)) => {
                        if let Some(a) = tungstenite_to_axum(m)
                            && let Err(e) = client_sink.send(a).await
                        {
                            break format!("forward to client failed: {}", e);
                        }
                    }
                    Some(Err(e)) => {
                        break format!("peer websocket error: {}", e);
                    }
                    None => {
                        break "peer websocket closed".to_string();
                    }
                }
            }
        }
    };

    debug!("websocket tunnel ended: {}", reason);
    let _ = peer_sink.close().await;
    let _ = client_sink.close().await;
}

fn tungstenite_utf8(
    t: axum::extract::ws::Utf8Bytes,
) -> tokio_tungstenite::tungstenite::protocol::frame::Utf8Bytes {
    tokio_tungstenite::tungstenite::protocol::frame::Utf8Bytes::from(t.as_str())
}

fn axum_to_tungstenite(m: AxumMessage) -> Option<TungsteniteMessage> {
    Some(match m {
        AxumMessage::Text(t) => TungsteniteMessage::Text(tungstenite_utf8(t)),
        AxumMessage::Binary(b) => TungsteniteMessage::Binary(b),
        AxumMessage::Ping(p) => TungsteniteMessage::Ping(p),
        AxumMessage::Pong(p) => TungsteniteMessage::Pong(p),
        AxumMessage::Close(c) => TungsteniteMessage::Close(c.map(|f| {
            tokio_tungstenite::tungstenite::protocol::frame::CloseFrame {
                code: f.code.into(),
                reason: tungstenite_utf8(f.reason),
            }
        })),
    })
}

fn tungstenite_to_axum(m: TungsteniteMessage) -> Option<AxumMessage> {
    match m {
        TungsteniteMessage::Text(t) => Some(AxumMessage::Text(
            axum::extract::ws::Utf8Bytes::from(t.as_str()),
        )),
        TungsteniteMessage::Binary(b) => Some(AxumMessage::Binary(b)),
        TungsteniteMessage::Ping(p) => Some(AxumMessage::Ping(p)),
        TungsteniteMessage::Pong(p) => Some(AxumMessage::Pong(p)),
        TungsteniteMessage::Close(c) => Some(AxumMessage::Close(c.map(|f| {
            axum::extract::ws::CloseFrame {
                code: f.code.into(),
                reason: axum::extract::ws::Utf8Bytes::from(f.reason.as_str()),
            }
        }))),
        TungsteniteMessage::Frame(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_ws_endpoint_normalization() {
        assert_eq!(
            Config::peer_ws_endpoint("10.0.0.2:8080").as_deref(),
            Some("ws://10.0.0.2:8080")
        );
        assert_eq!(
            Config::peer_ws_endpoint("ws://10.0.0.2:8080").as_deref(),
            Some("ws://10.0.0.2:8080")
        );
        assert_eq!(
            Config::peer_ws_endpoint("http://10.0.0.2:8080").as_deref(),
            Some("ws://10.0.0.2:8080")
        );
        assert_eq!(
            Config::peer_ws_endpoint("https://10.0.0.2:8080").as_deref(),
            Some("wss://10.0.0.2:8080")
        );
        assert_eq!(Config::peer_ws_endpoint(""), None);
    }

    #[test]
    fn test_call_params_forward_query() {
        let params = CallParams {
            id: Some("s.a/b c".to_string()),
            dump_events: Some(true),
            ping_interval: Some(30),
            server_side_track: Some("t.1".to_string()),
            forward: None,
            visited: None,
        };
        let q = params.to_forward_query("node1,node2");
        assert!(q.contains("id=s.a%2Fb%20c"));
        assert!(q.contains("dump=true"));
        assert!(q.contains("ping=30"));
        assert!(q.contains("server_side_track=t.1"));
        assert!(q.contains("forward=true"));
        assert!(q.contains("visited=node1%2Cnode2"));
    }

    #[test]
    fn test_visited_list_parsing() {
        let params = CallParams {
            id: None,
            dump_events: None,
            ping_interval: None,
            server_side_track: None,
            forward: None,
            visited: Some("node1, node2,node3".to_string()),
        };
        assert_eq!(params.visited_list(), vec!["node1", "node2", "node3"]);
    }

    #[test]
    fn test_addr_already_visited() {
        let visited = vec!["ws://10.0.0.2:8080".to_string()];
        assert!(addr_already_visited("10.0.0.2:8080", &visited, "0.0.0.0:8080"));
        assert!(addr_already_visited("http://10.0.0.2:8080/", &visited, "0.0.0.0:8080"));
        assert!(!addr_already_visited("10.0.0.3:8080", &visited, "0.0.0.0:8080"));
        assert!(addr_already_visited("0.0.0.0:8080", &[], "0.0.0.0:8080"));
    }
}
