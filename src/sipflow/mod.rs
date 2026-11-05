//! In-process SIP signaling capture ("sipflow").
//!
//! Every SIP message that crosses the endpoint (both directions, including
//! the dialogs created for REFER transfers) is recorded into a bounded
//! in-memory store keyed by Call-ID. When a call ends, the callrecord upload
//! hook ([`SipFlowUploadHook`]) merges the flows of all dialogs belonging to
//! the session (primary leg, bridge legs, REFER transfer legs) into a single
//! time-sorted JSONL document and uploads it to S3 or HTTP — the same wire
//! format rustpbx exports, plus a per-line `callId` so merged legs stay
//! identifiable.
//!
//! This is deliberately a lightweight store (no SQLite, no collector): it
//! only needs to survive until the callrecord is saved at hangup. Bounds:
//! `max_calls` (LRU over Call-IDs) and `max_items_per_call` (per-dialog
//! cap); both overflow paths count dropped messages instead of blocking the
//! SIP endpoint.
//!
//! ```toml
//! [sipflow]
//! max_calls = 4096
//! max_items_per_call = 8192
//!
//! [sipflow.upload]
//! type = "s3"
//! vendor = "aliyun"
//! bucket = "..."
//! ```

mod upload;

pub use upload::SipFlowUploadHook;

use lru::LruCache;
use rsipstack::rsip::prelude::HeadersExt;
use rsipstack::rsip::SipMessage;
use rsipstack::transaction::endpoint::MessageInspector;
use rsipstack::transport::SipAddr;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Default bound on tracked Call-IDs (LRU).
pub const DEFAULT_MAX_CALLS: usize = 4096;
/// Default bound on captured messages per Call-ID.
pub const DEFAULT_MAX_ITEMS_PER_CALL: usize = 8192;
/// Upper bound on the session → Call-IDs association table.
pub const DEFAULT_MAX_SESSIONS: usize = 10_000;

/// One captured SIP message. The serialized field names match rustpbx's
/// sipflow JSONL export (`timestamp`/`seq`/`leg`/`msgType`/`srcAddr`/
/// `dstAddr`/`payload`); `callId` is an active-call extension so a merged
/// multi-leg export stays attributable per dialog.
#[derive(Debug, Clone, Serialize)]
pub struct SipFlowItem {
    /// Unix timestamp in microseconds.
    pub timestamp: u64,
    /// Capture sequence within one Call-ID.
    pub seq: u64,
    /// RTP leg marker in rustpbx exports; always `null` for SIP signaling.
    pub leg: Option<i32>,
    /// Always `"Sip"`.
    pub msg_type: &'static str,
    /// SIP Call-ID this message belongs to.
    pub call_id: String,
    pub src_addr: String,
    pub dst_addr: String,
    /// Full raw SIP message text.
    pub payload: String,
}

/// `[sipflow]` — in-process SIP signaling capture.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, Default)]
pub struct SipFlowConfig {
    /// Set to `false` to disable capture even though the section exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// LRU bound on tracked Call-IDs (default 4096).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_calls: Option<usize>,
    /// Per-Call-ID message cap; further messages are dropped and counted
    /// (default 8192).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_items_per_call: Option<usize>,
    /// Post-call JSONL upload. Without it the flows are captured and
    /// discarded at hangup.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upload: Option<SipFlowUploadConfig>,
}

impl SipFlowConfig {
    /// Capture is on when the section exists, unless `enabled = false`.
    pub fn enabled(&self) -> bool {
        self.enabled.unwrap_or(true)
    }

    pub fn max_calls(&self) -> usize {
        self.max_calls.unwrap_or(DEFAULT_MAX_CALLS).max(1)
    }

    pub fn max_items_per_call(&self) -> usize {
        self.max_items_per_call
            .unwrap_or(DEFAULT_MAX_ITEMS_PER_CALL)
            .max(1)
    }
}

/// `[sipflow.upload]` — where the per-session signaling JSONL goes after the
/// call ends. S3 mirrors `[callrecord]`'s S3 flavour; HTTP posts the JSONL
/// as one multipart file part.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SipFlowUploadConfig {
    S3 {
        vendor: crate::config::S3Vendor,
        bucket: String,
        region: String,
        access_key: String,
        secret_key: String,
        endpoint: String,
        /// Object key prefix (no leading/trailing slashes); the key is
        /// `{root}/{YYYYMMDD}/{session_id}.jsonl`.
        #[serde(default)]
        root: String,
    },
    Http {
        url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        headers: Option<HashMap<String, String>>,
        /// Multipart field name (default `signaling`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        file_field: Option<String>,
        /// Uploaded file name (default `{session_id}.jsonl`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        file_name: Option<String>,
    },
}

struct CallFlow {
    items: Vec<SipFlowItem>,
    next_seq: u64,
    dropped: u64,
}

/// session_id -> the SIP Call-IDs of every dialog seen for it (primary leg,
/// bridge legs, REFER transfer legs), insertion-ordered and bounded.
#[derive(Default)]
struct SessionRegistry {
    map: HashMap<String, Vec<String>>,
    order: VecDeque<String>,
}

impl SessionRegistry {
    fn associate(&mut self, session_id: &str, call_id: &str, capacity: usize) {
        if session_id.is_empty() || call_id.is_empty() {
            return;
        }
        if let Some(entry) = self.map.get_mut(session_id) {
            if !entry.iter().any(|id| id == call_id) {
                entry.push(call_id.to_string());
            }
            return;
        }
        // Bound the table: drop the oldest session when over capacity.
        while self.map.len() >= capacity {
            match self.order.pop_front() {
                Some(oldest) => {
                    self.map.remove(&oldest);
                }
                None => break,
            }
        }
        self.map
            .insert(session_id.to_string(), vec![call_id.to_string()]);
        self.order.push_back(session_id.to_string());
    }

    fn evict(&mut self, session_id: &str) {
        self.map.remove(session_id);
        if let Some(pos) = self.order.iter().position(|id| id == session_id) {
            self.order.remove(pos);
        }
    }
}

struct SipFlowInner {
    max_items: usize,
    local_addr: String,
    flows: Mutex<LruCache<String, CallFlow>>,
    sessions: Mutex<(SessionRegistry, usize)>,
    dropped: AtomicU64,
    next: Mutex<Option<Box<dyn MessageInspector>>>,
}

/// Cloneable handle to the signaling recorder. Implements
/// [`MessageInspector`], recording every message and passing it through to
/// the next inspector in the chain unmodified.
#[derive(Clone)]
pub struct SipFlow {
    inner: Arc<SipFlowInner>,
}

impl SipFlow {
    pub fn new(
        config: SipFlowConfig,
        local_addr: String,
        next: Option<Box<dyn MessageInspector>>,
    ) -> Self {
        let max_calls = config.max_calls();
        let max_items = config.max_items_per_call();
        Self {
            inner: Arc::new(SipFlowInner {
                max_items,
                local_addr,
                flows: Mutex::new(LruCache::new(
                    NonZeroUsize::new(max_calls).unwrap_or(NonZeroUsize::MIN),
                )),
                sessions: Mutex::new((
                    SessionRegistry::default(),
                    DEFAULT_MAX_SESSIONS.max(max_calls),
                )),
                dropped: AtomicU64::new(0),
                next: Mutex::new(next),
            }),
        }
    }

    /// The local address recorded as the near end of each flow.
    pub fn local_addr(&self) -> &str {
        &self.inner.local_addr
    }

    /// Chains the next inspector behind this recorder (used by
    /// `AppStateBuilder` when this recorder is injected instead of built
    /// from config, where the rest of the inspector chain only exists at
    /// build time).
    pub fn set_next(&self, next: Option<Box<dyn MessageInspector>>) {
        if let Ok(mut guard) = self.inner.next.lock() {
            *guard = next;
        }
    }

    /// Total messages dropped due to the per-Call-ID cap.
    pub fn dropped_count(&self) -> u64 {
        self.inner.dropped.load(Ordering::Relaxed)
    }

    /// Number of Call-IDs currently tracked.
    pub fn tracked_calls(&self) -> usize {
        self.inner.flows.lock().map(|f| f.len()).unwrap_or(0)
    }

    /// Associates a SIP Call-ID with a session so the callrecord hook can
    /// merge all of a session's dialogs (REFER transfers included) into one
    /// JSONL. Called at dialog creation points: incoming INVITE accept and
    /// every outgoing INVITE (bridge leg, transfer leg, dial-out).
    pub fn associate(&self, session_id: &str, call_id: &str) {
        if let Ok(mut guard) = self.inner.sessions.lock() {
            let (registry, capacity) = &mut *guard;
            registry.associate(session_id, call_id, *capacity);
        }
    }

    /// All Call-IDs associated with a session (may be empty when capture is
    /// disabled or the session never had a SIP dialog).
    pub fn get_session_call_ids(&self, session_id: &str) -> Vec<String> {
        match self.inner.sessions.lock() {
            Ok(guard) => {
                let (registry, _) = &*guard;
                registry.map.get(session_id).cloned().unwrap_or_default()
            }
            Err(_) => Vec::new(),
        }
    }

    /// Captured items for one Call-ID within `[start_us, end_us]` (unix
    /// microseconds, inclusive bounds like the time window slack).
    pub fn query_flow(&self, call_id: &str, start_us: u64, end_us: u64) -> Vec<SipFlowItem> {
        match self.inner.flows.lock() {
            Ok(flows) => match flows.peek(call_id) {
                Some(flow) => flow
                    .items
                    .iter()
                    .filter(|item| item.timestamp >= start_us && item.timestamp <= end_us)
                    .cloned()
                    .collect(),
                None => Vec::new(),
            },
            Err(_) => Vec::new(),
        }
    }

    /// Drops the flows and associations of a finished session (called by the
    /// upload hook after export/upload so memory does not grow unbounded).
    pub fn evict(&self, session_id: &str, call_ids: &[String]) {
        if let Ok(mut flows) = self.inner.flows.lock() {
            for call_id in call_ids {
                flows.pop(call_id);
            }
        }
        if let Ok(mut guard) = self.inner.sessions.lock() {
            let (registry, _) = &mut *guard;
            registry.evict(session_id);
        }
    }

    /// Serializes items as JSONL (one JSON object per line), the wire format
    /// uploaded at call end. Items are expected pre-sorted by
    /// `(timestamp, seq)`.
    pub fn export_jsonl(items: &[SipFlowItem]) -> String {
        let mut out = String::new();
        for item in items {
            if let Ok(line) = serde_json::to_string(item) {
                out.push_str(&line);
                out.push('\n');
            }
        }
        out
    }

    fn record_sip(&self, is_outgoing: bool, msg: &SipMessage, addr: Option<&SipAddr>) {
        let Ok(call_id_header) = msg.call_id_header() else {
            return;
        };
        let call_id = call_id_header.value().trim().to_string();
        if call_id.is_empty() {
            return;
        }
        let timestamp = chrono::Utc::now().timestamp_micros().max(0) as u64;
        let payload = msg.to_string();
        let remote = addr.map(sip_addr_to_string).unwrap_or_default();
        let local = self.inner.local_addr.clone();
        let (src_addr, dst_addr) = if is_outgoing {
            let dst = if remote.is_empty() {
                via_sent_by(msg).unwrap_or_default()
            } else {
                remote
            };
            (local, dst)
        } else {
            let src = if remote.is_empty() {
                via_sent_by(msg).unwrap_or_default()
            } else {
                remote
            };
            (src, local)
        };

        let mut overflow = false;
        if let Ok(mut flows) = self.inner.flows.lock() {
            if !flows.contains(&call_id) {
                flows.put(
                    call_id.clone(),
                    CallFlow {
                        items: Vec::new(),
                        next_seq: 0,
                        dropped: 0,
                    },
                );
            }
            if let Some(flow) = flows.get_mut(&call_id) {
                if flow.items.len() >= self.inner.max_items {
                    flow.dropped += 1;
                    overflow = true;
                } else {
                    let seq = flow.next_seq;
                    flow.next_seq += 1;
                    flow.items.push(SipFlowItem {
                        timestamp,
                        seq,
                        leg: None,
                        msg_type: "Sip",
                        call_id,
                        src_addr,
                        dst_addr,
                        payload,
                    });
                }
            }
        }
        if overflow {
            let previous = self.inner.dropped.fetch_add(1, Ordering::Relaxed);
            if previous % 1000 == 0 {
                tracing::warn!(
                    total_dropped = previous + 1,
                    "sipflow per-call message cap reached, dropping messages"
                );
            }
        }
    }
}

impl MessageInspector for SipFlow {
    fn before_send(&self, msg: SipMessage, dest: Option<&SipAddr>) -> SipMessage {
        self.record_sip(true, &msg, dest);
        let next = self.inner.next.lock().ok();
        match next.as_deref() {
            Some(Some(n)) => n.before_send(msg, dest),
            _ => msg,
        }
    }

    fn after_received(&self, msg: SipMessage, from: Option<&SipAddr>) -> SipMessage {
        self.record_sip(false, &msg, from);
        let next = self.inner.next.lock().ok();
        match next.as_deref() {
            Some(Some(n)) => n.after_received(msg, from),
            _ => msg,
        }
    }
}

fn sip_addr_to_string(addr: &SipAddr) -> String {
    addr.addr.to_string()
}

/// Falls back to the top Via sent-by ("host:port") when the transport did
/// not provide the peer address.
fn via_sent_by(msg: &SipMessage) -> Option<String> {
    use rsipstack::rsip::ToTypedHeader;
    match msg {
        SipMessage::Request(req) => req
            .via_header()
            .ok()
            .and_then(|v| v.typed().ok())
            .map(|v| v.sent_by().to_string()),
        SipMessage::Response(resp) => resp
            .via_header()
            .ok()
            .and_then(|v| v.typed().ok())
            .map(|v| v.sent_by().to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsipstack::rsip::{Header, Headers, Method, Request, Response, Uri, Version};
    use std::sync::atomic::AtomicUsize;

    fn invite_request(call_id: &str) -> SipMessage {
        SipMessage::Request(Request {
            method: Method::Invite,
            uri: Uri::try_from("sip:bot@example.com").unwrap(),
            headers: Headers::from(vec![
                Header::CallId(call_id.into()),
                Header::CSeq("1 INVITE".into()),
                Header::Via("SIP/2.0/UDP 10.0.0.5:5060;branch=z9hG4bK-test".into()),
            ]),
            version: Version::V2,
            body: vec![],
        })
    }

    fn ok_response(call_id: &str) -> SipMessage {
        SipMessage::Response(Response {
            headers: Headers::from(vec![
                Header::CallId(call_id.into()),
                Header::CSeq("1 INVITE".into()),
                Header::Via("SIP/2.0/UDP 10.0.0.5:5060;branch=z9hG4bK-test".into()),
            ]),
            ..Default::default()
        })
    }

    fn test_flow(max_calls: usize, max_items: usize) -> SipFlow {
        SipFlow::new(
            SipFlowConfig {
                max_calls: Some(max_calls),
                max_items_per_call: Some(max_items),
                ..Default::default()
            },
            "192.168.1.10:5060".to_string(),
            None,
        )
    }

    fn udp_addr(ip: &str) -> SipAddr {
        SipAddr {
            addr: rsipstack::rsip::HostWithPort::try_from(format!("{ip}:5060")).unwrap(),
            r#type: Some(rsipstack::sip::Transport::Udp),
        }
    }

    #[test]
    fn records_both_directions_with_addrs() {
        let flow = test_flow(16, 100);

        flow.record_sip(false, &invite_request("c1"), Some(&udp_addr("10.0.0.5")));
        flow.record_sip(true, &ok_response("c1"), Some(&udp_addr("10.0.0.5")));

        let items = flow.query_flow("c1", 0, u64::MAX);
        assert_eq!(items.len(), 2);
        // Incoming: remote is src, local is dst.
        assert_eq!(items[0].src_addr, "10.0.0.5:5060");
        assert_eq!(items[0].dst_addr, "192.168.1.10:5060");
        // Outgoing: the other way around.
        assert_eq!(items[1].src_addr, "192.168.1.10:5060");
        assert_eq!(items[1].dst_addr, "10.0.0.5:5060");
        assert_eq!(items[0].call_id, "c1");
        assert_eq!(items[0].msg_type, "Sip");
        assert_eq!(items[0].leg, None);
        assert_eq!(items[0].seq, 0);
        assert_eq!(items[1].seq, 1);
        assert!(items[0].payload.starts_with("INVITE "));
        assert!(items[0].timestamp > 0);
    }

    #[test]
    fn falls_back_to_via_sent_by_without_transport_addr() {
        let flow = test_flow(16, 100);
        flow.record_sip(false, &invite_request("c1"), None);
        let items = flow.query_flow("c1", 0, u64::MAX);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].src_addr, "10.0.0.5:5060");
    }

    #[test]
    fn per_call_cap_drops_and_counts() {
        let flow = test_flow(16, 3);
        for _ in 0..5 {
            flow.record_sip(true, &ok_response("c1"), None);
        }
        let items = flow.query_flow("c1", 0, u64::MAX);
        assert_eq!(items.len(), 3);
        assert_eq!(flow.dropped_count(), 2);
    }

    #[test]
    fn lru_evicts_inactive_calls() {
        let flow = test_flow(2, 100);
        flow.record_sip(true, &ok_response("c1"), None);
        flow.record_sip(true, &ok_response("c2"), None);
        flow.record_sip(true, &ok_response("c1"), None); // promote c1
        flow.record_sip(true, &ok_response("c3"), None); // evicts c2

        assert_eq!(flow.tracked_calls(), 2);
        assert!(flow.query_flow("c2", 0, u64::MAX).is_empty());
        assert_eq!(flow.query_flow("c1", 0, u64::MAX).len(), 2);
        assert_eq!(flow.query_flow("c3", 0, u64::MAX).len(), 1);
    }

    #[test]
    fn query_filters_by_time_window() {
        let flow = test_flow(16, 100);
        flow.record_sip(true, &ok_response("c1"), None);
        let items = flow.query_flow("c1", 0, u64::MAX);
        let ts = items[0].timestamp;
        assert_eq!(flow.query_flow("c1", ts, ts).len(), 1);
        assert_eq!(flow.query_flow("c1", ts + 1, u64::MAX).len(), 0);
        assert_eq!(flow.query_flow("unknown", 0, u64::MAX).len(), 0);
    }

    #[test]
    fn session_association_and_eviction() {
        let flow = test_flow(16, 100);
        flow.associate("s.abc", "c1");
        flow.associate("s.abc", "c2");
        flow.associate("s.abc", "c1"); // dedup
        flow.associate("ref-1-s.abc", "c2"); // refer leg aliases the same dialog

        let mut ids = flow.get_session_call_ids("s.abc");
        ids.sort();
        assert_eq!(ids, vec!["c1".to_string(), "c2".to_string()]);
        assert_eq!(flow.get_session_call_ids("s.nope"), Vec::<String>::new());

        flow.evict("s.abc", &["c1".to_string(), "c2".to_string()]);
        assert!(flow.get_session_call_ids("s.abc").is_empty());
        assert_eq!(flow.tracked_calls(), 0);
    }

    #[test]
    fn jsonl_export_matches_rustpbx_shape() {
        let flow = test_flow(16, 100);
        flow.record_sip(false, &invite_request("c1"), Some(&udp_addr("10.0.0.5")));
        let items = flow.query_flow("c1", 0, u64::MAX);
        let jsonl = SipFlow::export_jsonl(&items);
        let lines: Vec<&str> = jsonl.lines().collect();
        assert_eq!(lines.len(), 1);
        let obj: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        for key in [
            "timestamp",
            "seq",
            "leg",
            "msg_type",
            "call_id",
            "src_addr",
            "dst_addr",
            "payload",
        ] {
            assert!(obj.get(key).is_some(), "missing key {key}");
        }
        assert_eq!(obj["msg_type"], "Sip");
        assert!(obj["leg"].is_null());
    }

    #[test]
    fn chained_inspector_sees_unmodified_message() {
        struct CountingInspector {
            seen: AtomicUsize,
        }
        impl MessageInspector for CountingInspector {
            fn before_send(&self, msg: SipMessage, _dest: Option<&SipAddr>) -> SipMessage {
                self.seen.fetch_add(1, Ordering::Relaxed);
                msg
            }
            fn after_received(&self, msg: SipMessage, _from: Option<&SipAddr>) -> SipMessage {
                self.seen.fetch_add(1, Ordering::Relaxed);
                msg
            }
        }

        let counter = Arc::new(CountingInspector {
            seen: AtomicUsize::new(0),
        });
        struct CountingHolder(Arc<CountingInspector>);
        impl MessageInspector for CountingHolder {
            fn before_send(&self, msg: SipMessage, dest: Option<&SipAddr>) -> SipMessage {
                self.0.before_send(msg, dest)
            }
            fn after_received(&self, msg: SipMessage, from: Option<&SipAddr>) -> SipMessage {
                self.0.after_received(msg, from)
            }
        }

        let flow = SipFlow::new(
            SipFlowConfig::default(),
            "192.168.1.10:5060".to_string(),
            None,
        );
        flow.set_next(Some(Box::new(CountingHolder(counter.clone()))));

        let dest = udp_addr("10.0.0.5");
        let out = flow.before_send(ok_response("c1"), Some(&dest));
        let inbound = flow.after_received(invite_request("c1"), Some(&dest));
        assert_eq!(counter.seen.load(Ordering::Relaxed), 2);
        // Messages pass through byte-identical.
        assert_eq!(out.to_string(), ok_response("c1").to_string());
        assert_eq!(inbound.to_string(), invite_request("c1").to_string());
        // And were recorded.
        assert_eq!(flow.query_flow("c1", 0, u64::MAX).len(), 2);
    }
}
