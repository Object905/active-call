//! Callrecord hook that uploads a session's captured SIP signaling as one
//! JSONL document when the call ends (the active-call counterpart of
//! rustpbx's `SipFlowUploadHook`).
//!
//! All dialogs of the session — primary leg, bridge legs and REFER transfer
//! legs — are merged into a single time-sorted JSONL named after the root
//! session id (`{YYYYMMDD}/{session_id}.jsonl` on S3). The upload runs in a
//! background task so the callrecord pipeline is never blocked; the flows
//! are evicted from memory once handled.

use super::{SipFlow, SipFlowUploadConfig};
use crate::callrecord::{CallRecord, CallRecordHook};
use std::sync::Arc;

/// Slack added around the call window so the first INVITE (arrival may
/// predate the recorded `start_time`) and the BYE tail are included.
const WINDOW_SLACK_SECS: i64 = 10;

pub struct SipFlowUploadHook {
    pub flow: Arc<SipFlow>,
    /// `None` (capture without upload) still evicts the flows at hangup.
    pub upload: Option<SipFlowUploadConfig>,
}

#[async_trait::async_trait]
impl CallRecordHook for SipFlowUploadHook {
    async fn on_record_completed(&self, record: &mut CallRecord) -> anyhow::Result<()> {
        let session_id = record.call_id.clone();
        let mut call_ids = record.sip_call_ids.clone();
        if let Some(refer) = record.refer_callrecord.as_ref() {
            for call_id in &refer.sip_call_ids {
                if !call_ids.contains(call_id) {
                    call_ids.push(call_id.clone());
                }
            }
        }
        if call_ids.is_empty() {
            return Ok(());
        }
        let start_us = (record.start_time - chrono::Duration::seconds(WINDOW_SLACK_SECS))
            .timestamp_micros()
            .max(0) as u64;
        let end_us = (record.end_time + chrono::Duration::seconds(WINDOW_SLACK_SECS))
            .timestamp_micros()
            .max(0) as u64;
        let flow = self.flow.clone();
        let upload = self.upload.clone();
        crate::spawn(async move {
            let mut items = Vec::new();
            for call_id in &call_ids {
                items.extend(flow.query_flow(call_id, start_us, end_us));
            }
            if !items.is_empty() {
                items.sort_by_key(|item| (item.timestamp, item.seq));
                if let Some(config) = &upload {
                    upload_jsonl(config, &session_id, &items).await;
                }
            } else {
                tracing::debug!(
                    session_id,
                    "sipflow: no captured signaling for session, skipping upload"
                );
            }
            flow.evict(&session_id, &call_ids);
        });
        Ok(())
    }
}

async fn upload_jsonl(
    config: &SipFlowUploadConfig,
    session_id: &str,
    items: &[super::SipFlowItem],
) {
    let jsonl = SipFlow::export_jsonl(items);
    let date = chrono::Local::now().format("%Y%m%d").to_string();
    let started = std::time::Instant::now();
    let result = match config {
        SipFlowUploadConfig::S3 {
            vendor,
            bucket,
            region,
            access_key,
            secret_key,
            endpoint,
            root,
        } => {
            let key = match root.trim().is_empty() {
                true => format!("{date}/{session_id}.jsonl"),
                false => format!("{}/{date}/{session_id}.jsonl", root.trim_matches('/')),
            };
            upload_to_s3(
                vendor, bucket, region, access_key, secret_key, endpoint, &key, jsonl.as_bytes(),
            )
            .await
            .map(|_| key)
        }
        SipFlowUploadConfig::Http {
            url,
            headers,
            file_field,
            file_name,
        } => upload_to_http(url, headers, file_field.as_deref(), file_name.as_deref(), session_id, jsonl.as_bytes())
            .await
            .map(|_| url.clone()),
    };
    match result {
        Ok(target) => {
            tracing::info!(
                session_id,
                elapsed = started.elapsed().as_secs_f64(),
                bytes = jsonl.len(),
                %target,
                "sipflow signaling uploaded"
            );
        }
        Err(e) => {
            tracing::warn!(session_id, error = %e, "sipflow signaling upload failed");
        }
    }
}

async fn upload_to_s3(
    vendor: &crate::config::S3Vendor,
    bucket: &str,
    region: &str,
    access_key: &str,
    secret_key: &str,
    endpoint: &str,
    key: &str,
    bytes: &[u8],
) -> anyhow::Result<String> {
    use object_store::ObjectStoreExt;
    let store = crate::callrecord::build_object_store_from_s3(
        vendor, bucket, region, access_key, secret_key, endpoint,
    )?;
    let path = object_store::path::Path::from(key);
    store.put(&path, object_store::PutPayload::from(bytes.to_vec())).await?;
    Ok(key.to_string())
}

async fn upload_to_http(
    url: &str,
    headers: &Option<std::collections::HashMap<String, String>>,
    file_field: Option<&str>,
    file_name: Option<&str>,
    session_id: &str,
    bytes: &[u8],
) -> anyhow::Result<()> {
    let part = reqwest::multipart::Part::bytes(bytes.to_vec())
        .file_name(file_name.unwrap_or("signaling").replace("{session_id}", session_id))
        .mime_str("application/jsonl")?;
    let form = reqwest::multipart::Form::new()
        .part(file_field.unwrap_or("signaling").to_string(), part);
    let client = reqwest::Client::new();
    let mut request = client.post(url).multipart(form);
    if let Some(headers) = headers {
        for (key, value) in headers {
            request = request.header(key, value);
        }
    }
    let response = request.send().await?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("HTTP upload failed with status: {status} - {body}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sipflow::SipFlowConfig;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn hook_with_http(url: String) -> SipFlowUploadHook {
        SipFlowUploadHook {
            flow: Arc::new(SipFlow::new(SipFlowConfig::default(), "127.0.0.1:5060".into(), None)),
            upload: Some(SipFlowUploadConfig::Http {
                url,
                headers: None,
                file_field: Some("signaling".into()),
                file_name: Some("{session_id}.jsonl".into()),
            }),
        }
    }

    fn sample_record(session_id: &str, call_ids: Vec<String>) -> CallRecord {
        CallRecord {
            call_type: crate::call::ActiveCallType::Sip,
            option: None,
            call_id: session_id.to_string(),
            start_time: chrono::Utc::now() - chrono::Duration::seconds(30),
            ring_time: None,
            answer_time: Some(chrono::Utc::now() - chrono::Duration::seconds(20)),
            end_time: chrono::Utc::now(),
            caller: "sip:a@x".into(),
            callee: "sip:b@x".into(),
            status_code: 200,
            hangup_reason: None,
            hangup_messages: Vec::new(),
            recorder: Vec::new(),
            extras: None,
            dump_event_file: None,
            refer_callrecord: None,
            sip_call_ids: call_ids,
        }
    }

    #[tokio::test]
    async fn uploads_merged_jsonl_and_evicts_flows() {
        let server = MockServer::start().await;
        server
            .register(
                Mock::given(method("POST"))
                    .and(path("/upload"))
                    .respond_with(ResponseTemplate::new(200))
                    .expect(1),
            )
            .await;

        let hook = hook_with_http(format!("{}/upload", server.uri()));
        hook.flow.record_sip(
            false,
            &rsipstack::rsip::SipMessage::Response(rsipstack::rsip::Response {
                headers: rsipstack::rsip::Headers::from(vec![rsipstack::rsip::Header::CallId(
                    "c1".into(),
                )]),
                ..Default::default()
            }),
            None,
        );

        let mut record = sample_record("s.abc", vec!["c1".to_string()]);
        hook.on_record_completed(&mut record).await.unwrap();
        // Upload runs in a background task; give it a moment.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        let body = String::from_utf8(requests[0].body.clone()).unwrap();
        assert!(body.contains("\"call_id\":\"c1\""), "jsonl body: {body}");

        // Flows evicted after the upload.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(hook.flow.tracked_calls(), 0);
        assert!(hook.flow.get_session_call_ids("s.abc").is_empty());
    }

    #[tokio::test]
    async fn evicts_even_without_upload_config() {
        let hook = SipFlowUploadHook {
            flow: Arc::new(SipFlow::new(SipFlowConfig::default(), "127.0.0.1:5060".into(), None)),
            upload: None,
        };
        hook.flow.associate("s.x", "c9");
        hook.flow.record_sip(
            true,
            &rsipstack::rsip::SipMessage::Response(rsipstack::rsip::Response {
                headers: rsipstack::rsip::Headers::from(vec![rsipstack::rsip::Header::CallId(
                    "c9".into(),
                )]),
                ..Default::default()
            }),
            None,
        );
        let mut record = sample_record("s.x", vec!["c9".to_string()]);
        hook.on_record_completed(&mut record).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(hook.flow.tracked_calls(), 0);
        assert!(hook.flow.get_session_call_ids("s.x").is_empty());
    }
}
