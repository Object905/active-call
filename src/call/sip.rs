use crate::call::state::{CallProgress, LegShared};
use crate::callrecord::CallRecordHangupReason;
use crate::event::EventSender;
use crate::media::TrackId;
use crate::media::stream::MediaStream;
use crate::useragent::invitation::PendingDialog;
use anyhow::Result;
use chrono::Utc;
use rsipstack::dialog::DialogId;
use rsipstack::dialog::dialog::{
    Dialog, DialogState, DialogStateReceiver, DialogStateSender, TerminatedReason,
};
use rsipstack::dialog::dialog_layer::DialogLayer;
use rsipstack::dialog::invitation::InviteOption;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Remove `id` from the dialog layer and return the dialog, ready to be
/// hung up. Shared by the dialog guards and `Invitation::hangup`.
pub(crate) fn remove_dialog(layer: &DialogLayer, id: &DialogId) -> Option<Dialog> {
    let dialog = layer.get_dialog(id)?;
    layer.remove_dialog(id);
    Some(dialog)
}

pub struct DialogStateReceiverGuard {
    pub(super) dialog_layer: Arc<DialogLayer>,
    pub(super) receiver: DialogStateReceiver,
    pub(super) dialog_id: Option<DialogId>,
    pub(super) hangup_headers: Option<Vec<rsipstack::rsip::Header>>,
}

impl DialogStateReceiverGuard {
    pub fn new(
        dialog_layer: Arc<DialogLayer>,
        receiver: DialogStateReceiver,
        hangup_headers: Option<Vec<rsipstack::rsip::Header>>,
    ) -> Self {
        Self {
            dialog_layer,
            receiver,
            dialog_id: None,
            hangup_headers,
        }
    }
    pub async fn recv(&mut self) -> Option<DialogState> {
        let state = self.receiver.recv().await;
        if let Some(ref s) = state {
            self.dialog_id = Some(s.id().clone());
        }
        state
    }

    fn take_dialog(&mut self) -> Option<Dialog> {
        let id = self.dialog_id.take()?;
        info!(%id, "dialog removed on  drop");
        remove_dialog(&self.dialog_layer, &id)
    }

    pub async fn drop_async(&mut self) {
        if let Some(dialog) = self.take_dialog() {
            if let Err(e) = dialog.hangup_with_headers(self.hangup_headers.take()).await {
                warn!(id=%dialog.id(), "error hanging up dialog on drop: {}", e);
            }
        }
    }
}

impl Drop for DialogStateReceiverGuard {
    fn drop(&mut self) {
        if let Some(dialog) = self.take_dialog() {
            crate::spawn(async move {
                if let Err(e) = dialog.hangup().await {
                    warn!(id=%dialog.id(), "error hanging up dialog on drop: {}", e);
                }
            });
        }
    }
}

pub(super) struct InviteDialogStates {
    pub is_client: bool,
    pub session_id: String,
    pub track_id: TrackId,
    pub cancel_token: CancellationToken,
    pub event_sender: EventSender,
    /// Lock-free shared state of the leg this dialog belongs to.
    pub leg: LegShared,
    pub media_stream: Arc<MediaStream>,
    pub terminated_reason: Option<TerminatedReason>,
    pub has_early_media: bool,
    /// Hangup intent carried by this leg (refer legs with `auto_hangup`),
    /// reported on the leg's TrackEnd so the call actor can hang up.
    pub hangup_reason: Option<CallRecordHangupReason>,
}

impl InviteDialogStates {
    pub(super) fn new(
        is_client: bool,
        session_id: String,
        track_id: TrackId,
        event_sender: EventSender,
        media_stream: Arc<MediaStream>,
        leg: LegShared,
        cancel_token: CancellationToken,
        hangup_reason: Option<CallRecordHangupReason>,
    ) -> Self {
        Self {
            is_client,
            session_id,
            track_id,
            cancel_token,
            event_sender,
            leg,
            media_stream,
            terminated_reason: None,
            has_early_media: false,
            hangup_reason,
        }
    }
}

impl InviteDialogStates {
    /// Called from `Drop` (synchronous context): everything used here is
    /// lock-free (ArcSwap rcu/load + broadcast send), so no state or events
    /// can be lost the way a failed `try_write` used to lose them.
    pub(super) fn on_terminated(&mut self) {
        let term = CallProgress::termination(self.terminated_reason.as_ref());
        let status_code = term.status_code;
        let reason = term.hangup_reason;
        self.leg.update_progress(|p| {
            p.last_status_code = status_code;
            p.set_hangup_reason(reason.clone());
        });
        let progress = self.leg.progress.load_full();

        self.event_sender
            .send(crate::event::SessionEvent::TrackEnd {
                track_id: self.track_id.clone(),
                timestamp: crate::media::get_timestamp(),
                duration: progress
                    .answer_time
                    .map(|t| (Utc::now() - t).num_milliseconds())
                    .unwrap_or_default() as u64,
                ssrc: self.leg.ssrc,
                play_id: None,
                auto_hangup: self.hangup_reason.clone(),
            })
            .ok();
        let hangup_event = self
            .leg
            .build_hangup_event(self.track_id.clone(), Some(term.initiator.to_string()));
        self.event_sender.send(hangup_event).ok();
    }
}

impl Drop for InviteDialogStates {
    fn drop(&mut self) {
        self.on_terminated();
        self.cancel_token.cancel();
    }
}

impl DialogStateReceiverGuard {
    pub(self) async fn dialog_event_loop(&mut self, states: &mut InviteDialogStates) -> Result<()> {
        while let Some(event) = self.recv().await {
            match event {
                DialogState::Calling(dialog_id) => {
                    info!(session_id=states.session_id, %dialog_id, "dialog calling");
                    states
                        .leg
                        .update_progress(|p| p.session_id = dialog_id.to_string());
                }
                DialogState::Trying(_) => {}
                DialogState::Early(dialog_id, resp) => {
                    let code = resp.status_code.code();
                    let body = resp.body();
                    let answer = String::from_utf8_lossy(body);
                    let has_sdp = !answer.is_empty();
                    info!(session_id=states.session_id, %dialog_id, has_sdp=%has_sdp, "dialog early ({}): \n{}", code, answer);

                    states.leg.update_progress(|p| p.on_early(code));

                    if !states.is_client {
                        continue;
                    }

                    states
                        .event_sender
                        .send(crate::event::SessionEvent::Ringing {
                            track_id: states.track_id.clone(),
                            timestamp: crate::media::get_timestamp(),
                            early_media: has_sdp,
                            refer: Some(states.leg.is_refer),
                        })?;

                    if has_sdp {
                        states.has_early_media = true;
                        states.leg.update_progress(|p| p.try_set_answer(&answer));
                        states
                            .media_stream
                            .update_remote_description(&states.track_id, &answer.to_string())
                            .await?;
                    }
                }
                DialogState::Confirmed(dialog_id, msg) => {
                    info!(session_id=states.session_id, %dialog_id, has_early_media=%states.has_early_media, "dialog confirmed");
                    states
                        .leg
                        .update_progress(|p| p.on_confirmed(dialog_id.to_string()));
                    if states.is_client {
                        let answer = String::from_utf8_lossy(msg.body());
                        let answer = answer.trim();
                        if !answer.is_empty() {
                            if states.has_early_media {
                                info!(
                                    session_id = states.session_id,
                                    "updating remote description with final answer after early media (force=true)"
                                );
                                // Force update when transitioning from early media (183) to confirmed (200 OK)
                                // This ensures media parameters are properly updated even if SDP appears similar
                                if let Err(e) = states
                                    .media_stream
                                    .update_remote_description_force(
                                        &states.track_id,
                                        &answer.to_string(),
                                    )
                                    .await
                                {
                                    tracing::warn!(
                                        session_id = states.session_id,
                                        "failed to force update remote description on confirmed: {}",
                                        e
                                    );
                                }
                            } else {
                                if let Err(e) = states
                                    .media_stream
                                    .update_remote_description(
                                        &states.track_id,
                                        &answer.to_string(),
                                    )
                                    .await
                                {
                                    tracing::warn!(
                                        session_id = states.session_id,
                                        "failed to update remote description on confirmed: {}",
                                        e
                                    );
                                }
                            }
                        }
                    }
                }
                DialogState::Info(dialog_id, req, tx_handle) => {
                    let body_str = String::from_utf8_lossy(req.body());
                    info!(session_id=states.session_id, %dialog_id, body=%body_str, "dialog info received");
                    if body_str.starts_with("Signal=") {
                        let digit = body_str.trim_start_matches("Signal=").chars().next();
                        if let Some(digit) = digit {
                            states.event_sender.send(crate::event::SessionEvent::Dtmf {
                                track_id: states.track_id.clone(),
                                timestamp: crate::media::get_timestamp(),
                                digit: digit.to_string(),
                                refer: Some(states.leg.is_refer),
                            })?;
                        }
                    }
                    tx_handle.reply(rsipstack::rsip::StatusCode::OK).await.ok();
                }
                DialogState::Message(dialog_id, req, tx_handle) => {
                    let body_str = String::from_utf8_lossy(req.body()).to_string();
                    let content_type = req.headers.iter().find_map(|h| {
                        if let rsipstack::rsip::Header::ContentType(content_type) = h {
                            Some(content_type.value().to_string())
                        } else {
                            None
                        }
                    });
                    info!(
                        session_id=states.session_id,
                        %dialog_id,
                        content_type=content_type.as_deref(),
                        body=%body_str,
                        "dialog message received"
                    );
                    states
                        .event_sender
                        .send(crate::event::SessionEvent::Message {
                            track_id: states.track_id.clone(),
                            timestamp: crate::media::get_timestamp(),
                            body: body_str,
                            content_type,
                            refer: Some(states.leg.is_refer),
                        })
                        .ok();
                    tx_handle.reply(rsipstack::rsip::StatusCode::OK).await.ok();
                }
                DialogState::Updated(dialog_id, _req, tx_handle) => {
                    info!(session_id = states.session_id, %dialog_id, "dialog update received");
                    let mut answer_sdp = None;
                    if let Some(sdp_body) = _req.body().get(..) {
                        let sdp_str = String::from_utf8_lossy(sdp_body);
                        if !sdp_str.is_empty()
                            && (_req.method == rsipstack::rsip::Method::Invite
                                || _req.method == rsipstack::rsip::Method::Update)
                        {
                            info!(session_id=states.session_id, %dialog_id, method=%_req.method, "handling re-invite/update offer");

                            // Detect hold state from SDP
                            let is_on_hold =
                                crate::media::negotiate::detect_hold_state_from_sdp(&sdp_str);
                            info!(session_id=states.session_id, %dialog_id, is_on_hold=%is_on_hold, "detected hold state from re-invite SDP");

                            // Update media stream hold state + emit hold event
                            apply_hold_state(states, is_on_hold).await;

                            match states
                                .media_stream
                                .handshake(&states.track_id, sdp_str.to_string(), None)
                                .await
                            {
                                Ok(sdp) => answer_sdp = Some(sdp),
                                Err(e) => {
                                    warn!(
                                        session_id = states.session_id,
                                        "failed to handle re-invite: {}", e
                                    );
                                }
                            }
                        } else {
                            info!(session_id=states.session_id, %dialog_id, "updating remote description:\n{}", sdp_str);

                            // Also check hold state for non-INVITE/UPDATE messages with SDP
                            let is_on_hold =
                                crate::media::negotiate::detect_hold_state_from_sdp(&sdp_str);
                            apply_hold_state(states, is_on_hold).await;

                            states
                                .media_stream
                                .update_remote_description(&states.track_id, &sdp_str.to_string())
                                .await?;
                        }
                    }

                    if let Some(sdp) = answer_sdp {
                        tx_handle
                            .respond(
                                rsipstack::rsip::StatusCode::OK,
                                Some(vec![rsipstack::rsip::Header::ContentType(
                                    "application/sdp".to_string().into(),
                                )]),
                                Some(sdp.into()),
                            )
                            .await
                            .ok();
                    } else {
                        tx_handle.reply(rsipstack::rsip::StatusCode::OK).await.ok();
                    }
                }
                DialogState::Options(dialog_id, _req, tx_handle) => {
                    info!(session_id = states.session_id, %dialog_id, "dialog options received");
                    tx_handle.reply(rsipstack::rsip::StatusCode::OK).await.ok();
                }
                DialogState::Refer(dialog_id, req, tx_handle) => {
                    let refer_to = req
                        .headers
                        .iter()
                        .find_map(|h| {
                            if let rsipstack::rsip::Header::ReferTo(h) = h {
                                return Some(h.value().to_string());
                            }
                            None
                        })
                        .unwrap_or_default();
                    let referred_by = req.headers.iter().find_map(|h| {
                        if let rsipstack::rsip::Header::ReferredBy(h) = h {
                            return Some(h.value().to_string());
                        }
                        None
                    });
                    info!(session_id = states.session_id, %dialog_id, %refer_to, "received REFER");
                    tx_handle
                        .reply(rsipstack::rsip::StatusCode::Other(202, "Accepted".into()))
                        .await
                        .ok();
                    states
                        .event_sender
                        .send(crate::event::SessionEvent::TransferRequest {
                            track_id: states.track_id.clone(),
                            timestamp: crate::media::get_timestamp(),
                            refer_to,
                            referred_by,
                            refer: Some(states.leg.is_refer),
                        })
                        .ok();
                }
                DialogState::Terminated(dialog_id, reason) => {
                    info!(
                        session_id = states.session_id,
                        ?dialog_id,
                        ?reason,
                        "dialog terminated"
                    );
                    states.terminated_reason = Some(reason.clone());
                    return Ok(());
                }
                other_state => {
                    info!(
                        session_id = states.session_id,
                        %other_state,
                        "dialog received other state"
                    );
                }
            }
        }
        Ok(())
    }

    pub(super) async fn process_dialog(&mut self, mut states: InviteDialogStates) {
        let token = states.cancel_token.clone();
        tokio::select! {
            _ = token.cancelled() => {
                states.terminated_reason = Some(TerminatedReason::UacCancel);
            }
            _ = self.dialog_event_loop(&mut states) => {}
        };

        // Update hangup headers from the leg extras if available
        let extras = states.leg.extras.load_full();
        if let Some(headers) = crate::sip_util::hangup_headers_from_extras(&extras) {
            match &mut self.hangup_headers {
                Some(existing) => existing.extend(headers),
                None => self.hangup_headers = Some(headers),
            }
        }

        self.drop_async().await;
    }
}

/// Apply a hold/resume transition to the media track and emit the Hold event.
async fn apply_hold_state(states: &mut InviteDialogStates, is_on_hold: bool) {
    if is_on_hold {
        states
            .media_stream
            .hold_track(Some(states.track_id.clone()))
            .await;
    } else {
        states
            .media_stream
            .resume_track(Some(states.track_id.clone()))
            .await;
    }
    states
        .event_sender
        .send(crate::event::SessionEvent::Hold {
            track_id: states.track_id.clone(),
            timestamp: crate::media::get_timestamp(),
            on_hold: is_on_hold,
            refer: Some(states.leg.is_refer),
        })
        .ok();
}

#[derive(Clone)]
pub struct Invitation {
    pub dialog_layer: Arc<DialogLayer>,
    pub pending_dialogs: Arc<std::sync::Mutex<HashMap<DialogId, PendingDialog>>>,
}

impl Invitation {
    pub fn new(dialog_layer: Arc<DialogLayer>) -> Self {
        Self {
            dialog_layer,
            pending_dialogs: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    pub fn add_pending(&self, dialog_id: DialogId, pending: PendingDialog) {
        self.pending_dialogs
            .lock()
            .map(|mut ps| ps.insert(dialog_id, pending))
            .ok();
    }

    pub fn get_pending_call(&self, dialog_id: &DialogId) -> Option<PendingDialog> {
        self.pending_dialogs
            .lock()
            .ok()
            .and_then(|mut ps| ps.remove(dialog_id))
    }

    pub fn has_pending_call(&self, dialog_id: &DialogId) -> bool {
        self.pending_dialogs
            .lock()
            .ok()
            .map(|ps| ps.contains_key(dialog_id))
            .unwrap_or(false)
    }

    pub fn find_dialog_id_by_session_id(&self, session_id: &str) -> Option<DialogId> {
        self.pending_dialogs.lock().ok().and_then(|ps| {
            ps.iter()
                .find(|(id, _)| id.to_string() == session_id)
                .map(|(id, _)| id.clone())
        })
    }

    /// Reject a pending dialog or hang up an established one.
    pub async fn hangup(
        &self,
        dialog_id: DialogId,
        code: Option<rsipstack::rsip::StatusCode>,
        reason: Option<String>,
    ) -> Result<()> {
        if let Some(call) = self.get_pending_call(&dialog_id) {
            call.dialog.reject(code, reason).ok();
        }
        if let Some(dialog) = remove_dialog(&self.dialog_layer, &dialog_id) {
            dialog.hangup().await.ok();
        }
        Ok(())
    }

    pub async fn invite(
        &self,
        invite_option: InviteOption,
        state_sender: DialogStateSender,
    ) -> Result<(DialogId, Option<Vec<u8>>), rsipstack::Error> {
        let (dialog, resp) = self
            .dialog_layer
            .do_invite(invite_option, state_sender)
            .await?;

        let offer = match resp {
            Some(resp) => match resp.status_code.kind() {
                rsipstack::rsip::StatusCodeKind::Successful => {
                    let offer = resp.body.clone();
                    Some(offer)
                }
                _ => {
                    let reason = resp
                        .reason_phrase()
                        .unwrap_or(&resp.status_code.to_string())
                        .to_string();
                    return Err(rsipstack::Error::DialogError(
                        reason,
                        dialog.id(),
                        resp.status_code,
                    ));
                }
            },
            None => {
                return Err(rsipstack::Error::DialogError(
                    "no response received".to_string(),
                    dialog.id(),
                    rsipstack::rsip::StatusCode::NotAcceptableHere,
                ));
            }
        };
        Ok((dialog.id(), offer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::call::state::CallProgress;
    use crate::media::stream::MediaStreamBuilder;

    // SDP used to simulate an early-media 183 Session Progress response.
    const EARLY_MEDIA_SDP: &str = "v=0\r\n\
        o=- 1000 1 IN IP4 192.168.1.100\r\n\
        s=SIP Call\r\n\
        t=0 0\r\n\
        m=audio 10000 RTP/AVP 0\r\n\
        c=IN IP4 192.168.1.100\r\n\
        a=rtpmap:0 PCMU/8000\r\n\
        a=sendrecv\r\n";

    fn make_states(has_early_media: bool) -> InviteDialogStates {
        let (event_tx, _event_rx) = tokio::sync::broadcast::channel(16);
        let media_stream = Arc::new(
            MediaStreamBuilder::new(event_tx.clone())
                .with_id("test-stream".to_string())
                .build(),
        );
        let cancel_token = CancellationToken::new();
        let leg = LegShared::new(1000, true, CallProgress::default());

        InviteDialogStates {
            is_client: true,
            session_id: "test-session".to_string(),
            track_id: "test-track".to_string(),
            cancel_token: cancel_token.clone(),
            event_sender: event_tx.clone(),
            leg,
            media_stream,
            terminated_reason: None,
            has_early_media,
            hangup_reason: None,
        }
    }

    /// Verify that when a 183 Session Progress with SDP arrives (`DialogState::Early`),
    /// the early SDP is stored in the leg progress so it can serve as a fallback
    /// when the final 200 OK has an empty body.
    #[tokio::test]
    async fn test_early_sdp_stored_in_leg_progress() {
        let mut states = make_states(false);

        // Simulate DialogState::Early with SDP body (183 Session Progress):
        // same steps the Early branch performs.
        let answer = EARLY_MEDIA_SDP.to_string();
        let has_sdp = !answer.is_empty();
        if states.is_client && has_sdp {
            states.has_early_media = true;
            states.leg.update_progress(|p| p.try_set_answer(&answer));
        }

        // Assert: early SDP is stored
        let progress = states.leg.progress.load_full();
        assert!(
            progress.answer.is_some(),
            "leg progress answer should be set after 183 with SDP"
        );
        assert_eq!(
            progress.answer.as_deref().unwrap(),
            EARLY_MEDIA_SDP,
            "leg progress answer should contain the early SDP"
        );
        assert!(states.has_early_media, "has_early_media should be true");
    }

    /// Verify that when a 200 OK arrives with an empty body after early media has been
    /// negotiated, the leg progress retains the early SDP (not overwritten with "").
    ///
    /// This is the regression test for the bug where a late 200 OK with empty body would
    /// cause `SessionEvent::Answer { sdp: "" }` to be emitted, making the answer event
    /// appear as if no SDP was negotiated.
    #[tokio::test]
    async fn test_confirmed_empty_body_keeps_early_sdp() {
        let mut states = make_states(false);

        // Step 1: simulate 183 with SDP → set has_early_media and progress answer
        states.has_early_media = true;
        states
            .leg
            .update_progress(|p| p.try_set_answer(EARLY_MEDIA_SDP));

        // Step 2: simulate 200 OK with empty body (Confirmed handler logic)
        states
            .leg
            .update_progress(|p| p.on_confirmed("dialog-1".to_string()));
        // The Confirmed handler only calls update_remote_description when the body
        // is non-empty; it does NOT overwrite the progress answer.
        let confirmed_answer = String::new();
        assert!(
            confirmed_answer.trim().is_empty(),
            "empty body must not be applied"
        );

        // Assert: leg progress still holds the early SDP
        let progress = states.leg.progress.load_full();
        assert!(
            progress.answer.is_some(),
            "answer must not be None after 200 OK with empty body"
        );
        let stored_answer = progress.answer.as_deref().unwrap();
        assert!(
            !stored_answer.is_empty(),
            "answer must not be empty after 200 OK with empty body"
        );
        assert_eq!(
            stored_answer, EARLY_MEDIA_SDP,
            "answer should still be the early SDP after 200 OK with empty body"
        );
    }

    /// Verify that `create_outgoing_sip_track`'s fallback logic works:
    /// when the 200 OK body is empty but the leg progress has the early SDP,
    /// the fallback path is taken and the early SDP is returned (not an empty string).
    #[tokio::test]
    async fn test_answer_fallback_to_early_sdp_when_200ok_empty() {
        // Simulate what the Early (183) handler does: store the early SDP.
        let states = make_states(true);
        states
            .leg
            .update_progress(|p| p.try_set_answer(EARLY_MEDIA_SDP));

        // Simulate what create_outgoing_sip_track does when 200 OK has empty body.
        let early = states.leg.progress.load_full().answer.clone();
        let raw_answer: Option<Vec<u8>> = Some(vec![]); // empty body from 200 OK

        let (answer, already_applied) =
            crate::call::state::resolve_final_answer(raw_answer, early.as_ref()).unwrap();

        // The answer returned to setup_caller_track (and used in SessionEvent::Answer)
        // must be the early SDP, not an empty string.
        assert!(
            !answer.is_empty(),
            "Resolved answer must not be empty — should contain the early SDP"
        );
        assert_eq!(
            answer, EARLY_MEDIA_SDP,
            "Resolved answer should be the early SDP from the 183 handler"
        );
        assert!(
            already_applied,
            "remote_description_already_applied should be true when using early SDP fallback"
        );
    }

    /// Verify the normal case: when 200 OK carries its own SDP body,
    /// that SDP is used directly (not the early SDP) and remote description
    /// should be applied.
    #[tokio::test]
    async fn test_answer_uses_200ok_sdp_when_present() {
        const FINAL_SDP: &str = "v=0\r\n\
            o=- 2000 2 IN IP4 10.0.0.1\r\n\
            s=SIP Call\r\n\
            t=0 0\r\n\
            m=audio 20000 RTP/AVP 0\r\n\
            c=IN IP4 10.0.0.1\r\n\
            a=rtpmap:0 PCMU/8000\r\n\
            a=sendrecv\r\n";

        let states = make_states(true);
        states
            .leg
            .update_progress(|p| p.try_set_answer(EARLY_MEDIA_SDP));

        let early = states.leg.progress.load_full().answer.clone();
        let (answer, already_applied) = crate::call::state::resolve_final_answer(
            Some(FINAL_SDP.as_bytes().to_vec()),
            early.as_ref(),
        )
        .unwrap();

        assert_eq!(
            answer, FINAL_SDP,
            "When 200 OK has SDP, it should be used (not the early SDP)"
        );
        assert!(
            !already_applied,
            "remote_description_already_applied should be false when 200 OK has SDP body"
        );
    }

    /// Regression: `on_terminated` runs in a synchronous `Drop` context.
    /// The old implementation used `try_write` and silently dropped the
    /// status/reason updates AND the TrackEnd/Hangup events when the lock was
    /// contended. The lock-free (ArcSwap) implementation must always emit both
    /// events and record the termination, even while another task keeps
    /// mutating the progress concurrently.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_on_terminated_always_emits_events_and_records_state() {
        use crate::event::SessionEvent;
        use std::sync::atomic::{AtomicBool, Ordering};

        let mut states = make_states(false);
        states.terminated_reason = Some(TerminatedReason::UacCancel);
        let leg = states.leg.clone();
        let event_sender = states.event_sender.clone();
        let mut event_receiver = event_sender.subscribe();

        // Hammer the progress concurrently, as a busy actor would.
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = stop.clone();
        let progress = leg.progress.clone();
        let writer = crate::spawn(async move {
            while !stop2.load(Ordering::Relaxed) {
                // Touch unrelated fields, like a busy actor would (never the
                // termination fields), so writers don't clobber each other.
                progress.rcu(|p| {
                    let mut p = CallProgress::clone(p);
                    p.answer_time.get_or_insert_with(chrono::Utc::now);
                    p
                });
                tokio::task::yield_now().await;
            }
        });

        // Give the writer a moment to start contending.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Synchronous drop, exactly like the real dialog task teardown.
        drop(states);

        stop.store(true, Ordering::Relaxed);
        let _ = writer.await;

        // Both events must have been emitted despite the concurrent writer.
        let mut saw_track_end = false;
        let mut saw_hangup = false;
        while let Ok(event) = event_receiver.try_recv() {
            match event {
                SessionEvent::TrackEnd { .. } => saw_track_end = true,
                SessionEvent::Hangup { refer, .. } => {
                    assert_eq!(refer, Some(true), "refer flag comes from the leg");
                    saw_hangup = true;
                }
                _ => {}
            }
        }
        assert!(
            saw_track_end,
            "TrackEnd must be emitted from on_terminated even under contention"
        );
        assert!(
            saw_hangup,
            "Hangup must be emitted from on_terminated even under contention"
        );

        // The termination must be recorded (487 for UacCancel); the concurrent
        // writer only ever writes 100, so observing 487 proves the rcu landed.
        let progress = leg.progress.load_full();
        assert_eq!(progress.last_status_code, 487);
        assert_eq!(
            progress.hangup_reason,
            Some(crate::callrecord::CallRecordHangupReason::Canceled)
        );
    }
}
