//! Track construction and wiring for [`ActiveCall`].
//!
//! Split out of `active_call.rs` so the call-flow orchestration stays separate
//! from the (heavily reused) track assembly building blocks.

use super::active_call::{ActiveCall, ActiveCallType, PendingCallerTrack};
use super::state::{CallRuntime, LegShared};
use crate::CallOption;
use crate::event::SessionEvent;
use crate::media::TrackId;
use crate::media::ambiance::SharedAmbianceProcessor;
use crate::media::engine::StreamEngine;
use crate::media::negotiate::strip_ipv6_candidates;
use crate::media::processor::SubscribeProcessor;
use crate::media::track::Track;
use crate::media::track::file::FileTrack;
use crate::media::track::rtc::{RtcTrack, RtcTrackConfig};
use crate::media::track::websocket::{WebsocketBytesReceiver, WebsocketTrack};
use crate::useragent::invitation::PendingDialog;
use crate::useragent::public_address::{
    build_public_contact_uri, contact_needs_public_resolution, find_local_addr_for_uri,
};
use anyhow::Result;
use audio_codec::CodecType;
use chrono::Utc;
use rsipstack::dialog::invitation::InviteOption;
use rsipstack::rsip::prelude::HeadersExt;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::sip::{DialogStateReceiverGuard, InviteDialogStates};

impl ActiveCall {
    /// Apply the configured codec list to an RTC track config.
    fn rtc_apply_codecs(&self, rtc_config: &mut RtcTrackConfig) {
        if let Some(codecs) = &self.app_state.config.codecs {
            let mut codec_types = Vec::new();
            for c in codecs {
                match c.to_lowercase().as_str() {
                    "pcmu" => codec_types.push(CodecType::PCMU),
                    "pcma" => codec_types.push(CodecType::PCMA),
                    "g722" => codec_types.push(CodecType::G722),
                    "g729" => codec_types.push(CodecType::G729),
                    "opus" => codec_types.push(CodecType::Opus),
                    "dtmf" | "2833" | "telephone_event" => {
                        codec_types.push(CodecType::TelephoneEvent)
                    }
                    _ => {}
                }
            }
            if !codec_types.is_empty() {
                rtc_config.preferred_codec = Some(codec_types[0].clone());
                rtc_config.codecs = codec_types;
            }
        }
    }

    /// Apply external/bind addresses from the global config.
    fn rtc_apply_network(&self, rtc_config: &mut RtcTrackConfig) {
        if let Some(ref external_ip) = self.app_state.config.external_ip {
            rtc_config.external_ip = Some(external_ip.clone());
        }
        if let Some(ref bind_ip) = self.app_state.config.rtp_bind_ip {
            rtc_config.bind_ip = Some(bind_ip.clone());
        }
    }

    /// Apply RTP latching and ICE-lite flags (per-call option overrides config).
    fn rtc_apply_latching(&self, rtc_config: &mut RtcTrackConfig) {
        rtc_config.enable_latching = self.app_state.config.enable_rtp_latching;
        rtc_config.enable_ice_lite = self
            .progress
            .load()
            .option
            .as_ref()
            .and_then(|o| o.enable_ice_lite)
            .or(self.app_state.config.enable_ice_lite);
    }

    /// Build a looping file track on the server-side track.
    pub(super) fn make_file_track(&self, path: String, ssrc: u32) -> FileTrack {
        FileTrack::new(self.server_side_track_id.clone())
            .with_play_id(Some(path.clone()))
            .with_ssrc(ssrc)
            .with_path(path)
            .with_cancel_token(self.cancel_token.child_token())
    }

    /// Emit a `Reject` session event from an rsipstack dialog error.
    pub(super) fn emit_reject_from_rsip_error(
        &self,
        track_id: TrackId,
        refer: bool,
        e: &rsipstack::Error,
    ) {
        if let rsipstack::Error::DialogError(reason, _, code) = e {
            self.event_sender
                .send(SessionEvent::Reject {
                    track_id,
                    timestamp: crate::media::get_timestamp(),
                    reason: reason.clone(),
                    code: Some(code.code() as u32),
                    refer: Some(refer),
                })
                .ok();
        }
    }

    /// If a pending incoming dialog exists for this session, start preparing
    /// the incoming SIP track; shared by the Sip and B2bua call types.
    pub(super) async fn try_prepare_incoming_sip_track(
        &self,
        runtime: &mut CallRuntime,
        hangup_headers: Option<Vec<rsipstack::rsip::Header>>,
    ) -> Option<Result<()>> {
        let dialog_id = self
            .invitation
            .find_dialog_id_by_session_id(&self.session_id)?;
        let pending_dialog = self.invitation.get_pending_call(&dialog_id)?;
        Some(
            self.prepare_incoming_sip_track(
                runtime,
                self.cancel_token.clone(),
                &self.session_id,
                pending_dialog,
                hangup_headers,
            )
            .await,
        )
    }

    pub(super) async fn create_rtp_track(
        &self,
        track_id: TrackId,
        ssrc: u32,
        enable_srtp: Option<bool>,
    ) -> Result<RtcTrack> {
        let mut rtc_config = RtcTrackConfig::default();
        // Per-call flag takes precedence over global config.
        let use_srtp = enable_srtp
            .or(self.app_state.config.enable_srtp)
            .unwrap_or(false);
        rtc_config.mode = if use_srtp {
            rustrtc::TransportMode::Srtp
        } else {
            rustrtc::TransportMode::Rtp
        };

        self.rtc_apply_codecs(&mut rtc_config);

        if rtc_config.preferred_codec.is_none() {
            rtc_config.preferred_codec = Some(self.track_config.codec.clone());
        }

        rtc_config.rtp_port_range = self
            .app_state
            .config
            .rtp_start_port
            .zip(self.app_state.config.rtp_end_port);

        self.rtc_apply_network(&mut rtc_config);
        self.rtc_apply_latching(&mut rtc_config);

        let mut track = RtcTrack::new(
            self.cancel_token.child_token(),
            track_id,
            self.track_config.clone(),
            rtc_config,
        )
        .with_ssrc(ssrc);

        track.create().await?;

        Ok(track)
    }

    pub(super) async fn setup_track_with_stream(
        &self,
        option: &CallOption,
        mut track: Box<dyn Track>,
    ) -> Result<()> {
        let processors = match StreamEngine::create_processors(
            self.app_state.stream_engine.clone(),
            track.id().clone(),
            self.cancel_token.child_token(),
            self.event_sender.clone(),
            self.media_stream.packet_sender.clone(),
            option,
        )
        .await
        {
            Ok(processors) => processors,
            Err(e) => {
                warn!(
                    session_id = self.session_id,
                    "failed to prepare stream processors: {}", e
                );
                vec![]
            }
        };

        // Add all processors from the hook
        for processor in processors {
            track.append_processor(processor);
        }

        self.update_track_wrapper(track, None).await;
        Ok(())
    }

    pub(super) async fn update_track_wrapper(
        &self,
        mut track: Box<dyn Track>,
        play_id: Option<String>,
    ) {
        let (ambiance_opt, subscribe) = {
            let state = self.progress.load_full();
            let mut opt = state
                .option
                .as_ref()
                .and_then(|o| o.ambiance.clone())
                .unwrap_or_default();

            if let Some(global) = &self.app_state.config.ambiance {
                opt.merge(global);
            }

            let subscribe = state
                .option
                .as_ref()
                .and_then(|o| o.subscribe)
                .unwrap_or_default();

            (opt, subscribe)
        };

        let shared_ambiance = match self
            .media_stream
            .ensure_ambiance(ambiance_opt, self.server_side_track_id.clone())
            .await
        {
            Ok(shared) => shared,
            Err(e) => {
                tracing::error!("failed to load ambiance wav {}", e);
                None
            }
        };

        if track.id() == &self.server_side_track_id {
            if let Some(shared) = shared_ambiance {
                info!(session_id = self.session_id, "loaded ambiance processor");
                track.append_processor(Box::new(SharedAmbianceProcessor::new(shared)));
            }
        }

        if subscribe && self.call_type != ActiveCallType::WebSocket {
            let (track_index, sub_track_id) = if track.id() == &self.server_side_track_id {
                (0, self.server_side_track_id.clone())
            } else {
                (1, self.session_id.clone())
            };
            let sub_processor =
                SubscribeProcessor::new(self.event_sender.clone(), sub_track_id, track_index);
            track.append_processor(Box::new(sub_processor));
        }

        self.set_current_play(play_id.clone());
        self.media_stream.update_track(track, play_id).await;
    }

    pub(super) async fn create_websocket_track(
        &self,
        audio_receiver: WebsocketBytesReceiver,
    ) -> Result<Box<dyn Track>> {
        let codec = self
            .progress
            .load_full()
            .option
            .as_ref()
            .map(|o| o.codec.clone())
            .unwrap_or_default();

        let ws_track = WebsocketTrack::new(
            self.cancel_token.child_token(),
            self.session_id.clone(),
            self.track_config.clone(),
            self.event_sender.clone(),
            audio_receiver,
            codec,
            self.ssrc,
        );

        self.leg().update_progress(|p| {
            p.answer = Some("".to_string());
            p.on_answered();
        });

        Ok(Box::new(ws_track))
    }

    pub(super) async fn create_webrtc_track(&self) -> Result<Box<dyn Track>> {
        let option = self.progress.load_full().option.clone().unwrap_or_default();
        let ssrc = self.ssrc;

        let mut rtc_config = RtcTrackConfig::default();
        rtc_config.mode = rustrtc::TransportMode::WebRtc; // WebRTC
        rtc_config.ice_servers = self.app_state.config.ice_servers.clone();

        self.rtc_apply_codecs(&mut rtc_config);
        self.rtc_apply_network(&mut rtc_config);

        let mut webrtc_track = RtcTrack::new(
            self.cancel_token.child_token(),
            self.session_id.clone(),
            self.track_config.clone(),
            rtc_config,
        )
        .with_ssrc(ssrc);

        let timeout = option.handshake_timeout.map(|t| Duration::from_secs(t));
        let offer = match option.enable_ipv6 {
            Some(false) | None => {
                strip_ipv6_candidates(option.offer.as_ref().unwrap_or(&"".to_string()))
            }
            _ => option.offer.clone().unwrap_or("".to_string()),
        };
        let answer: Option<String>;
        match webrtc_track.handshake(offer, timeout).await {
            Ok(answer_sdp) => {
                answer = match option.enable_ipv6 {
                    Some(false) | None => Some(strip_ipv6_candidates(&answer_sdp)),
                    Some(true) => Some(answer_sdp.to_string()),
                };
            }
            Err(e) => {
                warn!(session_id = self.session_id, "failed to setup track: {}", e);
                return Err(anyhow::anyhow!("Failed to setup track: {}", e));
            }
        }

        self.leg().update_progress(|p| {
            p.answer = answer.clone();
            p.on_answered();
        });
        Ok(Box::new(webrtc_track))
    }

    pub(super) async fn create_outgoing_sip_track(
        &self,
        cancel_token: CancellationToken,
        leg: LegShared,
        track_id: &String,
        mut invite_option: InviteOption,
        call_option: &CallOption,
        moh: Option<String>,
    ) -> Result<String, rsipstack::Error> {
        // Apply trunk rules (match + rewrite caller/callee/contact) to the
        // outgoing INVITE/REFER before it is sent. Covers both normal invite
        // calls and refer legs since both flow through this function.
        self.app_state.config.apply_trunk_rules(&mut invite_option);

        let ssrc = leg.ssrc;
        let per_call_srtp = call_option.sip.as_ref().and_then(|s| s.enable_srtp);
        let rtp_track = self
            .create_rtp_track(track_id.clone(), ssrc, per_call_srtp)
            .await
            .map_err(|e| rsipstack::Error::Error(e.to_string()))?;

        let offer = Some(
            rtp_track
                .local_description()
                .await
                .map_err(|e| rsipstack::Error::Error(e.to_string()))?,
        );

        leg.update_progress(|p| {
            if let Some(o) = p.option.as_mut() {
                o.offer = offer.clone();
            }
            p.start_time = Some(Utc::now());
        });

        invite_option.offer = offer.clone().map(|s| s.into());

        // Set contact to local SIP endpoint address if not already set explicitly
        // Check if contact is still default (no scheme set) or if host is localhost-like
        let needs_contact = contact_needs_public_resolution(&invite_option.contact);

        if needs_contact {
            let addrs = self.invitation.dialog_layer.endpoint.get_addrs();
            if let Some(addr) = find_local_addr_for_uri(&addrs, &invite_option.callee) {
                let contact_username = invite_option
                    .contact
                    .auth
                    .as_ref()
                    .map(|auth| auth.user.as_str())
                    .or_else(|| {
                        invite_option
                            .caller
                            .auth
                            .as_ref()
                            .map(|auth| auth.user.as_str())
                    });
                invite_option.contact = build_public_contact_uri(
                    &self.app_state.learned_public_address,
                    self.app_state.auto_learn_public_address_enabled(),
                    &addr,
                    contact_username,
                    Some(&invite_option.contact),
                );
            } else {
                return Err(rsipstack::Error::Error(format!(
                    "missing local SIP address for callee transport: {}",
                    invite_option.callee
                )));
            }
        }

        let mut rtp_track_to_setup = Some(Box::new(rtp_track) as Box<dyn Track>);

        if let Some(moh) = moh {
            let ssrc_and_moh = {
                self.set_moh(Some(moh.clone()));
                if self.current_play().is_none() {
                    let ssrc = rand::random::<u32>();
                    Some((ssrc, moh.clone()))
                } else {
                    info!(
                        session_id = self.session_id,
                        "Something is playing, MOH will start after it ends"
                    );
                    None
                }
            };

            if let Some((ssrc, moh_path)) = ssrc_and_moh {
                let file_track = self.make_file_track(moh_path.clone(), ssrc);
                self.update_track_wrapper(Box::new(file_track), Some(moh_path))
                    .await;
            }
        } else {
            let track = rtp_track_to_setup.take().unwrap();
            self.setup_track_with_stream(&call_option, track)
                .await
                .map_err(|e| rsipstack::Error::Error(e.to_string()))?;
        }

        info!(
            session_id = self.session_id,
            track_id,
            contact = %invite_option.contact,
            "invite {} -> {} offer: \n{}",
            invite_option.caller,
            invite_option.callee,
            offer.as_ref().map(|s| s.as_str()).unwrap_or("<NO OFFER>")
        );

        let (dlg_state_sender, dlg_state_receiver) =
            self.invitation.dialog_layer.new_dialog_state_channel();

        let states = InviteDialogStates::new(
            true,
            self.session_id.clone(),
            track_id.clone(),
            self.event_sender.clone(),
            self.media_stream.clone(),
            leg.clone(),
            cancel_token,
        );

        let hangup_headers = call_option
            .sip
            .as_ref()
            .and_then(|s| s.hangup_headers.as_ref())
            .map(crate::sip_util::sip_headers_from_map);

        let mut client_dialog_handler = DialogStateReceiverGuard::new(
            self.invitation.dialog_layer.clone(),
            dlg_state_receiver,
            hangup_headers,
        );

        crate::spawn(async move {
            client_dialog_handler.process_dialog(states).await;
        });

        let (dialog_id, answer) = self
            .invitation
            .invite(invite_option, dlg_state_sender)
            .await?;

        self.set_moh(None);

        if let Some(track) = rtp_track_to_setup {
            info!(
                session_id = self.session_id,
                track_id, "Stopping MOH and setting up RTP track"
            );
            self.media_stream
                .remove_track(&self.server_side_track_id, false)
                .await;

            self.setup_track_with_stream(&call_option, track)
                .await
                .map_err(|e| rsipstack::Error::Error(e.to_string()))?;
        }

        // Resolve the final answer SDP, falling back to the early-media (183)
        // SDP when the 200 OK carries no body.
        let early_answer = leg.progress.load_full().answer.clone();
        let (answer, remote_description_already_applied) =
            match crate::call::state::resolve_final_answer(answer, early_answer.as_ref()) {
                Ok(resolved) => resolved,
                Err(msg) => {
                    warn!(session_id = self.session_id, "{}", msg);
                    return Err(rsipstack::Error::DialogError(
                        "No answer received".to_string(),
                        dialog_id,
                        rsipstack::rsip::StatusCode::NotAcceptableHere,
                    ));
                }
            };

        leg.update_progress(|p| p.try_set_answer(&answer));

        if !remote_description_already_applied {
            self.media_stream
                .update_remote_description(&track_id, &answer)
                .await
                .ok();
        }

        Ok(answer)
    }

    /// Detect if SDP is WebRTC format
    pub(super) fn is_webrtc_sdp(sdp: &str) -> bool {
        (sdp.contains("a=ice-ufrag:") || sdp.contains("a=ice-pwd:"))
            && sdp.contains("a=fingerprint:")
    }

    pub(super) async fn setup_answer_track(
        &self,
        ssrc: u32,
        option: &CallOption,
        offer: String,
    ) -> Result<(String, Box<dyn Track>)> {
        let offer = match option.enable_ipv6 {
            Some(false) | None => strip_ipv6_candidates(&offer),
            _ => offer.clone(),
        };

        let timeout = option.handshake_timeout.map(|t| Duration::from_secs(t));

        let mut media_track = if Self::is_webrtc_sdp(&offer) {
            let mut rtc_config = RtcTrackConfig::default();
            rtc_config.mode = rustrtc::TransportMode::WebRtc;
            rtc_config.ice_servers = self.app_state.config.ice_servers.clone();
            self.rtc_apply_network(&mut rtc_config);
            self.rtc_apply_latching(&mut rtc_config);

            let webrtc_track = RtcTrack::new(
                self.cancel_token.child_token(),
                self.session_id.clone(),
                self.track_config.clone(),
                rtc_config,
            )
            .with_ssrc(ssrc);

            Box::new(webrtc_track) as Box<dyn Track>
        } else {
            let per_call_srtp = option.sip.as_ref().and_then(|s| s.enable_srtp);
            let rtp_track = self
                .create_rtp_track(self.session_id.clone(), ssrc, per_call_srtp)
                .await?;
            Box::new(rtp_track) as Box<dyn Track>
        };

        let answer = match media_track.handshake(offer.clone(), timeout).await {
            Ok(answer) => answer,
            Err(e) => {
                return Err(anyhow::anyhow!("handshake failed: {e}"));
            }
        };

        return Ok((answer, media_track));
    }

    pub(super) async fn prepare_incoming_sip_track(
        &self,
        runtime: &mut CallRuntime,
        cancel_token: CancellationToken,
        track_id: &String,
        pending_dialog: PendingDialog,
        hangup_headers: Option<Vec<rsipstack::rsip::Header>>,
    ) -> Result<()> {
        let state_receiver = pending_dialog.state_receiver;

        let states = InviteDialogStates::new(
            false,
            self.session_id.clone(),
            track_id.clone(),
            self.event_sender.clone(),
            self.media_stream.clone(),
            self.leg(),
            cancel_token,
        );

        let initial_request = pending_dialog.dialog.initial_request();
        let offer = String::from_utf8_lossy(&initial_request.body).to_string();

        let caller = initial_request
            .from_header()
            .ok()
            .and_then(|h| h.uri().ok())
            .map(|u| u.to_string())
            .unwrap_or_default();
        let callee = initial_request
            .to_header()
            .ok()
            .and_then(|h| h.uri().ok())
            .map(|u| u.to_string())
            .unwrap_or_default();
        let headers: Option<std::collections::HashMap<String, String>> = {
            let mut h = std::collections::HashMap::new();
            for header in initial_request.headers.iter() {
                if let rsipstack::rsip::Header::Other(name, value) = header {
                    h.insert(name.to_string(), value.to_string());
                }
            }
            if h.is_empty() { None } else { Some(h) }
        };
        self.event_sender
            .send(SessionEvent::Incoming {
                track_id: self.session_id.clone(),
                timestamp: crate::media::get_timestamp(),
                caller,
                callee,
                sdp: offer.clone(),
                headers,
            })
            .ok();

        let (ssrc, option) = {
            let call_state = self.progress.load_full();
            (self.ssrc, call_state.option.clone().unwrap_or_default())
        };

        match self.setup_answer_track(ssrc, &option, offer).await {
            Ok((offer, track)) => {
                // Start the track in the media stream now — early-media ringtone
                // requires the RTP sender loop to be running during ringing.
                // Processors are intentionally omitted here; they will be built from
                // the accept option (which carries VAD/ASR/AGC config) when Accept
                // is issued, via finish_caller_stack(StartedForEarlyMedia).
                //
                // Do NOT call setup_track_with_stream here: it builds the VAD/ASR/AGC
                // processors from the stored option. When Accept arrives without a prior
                // Ringing, that option already carries `asr`, so processors would be built
                // both here and again in finish_caller_stack(StartedForEarlyMedia),
                // resulting in two ASR clients (two WebSocket connections) per session.
                // update_track_wrapper only starts the track (plus ambiance/subscribe),
                // deferring all VAD/ASR/AGC processors to accept time.
                self.update_track_wrapper(track, None).await;
                runtime.ready_to_answer = Some((
                    offer,
                    PendingCallerTrack::StartedForEarlyMedia,
                    pending_dialog.dialog,
                ));
            }
            Err(e) => {
                return Err(anyhow::anyhow!("error creating track: {}", e));
            }
        }

        let mut client_dialog_handler = DialogStateReceiverGuard::new(
            self.invitation.dialog_layer.clone(),
            state_receiver,
            hangup_headers,
        );

        crate::spawn(async move {
            client_dialog_handler.process_dialog(states).await;
        });
        Ok(())
    }
}
