use crate::event::{EventSender, SessionEvent};
use crate::media::dtmf::DtmfDetector;
use crate::media::{AudioFrame, Samples, TrackId};
use crate::media::{
    processor::Processor,
    recorder::{Recorder, RecorderOption},
    track::{Track, TrackPacketReceiver, TrackPacketSender},
};
use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::{
    select,
    sync::{Mutex, mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid;

pub struct MediaStream {
    id: String,
    pub cancel_token: CancellationToken,
    recorder_option: Mutex<Option<RecorderOption>>,
    event_sender: EventSender,
    pub packet_sender: TrackPacketSender,
    packet_receiver: Mutex<Option<TrackPacketReceiver>>,
    recorder_sender: mpsc::UnboundedSender<AudioFrame>,
    recorder_receiver: Mutex<Option<mpsc::UnboundedReceiver<AudioFrame>>>,
    recorder_handle: Mutex<Option<JoinHandle<()>>>,
    track_command_receiver: Mutex<Option<mpsc::UnboundedReceiver<TrackCommand>>>,
    track_command_sender: mpsc::UnboundedSender<TrackCommand>,
}

const CALLEE_TRACK_ID: &str = "callee-track";
const QUEUE_HOLD_TRACK_ID: &str = "queue-hold-track";

pub struct MediaStreamBuilder {
    cancel_token: Option<CancellationToken>,
    id: Option<String>,
    event_sender: EventSender,
    recorder_config: Option<RecorderOption>,
}

impl MediaStreamBuilder {
    pub fn new(event_sender: EventSender) -> Self {
        Self {
            id: Some(format!("ms:{}", uuid::Uuid::new_v4())),
            cancel_token: None,
            event_sender,
            recorder_config: None,
        }
    }
    pub fn with_id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }

    pub fn with_cancel_token(mut self, cancel_token: CancellationToken) -> Self {
        self.cancel_token = Some(cancel_token);
        self
    }

    pub fn with_recorder_config(mut self, recorder_config: RecorderOption) -> Self {
        self.recorder_config = Some(recorder_config);
        self
    }

    pub fn build(self) -> MediaStream {
        let cancel_token = self
            .cancel_token
            .unwrap_or_else(|| CancellationToken::new());
        let (track_packet_sender, track_packet_receiver) = mpsc::unbounded_channel();
        let (recorder_sender, recorder_receiver) = mpsc::unbounded_channel();
        let (track_command_sender, track_command_receiver) = mpsc::unbounded_channel();

        MediaStream {
            id: self.id.unwrap_or_default(),
            cancel_token,
            recorder_option: Mutex::new(self.recorder_config),
            event_sender: self.event_sender,
            packet_sender: track_packet_sender,
            packet_receiver: Mutex::new(Some(track_packet_receiver)),
            recorder_sender,
            recorder_receiver: Mutex::new(Some(recorder_receiver)),
            recorder_handle: Mutex::new(None),
            track_command_sender,
            track_command_receiver: Mutex::new(Some(track_command_receiver)),
        }
    }
}

impl MediaStream {
    pub async fn serve(&self) -> Result<()> {
        let packet_receiver = match self.packet_receiver.lock().await.take() {
            Some(receiver) => receiver,
            None => {
                warn!(
                    session_id = self.id,
                    "MediaStream::serve() called multiple times, stream already serving"
                );
                return Ok(());
            }
        };
        let track_command_receiver = match self.track_command_receiver.lock().await.take() {
            Some(receiver) => receiver,
            None => {
                warn!(
                    session_id = self.id,
                    "MediaStream::serve() called multiple times, stream already serving"
                );
                return Ok(());
            }
        };

        self.start_recorder().await.ok();
        info!(session_id = self.id, "mediastream serving");
        select! {
            _ = self.cancel_token.cancelled() => {}
            r = self.handle_forward_track(packet_receiver, track_command_receiver) => {
                info!(session_id = self.id, "track packet receiver stopped {:?}", r);
            }
        }
        Ok(())
    }

    pub fn stop(&self, _reason: Option<String>, _initiator: Option<String>) {
        self.cancel_token.cancel()
    }

    pub async fn cleanup(&self) -> Result<()> {
        self.cancel_token.cancel();
        if let Some(recorder_handle) = self.recorder_handle.lock().await.take() {
            if let Ok(Ok(_)) = tokio::time::timeout(Duration::from_secs(30), recorder_handle).await
            {
                info!(session_id = self.id, "recorder stopped");
            } else {
                warn!(session_id = self.id, "recorder timeout");
            }
        }
        Ok(())
    }

    pub async fn update_recorder_option(&self, recorder_config: RecorderOption) {
        *self.recorder_option.lock().await = Some(recorder_config);
        self.start_recorder().await.ok();
    }

    pub async fn remove_track(&self, id: &TrackId, graceful: bool) {
        self.send_track_command(TrackCommand::RemoveTrack {
            track_id: id.clone(),
            graceful,
        })
        .await;
    }

    pub async fn update_remote_description(
        &self,
        track_id: &TrackId,
        answer: &String,
    ) -> Result<()> {
        let (result_tx, result_rx) = oneshot::channel();
        self.send_track_command(TrackCommand::UpdateRemoteDescription {
            track_id: track_id.clone(),
            answer: answer.clone(),
            force: false,
            result_tx,
        })
        .await;
        warn!("update_remote_description cmd sent");
        result_rx.await.expect("sender shouldn't be dropped")
    }

    pub async fn update_remote_description_force(
        &self,
        track_id: &TrackId,
        answer: &String,
    ) -> Result<()> {
        let (result_tx, result_rx) = oneshot::channel();
        self.send_track_command(TrackCommand::UpdateRemoteDescription {
            track_id: track_id.clone(),
            answer: answer.clone(),
            force: true,
            result_tx,
        })
        .await;
        result_rx.await.expect("sender shouldn't be dropped")
    }

    pub async fn handshake(
        &self,
        track_id: &TrackId,
        offer: String,
        timeout: Option<Duration>,
    ) -> Result<String> {
        let (result_tx, result_rx) = oneshot::channel();
        self.send_track_command(TrackCommand::Handshake {
            track_id: track_id.clone(),
            offer,
            timeout,
            result_tx,
        })
        .await;
        result_rx.await.expect("sender shouldn't be dropped")
    }

    pub async fn update_track(&self, mut track: Box<dyn Track>, play_id: Option<String>) {
        self.remove_track(track.id(), false).await;
        if self.recorder_option.lock().await.is_some() {
            track.insert_processor(Box::new(RecorderProcessor::new(
                self.recorder_sender.clone(),
            )));
        }
        match track
            .start(self.event_sender.clone(), self.packet_sender.clone())
            .await
        {
            Ok(_) => {
                info!(session_id = self.id, track_id = track.id(), "track started");
                let track_id = track.id().clone();
                self.send_track_command(TrackCommand::AddTrack {
                    track: track,
                    track_id: track_id.clone(),
                })
                .await;
                self.event_sender
                    .send(SessionEvent::TrackStart {
                        track_id,
                        timestamp: crate::media::get_timestamp(),
                        play_id,
                    })
                    .ok();
            }
            Err(e) => {
                warn!(
                    session_id = self.id,
                    track_id = track.id(),
                    play_id = play_id.as_deref(),
                    "Failed to start track: {}",
                    e
                );
            }
        }
    }

    pub async fn mute_track(&self, id: Option<TrackId>) {
        self.send_track_command(TrackCommand::MuteTrack {
            track_id: id.clone(),
        })
        .await;
    }

    pub async fn unmute_track(&self, id: Option<TrackId>) {
        self.send_track_command(TrackCommand::UnmuteTrack {
            track_id: id.clone(),
        })
        .await;
    }

    async fn send_track_command(&self, cmd: TrackCommand) {
        self.track_command_sender
            .send(cmd)
            .expect("track_command_receiver is dropped");
    }
}

pub enum TrackCommand {
    AddTrack {
        track: Box<dyn Track>,
        track_id: String,
    },
    RemoveTrack {
        track_id: TrackId,
        graceful: bool,
    },
    UpdateRemoteDescription {
        track_id: TrackId,
        answer: String,
        force: bool,
        result_tx: oneshot::Sender<Result<()>>,
    },
    Handshake {
        track_id: TrackId,
        offer: String,
        timeout: Option<Duration>,
        result_tx: oneshot::Sender<Result<String>>,
    },
    MuteTrack {
        track_id: Option<TrackId>,
    },
    UnmuteTrack {
        track_id: Option<TrackId>,
    },
    AudioFrame {
        packet: AudioFrame,
    },
}

#[derive(Clone)]
pub struct RecorderProcessor {
    sender: mpsc::UnboundedSender<AudioFrame>,
}

impl RecorderProcessor {
    pub fn new(sender: mpsc::UnboundedSender<AudioFrame>) -> Self {
        Self { sender }
    }
}

impl Processor for RecorderProcessor {
    fn process_frame(&mut self, frame: &mut AudioFrame) -> Result<()> {
        let frame_clone = frame.clone();
        let _ = self.sender.send(frame_clone);
        Ok(())
    }
}

impl MediaStream {
    pub async fn start_recorder(&self) -> Result<()> {
        let recorder_option = self.recorder_option.lock().await.clone();
        if let Some(recorder_option) = recorder_option {
            if recorder_option.recorder_file.is_empty() {
                warn!(
                    session_id = self.id,
                    "recorder file is empty, skipping recorder start"
                );
                return Ok(());
            }
            let recorder_receiver = match self.recorder_receiver.lock().await.take() {
                Some(receiver) => receiver,
                None => {
                    return Ok(());
                }
            };
            let cancel_token = self.cancel_token.child_token();
            let session_id_clone = self.id.clone();

            info!(
                session_id = session_id_clone,
                sample_rate = recorder_option.samplerate,
                ptime = recorder_option.ptime,
                "start recorder",
            );

            let recorder_handle = crate::spawn(async move {
                let recorder_file = recorder_option.recorder_file.clone();
                let recorder =
                    Recorder::new(cancel_token, session_id_clone.clone(), recorder_option);
                match recorder
                    .process_recording(Path::new(&recorder_file), recorder_receiver)
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        warn!(
                            session_id = session_id_clone,
                            "Failed to process recorder: {}", e
                        );
                    }
                }
            });
            *self.recorder_handle.lock().await = Some(recorder_handle);
        }
        Ok(())
    }

    async fn handle_forward_track(
        &self,
        mut packet_receiver: TrackPacketReceiver,
        mut track_command_receiver: mpsc::UnboundedReceiver<TrackCommand>,
    ) {
        let mut tracks: HashMap<TrackId, (Box<dyn Track>, DtmfDetector)> = HashMap::new();
        loop {
            select! {
                Some(track_event) = track_command_receiver.recv() => {
                    self.handle_track_command(&mut tracks, track_event).await;
                }
                Some(packet) = packet_receiver.recv() => {
                    self.send_track_command(TrackCommand::AudioFrame { packet }).await;
                }
                else => break,
            }
        }
    }

    async fn handle_track_command(
        &self,
        tracks: &mut HashMap<TrackId, (Box<dyn Track>, DtmfDetector)>,
        cmd: TrackCommand,
    ) {
        warn!("cmd start");
        match cmd {
            TrackCommand::AddTrack { track, track_id } => {
                warn!("AddTrack {}", track_id);
                tracks.insert(track_id, (track, DtmfDetector::new()));
            }
            TrackCommand::RemoveTrack { track_id, graceful } => {
                if let Some((track, _)) = tracks.remove(&track_id) {
                    let res = if graceful {
                        track.stop_graceful().await
                    } else {
                        track.stop().await
                    };
                    if let Err(e) = res {
                        warn!(session_id = self.id, "failed to stop track: {}", e);
                    }
                }
            }
            TrackCommand::UpdateRemoteDescription {
                track_id,
                answer,
                force,
                result_tx,
            } => {
                warn!("UpdateRemoteDescription {}", track_id);
                if let Some((track, _)) = tracks.get_mut(&track_id) {
                    let res = if force {
                        track.update_remote_description_force(&answer).await
                    } else {
                        track.update_remote_description(&answer).await
                    };
                    result_tx
                        .send(res)
                        .expect("update_remote_description receiver shouldn't be dropped");
                } else {
                    result_tx
                        .send(Ok(()))
                        .expect("update_remote_description receiver shouldn't be dropped");
                }
            }
            TrackCommand::Handshake {
                track_id,
                offer,
                timeout,
                result_tx,
            } => match tracks.get_mut(&track_id) {
                Some(track) => {
                    warn!("Handshake {}", track_id);
                    let res = track.0.handshake(offer, timeout).await;
                    result_tx
                        .send(res)
                        .expect("handshake receiver shouldn't be dropped");
                }
                None => {
                    result_tx
                        .send(Err(anyhow::anyhow!("track not found {}", track_id)))
                        .expect("handshake receiver shouldn't be dropped");
                }
            },
            TrackCommand::MuteTrack { track_id } => {
                warn!("MuteTrack {:?}", track_id);
                if let Some(id) = track_id {
                    if let Some((track, _)) = tracks.get_mut(&id) {
                        MuteProcessor::mute_track(track.as_mut());
                    }
                } else {
                    for (track, _) in tracks.values_mut() {
                        MuteProcessor::mute_track(track.as_mut());
                    }
                }
            }
            TrackCommand::UnmuteTrack { track_id } => {
                warn!("UnmuteTrack {:?}", track_id);
                if let Some(id) = track_id {
                    if let Some((track, _)) = tracks.get_mut(&id) {
                        MuteProcessor::unmute_track(track.as_mut());
                    }
                } else {
                    for (track, _) in tracks.values_mut() {
                        MuteProcessor::unmute_track(track.as_mut());
                    }
                }
            }
            TrackCommand::AudioFrame { packet } => self.handle_packet(tracks, packet).await,
        }
        warn!("cmd done");
    }

    async fn handle_packet(
        &self,
        tracks: &mut HashMap<TrackId, (Box<dyn Track>, DtmfDetector)>,
        packet: AudioFrame,
    ) {
        warn!("packet start");
        // Process the packet with each track
        for (track_id, (track, dtmf_detector)) in tracks.iter_mut() {
            if track_id == &packet.track_id {
                match &packet.samples {
                    Samples::RTP {
                        payload_type,
                        payload,
                        ..
                    } => {
                        if let Some(digit) = dtmf_detector.detect_rtp(*payload_type, payload) {
                            debug!(track_id = track_id, digit, "DTMF detected");
                            self.event_sender
                                .send(SessionEvent::Dtmf {
                                    track_id: packet.track_id.to_string(),
                                    timestamp: packet.timestamp,
                                    digit,
                                })
                                .ok();
                        }
                    }
                    _ => {}
                }
                continue;
            }

            if packet.track_id == QUEUE_HOLD_TRACK_ID && track_id == CALLEE_TRACK_ID {
                continue;
            }
            if let Err(e) = track.send_packet(&packet).await {
                warn!(
                    id = track_id,
                    "media_stream: Failed to send packet to track: {}", e
                );
            }
        }
        warn!("packet done");
    }
}

pub struct MuteProcessor;

impl MuteProcessor {
    pub fn mute_track(track: &mut dyn Track) {
        let chain = track.processor_chain();
        if !chain.has_processor::<MuteProcessor>() {
            chain.insert_processor(Box::new(MuteProcessor));
        }
    }

    pub fn unmute_track(track: &mut dyn Track) {
        let chain = track.processor_chain();
        chain.remove_processor::<MuteProcessor>();
    }
}

impl Processor for MuteProcessor {
    fn process_frame(&mut self, frame: &mut AudioFrame) -> Result<()> {
        match &mut frame.samples {
            Samples::PCM { samples } => {
                samples.fill(0);
            }
            // discard DTMF frames
            Samples::RTP { payload_type, .. } if *payload_type >= 96 && *payload_type <= 127 => {
                frame.samples = Samples::Empty;
            }
            _ => {}
        }
        Ok(())
    }
}
