use crate::event::EventSender;
use crate::media::processor::ProcessorChain;
use crate::media::track::{Track, TrackConfig, TrackPacketSender};
use crate::media::track::track_codec::TrackCodec;
use crate::media::{AudioFrame, INTERNAL_SAMPLERATE, Samples, TrackId};
use anyhow::Result;
use async_trait::async_trait;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;

/// One input source for the mixer: a channel carrying audio from a remote call.
pub struct MixInput {
    pub inbound_receiver: mpsc::Receiver<AudioFrame>,
}

/// Mixes audio from multiple inbound sources and emits a single blended PCM stream.
///
/// Outbound direction (this session speaking): packets from `source_session_id` are
/// forwarded to all `outbound_senders`.  Callers omit a sender for sources that must
/// not hear this session (one-sided / listen-only connection).
pub struct MixingForwardingTrack {
    track_id: TrackId,
    /// track_id emitted by this session's callee track — used to gate send_packet forwarding.
    source_session_id: TrackId,
    inputs: Option<Vec<MixInput>>,
    outbound_senders: Vec<mpsc::Sender<AudioFrame>>,
    processor_chain: ProcessorChain,
    config: TrackConfig,
    cancel_token: CancellationToken,
    ssrc: u32,
    paused: Arc<AtomicBool>,
}

impl MixingForwardingTrack {
    pub fn new(
        track_id: TrackId,
        source_session_id: TrackId,
        inputs: Vec<MixInput>,
        outbound_senders: Vec<mpsc::Sender<AudioFrame>>,
        config: TrackConfig,
        cancel_token: CancellationToken,
        ssrc: u32,
        paused: Arc<AtomicBool>,
    ) -> Self {
        Self {
            processor_chain: ProcessorChain::new(config.samplerate),
            track_id,
            source_session_id,
            inputs: Some(inputs),
            outbound_senders,
            config,
            cancel_token,
            ssrc,
            paused,
        }
    }
}

fn decode_to_pcm(frame: AudioFrame, codec: &mut TrackCodec) -> Vec<i16> {
    match frame.samples {
        Samples::PCM { mut samples } => {
            if frame.sample_rate != INTERNAL_SAMPLERATE {
                codec.resample(samples, frame.sample_rate, INTERNAL_SAMPLERATE)
            } else {
                if frame.channels == 2 {
                    crate::media::processor::convert_to_mono(&mut samples, 2);
                }
                samples
            }
        }
        Samples::RTP { payload_type, payload, .. } => {
            if !TrackCodec::is_audio(payload_type) {
                return Vec::new();
            }
            let (_, channels, mut samples) = codec.decode(payload_type, &payload, INTERNAL_SAMPLERATE);
            if channels == 2 {
                crate::media::processor::convert_to_mono(&mut samples, 2);
            }
            samples
        }
        _ => Vec::new(),
    }
}

#[async_trait]
impl Track for MixingForwardingTrack {
    fn ssrc(&self) -> u32 {
        self.ssrc
    }

    fn id(&self) -> &TrackId {
        &self.track_id
    }

    fn config(&self) -> &TrackConfig {
        &self.config
    }

    fn processor_chain(&mut self) -> &mut ProcessorChain {
        &mut self.processor_chain
    }

    async fn handshake(&mut self, _offer: String, _timeout: Option<Duration>) -> Result<String> {
        Ok(String::new())
    }

    async fn update_remote_description(&mut self, _answer: &String) -> Result<()> {
        Ok(())
    }

    async fn start(
        &mut self,
        _event_sender: EventSender,
        packet_sender: TrackPacketSender,
    ) -> Result<()> {
        let inputs = self
            .inputs
            .take()
            .ok_or_else(|| anyhow::anyhow!("mixing forwarding track already started"))?;

        let ptime_samples =
            (INTERNAL_SAMPLERATE as u64 * self.config.ptime.as_millis() as u64 / 1000) as usize;

        // One accumulator buffer per input source.
        let buffers: Vec<Arc<Mutex<Vec<i16>>>> = inputs
            .iter()
            .map(|_| Arc::new(Mutex::new(Vec::new())))
            .collect();

        let track_id = self.track_id.clone();
        let cancel_token = self.cancel_token.clone();

        // Spawn one reader task per input source.
        for (input, buf) in inputs.into_iter().zip(buffers.iter().cloned()) {
            let cancel = cancel_token.clone();
            let mut inbound = input.inbound_receiver;
            let track_id_log = track_id.clone();
            crate::spawn(async move {
                let mut codec = TrackCodec::new();
                let stop_reason = loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break "track stopped",
                        frame = inbound.recv() => {
                            match frame {
                                Some(frame) => {
                                    let pcm = decode_to_pcm(frame, &mut codec);
                                    if !pcm.is_empty() {
                                        buf.lock().unwrap().extend_from_slice(&pcm);
                                    }
                                }
                                None => break "source channel closed",
                            }
                        }
                    }
                };
                info!(track_id = track_id_log, reason = stop_reason, "mixer input reader stopped");
            });
        }

        // Timer-driven mix-and-emit task.
        let buffers_out = buffers.clone();
        let cancel_out = cancel_token.clone();
        let track_id_out = track_id.clone();
        let ptime = self.config.ptime;
        crate::spawn(async move {
            let mut interval = tokio::time::interval(ptime);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = cancel_out.cancelled() => break,
                    _ = interval.tick() => {
                        let mixed: Vec<i16> = (0..ptime_samples)
                            .map(|i| {
                                let sum: i32 = buffers_out
                                    .iter()
                                    .map(|b| {
                                        b.lock()
                                            .unwrap()
                                            .get(i)
                                            .copied()
                                            .unwrap_or(0) as i32
                                    })
                                    .sum();
                                sum.clamp(i16::MIN as i32, i16::MAX as i32) as i16
                            })
                            .collect();

                        // Drain consumed samples from each buffer.
                        for buf in &buffers_out {
                            let mut guard = buf.lock().unwrap();
                            let drain = guard.len().min(ptime_samples);
                            guard.drain(..drain);
                        }

                        let frame = AudioFrame {
                            track_id: track_id_out.clone(),
                            samples: Samples::PCM { samples: mixed },
                            sample_rate: INTERNAL_SAMPLERATE,
                            channels: 1,
                            timestamp: crate::media::get_timestamp(),
                            src_packet: None,
                        };
                        if packet_sender.send(frame).is_err() {
                            break;
                        }
                    }
                }
            }
            cancel_out.cancel();
            info!(track_id = track_id_out, "mixer output task stopped");
        });

        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        self.cancel_token.cancel();
        Ok(())
    }

    async fn send_packet(&mut self, packet: &AudioFrame) -> Result<()> {
        if self.cancel_token.is_cancelled()
            || self.paused.load(Ordering::Relaxed)
            || packet.track_id != self.source_session_id
        {
            return Ok(());
        }

        // Skip DTMF.
        if let Samples::RTP { payload_type, .. } = &packet.samples {
            if *payload_type >= 96 && *payload_type <= 127 {
                return Ok(());
            }
        }

        for sender in &self.outbound_senders {
            match sender.try_send(packet.clone()) {
                Ok(_) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {}
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    self.cancel_token.cancel();
                    return Ok(());
                }
            }
        }

        Ok(())
    }
}
