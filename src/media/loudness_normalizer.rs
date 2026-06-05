use crate::media::{AudioFrame, Sample, Samples, processor::Processor};
use anyhow::Result;
use ebur128::{EbuR128, Mode};

// -18 LUFS matches the WebRTC AGC ballpark for voice telephony.
// Quieter than streaming targets (-14 LUFS) to leave headroom for transients,
// louder than broadcast (-23 LUFS) which sounds thin on voice-only calls.
const DEFAULT_TARGET_LUFS: f64 = -18.0;
// Ceiling on amplification. Anything quieter than ~-30 LUFS source caps here,
// which keeps in-sentence noise from being boosted dramatically.
const MAX_GAIN_DB: f32 = 12.0;
// Slow attack guards against boosting in-sentence pauses and ambient noise
// that escapes the silence threshold. Moderate release handles the occasional
// loud word without audibly ducking volume mid-phrase.
const ATTACK_TIME_SEC: f32 = 0.5;
const RELEASE_TIME_SEC: f32 = 0.25;
// EBU R128 updates short-term/integrated loudness on a 100ms grid internally,
// so re-querying more often returns stale values.
const LOUDNESS_QUERY_INTERVAL_SEC: f32 = 0.1;
// Peak threshold below which a frame is treated as silence and gain is frozen.
// Prevents pumping room noise up during the other party's turn in turn-based calls.
const SILENCE_THRESHOLD_DBFS: f32 = -50.0;

pub struct LoudnessNormalizer {
    meter: EbuR128,
    sample_rate: u32,
    target_gain: f32,
    current_gain: f32,
    attack_time_sec: f32,
    release_time_sec: f32,
    // Counts samples accumulated since the last loudness query. Loudness is
    // re-queried only every ~100ms because EBU R128 updates its measurements
    // on that grid internally — querying more often returns the same value.
    samples_since_query: u32,
    query_interval_samples: u32,
    // Total samples fed to the meter. Short-term loudness computes over a fixed
    // 3-second internal buffer; before it is filled, the buffer still contains
    // zeros that drag the reading toward -inf and would push gain to the ceiling.
    // We defer to integrated loudness until short-term is trustworthy.
    samples_fed: u64,
    shortterm_window_samples: u64,
}

impl LoudnessNormalizer {
    pub fn new(sample_rate: u32) -> Result<Self> {
        // Mode::S: short-term (3s window) drives gain once the window fills.
        // Mode::I: integrated (gated, whole-call average) used as fallback before
        //          short-term is valid; provides a stable anchor.
        // Mode::HISTOGRAM: stores gating blocks in a fixed-size histogram instead
        //          of an unbounded Vec, so memory stays constant on long calls.
        let meter = EbuR128::new(1, sample_rate, Mode::S | Mode::I | Mode::HISTOGRAM)?;
        Ok(Self {
            meter,
            sample_rate,
            target_gain: 1.0,
            current_gain: 1.0,
            attack_time_sec: ATTACK_TIME_SEC,
            release_time_sec: RELEASE_TIME_SEC,
            samples_since_query: 0,
            query_interval_samples: (sample_rate as f32 * LOUDNESS_QUERY_INTERVAL_SEC) as u32,
            samples_fed: 0,
            shortterm_window_samples: sample_rate as u64 * 3,
        })
    }

    #[cfg(test)]
    pub(crate) fn current_gain_for_test(&self) -> f32 {
        self.current_gain
    }

    fn current_loudness(&self) -> Option<f64> {
        let lufs = if self.samples_fed >= self.shortterm_window_samples {
            self.meter
                .loudness_shortterm()
                .or_else(|_| self.meter.loudness_global())
                .ok()?
        } else {
            self.meter.loudness_global().ok()?
        };
        lufs.is_finite().then_some(lufs)
    }
}

impl Processor for LoudnessNormalizer {
    fn process_frame(&mut self, frame: &mut AudioFrame) -> Result<()> {
        let samples = match &mut frame.samples {
            Samples::PCM { samples } if !samples.is_empty() => samples,
            _ => return Ok(()),
        };

        self.meter.add_frames_i16(samples)?;
        self.samples_fed = self.samples_fed.saturating_add(samples.len() as u64);

        // Silence detection freezes both the target and the smoother so the
        // other party's turn doesn't push gain around. Computed once per frame.
        let silence_threshold = (i16::MAX as f32 * db_to_linear(SILENCE_THRESHOLD_DBFS)) as u16;
        let frame_is_silent = !samples
            .iter()
            .any(|&s| s.unsigned_abs() > silence_threshold);

        self.samples_since_query += samples.len() as u32;
        if self.samples_since_query >= self.query_interval_samples {
            self.samples_since_query = 0;
            if !frame_is_silent {
                if let Some(lufs) = self.current_loudness() {
                    self.target_gain = db_to_linear((DEFAULT_TARGET_LUFS - lufs) as f32)
                        .clamp(0.0, db_to_linear(MAX_GAIN_DB));
                }
            }
        }

        if !frame_is_silent {
            let frame_duration_sec = samples.len() as f32 / self.sample_rate as f32;
            let time_constant = if self.target_gain > self.current_gain {
                self.attack_time_sec
            } else {
                self.release_time_sec
            };
            let coefficient = 1.0 - (-frame_duration_sec / time_constant).exp();
            self.current_gain += (self.target_gain - self.current_gain) * coefficient;
        }

        if (self.current_gain - 1.0).abs() > f32::EPSILON {
            for sample in samples.iter_mut() {
                // f32 -> i16 `as` cast saturates since Rust 1.45, no explicit clamp needed.
                *sample = (*sample as f32 * self.current_gain) as Sample;
            }
        }

        Ok(())
    }
}

#[inline]
fn db_to_linear(db: f32) -> f32 {
    10f32.powf(db / 20.0)
}
