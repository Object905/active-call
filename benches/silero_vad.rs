use active_call::media::vad::{TinySilero, VADOption, VadEngine};
use active_call::media::{AudioFrame, Samples};
use criterion::{BatchSize, Criterion, Throughput, black_box, criterion_group, criterion_main};

const SPEECH: &[u8] = include_bytes!("../fixtures/vad/ru_alena_address.raw");

fn new_vad() -> TinySilero {
    TinySilero::new(VADOption {
        samplerate: 16000,
        ..Default::default()
    })
    .unwrap()
}

fn speech_pcm() -> Vec<i16> {
    SPEECH
        .chunks_exact(2)
        .map(|b| i16::from_le_bytes([b[0], b[1]]))
        .collect()
}

/// Deterministic LCG white noise in roughly [-1/scale, 1/scale].
fn noise_f32(len: usize, scale: f32) -> Vec<f32> {
    let mut state = 0x1234_5678u32;
    (0..len)
        .map(|_| {
            state = state.wrapping_mul(1664525).wrapping_add(1013904223);
            ((state >> 16) as i16) as f32 / 32768.0 / scale
        })
        .collect()
}

fn bench_predict(c: &mut Criterion) {
    let pcm = speech_pcm();
    // a chunk from the voiced middle of the clip, so conv/LSTM zero-skips don't dominate
    let chunk: Vec<f32> = pcm[16000..16512]
        .iter()
        .map(|&s| s as f32 / 32768.0)
        .collect();
    // background noise (~-50 dBFS) instead of exact zeros, which the zero-skips would short-circuit
    let noise = noise_f32(512, 100.0);

    let mut group = c.benchmark_group("predict");
    group.throughput(Throughput::Elements(512));
    group.bench_function("speech_chunk", |b| {
        let mut vad = new_vad();
        b.iter(|| black_box(vad.predict(black_box(&chunk))));
    });
    group.bench_function("noise_chunk", |b| {
        let mut vad = new_vad();
        b.iter(|| black_box(vad.predict(black_box(&noise))));
    });
    group.finish();
}

fn frame(samples: &[i16], timestamp: u64) -> AudioFrame {
    AudioFrame {
        track_id: "bench".to_string(),
        samples: Samples::PCM {
            samples: samples.to_vec(),
        },
        sample_rate: 16000,
        timestamp,
        channels: 1,
        ..Default::default()
    }
}

/// Goes through the public `VadEngine::process` path, as the call pipeline does.
fn bench_process(c: &mut Criterion) {
    let pcm = speech_pcm();
    let mut group = c.benchmark_group("process");
    group.throughput(Throughput::Elements(pcm.len() as u64));
    group.bench_function("full_clip", |b| {
        b.iter_batched(
            || (new_vad(), frame(&pcm, 0)),
            |(mut vad, mut frame)| black_box(vad.process(black_box(&mut frame))),
            BatchSize::SmallInput,
        );
    });
    // realtime-like: 20 ms frames (320 samples @ 16 kHz)
    group.bench_function("frames_20ms", |b| {
        b.iter_batched(
            || {
                let frames: Vec<AudioFrame> = pcm
                    .chunks(320)
                    .enumerate()
                    .map(|(i, c)| frame(c, i as u64 * 20))
                    .collect();
                (new_vad(), frames)
            },
            |(mut vad, mut frames)| {
                for frame in frames.iter_mut() {
                    black_box(vad.process(black_box(frame)));
                }
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(benches, bench_predict, bench_process);
criterion_main!(benches);
