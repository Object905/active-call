use fearless_simd::{Level, Simd, prelude::*};
use realfft::num_complex::Complex;
use std::sync::OnceLock;

/// Best SIMD level for this CPU, detected once.
pub fn level() -> Level {
    static LEVEL: OnceLock<Level> = OnceLock::new();
    *LEVEL.get_or_init(Level::new)
}

/// Loads `U * f32s::LEN` i16 values as `U` f32 vectors (sign-extend + convert).
///
/// A native i16 vector holds two f32 vectors' worth of lanes, so each pair of f32 vectors costs
/// one i16 load, one widen and two int-to-float converts.
#[inline(always)]
fn load_i16<S: Simd, const U: usize>(simd: S, q: &[i16]) -> [S::f32s; U] {
    let n = S::f32s::LEN;
    let mut out = [S::f32s::splat(simd, 0.0); U];
    for p in 0..U / 2 {
        let (lo, hi) = S::i16s::from_slice(simd, &q[p * 2 * n..(p + 1) * 2 * n]).widen();
        out[2 * p] = lo.to_float::<S::f32s>();
        out[2 * p + 1] = hi.to_float::<S::f32s>();
    }
    if U % 2 == 1 {
        let tail = &q[(U - 1) * n..U * n];
        out[U - 1] = S::f32s::from_fn(simd, |i| tail[i] as f32);
    }
    out
}

/// `out[t * oc + j] += scales[j] * sum over i of xs[i * T + t] * w[rows[i] * oc + j]`,
/// for `t < T`, `j < oc`. Weights `w` are i16 with one scale per output channel.
///
/// Accumulates `T` output rows at once so each weight vector is loaded and converted once and
/// reused for all `T` timesteps; the weights (far larger than L1) dominate memory traffic.
/// `U` is the number of vectors per output block (`T * U` accumulators live in registers).
///
/// Meant to be called from inside a `dispatch!` region so it inlines into a
/// target-feature context; dispatch once per inference, not per call.
#[inline(always)]
pub fn fma_rows_multi<S: Simd, const T: usize, const U: usize>(
    simd: S,
    out: &mut [f32],
    oc: usize,
    w: &[i16],
    scales: &[f32],
    rows: &[usize],
    xs: &[f32],
) {
    let n = S::f32s::LEN;
    let mut j = 0;
    while j + U * n <= oc {
        fma_block::<S, T, U>(simd, out, oc, w, scales, rows, xs, j);
        j += U * n;
    }
    while j + n <= oc {
        fma_block::<S, T, 1>(simd, out, oc, w, scales, rows, xs, j);
        j += n;
    }
    for jj in j..oc {
        for t in 0..T {
            let mut acc = 0.0f32;
            for (&r, xr) in rows.iter().zip(xs.chunks_exact(T)) {
                acc += w[r * oc + jj] as f32 * xr[t];
            }
            out[t * oc + jj] += acc * scales[jj];
        }
    }
}

#[inline(always)]
fn fma_block<S: Simd, const T: usize, const U: usize>(
    simd: S,
    out: &mut [f32],
    oc: usize,
    w: &[i16],
    scales: &[f32],
    rows: &[usize],
    xs: &[f32],
    j: usize,
) {
    let n = S::f32s::LEN;
    let zero = S::f32s::splat(simd, 0.0);
    let mut acc = [[zero; U]; T];

    for (&r, xr) in rows.iter().zip(xs.chunks_exact(T)) {
        let start = r * oc + j;
        let wr = &w[start..start + U * n];
        let w_v = load_i16::<S, U>(simd, wr);
        for t in 0..T {
            let x_vec = S::f32s::splat(simd, xr[t]);
            for u in 0..U {
                acc[t][u] = w_v[u].mul_add(x_vec, acc[t][u]);
            }
        }
    }

    for u in 0..U {
        let scale_v = S::f32s::from_slice(simd, &scales[j + u * n..j + (u + 1) * n]);
        for t in 0..T {
            let start = t * oc + j + u * n;
            let out_v = S::f32s::from_slice(simd, &out[start..start + n]);
            acc[t][u]
                .mul_add(scale_v, out_v)
                .store_slice(&mut out[start..start + n]);
        }
    }
}

/// Vectors of accumulators kept in registers per output block.
const UNROLL: usize = 8;

/// `out[j] += scales[j] * sum over (off, x) in rows of x * w[off + j]`, for `j in 0..out.len()`.
///
/// Single-timestep, register-blocked variant of [`fma_rows_multi`] with explicit row offsets.
#[inline(always)]
pub fn fma_rows<S: Simd>(
    simd: S,
    out: &mut [f32],
    w: &[i16],
    scales: &[f32],
    rows: &[(usize, f32)],
) {
    let n = S::f32s::LEN;
    let len = out.len();
    let zero = S::f32s::splat(simd, 0.0);
    let mut j = 0;

    while j + UNROLL * n <= len {
        let mut acc = [zero; UNROLL];
        for &(off, x) in rows {
            let x_vec = S::f32s::splat(simd, x);
            let w_v = load_i16::<S, UNROLL>(simd, &w[off + j..off + j + UNROLL * n]);
            for u in 0..UNROLL {
                acc[u] = w_v[u].mul_add(x_vec, acc[u]);
            }
        }
        for u in 0..UNROLL {
            let range = j + u * n..j + (u + 1) * n;
            let scale_v = S::f32s::from_slice(simd, &scales[range.clone()]);
            let out_v = S::f32s::from_slice(simd, &out[range.clone()]);
            acc[u].mul_add(scale_v, out_v).store_slice(&mut out[range]);
        }
        j += UNROLL * n;
    }

    while j + n <= len {
        let mut acc = zero;
        for &(off, x) in rows {
            let x_vec = S::f32s::splat(simd, x);
            let [w_v] = load_i16::<S, 1>(simd, &w[off + j..off + j + n]);
            acc = w_v.mul_add(x_vec, acc);
        }
        let scale_v = S::f32s::from_slice(simd, &scales[j..j + n]);
        let out_v = S::f32s::from_slice(simd, &out[j..j + n]);
        acc.mul_add(scale_v, out_v).store_slice(&mut out[j..j + n]);
        j += n;
    }

    for jj in j..len {
        let mut acc = 0.0f32;
        for &(off, x) in rows {
            acc += w[off + jj] as f32 * x;
        }
        out[jj] += acc * scales[jj];
    }
}

/// `out[i] = a[i] * b[i]` over `out.len()` elements.
#[inline(always)]
pub fn mul_slices<S: Simd>(simd: S, out: &mut [f32], a: &[f32], b: &[f32]) {
    let n = S::f32s::LEN;
    let len = out.len();
    let (a, b) = (&a[..len], &b[..len]);

    let mut j = 0;
    while j + n <= len {
        let a_v = S::f32s::from_slice(simd, &a[j..j + n]);
        let b_v = S::f32s::from_slice(simd, &b[j..j + n]);
        (a_v * b_v).store_slice(&mut out[j..j + n]);
        j += n;
    }
    for i in j..len {
        out[i] = a[i] * b[i];
    }
}

/// `out[i] = sqrt(re^2 + im^2)` for each complex value.
#[inline(always)]
pub fn complex_norm<S: Simd>(simd: S, out: &mut [f32], input: &[Complex<f32>]) {
    let n = S::f32s::LEN;
    let len = out.len();
    let input = &input[..len];

    let mut j = 0;
    while j + n <= len {
        let c = &input[j..j + n];
        let re = S::f32s::from_fn(simd, |i| c[i].re);
        let im = S::f32s::from_fn(simd, |i| c[i].im);
        re.mul_add(re, im * im)
            .sqrt()
            .store_slice(&mut out[j..j + n]);
        j += n;
    }
    for i in j..len {
        out[i] = input[i].norm();
    }
}
