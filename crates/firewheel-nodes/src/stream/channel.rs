use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use audioadapter::{Adapter, AdapterMut};
use audioadapter_buffers::direct::InterleavedSlice;
use firewheel_core::collector::ArcGc;
use fixed_resample::{Interleaved, LastPacketInfo, PacketResampler};
use ringbuf::traits::{Consumer, Observer, Producer, Split};

use super::{PushStatus, ReadStatus, ResamplingChannelConfig, StreamError, StreamStats};

pub(super) struct Shared {
    pub active: AtomicBool,
    pub ready: AtomicBool,
    pub paused: AtomicBool,
    pub finish_requested: AtomicBool,
    finished: AtomicBool,
    pub overflow: AtomicBool,
    pub underflow: AtomicBool,
    accepted: AtomicU64,
    produced: AtomicU64,
    consumed: AtomicU64,
    rejected: AtomicU64,
    underrun: AtomicU64,
    latency_consumed: AtomicU64,
    discarded: AtomicU64,
    pub input_rate: u32,
    pub output_rate: u32,
    pub capacity: usize,
}

impl Shared {
    pub fn stats(&self) -> StreamStats {
        // EOF is the publication fence for the final accepted/produced values.
        // Acquiring it after those values could observe finished=true with an
        // old produced=0 and incorrectly retire queued final output as drained.
        let finished = self.finished.load(Ordering::Acquire);
        let produced = self.produced.load(Ordering::Acquire);
        let accepted = self.accepted.load(Ordering::Acquire);
        let consumed = self.consumed.load(Ordering::Acquire);
        let discarded = self.discarded.load(Ordering::Acquire);
        let drained = finished && consumed.saturating_add(discarded) >= produced;
        let presented = if drained {
            accepted
        } else {
            ((u128::from(consumed.saturating_add(discarded)) * u128::from(self.input_rate))
                / u128::from(self.output_rate))
            .min(u128::from(accepted)) as u64
        };
        StreamStats {
            accepted_source_frames: accepted,
            presented_source_frames: presented,
            queued_source_frames: accepted.saturating_sub(presented),
            produced_output_frames: produced,
            consumed_output_frames: consumed,
            rejected_source_frames: self.rejected.load(Ordering::Relaxed),
            underrun_output_frames: self.underrun.load(Ordering::Relaxed),
            latency_output_frames: self.latency_consumed.load(Ordering::Relaxed),
            discarded_output_frames: discarded,
            capacity_output_frames: self.capacity,
            finished,
            drained,
        }
    }

    pub fn enabled(&self) -> bool {
        self.active.load(Ordering::Acquire) && !self.paused.load(Ordering::Acquire)
    }
}

pub(super) struct Prod {
    ring: ringbuf::HeapProd<f32>,
    pub shared: ArcGc<Shared>,
    channels: usize,
    resampler: Option<PacketResampler<f32, Interleaved<f32>>>,
    scratch: Vec<f32>,
    accepted: u64,
    produced: u64,
}

pub(super) struct Cons {
    ring: ringbuf::HeapCons<f32>,
    pub shared: ArcGc<Shared>,
    channels: usize,
    latency_left: usize,
}

fn validate_resources(
    channels: usize,
    input_rate: u32,
    output_rate: u32,
    config: ResamplingChannelConfig,
    playback: bool,
) -> Result<(), StreamError> {
    use super::ResampleQuality;
    const MAX_QUEUE_SAMPLES: usize = 16_777_216;
    if channels == 0
        || channels > super::MAX_CHANNELS
        || !(super::MIN_SAMPLE_RATE..=super::MAX_SAMPLE_RATE).contains(&input_rate)
        || !(super::MIN_SAMPLE_RATE..=super::MAX_SAMPLE_RATE).contains(&output_rate)
        || u64::from(input_rate) > u64::from(output_rate) * 64
        || u64::from(output_rate) > u64::from(input_rate) * 64
        || !config.capacity_seconds.is_finite()
        || config.capacity_seconds <= 0.0
        || !config.latency_seconds.is_finite()
        || config.latency_seconds < 0.0
        || config.resampler_config.chunk_size == 0
        || config.resampler_config.chunk_size > 8192
    {
        return Err(StreamError::InvalidConfig);
    }
    let max_queue_frames = MAX_QUEUE_SAMPLES / channels;
    let requested = (config.capacity_seconds * f64::from(output_rate)).ceil();
    let latency = if playback {
        (config.latency_seconds * f64::from(output_rate)).round()
    } else {
        0.0
    };
    if requested > max_queue_frames as f64 || latency > max_queue_frames as f64 {
        return Err(StreamError::InvalidConfig);
    }
    // Bound the queue and resampler *before* constructing PacketResampler.
    // FFT grain sizes grow with coprime rates, even with a small chunk_size.
    let packet_bound = if input_rate == output_rate {
        1
    } else if config.resampler_config.quality == ResampleQuality::High {
        let (mut a, mut b) = (input_rate, output_rate);
        while b != 0 {
            (a, b) = (b, a % b);
        }
        let input_grain = (input_rate / a) as usize;
        let output_grain = (output_rate / a) as usize;
        if input_grain > 8192 || output_grain > 8192 {
            return Err(StreamError::InvalidConfig);
        }
        let chunk = config.resampler_config.chunk_size;
        let sub_chunks = (chunk / 256).max(1);
        let fft_chunks = (chunk / sub_chunks).div_ceil(input_grain).max(1);
        let fft_input = fft_chunks * input_grain;
        let fft_output = fft_chunks * output_grain;
        chunk.div_ceil(fft_input) * fft_output
    } else {
        // Polynomial and sinc use fixed input blocks. This intentionally
        // conservative bound covers filter lookahead and output rounding.
        ((config.resampler_config.chunk_size + 512) * output_rate as usize)
            .div_ceil(input_rate as usize)
            + 1024
    };
    if packet_bound > 65_536
        || packet_bound
            .saturating_mul(3)
            .saturating_add(latency as usize)
            > max_queue_frames
    {
        return Err(StreamError::InvalidConfig);
    }
    Ok(())
}

pub(super) fn bounded_channel(
    channels: usize,
    input_rate: u32,
    output_rate: u32,
    config: ResamplingChannelConfig,
    playback: bool,
) -> Result<(Prod, Cons), StreamError> {
    validate_resources(channels, input_rate, output_rate, config, playback)?;
    if config.underflow_autocorrect_percent_threshold.is_some()
        || config.overflow_autocorrect_percent_threshold.is_some()
    {
        return Err(StreamError::AutocorrectionUnsupported);
    }
    let resampler = (input_rate != output_rate)
        .then(|| PacketResampler::new(channels, input_rate, output_rate, config.resampler_config));
    let latency = if playback {
        (config.latency_seconds * f64::from(output_rate)).round() as usize
    } else {
        0
    };
    // Reserve actual resampler packet bounds, including a pending partial
    // packet and an EOF tail. In particular, low-rate upsampling must admit its
    // first packet without relying on a nominal floating-point rate ratio.
    let packet = resampler
        .as_ref()
        .map(|r| r.max_output_block_frames().max(r.output_delay()))
        .unwrap_or(1);
    let minimum = packet.saturating_mul(3).saturating_add(latency);
    let capacity =
        ((config.capacity_seconds * f64::from(output_rate)).ceil() as usize).max(minimum);
    // Validate before multiplying or allocating. This is a bounded audio
    // bridge, not storage for arbitrary-length files.
    if capacity > 16_777_216 / channels || latency > capacity {
        return Err(StreamError::InvalidConfig);
    }
    let (mut ring_prod, ring_cons) = ringbuf::HeapRb::new(capacity * channels).split();
    for _ in 0..latency * channels {
        let _ = ring_prod.try_push(0.0);
    }
    let shared = ArcGc::new(Shared {
        active: AtomicBool::new(true),
        ready: AtomicBool::new(false),
        paused: AtomicBool::new(false),
        finish_requested: AtomicBool::new(false),
        finished: AtomicBool::new(false),
        overflow: AtomicBool::new(false),
        underflow: AtomicBool::new(false),
        accepted: AtomicU64::new(0),
        produced: AtomicU64::new(0),
        consumed: AtomicU64::new(0),
        rejected: AtomicU64::new(0),
        underrun: AtomicU64::new(0),
        latency_consumed: AtomicU64::new(0),
        discarded: AtomicU64::new(0),
        input_rate,
        output_rate,
        capacity,
    });
    Ok((
        Prod {
            ring: ring_prod,
            shared: ArcGc::clone(&shared),
            channels,
            resampler,
            scratch: vec![0.0; 512 * channels],
            accepted: 0,
            produced: 0,
        },
        Cons {
            ring: ring_cons,
            shared,
            channels,
            latency_left: latency,
        },
    ))
}

impl Prod {
    pub fn available_frames(&self) -> usize {
        if !self.shared.enabled() || self.shared.finish_requested.load(Ordering::Acquire) {
            return 0;
        }
        let vacant = self.ring.vacant_len() / self.channels;
        match &self.resampler {
            // One extra max output packet accounts conservatively for partial
            // input left inside PacketResampler. Accepted frames are never
            // overwritten, and callers may offer larger packets for partial push.
            Some(r) => (vacant / r.max_output_block_frames())
                .saturating_sub(1)
                .saturating_mul(r.max_input_block_frames()),
            None => vacant,
        }
    }

    pub fn push(&mut self, input: &dyn Adapter<f32>, start: usize, frames: usize) -> PushStatus {
        if !self.shared.enabled() || self.shared.finish_requested.load(Ordering::Acquire) {
            return PushStatus::OutputNotReady;
        }
        assert!(start <= input.frames() && frames <= input.frames() - start);
        let accepted = frames.min(self.available_frames());
        if let Some(resampler) = &mut self.resampler {
            let ring = &mut self.ring;
            let produced = &mut self.produced;
            resampler.process(
                input,
                Some(start..start + accepted),
                None,
                |packet, output_frames| {
                    let pushed = ring.push_slice(packet);
                    assert_eq!(pushed, packet.len(), "bounded resampler packet overflow");
                    *produced += output_frames as u64;
                },
                None,
                true,
            );
        } else {
            let mut copied = 0;
            while copied < accepted {
                let chunk = (accepted - copied).min(512);
                let mut scratch =
                    InterleavedSlice::new_mut(&mut self.scratch, self.channels, 512).unwrap();
                for ch in 0..self.channels {
                    for frame in 0..chunk {
                        let value = input.read_sample(ch, start + copied + frame).unwrap_or(0.0);
                        scratch.write_sample(ch, frame, &value);
                    }
                }
                let samples = chunk * self.channels;
                let pushed = self.ring.push_slice(&self.scratch[..samples]);
                assert_eq!(pushed, samples);
                copied += chunk;
            }
            self.produced += accepted as u64;
        }
        self.accepted += accepted as u64;
        self.shared.accepted.store(self.accepted, Ordering::Release);
        self.shared.produced.store(self.produced, Ordering::Release);
        if accepted < frames {
            self.shared.overflow.store(true, Ordering::Relaxed);
            self.shared
                .rejected
                .fetch_add((frames - accepted) as u64, Ordering::Relaxed);
            PushStatus::OverflowOccurred {
                num_frames_pushed: accepted,
            }
        } else {
            PushStatus::Ok
        }
    }

    /// Flush EOF without accepting source padding. False means backpressure:
    /// the caller must drain output and retry. No allocations occur here.
    pub fn finish(&mut self) -> bool {
        self.shared.finish_requested.store(true, Ordering::Release);
        if self.shared.finished.load(Ordering::Acquire) {
            return true;
        }
        if let Some(resampler) = &mut self.resampler {
            let desired = ((u128::from(self.accepted) * u128::from(self.shared.output_rate)
                + u128::from(self.shared.input_rate / 2))
                / u128::from(self.shared.input_rate)) as u64;
            let remaining = desired.saturating_sub(self.produced);
            if remaining > (self.ring.vacant_len() / self.channels) as u64 {
                return false;
            }
            let empty = InterleavedSlice::new(&[], self.channels, 0).unwrap();
            let ring = &mut self.ring;
            let produced = &mut self.produced;
            resampler.process(
                &empty,
                None,
                None,
                |packet, frames| {
                    assert_eq!(ring.push_slice(packet), packet.len());
                    *produced += frames as u64;
                },
                Some(LastPacketInfo {
                    desired_output_frames: Some(remaining),
                }),
                true,
            );
        }
        self.shared.produced.store(self.produced, Ordering::Release);
        self.shared.finished.store(true, Ordering::Release);
        true
    }
}

impl Cons {
    pub fn available_frames(&self) -> usize {
        self.ring.occupied_len() / self.channels
    }

    pub fn read(
        &mut self,
        output: &mut dyn AdapterMut<f32>,
        start: usize,
        frames: usize,
    ) -> ReadStatus {
        assert!(start <= output.frames() && frames <= output.frames() - start);
        output.fill_frames_with(start, frames, &0.0);
        let read = frames.min(self.available_frames());
        let (a, b) = self.ring.as_slices();
        for (samples, offset) in [(a, 0), (b, a.len() / self.channels)] {
            let chunk = (samples.len() / self.channels).min(read.saturating_sub(offset));
            let input =
                InterleavedSlice::new(samples, self.channels, samples.len() / self.channels)
                    .unwrap();
            for ch in 0..self.channels.min(output.channels()) {
                output.copy_from_other_to_channel(&input, ch, ch, 0, start + offset, chunk);
            }
        }
        self.ring.skip(read * self.channels);
        self.account_consumed(read, false);
        if read < frames {
            self.shared.underflow.store(true, Ordering::Relaxed);
            self.shared
                .underrun
                .fetch_add((frames - read) as u64, Ordering::Relaxed);
            ReadStatus::UnderflowOccurred {
                num_frames_read: read,
            }
        } else {
            ReadStatus::Ok
        }
    }

    pub fn discard(&mut self) -> usize {
        let frames = self.available_frames();
        self.ring.skip(frames * self.channels);
        self.account_consumed(frames, true);
        frames
    }

    fn account_consumed(&mut self, frames: usize, discard: bool) {
        let latency = frames.min(self.latency_left);
        self.latency_left -= latency;
        self.shared
            .latency_consumed
            .fetch_add(latency as u64, Ordering::Release);
        if discard {
            self.shared
                .discarded
                .fetch_add((frames - latency) as u64, Ordering::Release);
        } else {
            self.shared
                .consumed
                .fetch_add((frames - latency) as u64, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_eof_observation_never_retires_an_unconsumed_final_packet() {
        struct ReleaseOnExit<'a>(&'a std::sync::atomic::AtomicUsize);
        impl Drop for ReleaseOnExit<'_> {
            fn drop(&mut self) {
                self.0.store(usize::MAX, Ordering::Release);
            }
        }
        // Each generation publishes exactly one final output frame and has no
        // consumer advancement. Publication races stats() across many fresh
        // generations, without a reset that could itself change the contract.
        let channels: Vec<_> = (0..10_000)
            .map(|_| {
                bounded_channel(
                    1,
                    48_000,
                    48_000,
                    super::super::media_channel_config(0.001),
                    true,
                )
                .unwrap()
            })
            .collect();
        let observed: Vec<_> = channels
            .iter()
            .map(|(prod, _)| ArcGc::clone(&prod.shared))
            .collect();
        let ready = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|scope| {
            let ready = &ready;
            scope.spawn(move || {
                let input = InterleavedSlice::new(&[0.25], 1, 1).unwrap();
                for (index, (mut prod, _cons)) in channels.into_iter().enumerate() {
                    while ready.load(Ordering::Acquire) <= index {
                        std::hint::spin_loop();
                    }
                    assert_eq!(prod.push(&input, 0, 1), PushStatus::Ok);
                    assert!(prod.finish());
                    // _cons remains undrained; stats must not mistake old
                    // produced=0 for a finished generation with no output.
                }
            });
            // A failed assertion must release the publisher's handshake so a
            // reproducing regression fails rather than hanging scope shutdown.
            let _release = ReleaseOnExit(ready);
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            for (index, shared) in observed.into_iter().enumerate() {
                ready.store(index + 1, Ordering::Release);
                loop {
                    let stats = shared.stats();
                    assert!(
                        !stats.drained,
                        "final output was retired before consumption: {stats:?}"
                    );
                    if stats.finished {
                        assert_eq!(stats.produced_output_frames, 1);
                        assert_eq!(stats.accepted_source_frames, 1);
                        break;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "EOF publisher did not finish"
                    );
                    std::hint::spin_loop();
                }
            }
        });
    }
}
