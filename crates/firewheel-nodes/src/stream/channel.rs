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
        let consumed = self.consumed.load(Ordering::Acquire);
        let discarded = self.discarded.load(Ordering::Acquire);
        let produced = self.produced.load(Ordering::Acquire);
        let accepted = self.accepted.load(Ordering::Acquire);
        let finished = self.finished.load(Ordering::Acquire);
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

pub(super) fn bounded_channel(
    channels: usize,
    input_rate: u32,
    output_rate: u32,
    config: ResamplingChannelConfig,
    playback: bool,
) -> Result<(Prod, Cons), StreamError> {
    if !config.capacity_seconds.is_finite()
        || config.capacity_seconds <= 0.0
        || !config.latency_seconds.is_finite()
        || config.latency_seconds < 0.0
        || config.resampler_config.chunk_size == 0
        || config.resampler_config.chunk_size > 65_536
    {
        return Err(StreamError::InvalidConfig);
    }
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
