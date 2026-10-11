//! Bounded continuous capture and producer playback for standalone hosts.
//!
//! The node/config/state names retain the old Firewheel stream-node surface.
//! Capacity is now measured at the **output** sample rate. Autocorrection must
//! be explicitly disabled: this bridge never silently inserts source time or
//! discards queued media. Capture does not add latency zeros. Playback's optional
//! initial latency zeros and underrun zeros are excluded from source accounting.
//!
//! Host handles serialize their endpoint with a mutex. The audio processor owns
//! the other SPSC endpoint and never locks or allocates while processing. Stream
//! replacement is an event; old endpoints are collected away from the callback.
//! File/network decoding and application session schemas remain host concerns.

mod channel;
pub mod reader;
pub mod writer;

pub use fixed_resample::{
    PushStatus, ReadStatus, ResampleQuality, ResamplerConfig, ResamplingChannelConfig,
};

/// Largest channel count supported by these nodes, inclusive.
pub const MAX_CHANNELS: usize = 16;

/// A configuration for bounded media with no source-time autocorrection.
///
/// Unlike fixed-resample's default, this adds no initial latency zeros.
pub fn media_channel_config(capacity_seconds: f64) -> ResamplingChannelConfig {
    ResamplingChannelConfig {
        capacity_seconds,
        latency_seconds: 0.0,
        underflow_autocorrect_percent_threshold: None,
        overflow_autocorrect_percent_threshold: None,
        ..Default::default()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StreamError {
    #[error("a stream is already active")]
    AlreadyActive,
    #[error("stream configuration is invalid")]
    InvalidConfig,
    #[error("source-time autocorrection is unsupported; set both thresholds to None")]
    AutocorrectionUnsupported,
}

/// Monotonic counters for the current stream generation.
///
/// Presented means consumed by the graph (writer) or host (reader), not yet
/// physically audible at the device. Device output latency is a host concern.
/// Explicit capture discard retires source position and is reported separately
/// as discarded_output_frames; it never happens automatically.
/// Atomics form a conservative observation, not a transactional snapshot.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StreamStats {
    pub accepted_source_frames: u64,
    pub presented_source_frames: u64,
    pub queued_source_frames: u64,
    pub produced_output_frames: u64,
    pub consumed_output_frames: u64,
    /// Offered source frames not accepted due to capacity. Playback callers
    /// retain/retry them; a live capture producer cannot do so.
    pub rejected_source_frames: u64,
    /// Zeros emitted to fill a short read; these never advance source time.
    pub underrun_output_frames: u64,
    /// Initial latency zeros consumed before media. Capture always reports zero.
    pub latency_output_frames: u64,
    pub discarded_output_frames: u64,
    pub capacity_output_frames: usize,
    /// EOF requested and every partial resampler packet has been published.
    pub finished: bool,
    /// Finished and all queued media consumed or explicitly discarded.
    pub drained: bool,
}
