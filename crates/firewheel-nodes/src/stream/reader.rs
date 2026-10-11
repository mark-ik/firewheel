use std::{
    num::NonZeroU32,
    ops::Range,
    sync::{Arc, Mutex, atomic::Ordering},
};

use audioadapter_buffers::direct::{InterleavedSlice, SequentialSliceOfSlices};
use firewheel_core::{
    channel_config::{ChannelConfig, ChannelCount, NonZeroChannelCount},
    collector::OwnedGc,
    event::{NodeEventType, ProcEvents},
    node::{
        AudioNode, AudioNodeInfo, AudioNodeProcessor, ConstructProcessorContext, NodeError,
        ProcBuffers, ProcExtra, ProcInfo, ProcStreamCtx, ProcessStatus,
    },
};

use super::{
    MAX_CHANNELS, ReadStatus, ResamplingChannelConfig, StreamError, StreamStats,
    channel::{Cons, Prod, bounded_channel},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "bevy", derive(bevy_ecs::prelude::Component))]
#[cfg_attr(feature = "bevy_reflect", derive(bevy_reflect::Reflect))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StreamReaderConfig {
    pub channels: NonZeroChannelCount,
}

impl Default for StreamReaderConfig {
    fn default() -> Self {
        Self {
            channels: NonZeroChannelCount::STEREO,
        }
    }
}

/// Input-only capture tap. No silent-monitor path to graph output is needed.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "bevy", derive(bevy_ecs::prelude::Component))]
#[cfg_attr(feature = "bevy_reflect", derive(bevy_reflect::Reflect))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StreamReaderNode;

/// Cloneable host handle; dropping one clone does not stop another clone's
/// stream. All clones address the same generation and serialize host reads.
#[derive(Clone)]
pub struct StreamReaderState {
    channels: NonZeroChannelCount,
    active: Arc<Mutex<Option<HostEnd>>>,
}

struct HostEnd(Cons);
impl Drop for HostEnd {
    fn drop(&mut self) {
        self.0.shared.active.store(false, Ordering::Release);
    }
}

impl StreamReaderState {
    pub fn new(channels: NonZeroChannelCount) -> Self {
        Self {
            channels,
            active: Arc::new(Mutex::new(None)),
        }
    }

    /// Start a fresh generation. Queue the returned event to this reader.
    ///
    /// sample_rate is graph input rate; output_stream_sample_rate is the host
    /// reader's desired rate. Capacity uses the latter. Capture does not insert
    /// latency zeros, regardless of config.latency_seconds.
    pub fn start_stream(
        &mut self,
        sample_rate: NonZeroU32,
        output_stream_sample_rate: NonZeroU32,
        config: ResamplingChannelConfig,
    ) -> Result<NewOutputStreamEvent, StreamError> {
        let mut active = self.active.lock().unwrap();
        if active
            .as_ref()
            .is_some_and(|s| s.0.shared.active.load(Ordering::Acquire))
        {
            return Err(StreamError::AlreadyActive);
        }
        if self.channels.get().get() as usize > MAX_CHANNELS {
            return Err(StreamError::InvalidConfig);
        }
        let (prod, cons) = bounded_channel(
            self.channels.get().get() as usize,
            sample_rate.get(),
            output_stream_sample_rate.get(),
            config,
            false,
        )?;
        *active = Some(HostEnd(cons));
        Ok(NewOutputStreamEvent {
            prod: Some(OwnedGc::new(prod)),
        })
    }

    pub fn is_active(&self) -> bool {
        self.active
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|s| s.0.shared.active.load(Ordering::Acquire))
    }

    pub fn is_ready(&self) -> bool {
        self.active
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|s| s.0.shared.enabled() && s.0.shared.ready.load(Ordering::Acquire))
    }

    pub fn available_frames(&self) -> usize {
        self.active
            .lock()
            .unwrap()
            .as_ref()
            .filter(|s| s.0.shared.enabled() && s.0.shared.ready.load(Ordering::Acquire))
            .map(|s| s.0.available_frames())
            .unwrap_or(0)
    }

    /// Queued source duration, including a partial resampler packet.
    pub fn occupied_seconds(&self) -> Option<f64> {
        self.active.lock().unwrap().as_ref().map(|s| {
            s.0.shared.stats().queued_source_frames as f64 / f64::from(s.0.shared.input_rate)
        })
    }

    pub fn num_channels(&self) -> NonZeroChannelCount {
        self.channels
    }
    pub fn sample_rate(&self) -> Option<NonZeroU32> {
        self.active
            .lock()
            .unwrap()
            .as_ref()
            .map(|s| NonZeroU32::new(s.0.shared.output_rate).unwrap())
    }

    pub fn read_interleaved(&mut self, output: &mut [f32]) -> Option<ReadStatus> {
        let channels = self.channels.get().get() as usize;
        assert_eq!(
            output.len() % channels,
            0,
            "interleaved output must contain complete frames"
        );
        let frames = output.len() / channels;
        let mut active = self.active.lock().unwrap();
        if let Some(s) = active
            .as_mut()
            .filter(|s| s.0.shared.enabled() && s.0.shared.ready.load(Ordering::Acquire))
        {
            let mut adapter = InterleavedSlice::new_mut(output, channels, frames).unwrap();
            Some(s.0.read(&mut adapter, 0, frames))
        } else {
            output.fill(0.0);
            None
        }
    }

    pub fn read<V: AsMut<[f32]>>(
        &mut self,
        output: &mut [V],
        range: Range<usize>,
    ) -> Option<ReadStatus> {
        assert!(output.len() <= MAX_CHANNELS);
        let mut active = self.active.lock().unwrap();
        if let Some(s) = active
            .as_mut()
            .filter(|s| s.0.shared.enabled() && s.0.shared.ready.load(Ordering::Acquire))
        {
            let channels = output.len();
            let mut iter = output.iter_mut();
            let mut slices: [&mut [f32]; MAX_CHANNELS] =
                std::array::from_fn(|_| iter.next().map(|v| v.as_mut()).unwrap_or_default());
            let mut adapter =
                SequentialSliceOfSlices::new_mut(&mut slices[..channels], channels, range.end)
                    .unwrap();
            Some(s.0.read(&mut adapter, range.start, range.end - range.start))
        } else {
            for channel in output {
                channel.as_mut()[range.clone()].fill(0.0);
            }
            None
        }
    }

    /// Explicitly discard the currently published output frames. Partial input
    /// held inside the resampler remains queued; stop/restart for a full reset.
    pub fn discard_frames(&mut self) -> usize {
        self.active
            .lock()
            .unwrap()
            .as_mut()
            .map(|s| s.0.discard())
            .unwrap_or(0)
    }

    pub fn stats(&self) -> Option<StreamStats> {
        self.active
            .lock()
            .unwrap()
            .as_ref()
            .map(|s| s.0.shared.stats())
    }

    /// Stop admitting capture at the next processor boundary, then publish its
    /// final partial packet. Keep draining until stats().drained is true.
    pub fn finish_stream(&mut self) {
        if let Some(s) = self.active.lock().unwrap().as_ref() {
            s.0.shared.finish_requested.store(true, Ordering::Release);
        }
    }

    pub fn pause_stream(&mut self) {
        if let Some(s) = self.active.lock().unwrap().as_ref() {
            s.0.shared.paused.store(true, Ordering::Release);
        }
    }
    pub fn resume(&mut self) {
        if let Some(s) = self.active.lock().unwrap().as_ref() {
            s.0.shared.paused.store(false, Ordering::Release);
        }
    }
    /// Discard this generation. Any pending old start event remains inactive.
    pub fn stop_stream(&mut self) {
        *self.active.lock().unwrap() = None;
    }
    pub fn underflow_occurred(&self) -> bool {
        self.active
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|s| s.0.shared.underflow.swap(false, Ordering::Relaxed))
    }
    pub fn overflow_occurred(&self) -> bool {
        self.active
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|s| s.0.shared.overflow.swap(false, Ordering::Relaxed))
    }
    /// Autocorrection is deliberately unsupported. Use explicit discard/stop.
    pub fn autocorrect_overflows(&mut self) -> Option<usize> {
        None
    }
    pub fn handle(&self) -> Mutex<Self> {
        Mutex::new(self.clone())
    }
}

impl AudioNode for StreamReaderNode {
    type Configuration = StreamReaderConfig;
    fn info(&self, config: &Self::Configuration) -> Result<AudioNodeInfo, NodeError> {
        if config.channels.get().get() as usize > MAX_CHANNELS {
            return Err(StreamError::InvalidConfig.into());
        }
        Ok(AudioNodeInfo::new()
            .debug_name("stream_reader")
            .channel_config(ChannelConfig {
                num_inputs: config.channels.get(),
                num_outputs: ChannelCount::ZERO,
            })
            .custom_state(StreamReaderState::new(config.channels)))
    }
    fn construct_processor(
        &self,
        _: &Self::Configuration,
        _: ConstructProcessorContext,
    ) -> Result<impl AudioNodeProcessor, NodeError> {
        Ok(Processor { prod: None })
    }
}

struct Processor {
    prod: Option<OwnedGc<Prod>>,
}
impl AudioNodeProcessor for Processor {
    fn events(&mut self, _: &ProcInfo, events: &mut ProcEvents, _: &mut ProcExtra) {
        for mut event in events.drain() {
            if let Some(new) = event.downcast_mut::<NewOutputStreamEvent>() {
                core::mem::swap(&mut self.prod, &mut new.prod);
                if let Some(prod) = &self.prod {
                    prod.shared.ready.store(true, Ordering::Release);
                }
            }
        }
    }
    fn process(
        &mut self,
        info: &ProcInfo,
        buffers: ProcBuffers,
        _: &mut ProcExtra,
    ) -> ProcessStatus {
        if let Some(prod) = self.prod.as_mut().filter(|p| p.shared.enabled()) {
            if prod.shared.finish_requested.load(Ordering::Acquire) {
                prod.finish();
            } else {
                let input =
                    SequentialSliceOfSlices::new(buffers.inputs, buffers.inputs.len(), info.frames)
                        .unwrap();
                prod.push(&input, 0, info.frames);
            }
        }
        ProcessStatus::ClearAllOutputs
    }
    fn stream_stopped(&mut self, _: &mut ProcStreamCtx) {
        if let Some(prod) = &self.prod {
            prod.shared.active.store(false, Ordering::Release);
        }
        // Retain allocation; OwnedGc also makes unexpected realtime drop safe.
    }
}

pub struct NewOutputStreamEvent {
    prod: Option<OwnedGc<Prod>>,
}
impl From<NewOutputStreamEvent> for NodeEventType {
    fn from(value: NewOutputStreamEvent) -> Self {
        NodeEventType::custom(value)
    }
}
