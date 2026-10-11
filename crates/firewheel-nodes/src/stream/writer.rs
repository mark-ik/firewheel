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

pub use super::PushStatus;
use super::{
    MAX_CHANNELS, ResamplingChannelConfig, StreamError, StreamStats,
    channel::{Cons, Prod, bounded_channel},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "bevy", derive(bevy_ecs::prelude::Component))]
#[cfg_attr(feature = "bevy_reflect", derive(bevy_reflect::Reflect))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StreamWriterConfig {
    pub channels: NonZeroChannelCount,
    pub check_for_silence: bool,
}
impl Default for StreamWriterConfig {
    fn default() -> Self {
        Self {
            channels: NonZeroChannelCount::STEREO,
            check_for_silence: true,
        }
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "bevy", derive(bevy_ecs::prelude::Component))]
#[cfg_attr(feature = "bevy_reflect", derive(bevy_reflect::Reflect))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StreamWriterNode;

#[derive(Clone)]
pub struct StreamWriterState {
    channels: NonZeroChannelCount,
    active: Arc<Mutex<Option<HostEnd>>>,
}
struct HostEnd(Prod);
impl Drop for HostEnd {
    fn drop(&mut self) {
        self.0.shared.active.store(false, Ordering::Release);
    }
}

impl StreamWriterState {
    pub fn new(channels: NonZeroChannelCount) -> Self {
        Self {
            channels,
            active: Arc::new(Mutex::new(None)),
        }
    }
    /// Source rate, graph output rate, bounded queue options. Queue the returned
    /// event, then wait for is_ready(). Capacity_seconds uses graph output rate.
    pub fn start_stream(
        &mut self,
        sample_rate: NonZeroU32,
        output_stream_sample_rate: NonZeroU32,
        config: ResamplingChannelConfig,
    ) -> Result<NewInputStreamEvent, StreamError> {
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
            true,
        )?;
        *active = Some(HostEnd(prod));
        Ok(NewInputStreamEvent {
            cons: Some(OwnedGc::new(cons)),
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
        self.active.lock().unwrap().as_ref().is_some_and(|s| {
            s.0.shared.enabled()
                && s.0.shared.ready.load(Ordering::Acquire)
                && !s.0.shared.finish_requested.load(Ordering::Acquire)
        })
    }
    pub fn available_frames(&self) -> usize {
        self.active
            .lock()
            .unwrap()
            .as_ref()
            .filter(|s| s.0.shared.ready.load(Ordering::Acquire))
            .map(|s| s.0.available_frames())
            .unwrap_or(0)
    }
    /// Queued source duration including pending resampler input. Excludes
    /// initial latency and underrun zeros. Prefer stats() for integer clocks.
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
            .map(|s| NonZeroU32::new(s.0.shared.input_rate).unwrap())
    }
    pub fn push_interleaved(&mut self, input: &[f32]) -> PushStatus {
        let channels = self.channels.get().get() as usize;
        assert_eq!(
            input.len() % channels,
            0,
            "interleaved input must contain complete frames"
        );
        let frames = input.len() / channels;
        let mut active = self.active.lock().unwrap();
        if let Some(s) = active
            .as_mut()
            .filter(|s| s.0.shared.ready.load(Ordering::Acquire))
        {
            let adapter = InterleavedSlice::new(input, channels, frames).unwrap();
            s.0.push(&adapter, 0, frames)
        } else {
            PushStatus::OutputNotReady
        }
    }
    pub fn push<V: AsRef<[f32]>>(&mut self, input: &[V], range: Range<usize>) -> PushStatus {
        assert!(input.len() <= MAX_CHANNELS);
        let mut active = self.active.lock().unwrap();
        if let Some(s) = active
            .as_mut()
            .filter(|s| s.0.shared.ready.load(Ordering::Acquire))
        {
            let slices: [&[f32]; MAX_CHANNELS] =
                std::array::from_fn(|i| input.get(i).map(|v| v.as_ref()).unwrap_or_default());
            let adapter =
                SequentialSliceOfSlices::new(&slices[..input.len()], input.len(), range.end)
                    .unwrap();
            s.0.push(&adapter, range.start, range.end - range.start)
        } else {
            PushStatus::OutputNotReady
        }
    }

    /// Declare EOF and flush the partial resampler packet. No further pushes
    /// are accepted. Returns false under backpressure; drain and retry until
    /// true, then wait for stats().drained. Pause must be resumed to drain.
    pub fn finish_stream(&mut self) -> bool {
        self.active
            .lock()
            .unwrap()
            .as_mut()
            .map(|s| s.0.finish())
            .unwrap_or(true)
    }
    pub fn stats(&self) -> Option<StreamStats> {
        self.active
            .lock()
            .unwrap()
            .as_ref()
            .map(|s| s.0.shared.stats())
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
    /// Discard the entire generation (including pending resampler input).
    /// Call start_stream and queue its event to reset/seek at a new anchor.
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
    pub fn autocorrect_underflows(&mut self) -> Option<usize> {
        None
    }
    pub fn handle(&self) -> Mutex<Self> {
        Mutex::new(self.clone())
    }
}

impl AudioNode for StreamWriterNode {
    type Configuration = StreamWriterConfig;
    fn info(&self, config: &Self::Configuration) -> Result<AudioNodeInfo, NodeError> {
        if config.channels.get().get() as usize > MAX_CHANNELS {
            return Err(StreamError::InvalidConfig.into());
        }
        Ok(AudioNodeInfo::new()
            .debug_name("stream_writer")
            .channel_config(ChannelConfig {
                num_inputs: ChannelCount::ZERO,
                num_outputs: config.channels.get(),
            })
            .custom_state(StreamWriterState::new(config.channels)))
    }
    fn construct_processor(
        &self,
        config: &Self::Configuration,
        _: ConstructProcessorContext,
    ) -> Result<impl AudioNodeProcessor, NodeError> {
        Ok(Processor {
            cons: None,
            check_for_silence: config.check_for_silence,
        })
    }
}

struct Processor {
    cons: Option<OwnedGc<Cons>>,
    check_for_silence: bool,
}
impl AudioNodeProcessor for Processor {
    fn events(&mut self, _: &ProcInfo, events: &mut ProcEvents, _: &mut ProcExtra) {
        for mut event in events.drain() {
            if let Some(new) = event.downcast_mut::<NewInputStreamEvent>() {
                core::mem::swap(&mut self.cons, &mut new.cons);
                if let Some(cons) = &self.cons {
                    cons.shared.ready.store(true, Ordering::Release);
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
        let Some(cons) = self.cons.as_mut().filter(|c| c.shared.enabled()) else {
            return ProcessStatus::ClearAllOutputs;
        };
        let channels = buffers.outputs.len();
        let mut iter = buffers.outputs.iter_mut();
        let mut slices: [&mut [f32]; MAX_CHANNELS] =
            std::array::from_fn(|_| iter.next().map(|s| &mut **s).unwrap_or_default());
        let mut output =
            SequentialSliceOfSlices::new_mut(&mut slices[..channels], channels, info.frames)
                .unwrap();
        cons.read(&mut output, 0, info.frames);
        if self.check_for_silence {
            buffers.check_for_silence_on_outputs(0.0)
        } else {
            ProcessStatus::OutputsModified
        }
    }
    fn stream_stopped(&mut self, _: &mut ProcStreamCtx) {
        if let Some(cons) = &self.cons {
            cons.shared.active.store(false, Ordering::Release);
        }
    }
}

pub struct NewInputStreamEvent {
    cons: Option<OwnedGc<Cons>>,
}
impl From<NewInputStreamEvent> for NodeEventType {
    fn from(value: NewInputStreamEvent) -> Self {
        NodeEventType::custom(value)
    }
}
