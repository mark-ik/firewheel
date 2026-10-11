use audioadapter::{Adapter, AdapterMut};
use audioadapter_buffers::{direct::InterleavedSlice, owned::InterleavedOwned};
use fixed_resample::{PacketResamplerBuffer, rubato};

/// Preallocated interleaved packet storage with exact frame-range copying.
///
/// fixed-resample 0.13's Interleaved buffer copies `src..count` when trimming
/// delay instead of `src..src + count`. That leaves delay zeroes in short EOF
/// packets (and truncates longer copies). Keep the packet algorithm, but own
/// its buffer implementation so all output frames are moved correctly.
pub(super) struct StreamPacketBuffer {
    input: InterleavedOwned<f32>,
    output: Vec<f32>,
    channels: usize,
    output_frames: usize,
}

impl PacketResamplerBuffer<f32> for StreamPacketBuffer {
    type Output = [f32];

    fn new(channels: usize, input_frames: usize, output_frames: usize) -> Self {
        Self {
            input: InterleavedOwned::new(0.0, channels, input_frames),
            output: vec![0.0; channels * output_frames],
            channels,
            output_frames,
        }
    }

    fn output(&self, frames: usize) -> &Self::Output {
        &self.output[..frames * self.channels]
    }

    fn resample(
        &mut self,
        indexing: Option<&rubato::Indexing>,
        resampler: &mut Box<dyn rubato::Resampler<f32>>,
    ) -> rubato::ResampleResult<(usize, usize)> {
        let mut output =
            InterleavedSlice::new_mut(&mut self.output, self.channels, self.output_frames).unwrap();
        resampler.process_into_buffer(&self.input, &mut output, indexing)
    }

    fn copy_from_other_to_input_channel(
        &mut self,
        other: &dyn Adapter<f32>,
        other_channel: usize,
        self_channel: usize,
        other_skip: usize,
        self_skip: usize,
        take: usize,
    ) -> Option<usize> {
        self.input.copy_from_other_to_channel(
            other,
            other_channel,
            self_channel,
            other_skip,
            self_skip,
            take,
        )
    }

    fn input_fill_frames_with(&mut self, start: usize, count: usize, value: &f32) -> Option<usize> {
        self.input.fill_frames_with(start, count, value)
    }

    fn input_fill_with(&mut self, value: &f32) {
        self.input.fill_with(value);
    }

    fn output_copy_frames_within(&mut self, src: usize, dest: usize, count: usize) {
        self.output.copy_within(
            src * self.channels..(src + count) * self.channels,
            dest * self.channels,
        );
    }
}
