#![cfg(feature = "stream_nodes")]

use audioadapter_buffers::direct::InterleavedSlice;
use firewheel::{
    ActivateInfo, FirewheelConfig, FirewheelContext,
    backend::BackendProcessInfo,
    channel_config::{ChannelCount, NonZeroChannelCount},
    node::{NodeID, StreamStatus},
    nodes::stream::{
        PushStatus, ReadStatus, ResampleQuality, ResamplingChannelConfig, StreamError,
        media_channel_config,
        reader::{StreamReaderConfig, StreamReaderNode, StreamReaderState},
        writer::{StreamWriterConfig, StreamWriterNode, StreamWriterState},
    },
    processor::FirewheelProcessor,
};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    num::NonZeroU32,
    time::Duration,
};

thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    static DEALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}
struct CountedAllocator;
unsafe impl GlobalAlloc for CountedAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        TRACK.with(|track| {
            if track.get() {
                ALLOCATIONS.with(|n| n.set(n.get() + 1));
            }
        });
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        TRACK.with(|track| {
            if track.get() {
                DEALLOCATIONS.with(|n| n.set(n.get() + 1));
            }
        });
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        TRACK.with(|track| {
            if track.get() {
                ALLOCATIONS.with(|n| n.set(n.get() + 1));
            }
        });
        unsafe { System.realloc(ptr, layout, size) }
    }
}
#[global_allocator]
static ALLOCATOR: CountedAllocator = CountedAllocator;

fn nz(rate: u32) -> NonZeroU32 {
    NonZeroU32::new(rate).unwrap()
}
fn channels(count: usize) -> NonZeroChannelCount {
    NonZeroChannelCount::new(count as u32).unwrap()
}

struct Offline {
    // Return the processor before context Drop tries to deactivate it.
    processor: FirewheelProcessor,
    cx: FirewheelContext,
    channels: usize,
    input_channels: usize,
    frame: u64,
}
impl Offline {
    fn activate(
        mut cx: FirewheelContext,
        rate: u32,
        channels: usize,
        input_channels: usize,
    ) -> Self {
        let processor = cx
            .activate(ActivateInfo {
                sample_rate: nz(rate),
                max_block_frames: nz(8192),
                num_stream_in_channels: input_channels as u32,
                num_stream_out_channels: channels as u32,
                input_to_output_latency_seconds: 0.0,
            })
            .unwrap();
        cx.update().unwrap();
        Self {
            cx,
            processor,
            channels,
            input_channels,
            frame: 0,
        }
    }
    fn block(&mut self, input: &[f32], output: &mut [f32]) {
        let frames = output.len() / self.channels;
        let input = InterleavedSlice::new(input, self.input_channels, frames).unwrap();
        let mut output = InterleavedSlice::new_mut(output, self.channels, frames).unwrap();
        ALLOCATIONS.with(|n| n.set(0));
        DEALLOCATIONS.with(|n| n.set(0));
        TRACK.with(|track| track.set(true));
        self.processor.process(
            &input,
            &mut output,
            BackendProcessInfo {
                frames,
                process_timestamp: None,
                duration_since_stream_start: Duration::from_secs_f64(self.frame as f64 / 48_000.0),
                input_stream_status: StreamStatus::empty(),
                output_stream_status: StreamStatus::empty(),
                dropped_frames: 0,
                process_to_playback_delay: None,
            },
        );
        TRACK.with(|track| track.set(false));
        assert_eq!(ALLOCATIONS.with(Cell::get), 0, "callback allocated");
        assert_eq!(DEALLOCATIONS.with(Cell::get), 0, "callback deallocated");
        self.frame += frames as u64;
        self.cx.update().unwrap();
    }
    fn render(&mut self, frames: usize) -> Vec<f32> {
        let mut output = vec![0.0; frames * self.channels];
        self.block(&[], &mut output);
        output
    }
}

#[test]
fn silent_capture_preserves_source_duration_and_pause_admits_no_frames() {
    let (mut offline, mut state, _) = reader(48_000, 48_000, 1, media_channel_config(0.01));
    offline.block(&[0.0; 40], &mut [1.0; 40]);
    assert_eq!(state.available_frames(), 40);
    state.pause_stream();
    offline.block(&[0.5; 20], &mut [1.0; 20]);
    assert_eq!(state.stats().unwrap().accepted_source_frames, 40);
    state.resume();
    let mut capture = [1.0; 40];
    assert_eq!(state.read_interleaved(&mut capture), Some(ReadStatus::Ok));
    assert_eq!(capture, [0.0; 40]);
}

fn writer(
    input_rate: u32,
    output_rate: u32,
    count: usize,
    config: ResamplingChannelConfig,
) -> (Offline, StreamWriterState, NodeID) {
    let mut cx = FirewheelContext::new(FirewheelConfig {
        num_graph_outputs: ChannelCount::new(count as u32).unwrap(),
        ..Default::default()
    });
    let node = cx
        .add_node(
            StreamWriterNode,
            Some(StreamWriterConfig {
                channels: channels(count),
                check_for_silence: true,
            }),
        )
        .unwrap();
    let edges: Vec<_> = (0..count as u32).map(|ch| (ch, ch)).collect();
    cx.connect(node, cx.graph_out_node_id(), &edges, false)
        .unwrap();
    let mut state = cx.node_state::<StreamWriterState>(node).unwrap().clone();
    let event = state
        .start_stream(nz(input_rate), nz(output_rate), config)
        .unwrap();
    cx.queue_event_for(node, event.into());
    let mut offline = Offline::activate(cx, output_rate, count, 0);
    offline.render(1);
    assert!(state.is_ready());
    (offline, state, node)
}

fn reader(
    input_rate: u32,
    output_rate: u32,
    count: usize,
    config: ResamplingChannelConfig,
) -> (Offline, StreamReaderState, NodeID) {
    let mut cx = FirewheelContext::new(FirewheelConfig {
        num_graph_inputs: ChannelCount::new(count as u32).unwrap(),
        num_graph_outputs: ChannelCount::new(count as u32).unwrap(),
        ..Default::default()
    });
    let node = cx
        .add_node(
            StreamReaderNode,
            Some(StreamReaderConfig {
                channels: channels(count),
            }),
        )
        .unwrap();
    let edges: Vec<_> = (0..count as u32).map(|ch| (ch, ch)).collect();
    cx.connect(cx.graph_in_node_id(), node, &edges, false)
        .unwrap();
    let mut state = cx.node_state::<StreamReaderState>(node).unwrap().clone();
    let event = state
        .start_stream(nz(input_rate), nz(output_rate), config)
        .unwrap();
    cx.queue_event_for(node, event.into());
    (Offline::activate(cx, input_rate, count, count), state, node)
}

#[test]
fn capture_is_input_only_preserves_channels_and_adds_no_latency_silence() {
    let mut config = media_channel_config(0.02);
    config.latency_seconds = 0.15;
    let (mut offline, mut state, _) = reader(48_000, 48_000, 2, config);
    let input: Vec<_> = (0..256).flat_map(|i| [i as f32 / 512.0, -0.25]).collect();
    let mut graph_output = vec![1.0; 512];
    offline.block(&input, &mut graph_output);
    assert!(graph_output.iter().all(|v| *v == 0.0));
    assert_eq!(state.available_frames(), 256);
    let mut captured = vec![0.0; 512];
    assert_eq!(state.read_interleaved(&mut captured), Some(ReadStatus::Ok));
    assert_eq!(captured, input);
    let stats = state.stats().unwrap();
    assert_eq!(stats.accepted_source_frames, 256);
    assert_eq!(stats.presented_source_frames, 256);
    assert_eq!(stats.latency_output_frames, 0);
}

#[test]
fn playback_backpressure_partial_push_and_underrun_do_not_advance_source_time() {
    let (mut offline, mut state, _) = writer(48_000, 48_000, 1, media_channel_config(0.002));
    let capacity = state.available_frames();
    assert_eq!(capacity, 96);
    let offered: Vec<_> = (0..capacity + 31).map(|i| i as f32).collect();
    assert_eq!(
        state.push_interleaved(&offered),
        PushStatus::OverflowOccurred {
            num_frames_pushed: capacity
        }
    );
    assert!(state.overflow_occurred());
    assert!(!state.overflow_occurred());
    let output = offline.render(capacity + 20);
    assert_eq!(&output[..capacity], &offered[..capacity]);
    assert!(output[capacity..].iter().all(|v| *v == 0.0));
    let stats = state.stats().unwrap();
    assert_eq!(stats.accepted_source_frames, capacity as u64);
    assert_eq!(stats.presented_source_frames, capacity as u64);
    assert_eq!(stats.queued_source_frames, 0);
    assert_eq!(stats.rejected_source_frames, 31);
    assert!(stats.underrun_output_frames >= 20);
    assert_eq!(state.push_interleaved(&offered[capacity..]), PushStatus::Ok);
    assert_eq!(offline.render(31), offered[capacity..]);
}

#[test]
fn pause_keeps_queue_and_stop_restart_discards_old_generation() {
    let (mut offline, mut state, node) = writer(48_000, 48_000, 2, media_channel_config(0.01));
    assert_eq!(state.push_interleaved(&vec![0.25; 200]), PushStatus::Ok);
    state.pause_stream();
    assert_eq!(state.available_frames(), 0);
    assert_eq!(
        state.push_interleaved(&[0.5, 0.5]),
        PushStatus::OutputNotReady
    );
    assert!(offline.render(77).iter().all(|v| *v == 0.0));
    assert_eq!(state.stats().unwrap().queued_source_frames, 100);
    state.resume();
    assert!(offline.render(40).iter().all(|v| *v == 0.25));
    state.stop_stream();
    let event = state
        .start_stream(nz(48_000), nz(48_000), media_channel_config(0.01))
        .unwrap();
    offline.cx.queue_event_for(node, event.into());
    offline.cx.update().unwrap();
    assert!(offline.render(1).iter().all(|v| *v == 0.0));
    assert_eq!(state.stats().unwrap().accepted_source_frames, 0);
    assert_eq!(state.push_interleaved(&[0.75, -0.5]), PushStatus::Ok);
    assert_eq!(offline.render(1), [0.75, -0.5]);
}

#[test]
fn dropping_a_handle_clone_does_not_stop_the_stream() {
    let (mut offline, mut state, _) = writer(48_000, 48_000, 1, media_channel_config(0.01));
    drop(state.clone());
    assert!(state.is_active());
    state.push_interleaved(&[0.5; 10]);
    assert_eq!(offline.render(10), [0.5; 10]);
}

#[test]
fn stale_start_event_cannot_revive_a_stopped_generation() {
    let mut cx = FirewheelContext::new(Default::default());
    let node = cx.add_node(StreamWriterNode, None).unwrap();
    cx.connect(node, cx.graph_out_node_id(), &[(0, 0), (1, 1)], false)
        .unwrap();
    let mut state = cx.node_state::<StreamWriterState>(node).unwrap().clone();
    let stale = state
        .start_stream(nz(48_000), nz(48_000), media_channel_config(0.01))
        .unwrap();
    state.stop_stream();
    cx.queue_event_for(node, stale.into());
    let mut offline = Offline::activate(cx, 48_000, 2, 0);
    assert!(offline.render(20).iter().all(|v| *v == 0.0));
    assert!(!state.is_ready());
}

#[test]
fn initial_latency_silence_is_excluded_from_source_clock() {
    let mut config = media_channel_config(0.01);
    config.latency_seconds = 0.001;
    let (mut offline, mut state, _) = writer(48_000, 48_000, 1, config);
    state.push_interleaved(&[0.5; 30]);
    let output = offline.render(70);
    assert_eq!(&output[..47], &[0.0; 47]);
    assert_eq!(&output[47..], &[0.5; 23]);
    let stats = state.stats().unwrap();
    assert_eq!(stats.latency_output_frames, 48);
    assert_eq!(stats.presented_source_frames, 23);
    assert_eq!(stats.queued_source_frames, 7);
}

#[test]
fn capture_overflow_and_partial_reads_are_explicit() {
    let (mut offline, mut state, _) = reader(48_000, 48_000, 1, media_channel_config(0.002));
    let mut output = [0.0; 160];
    offline.block(&[0.25; 160], &mut output);
    assert_eq!(state.available_frames(), 96);
    let stats = state.stats().unwrap();
    assert_eq!(stats.accepted_source_frames, 96);
    assert_eq!(stats.rejected_source_frames, 64);
    assert!(state.overflow_occurred());
    let mut captured = [9.0; 120];
    assert_eq!(
        state.read_interleaved(&mut captured),
        Some(ReadStatus::UnderflowOccurred {
            num_frames_read: 96
        })
    );
    assert_eq!(&captured[..96], &[0.25; 96]);
    assert_eq!(&captured[96..], &[0.0; 24]);
}

#[test]
fn resampled_backlogs_eof_and_source_clocks_cover_low_and_noninteger_rates() {
    for (input_rate, output_rate) in [
        (1_000, 48_000),
        (8_000, 44_100),
        (11_025, 48_000),
        (22_050, 48_000),
        (48_000, 44_100),
        (48_000, 8_000),
        (44_100, 48_000),
        (48_000, 48_000),
    ] {
        for channels in [1, 2] {
            let (mut offline, mut state, _) = writer(
                input_rate,
                output_rate,
                channels,
                media_channel_config(0.04),
            );
            let source_frames = input_rate as usize + 137;
            let mut accepted = 0;
            let mut heard = false;
            for iteration in 0..20_000 {
                let offered = [2713, 4097, 701, 23][iteration % 4].min(source_frames - accepted);
                if offered > 0 {
                    let input: Vec<_> = (0..offered)
                        .flat_map(|_| (0..channels).map(|ch| if ch == 0 { 0.25 } else { -0.5 }))
                        .collect();
                    let frames = match state.push_interleaved(&input) {
                        PushStatus::Ok => offered,
                        PushStatus::OverflowOccurred { num_frames_pushed } => num_frames_pushed,
                        PushStatus::OutputNotReady => 0,
                        other => panic!("unexpected correction {other:?}"),
                    };
                    accepted += frames;
                }
                if accepted == source_frames {
                    state.finish_stream();
                }
                let output = offline.render([127, 509, 211][iteration % 3]);
                heard |= output.iter().any(|v| v.abs() > 0.1);
                for frame in output.chunks_exact(channels) {
                    if channels == 2 && frame[0].abs() > 0.01 {
                        assert!((frame[1] + frame[0] * 2.0).abs() < 0.001);
                    }
                }
                let stats = state.stats().unwrap();
                assert!(stats.presented_source_frames <= stats.accepted_source_frames);
                assert_eq!(stats.accepted_source_frames, accepted as u64);
                if stats.drained {
                    break;
                }
                assert!(
                    iteration < 19_999,
                    "stalled {input_rate}->{output_rate}, ch {channels}"
                );
            }
            let stats = state.stats().unwrap();
            let expected_output = (source_frames as u64 * output_rate as u64
                + input_rate as u64 / 2)
                / input_rate as u64;
            assert!(heard, "silent {input_rate}->{output_rate}");
            assert_eq!(stats.produced_output_frames, expected_output);
            assert_eq!(stats.consumed_output_frames, expected_output);
            assert_eq!(stats.presented_source_frames, source_frames as u64);
            assert_eq!(stats.queued_source_frames, 0);
            assert!(stats.finished && stats.drained);
            assert_eq!(
                state.push_interleaved(&vec![0.5; channels]),
                PushStatus::OutputNotReady
            );
        }
    }
}

#[test]
fn subpacket_eof_flushes_capture_and_playback_without_source_padding() {
    // Delay trim must move the actual source into every retained output frame,
    // including coprime-rate EOF packets whose length is shorter than delay.
    for quality in [
        ResampleQuality::VeryLow,
        ResampleQuality::Low,
        ResampleQuality::High,
        ResampleQuality::HighWithLowLatency,
    ] {
        for (input_rate, output_rate) in [
            (1_000, 48_000),
            (8_000, 44_100),
            (8_000, 48_000),
            (11_025, 48_000),
            (48_000, 44_100),
            (48_000, 8_000),
        ] {
            for channels in [1, 2] {
                for source_frames in [37, 73, 317] {
                    let mut config = media_channel_config(1.0);
                    config.resampler_config.quality = quality;
                    let input: Vec<_> = (0..source_frames)
                        .flat_map(|_| (0..channels).map(|ch| if ch == 0 { 0.25 } else { -0.5 }))
                        .collect();
                    let expected = (source_frames as u64 * output_rate as u64
                        + input_rate as u64 / 2)
                        / input_rate as u64;
                    let (mut offline, mut state, _) =
                        writer(input_rate, output_rate, channels, config);
                    assert_eq!(state.push_interleaved(&input), PushStatus::Ok);
                    assert_eq!(
                        state.stats().unwrap().queued_source_frames,
                        source_frames as u64
                    );
                    assert!(state.finish_stream());
                    let output = offline.render(expected as usize + 1);
                    assert!(
                        output.iter().any(|v| v.abs() > 0.01),
                        "silent playback {quality:?} {input_rate}->{output_rate}/{channels}/{source_frames}"
                    );
                    let stats = state.stats().unwrap();
                    assert_eq!(stats.produced_output_frames, expected);
                    assert_eq!(stats.accepted_source_frames, source_frames as u64);
                    assert_eq!(stats.presented_source_frames, source_frames as u64);
                    assert!(stats.finished && stats.drained);
                    for frame in output.chunks_exact(channels) {
                        if channels == 2 {
                            assert!((frame[1] + 2.0 * frame[0]).abs() < 0.001);
                        }
                    }

                    let (mut offline, mut state, _) =
                        reader(input_rate, output_rate, channels, config);
                    offline.block(&input, &mut vec![0.0; input.len()]);
                    state.finish_stream();
                    offline.block(&vec![0.75; channels], &mut vec![0.0; channels]);
                    assert_eq!(state.available_frames(), expected as usize);
                    let mut output = vec![0.0; expected as usize * channels];
                    assert_eq!(state.read_interleaved(&mut output), Some(ReadStatus::Ok));
                    assert!(
                        output.iter().any(|v| v.abs() > 0.01),
                        "silent capture {quality:?} {input_rate}->{output_rate}/{channels}/{source_frames}"
                    );
                    let stats = state.stats().unwrap();
                    assert_eq!(stats.accepted_source_frames, source_frames as u64);
                    assert_eq!(stats.presented_source_frames, source_frames as u64);
                    assert!(stats.finished && stats.drained);
                    for frame in output.chunks_exact(channels) {
                        if channels == 2 {
                            assert!((frame[1] + 2.0 * frame[0]).abs() < 0.001);
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn configuration_refuses_implicit_clock_correction_and_invalid_bounds() {
    let mut state = StreamWriterState::new(channels(1));
    assert!(matches!(
        state.start_stream(nz(48_000), nz(48_000), ResamplingChannelConfig::default()),
        Err(StreamError::AutocorrectionUnsupported)
    ));
    for capacity in [0.0, -1.0, f64::NAN, f64::INFINITY, 1.0e20] {
        assert!(matches!(
            state.start_stream(nz(48_000), nz(48_000), media_channel_config(capacity)),
            Err(StreamError::InvalidConfig)
        ));
    }
    let mut config = media_channel_config(0.1);
    config.resampler_config.chunk_size = 0;
    assert!(matches!(
        state.start_stream(nz(8_000), nz(48_000), config),
        Err(StreamError::InvalidConfig)
    ));
    for (input_rate, output_rate) in [
        (1, 48_000),
        (48_000, u32::MAX),
        (999, 48_000),
        (384_001, 48_000),
        (1_000, 384_000),
        (48_000, 44_101),
    ] {
        ALLOCATIONS.with(|n| n.set(0));
        TRACK.with(|track| track.set(true));
        let result = state.start_stream(nz(input_rate), nz(output_rate), media_channel_config(0.1));
        TRACK.with(|track| track.set(false));
        assert!(matches!(result, Err(StreamError::InvalidConfig)));
        assert_eq!(
            ALLOCATIONS.with(Cell::get),
            0,
            "invalid rates allocated before refusal"
        );
    }
}

#[test]
fn all_sixteen_channels_are_supported_and_capture_discard_is_accounted() {
    let (mut offline, mut state, _) = reader(48_000, 48_000, 16, media_channel_config(0.01));
    let input: Vec<_> = (0..33).flat_map(|_| (0..16).map(|ch| ch as f32)).collect();
    let mut output = vec![0.0; input.len()];
    offline.block(&input, &mut output);
    assert_eq!(state.discard_frames(), 33);
    let stats = state.stats().unwrap();
    assert_eq!(stats.discarded_output_frames, 33);
    assert_eq!(stats.queued_source_frames, 0);
}

#[test]
fn each_resampler_quality_flushes_partial_eof_with_bounded_admission() {
    for quality in [
        ResampleQuality::VeryLow,
        ResampleQuality::Low,
        ResampleQuality::High,
        ResampleQuality::HighWithLowLatency,
    ] {
        let mut config = media_channel_config(0.03);
        config.resampler_config.quality = quality;
        let (mut offline, mut state, _) = writer(11_025, 48_000, 1, config);
        let source_frames = 2_711;
        let mut accepted = 0;
        let mut heard = false;
        for _ in 0..1_000 {
            if accepted < source_frames {
                let offered = (source_frames - accepted).min(733);
                match state.push_interleaved(&vec![0.25; offered]) {
                    PushStatus::Ok => accepted += offered,
                    PushStatus::OverflowOccurred { num_frames_pushed } => {
                        accepted += num_frames_pushed
                    }
                    _ => {}
                }
            } else {
                state.finish_stream();
            }
            heard |= offline.render(257).iter().any(|sample| sample.abs() > 0.1);
            if state.stats().unwrap().drained {
                break;
            }
        }
        let stats = state.stats().unwrap();
        assert!(heard && stats.drained, "{quality:?} stalled");
        assert_eq!(stats.accepted_source_frames, source_frames as u64);
        assert_eq!(stats.presented_source_frames, source_frames as u64);
        assert_eq!(
            stats.consumed_output_frames,
            (source_frames as u64 * 48_000 + 11_025 / 2) / 11_025
        );
    }
}

#[test]
fn planar_ranges_and_missing_channels_are_zero_filled_without_allocating() {
    let (mut offline, mut state, _) = writer(48_000, 48_000, 2, media_channel_config(0.01));
    assert_eq!(state.push(&[vec![0.75; 20]], 3..13), PushStatus::Ok);
    assert_eq!(offline.render(10), [0.75, 0.0].repeat(10));

    let (mut offline, mut state, _) = reader(48_000, 48_000, 2, media_channel_config(0.01));
    offline.block(&[0.25, -0.5].repeat(10), &mut [0.0; 20]);
    let mut output = [vec![1.0; 15], vec![1.0; 15], vec![1.0; 15]];
    assert_eq!(state.read(&mut output, 3..13), Some(ReadStatus::Ok));
    assert_eq!(&output[0][3..13], &[0.25; 10]);
    assert_eq!(&output[1][3..13], &[-0.5; 10]);
    assert_eq!(&output[2][3..13], &[0.0; 10]);
    assert_eq!(&output[0][..3], &[1.0; 3]);
}
