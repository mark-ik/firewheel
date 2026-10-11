# Standalone stream foundation

Date: 2026-10-11. Base: Firewheel 0.14, 34b35cad0535bf0a9542ac977b34744805d6cc03.
Lane: Woodshed-family migration F2.

## Purpose and scope

Restore a focused bounded capture and playback boundary for Hocket and Redshank.
Upstream removed the old reader/writer nodes in 71fbe0f7; the streamed sampler
is unfinished. This extension retains the recognizable old stream-node API while
implementing a queue whose capacity, resampler headroom, EOF, and source position
are explicit. It uses fixed-resample 0.13 PacketResampler and ringbuf 0.5. The
fork owns a small preallocated PacketResamplerBuffer implementation: the dependency's
interleaved delay-trim copy uses an incorrect source-range end, leaving zeros in
very short EOF output. The owned buffer copies src..src+count exactly.

Billy Messenger's upstream graph kernel, licenses, authorship and history remain
intact. No application schema, decoder, storage, network handle or device policy
belongs in these nodes. Bevy ECS is optional; stream_nodes enables std only.

## Supported contract

- Reader: graph inputs to a host-drained SPSC queue. Input-only taps work without
  routing silent data to graph output. Silence is captured with its full duration.
- Writer: host-produced media to graph outputs through a bounded SPSC queue.
  Mono/stereo/channel mapping remains a graph-routing decision.
- Channel counts 1 through 16 inclusive.
- Input/output rates 1,000 through 384,000 Hz inclusive and rate ratios within
  1/64 through 64. Chunk sizes are 1 through 8,192. High-quality FFT conversion
  additionally limits each reduced integer rate grain to 8,192 frames; other
  qualities can handle coprime rates within the same resource bounds. Estimated
  output packet bounds above 65,536 frames and queues above 16,777,216 samples
  return InvalidConfig before resampler construction. These are deliberate
  standalone resource limits, not support for every nonzero u32 rate.
- Equal-rate paths preserve sample values and frame order. Unequal rates use the
  configured fixed-ratio resampler; all buffers are allocated before processing.
- capacity_seconds means seconds at the **output rate**. Actual capacity is at
  least three maximum resampler output packets plus initial playback latency.
  stats().capacity_output_frames exposes the resulting bound. Hosts must remove
  the old input/output-rate capacity compensation and guessed FFT headroom.
- available_frames() reports conservative source-frame admission, reserving a
  maximum packet for pending resampler input. Larger pushes may accept a prefix;
  PushStatus::OverflowOccurred { num_frames_pushed } reports that exact prefix.
  Playback workers retain/retry the remainder. Live capture cannot retry rejected
  input; its rejection counter and overflow flag must be handled by the product.
- Both autocorrection thresholds must be None; otherwise start_stream returns
  StreamError::AutocorrectionUnsupported. media_channel_config(seconds) supplies
  those values and zero initial latency. The compatibility autocorrect methods
  always return None.
- Capture never pads its stream with latency zeros. Playback optionally prepends
  latency_seconds of zeros. The resampler's initial delay is trimmed. The inherited
  subtract_resampler_delay field does not alter that policy.
- Short reads fill the remainder with zero and report the exact frames read.
  Underrun zeros and initial latency zeros have separate counters and never advance
  accepted or presented source time.
- Host pause_stream stops producer admission and consumer advancement at the next
  observed processing boundary, preserving queued media. Resume continues it.
  The bridge does not fade across pause boundaries; a host can schedule a gain ramp.
- Writer finish_stream() -> bool seals admission and flushes the final partial
  resampler packet. If capacity is insufficient, drain and retry until true. Then
  wait for stats().drained. Output length is the rounded rational conversion of
  accepted source length, including streams shorter than one resampler packet.
- Reader finish_stream() requests sealing at the next callback boundary. That
  callback and subsequent callbacks retry EOF publication while the host drains.
  Reader capture requires the graph to keep processing until stats().finished.
- stop_stream() discards the whole generation, including unpublished resampler
  input. Start a new generation and queue its event for seek/reset. A delayed old
  start event cannot reactivate a stopped generation.
- Host clones share one generation and serialize their endpoint. Dropping one clone
  does not stop the stream. Backend stop invalidates the active generation; the host
  must create a new stream generation after reopening the device.

## Clock observations

stats() reports accepted, presented and queued source frames, produced/consumed
output frames, rejected offered frames, underrun/latency/discarded output frames,
and finished/drained state. Output consumption advances the source position with
integer rational conversion. EOF drain retires the final fractional source position
exactly to accepted length. An explicit reader discard advances the retired source
position and is reported separately; no discard occurs automatically.

These atomics provide conservative observations rather than a transactional
snapshot. Presented is the position consumed by the graph or host reader. It does
not include the physical device's playback delay, microphone latency compensation,
clock drift, or host WSOLA source/output mapping. occupied_seconds() is queued
source duration, including partial resampler input and excluding latency/underrun
zeros. Integer stats() is preferred over reconstructing positions from floats.

Audio processors own their endpoint and never take the host handle's mutex. Event
replacement swaps old endpoint ownership back into the event; OwnedGc protects
unexpected realtime drop and collection occurs away from the callback.

## Consumer migration

Keep stream_nodes enabled and use the same node/config/state imports.
start_stream(source_rate, destination_rate, config) still returns a custom event
to queue with .into(). Node construction now returns Result in Firewheel 0.14.

Hocket: use a zero-padding capture config, connect graph input directly to the
reader, remove the silent-monitor workaround under reproducing checks, and refuse
or mark captured material when input overflow is observed. Sample-accurate capture
selection/calibration remains a separate product task; this start/pause/finish API
uses callback boundaries.

Redshank: size the writer using media_channel_config(1.0) or explicit output-rate
capacity; remove the 0.9.2 ratio multiplier and guessed FFT reservation. Retry partial
pushes using their actual accepted count. Use stats().presented_source_frames for
the post-WSOLA writer position, then preserve the existing RateStage marks mapping
back to decoded source. At decoder EOF, after WSOLA flush and pending input admission,
call finish_stream until true and use stats().drained for final completion.

The separate CpalStream lifetime, new device enumeration, and sampler resource/play
events are other 0.14 migration changes. They remain outside the queue implementation.

## Validation and remaining gates

The maintained integration suite is tests/stream_nodes.rs. It runs real offline
graphs with a thread-local allocator guard around every callback, including new
stream events, endpoint replacement and resampler EOF:

~~~sh
cargo +1.97.1 test -p firewheel --no-default-features --features std,stream_nodes --test stream_nodes
cargo +1.97.1 test -p firewheel-nodes --no-default-features --features stream --lib
cargo +1.97.1 check -p firewheel --no-default-features --features std,stream_nodes,serde,scheduled_events
cargo +1.97.1 tree -p firewheel --no-default-features --features std,stream_nodes -i bevy_ecs
~~~

Coverage includes bounded partial pushes/reads, capture overflow, silence capture,
mono/stereo identity, planar ranges/missing channels, all four resampler qualities,
all 16 channels, clone lifetime, stale events, pause/reset,
initial latency, underrun clock behavior, subpacket EOF, and uneven backlog packets
at 1k→48k, 8k→44.1k, 11.025k→48k, 22.05k→48k, 48k→44.1k, 48k→8k,
44.1k→48k and equal-rate conversion.

Short EOF material is checked through both reader and writer graphs for all four
resampler qualities, mono/stereo, 37/73/317 source frames, and six rate pairs. These
288 cases assert actual nonzero material, channel proportionality and exact clocks,
including the 37-frame 8k→44.1k consumer reproducer; frame counts alone did not
detect the dependency's delay-copy defect. Capture EOF runs under the callback
allocation/deallocation guard.

Local result: 14 integration tests passed, zero callback allocations/deallocations,
plus a synchronized 10,000-generation concurrent EOF publication test. Extreme/
unsupported configurations are refused before resampler allocation; refusal tests
assert zero allocations. EOF stats acquires the finished publication fence before
reading final producer counters and subsequent consumption.
Strict Clippy passed for nodes and integration tests, as did rustfmt, warning-denied
node rustdoc, no_std/libm, serde/scheduled-events, all node features, and the
CPAL/sampler/peak-meter/tracing consumer feature combination. Standalone dependency
inspection contains no Bevy ECS. The initial GitHub matrix repeated these checks
on Linux, macOS and Windows. The maintainer subsequently requested no GitHub
workflows and direct main publication; `scripts/validate-standalone.sh` now runs
the local stream/no_std/no-ECS/formatting gate. Device and other-target acceptance
remain explicit checks, independent of the removed workflow.

Canonical source and plans carry the execution frontier. The local wrapper is
available for changes to this stream contract, not a mandatory gate for every
interim commit. During rapid prototyping, scope checks to concrete questions;
broader conformance and automation follow behavior intentionally stabilized
for evaluation.

CPU checks do not establish audible quality, calibrated latency, long-run drift,
hardware failure/restart, native rendering, fresh-process reopen, or other-target
acceptance. Hocket count-in/capture/replay and Redshank progressive playback/seek/
resume/timed anchors must be qualified independently at exact consumer pins.
