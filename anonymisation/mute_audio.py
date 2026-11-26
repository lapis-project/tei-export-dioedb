#!/usr/bin/env python3
"""Mute OGG audio intervals in constant memory using streaming FFmpeg pipes."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path


CSV_NAME_PATTERN = re.compile(
    r"^(?P<stem>.+)_(?P<date>\d{8})_(?P<time>\d{6})\.csv$",
    re.IGNORECASE,
)
TIME_PATTERN = re.compile(
    r"^(?P<hours>\d+):(?P<minutes>\d{2}):(?P<seconds>\d{2})"
    r"(?:\.(?P<fraction>\d+))?$"
)
END_TOLERANCE_MS = Decimal(100)
OPUS_SAMPLE_RATE = 48_000
PCM_SAMPLE_WIDTH = 2  # 16-bit signed little-endian PCM (s16le)
DEFAULT_CHUNK_FRAMES = 16_384
DEFAULT_FFMPEG_THREADS = 1
DEFAULT_WORKERS = 1
DEFAULT_NICE_LEVEL = 10
STDERR_SPOOL_LIMIT = 64 * 1024


class ValidationError(ValueError):
    """Raised when an anonymisation CSV or audio stream cannot be used safely."""


class CancelledError(RuntimeError):
    """Raised when batch processing is interrupted by the user."""


@dataclass(frozen=True)
class CsvCandidate:
    path: Path
    timestamp: datetime
    declared_path: str | None = None


@dataclass(frozen=True)
class CsvIndex:
    by_declared_path: dict[str, list[CsvCandidate]]
    by_stem: dict[str, list[CsvCandidate]]


@dataclass(frozen=True)
class AudioStreamInfo:
    codec_name: str
    sample_rate: int
    channels: int


class ProcessRegistry:
    """Track active child processes across worker threads for clean Ctrl+C shutdown."""

    def __init__(self):
        self._lock = threading.Lock()
        self._processes: set[subprocess.Popen] = set()
        self.cancel_event = threading.Event()

    def register(self, process):
        with self._lock:
            self._processes.add(process)
            cancelled = self.cancel_event.is_set()
        if cancelled:
            terminate_process(process)

    def unregister(self, process):
        with self._lock:
            self._processes.discard(process)

    def cancel_all(self):
        self.cancel_event.set()
        with self._lock:
            processes = list(self._processes)
        for process in processes:
            terminate_process(process)


def positive_int(value):
    """Argparse type validator for strictly positive integers."""
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"expected integer, got {value!r}") from error
    if parsed <= 0:
        raise argparse.ArgumentTypeError(f"expected positive integer, got {parsed}")
    return parsed


def bounded_int(minimum, maximum, label):
    """Create an argparse validator for integers in [minimum, maximum]."""

    def validator(value):
        try:
            parsed = int(value)
        except ValueError as error:
            raise argparse.ArgumentTypeError(f"expected integer, got {value!r}") from error
        if parsed < minimum or parsed > maximum:
            raise argparse.ArgumentTypeError(
                f"expected {label} between {minimum} and {maximum}, got {parsed}"
            )
        return parsed

    return validator


def normalize_declared_path(raw_line):
    """Normalize the first-line relative audio path declared in a CSV."""
    return raw_line.rstrip("\r\n").replace("\\", "/").strip()


def parse_arguments(argv=None):
    """Parse command-line directory, encoding, and resource-limit overrides."""
    base_dir = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description="Mute OGG intervals listed in timestamped CSV files using constant-RAM streaming."
    )
    parser.add_argument(
        "--audio-dir",
        type=Path,
        default=base_dir / "audio",
        help="input audio directory (default: anonymisation/audio)",
    )
    parser.add_argument(
        "--anon-dir",
        type=Path,
        default=base_dir / "anon",
        help="CSV directory (default: anonymisation/anon)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=base_dir / "audio_muted",
        help="output directory (default: anonymisation/audio_muted)",
    )
    parser.add_argument(
        "--opus-bitrate",
        help="target Opus bitrate, e.g. 96k (default: libopus channel-based bitrate)",
    )
    parser.add_argument(
        "--opus-compression-level",
        type=bounded_int(0, 10, "compression level"),
        default=None,
        help="libopus compression_level 0-10 (default: libopus default 10; 5-6 is faster for speech)",
    )
    parser.add_argument(
        "--workers",
        type=positive_int,
        default=DEFAULT_WORKERS,
        help=f"number of audio files to process concurrently (default: {DEFAULT_WORKERS})",
    )
    parser.add_argument(
        "--ffmpeg-threads",
        type=positive_int,
        default=DEFAULT_FFMPEG_THREADS,
        help=f"worker threads per ffmpeg process (default: {DEFAULT_FFMPEG_THREADS})",
    )
    parser.add_argument(
        "--chunk-frames",
        type=positive_int,
        default=DEFAULT_CHUNK_FRAMES,
        help=f"PCM frames processed per streaming chunk (default: {DEFAULT_CHUNK_FRAMES})",
    )
    parser.add_argument(
        "--nice",
        type=bounded_int(0, 19, "nice value"),
        default=DEFAULT_NICE_LEVEL,
        help=f"POSIX nice increment 0-19 to keep the system responsive (default: {DEFAULT_NICE_LEVEL})",
    )
    parser.add_argument("--ogg-quality", help=argparse.SUPPRESS)
    return parser.parse_args(argv)


def index_csv_files(anon_dir):
    """Index valid timestamped CSV files by declared relative path and stem."""
    by_declared_path = defaultdict(list)
    by_stem = defaultdict(list)

    csv_paths = sorted(
        p for p in anon_dir.rglob("*") if p.is_file() and p.suffix.lower() == ".csv"
    )
    for csv_path in csv_paths:
        match = CSV_NAME_PATTERN.fullmatch(csv_path.name)
        if not match:
            print(f"Ignoring CSV with unexpected filename: {csv_path.name}", flush=True)
            continue

        stamp = f"{match.group('date')}_{match.group('time')}"
        try:
            timestamp = datetime.strptime(stamp, "%Y%m%d_%H%M%S")
        except ValueError:
            print(f"Ignoring CSV with invalid timestamp: {csv_path.name}", flush=True)
            continue

        declared_path = None
        try:
            with csv_path.open(encoding="utf-8-sig", newline="") as csv_file:
                declared_path = normalize_declared_path(csv_file.readline())
        except (OSError, UnicodeError):
            declared_path = None

        candidate = CsvCandidate(
            path=csv_path,
            timestamp=timestamp,
            declared_path=declared_path,
        )
        if declared_path:
            by_declared_path[declared_path].append(candidate)
        by_stem[match.group("stem")].append(candidate)

    return CsvIndex(
        by_declared_path=dict(by_declared_path),
        by_stem=dict(by_stem),
    )


def parse_time_to_milliseconds(value, row_number, column):
    """Parse H:MM:SS[.fraction] as an exact Decimal number of milliseconds."""
    if value is None:
        raise ValidationError(f"row {row_number}: missing {column} timestamp")

    value = value.strip()
    match = TIME_PATTERN.fullmatch(value)
    if not match:
        raise ValidationError(
            f"row {row_number}: invalid {column} timestamp {value!r}"
        )

    minutes = int(match.group("minutes"))
    seconds = int(match.group("seconds"))
    if minutes >= 60 or seconds >= 60:
        raise ValidationError(
            f"row {row_number}: invalid {column} timestamp {value!r}"
        )

    fraction = match.group("fraction") or "0"
    try:
        fractional_seconds = Decimal(f"0.{fraction}")
    except InvalidOperation as error:
        raise ValidationError(
            f"row {row_number}: invalid {column} timestamp {value!r}"
        ) from error

    total_seconds = (
        Decimal(int(match.group("hours")) * 3600 + minutes * 60 + seconds)
        + fractional_seconds
    )
    return total_seconds * Decimal(1000)


def read_csv(csv_path, expected_audio_path, log_lines=None):
    """Read and validate the embedded path and exact millisecond timestamp rows."""
    try:
        csv_file = csv_path.open(encoding="utf-8-sig", newline="")
    except (OSError, UnicodeError) as error:
        raise ValidationError(f"could not open CSV: {error}") from error

    with csv_file:
        declared_path = normalize_declared_path(csv_file.readline())
        if declared_path != expected_audio_path:
            raise ValidationError(
                "path check failed "
                f"(CSV contains {declared_path!r}, expected {expected_audio_path!r})"
            )

        message = f"Path check successful: {expected_audio_path}"
        if log_lines is not None:
            log_lines.append(message)
        else:
            print(message, flush=True)

        reader = csv.DictReader(csv_file, delimiter=";")
        if reader.fieldnames is None:
            raise ValidationError("missing CSV header")

        missing_columns = {"start", "end"} - set(reader.fieldnames)
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValidationError(f"missing required column(s): {missing}")

        intervals = []
        for row_number, row in enumerate(reader, start=3):
            if None in row:
                raise ValidationError(f"row {row_number}: unexpected extra columns")
            if not row or not any(
                value and value.strip() for value in row.values()
            ):
                continue

            for offset_column in ("beep", "sync"):
                raw_offset = row.get(offset_column)
                if raw_offset and raw_offset.strip():
                    offset_ms = parse_time_to_milliseconds(
                        raw_offset, row_number, offset_column
                    )
                    if offset_ms != 0:
                        raise ValidationError(
                            f"row {row_number}: non-zero {offset_column} offset "
                            f"{raw_offset.strip()!r} is not supported"
                        )

            start = parse_time_to_milliseconds(row.get("start"), row_number, "start")
            end = parse_time_to_milliseconds(row.get("end"), row_number, "end")
            if start >= end:
                raise ValidationError(
                    f"row {row_number}: start must be earlier than end"
                )

            intervals.append((start, end))

    return intervals


def merge_intervals(intervals):
    """Merge overlapping and directly adjacent intervals."""
    merged = []
    for start, end in sorted(intervals):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def get_exact_duration_ms(total_frames, sample_rate):
    """Compute exact audio duration in milliseconds from decoded PCM frame count."""
    if total_frames < 0 or sample_rate <= 0:
        raise ValidationError("invalid decoded audio stream parameters")
    return (Decimal(total_frames) * Decimal(1000)) / Decimal(sample_rate)


def validate_audio_bounds(intervals, duration_ms, tolerance_ms=END_TOLERANCE_MS):
    """Validate intervals against audio duration, clamping minor EOF decoder drift."""
    clamped = []
    for start, end in intervals:
        start_d = Decimal(start)
        end_d = Decimal(end)
        if start_d < 0 or start_d >= duration_ms or end_d > duration_ms + tolerance_ms:
            raise ValidationError(
                f"interval {start_d}ms-{end_d}ms is outside audio duration "
                f"0ms-{duration_ms:.3f}ms"
            )
        clamped.append((start_d, min(end_d, duration_ms)))
    return clamped


def intervals_to_frame_ranges(intervals, sample_rate, total_frames):
    """Convert merged millisecond intervals into sorted, merged [start, end) frame ranges."""
    if not intervals:
        return []

    frame_rate = Decimal(sample_rate)
    frame_ranges = []
    for start_ms, end_ms in intervals:
        start_d = Decimal(start_ms)
        end_d = Decimal(end_ms)
        start_frame = max(
            0,
            int(
                ((start_d * frame_rate) / Decimal(1000)).to_integral_value(
                    rounding=ROUND_FLOOR
                )
            ),
        )
        end_frame = min(
            total_frames,
            int(
                ((end_d * frame_rate) / Decimal(1000)).to_integral_value(
                    rounding=ROUND_CEILING
                )
            ),
        )
        if end_frame > start_frame:
            frame_ranges.append((start_frame, end_frame))
    return merge_intervals(frame_ranges)


def read_spooled_stderr(spooled_file):
    """Read captured stderr text from a SpooledTemporaryFile safely."""
    spooled_file.seek(0)
    return spooled_file.read().decode("utf-8", errors="replace").strip()


def terminate_process(process):
    """Ensure a child process is terminated and reaped without hanging."""
    if process is None:
        return
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def read_full_chunk(stream, buffer):
    """Fill a reusable bytearray from a pipe, handling partial POSIX pipe reads."""
    view = memoryview(buffer)
    total = 0
    size = len(buffer)
    while total < size:
        bytes_read = stream.readinto(view[total:])
        if not bytes_read:
            break
        total += bytes_read
    return total


def check_cancelled(registry):
    """Raise CancelledError if the batch has been interrupted."""
    if registry is not None and registry.cancel_event.is_set():
        raise CancelledError("processing cancelled")


def probe_audio_stream(path):
    """Inspect single audio stream metadata via ffprobe without decoding frames."""
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "a",
            "-show_entries",
            "stream=codec_name,sample_rate,channels",
            "-of",
            "json",
            str(path),
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode or result.stderr.strip():
        raise ValidationError(f"could not probe {path.name}: {result.stderr.strip()}")

    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise ValidationError(f"invalid ffprobe output for {path.name}: {error}") from error

    streams = payload.get("streams")
    if not isinstance(streams, list) or len(streams) == 0:
        raise ValidationError(f"{path.name} contains no audio streams")
    if len(streams) > 1:
        raise ValidationError("audio file contains multiple audio streams")

    stream = streams[0]
    try:
        codec_name = str(stream["codec_name"])
        sample_rate = int(stream["sample_rate"])
        channels = int(stream["channels"])
    except (KeyError, TypeError, ValueError) as error:
        raise ValidationError(
            f"invalid audio stream metadata in {path.name}: {stream!r}"
        ) from error

    if sample_rate <= 0 or channels <= 0:
        raise ValidationError(
            f"invalid audio stream parameters in {path.name} "
            f"(sample_rate={sample_rate}, channels={channels})"
        )

    return AudioStreamInfo(
        codec_name=codec_name,
        sample_rate=sample_rate,
        channels=channels,
    )


def count_decoded_pcm_frames(
    path,
    channels,
    ffmpeg_threads=DEFAULT_FFMPEG_THREADS,
    chunk_frames=DEFAULT_CHUNK_FRAMES,
    registry=None,
):
    """Count exact decoded PCM frames per channel in O(1) memory via streaming ffmpeg."""
    check_cancelled(registry)
    frame_width = channels * PCM_SAMPLE_WIDTH
    buffer = bytearray(chunk_frames * frame_width)
    total_bytes = 0
    process = None

    with tempfile.SpooledTemporaryFile(max_size=STDERR_SPOOL_LIMIT) as stderr_file:
        try:
            process = subprocess.Popen(
                [
                    "ffmpeg",
                    "-nostdin",
                    "-v",
                    "error",
                    "-threads",
                    str(ffmpeg_threads),
                    "-i",
                    str(path),
                    "-map",
                    "0:a:0",
                    "-f",
                    "s16le",
                    "-acodec",
                    "pcm_s16le",
                    "pipe:1",
                ],
                stdout=subprocess.PIPE,
                stderr=stderr_file,
                bufsize=0,
            )
            if registry is not None:
                registry.register(process)
            assert process.stdout is not None
            while True:
                check_cancelled(registry)
                bytes_read = process.stdout.readinto(buffer)
                if not bytes_read:
                    break
                total_bytes += bytes_read
            process.stdout.close()
            returncode = process.wait()
        finally:
            terminate_process(process)
            if registry is not None and process is not None:
                registry.unregister(process)

        check_cancelled(registry)
        stderr_text = read_spooled_stderr(stderr_file)
        if returncode != 0 or stderr_text:
            raise ValidationError(
                f"could not decode {path.name}: {stderr_text or f'exit code {returncode}'}"
            )

    if total_bytes % frame_width != 0:
        raise ValidationError(
            f"decoded PCM length ({total_bytes} bytes) is not aligned to frame width {frame_width}"
        )

    return total_bytes // frame_width


def verify_opus_output(
    path,
    expected_frames,
    expected_channels,
    ffmpeg_threads=DEFAULT_FFMPEG_THREADS,
    chunk_frames=DEFAULT_CHUNK_FRAMES,
    registry=None,
):
    """Verify Opus stream parameters and exact decoded frame count in constant memory."""
    check_cancelled(registry)
    stream = probe_audio_stream(path)
    if stream.codec_name != "opus":
        raise ValidationError(f"expected codec_name=opus, found {stream.codec_name!r}")
    if stream.sample_rate != OPUS_SAMPLE_RATE:
        raise ValidationError(
            f"expected sample_rate={OPUS_SAMPLE_RATE}, found {stream.sample_rate!r}"
        )
    if stream.channels != expected_channels:
        raise ValidationError(
            f"expected channels={expected_channels}, found {stream.channels!r}"
        )

    frames = count_decoded_pcm_frames(
        path,
        expected_channels,
        ffmpeg_threads=ffmpeg_threads,
        chunk_frames=chunk_frames,
        registry=registry,
    )
    if frames != expected_frames:
        raise ValidationError(f"expected {expected_frames} decoded frames, found {frames}")


def stream_mute_and_encode(
    source_path,
    destination_path,
    stream_info,
    source_frames,
    frame_ranges,
    expected_frames,
    opus_bitrate=None,
    opus_compression_level=None,
    ffmpeg_threads=DEFAULT_FFMPEG_THREADS,
    chunk_frames=DEFAULT_CHUNK_FRAMES,
    registry=None,
):
    """Decode, zero muted PCM frame ranges in-place, and encode Opus in O(1) memory."""
    check_cancelled(registry)
    frame_width = stream_info.channels * PCM_SAMPLE_WIDTH
    chunk_byte_size = chunk_frames * frame_width
    chunk_buffer = bytearray(chunk_byte_size)
    chunk_view = memoryview(chunk_buffer)
    zero_view = memoryview(bytes(chunk_byte_size))

    # Supply enough trailing samples to flush the resampler, then trim at the
    # exact sample boundary before libopus adds its own codec delay.
    filters = f"apad,aresample={OPUS_SAMPLE_RATE},atrim=end_sample={expected_frames}"

    decoder_cmd = [
        "ffmpeg",
        "-nostdin",
        "-v",
        "error",
        "-threads",
        str(ffmpeg_threads),
        "-i",
        str(source_path),
        "-map",
        "0:a:0",
        "-f",
        "s16le",
        "-acodec",
        "pcm_s16le",
        "pipe:1",
    ]
    encoder_cmd = [
        "ffmpeg",
        "-nostdin",
        "-y",
        "-v",
        "error",
        "-threads",
        str(ffmpeg_threads),
        "-f",
        "s16le",
        "-ar",
        str(stream_info.sample_rate),
        "-ac",
        str(stream_info.channels),
        "-i",
        "pipe:0",
        "-af",
        filters,
        "-c:a",
        "libopus",
    ]
    if opus_bitrate:
        encoder_cmd.extend(["-b:a", opus_bitrate])
    if opus_compression_level is not None:
        encoder_cmd.extend(["-compression_level", str(opus_compression_level)])
    encoder_cmd.extend(["-f", "ogg", str(destination_path)])

    decoder = None
    encoder = None
    frames_processed = 0
    range_index = 0
    write_error = None

    with (
        tempfile.SpooledTemporaryFile(max_size=STDERR_SPOOL_LIMIT) as dec_stderr,
        tempfile.SpooledTemporaryFile(max_size=STDERR_SPOOL_LIMIT) as enc_stderr,
    ):
        try:
            decoder = subprocess.Popen(
                decoder_cmd,
                stdout=subprocess.PIPE,
                stderr=dec_stderr,
                bufsize=0,
            )
            encoder = subprocess.Popen(
                encoder_cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=enc_stderr,
                bufsize=0,
            )
            if registry is not None:
                registry.register(decoder)
                registry.register(encoder)
            assert decoder.stdout is not None
            assert encoder.stdin is not None

            try:
                while True:
                    check_cancelled(registry)
                    bytes_read = read_full_chunk(decoder.stdout, chunk_buffer)
                    if bytes_read == 0:
                        break
                    if bytes_read % frame_width != 0:
                        raise ValidationError(
                            f"unaligned PCM chunk ({bytes_read} bytes) for frame width {frame_width}"
                        )

                    current_chunk_frames = bytes_read // frame_width
                    chunk_start = frames_processed
                    chunk_end = chunk_start + current_chunk_frames

                    while (
                        range_index < len(frame_ranges)
                        and frame_ranges[range_index][1] <= chunk_start
                    ):
                        range_index += 1

                    scan = range_index
                    while scan < len(frame_ranges) and frame_ranges[scan][0] < chunk_end:
                        mute_start = max(frame_ranges[scan][0], chunk_start) - chunk_start
                        mute_end = min(frame_ranges[scan][1], chunk_end) - chunk_start
                        start_byte = mute_start * frame_width
                        end_byte = mute_end * frame_width
                        chunk_view[start_byte:end_byte] = zero_view[: end_byte - start_byte]
                        scan += 1

                    encoder.stdin.write(chunk_view[:bytes_read])
                    frames_processed = chunk_end
            except BrokenPipeError as error:
                write_error = error
                terminate_process(decoder)
            finally:
                try:
                    encoder.stdin.close()
                except OSError:
                    pass
                try:
                    decoder.stdout.close()
                except OSError:
                    pass

            encoder_rc = encoder.wait()
            decoder_rc = decoder.wait()
        finally:
            terminate_process(decoder)
            terminate_process(encoder)
            if registry is not None:
                if decoder is not None:
                    registry.unregister(decoder)
                if encoder is not None:
                    registry.unregister(encoder)

        check_cancelled(registry)
        dec_err = read_spooled_stderr(dec_stderr)
        enc_err = read_spooled_stderr(enc_stderr)

        if encoder_rc != 0 or enc_err:
            raise ValidationError(
                f"ffmpeg Opus encoding failed for {source_path.name}: "
                f"{enc_err or f'exit code {encoder_rc}'}"
            )
        if write_error is not None:
            raise ValidationError(f"encoder pipe closed early for {source_path.name}: {write_error}")
        if decoder_rc != 0 or dec_err:
            raise ValidationError(
                f"ffmpeg decoding failed for {source_path.name}: "
                f"{dec_err or f'exit code {decoder_rc}'}"
            )
        if frames_processed != source_frames:
            raise ValidationError(
                f"decoded frame count changed while streaming {source_path.name} "
                f"(expected {source_frames}, streamed {frames_processed})"
            )


def export_atomically(
    source_path,
    output_path,
    stream_info,
    source_frames,
    frame_ranges,
    expected_frames,
    opus_bitrate=None,
    opus_compression_level=None,
    ffmpeg_threads=DEFAULT_FFMPEG_THREADS,
    chunk_frames=DEFAULT_CHUNK_FRAMES,
    registry=None,
):
    """Stream-encode Opus to a temporary Ogg and verify it before publishing."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=f".{output_path.stem}.",
            suffix=".ogg",
            dir=output_path.parent,
            delete=False,
        ) as temporary_file:
            temporary_path = Path(temporary_file.name)

        stream_mute_and_encode(
            source_path=source_path,
            destination_path=temporary_path,
            stream_info=stream_info,
            source_frames=source_frames,
            frame_ranges=frame_ranges,
            expected_frames=expected_frames,
            opus_bitrate=opus_bitrate,
            opus_compression_level=opus_compression_level,
            ffmpeg_threads=ffmpeg_threads,
            chunk_frames=chunk_frames,
            registry=registry,
        )
        verify_opus_output(
            temporary_path,
            expected_frames,
            stream_info.channels,
            ffmpeg_threads=ffmpeg_threads,
            chunk_frames=chunk_frames,
            registry=registry,
        )
        os.replace(temporary_path, output_path)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


def choose_csv(candidates, audio_path, log_lines=None):
    """Choose the newest matching CSV and report ignored older versions."""
    ordered = sorted(
        candidates,
        key=lambda candidate: candidate.timestamp,
        reverse=True,
    )
    selected = ordered[0]
    messages = [f"Found CSV for {audio_path}: {selected.path.name}"]
    for ignored in ordered[1:]:
        messages.append(f"Ignoring older matching CSV: {ignored.path.name}")
    if log_lines is not None:
        log_lines.extend(messages)
    else:
        for message in messages:
            print(message, flush=True)
    return selected.path


def find_audio_files(audio_dir, output_dir):
    """Find input .ogg files case-insensitively while excluding output_dir if nested."""
    nested_output = output_dir.is_relative_to(audio_dir)
    results = []
    for path in audio_dir.rglob("*"):
        if nested_output and path.is_relative_to(output_dir):
            continue
        if (
            path.is_file()
            and not path.name.startswith(".")
            and path.suffix.lower() == ".ogg"
        ):
            results.append(path)
    return sorted(results)


def process_single_file(
    index,
    total_files,
    audio_path,
    audio_dir,
    output_dir,
    csv_index,
    opus_bitrate=None,
    opus_compression_level=None,
    ffmpeg_threads=DEFAULT_FFMPEG_THREADS,
    chunk_frames=DEFAULT_CHUNK_FRAMES,
    registry=None,
):
    """Process one OGG file and return (status, log_lines)."""
    check_cancelled(registry)
    log_lines: list[str] = []
    relative_path = audio_path.relative_to(audio_dir)
    relative_posix_path = relative_path.as_posix()
    output_path = output_dir / relative_path
    matches = csv_index.by_declared_path.get(relative_posix_path)
    if not matches:
        matches = csv_index.by_stem.get(audio_path.stem, [])
    if not matches:
        log_lines.append(
            f"[{index}/{total_files}] Not processed: {relative_posix_path} "
            "(no matching timestamped CSV found)"
        )
        return "skipped", log_lines

    csv_path = choose_csv(matches, relative_posix_path, log_lines=log_lines)
    try:
        intervals = read_csv(csv_path, relative_posix_path, log_lines=log_lines)
        stream_info = probe_audio_stream(audio_path)
        source_frames = count_decoded_pcm_frames(
            audio_path,
            stream_info.channels,
            ffmpeg_threads=ffmpeg_threads,
            chunk_frames=chunk_frames,
            registry=registry,
        )
        duration_ms = get_exact_duration_ms(source_frames, stream_info.sample_rate)
        clamped_intervals = validate_audio_bounds(intervals, duration_ms)
        expected_frames = (
            source_frames * OPUS_SAMPLE_RATE + stream_info.sample_rate // 2
        ) // stream_info.sample_rate
        if expected_frames == 0:
            raise ValidationError("source duration rounds to zero PCM frames at 48 kHz")

        if output_path.is_file():
            newest_input = max(
                audio_path.stat().st_mtime_ns, csv_path.stat().st_mtime_ns
            )
            if output_path.stat().st_mtime_ns >= newest_input:
                try:
                    verify_opus_output(
                        output_path,
                        expected_frames,
                        stream_info.channels,
                        ffmpeg_threads=ffmpeg_threads,
                        chunk_frames=chunk_frames,
                        registry=registry,
                    )
                except ValidationError as error:
                    log_lines.append(f"Rebuilding {relative_posix_path}: {error}")
                else:
                    log_lines.append(
                        f"[{index}/{total_files}] Already verified, skipping: "
                        f"{relative_posix_path} ({expected_frames} decoded frames)"
                    )
                    return "existing", log_lines

        frame_ranges = intervals_to_frame_ranges(
            merge_intervals(clamped_intervals),
            stream_info.sample_rate,
            source_frames,
        )
        export_atomically(
            source_path=audio_path,
            output_path=output_path,
            stream_info=stream_info,
            source_frames=source_frames,
            frame_ranges=frame_ranges,
            expected_frames=expected_frames,
            opus_bitrate=opus_bitrate,
            opus_compression_level=opus_compression_level,
            ffmpeg_threads=ffmpeg_threads,
            chunk_frames=chunk_frames,
            registry=registry,
        )
    except CancelledError:
        raise
    except (ValidationError, OSError, UnicodeError) as error:
        log_lines.append(
            f"[{index}/{total_files}] Not processed: {relative_posix_path} ({error})"
        )
        return "failed", log_lines
    except Exception as error:
        log_lines.append(
            f"[{index}/{total_files}] Not processed: {relative_posix_path} "
            f"(unexpected error: {error})"
        )
        return "failed", log_lines

    log_lines.append(
        f"[{index}/{total_files}] Processed successfully: {relative_posix_path} -> "
        f"{output_path} ({expected_frames} decoded frames at {OPUS_SAMPLE_RATE} Hz)"
    )
    return "processed", log_lines


def process_audio_files(
    audio_dir,
    anon_dir,
    output_dir,
    opus_bitrate=None,
    opus_compression_level=None,
    workers=DEFAULT_WORKERS,
    ffmpeg_threads=DEFAULT_FFMPEG_THREADS,
    chunk_frames=DEFAULT_CHUNK_FRAMES,
):
    """Process every OGG recursively and return (processed, existing, skipped, failed)."""
    csv_index = index_csv_files(anon_dir)
    audio_paths = find_audio_files(audio_dir, output_dir)
    counts = {"processed": 0, "existing": 0, "skipped": 0, "failed": 0}

    if not audio_paths:
        print(f"No .ogg files found in {audio_dir}", file=sys.stderr, flush=True)
        return 0, 0, 0, 0

    total_files = len(audio_paths)
    registry = ProcessRegistry()
    print_lock = threading.Lock()

    def emit_logs(lines):
        if not lines:
            return
        with print_lock:
            print("\n".join(lines), flush=True)

    if workers == 1:
        try:
            for index, audio_path in enumerate(audio_paths, start=1):
                status, log_lines = process_single_file(
                    index=index,
                    total_files=total_files,
                    audio_path=audio_path,
                    audio_dir=audio_dir,
                    output_dir=output_dir,
                    csv_index=csv_index,
                    opus_bitrate=opus_bitrate,
                    opus_compression_level=opus_compression_level,
                    ffmpeg_threads=ffmpeg_threads,
                    chunk_frames=chunk_frames,
                    registry=registry,
                )
                counts[status] += 1
                emit_logs(log_lines)
        except KeyboardInterrupt:
            registry.cancel_all()
            raise
    else:
        executor = ThreadPoolExecutor(max_workers=workers)
        futures = []
        try:
            for index, audio_path in enumerate(audio_paths, start=1):
                futures.append(
                    executor.submit(
                        process_single_file,
                        index=index,
                        total_files=total_files,
                        audio_path=audio_path,
                        audio_dir=audio_dir,
                        output_dir=output_dir,
                        csv_index=csv_index,
                        opus_bitrate=opus_bitrate,
                        opus_compression_level=opus_compression_level,
                        ffmpeg_threads=ffmpeg_threads,
                        chunk_frames=chunk_frames,
                        registry=registry,
                    )
                )
            for future in as_completed(futures):
                try:
                    status, log_lines = future.result()
                except CancelledError:
                    continue
                counts[status] += 1
                emit_logs(log_lines)
        except KeyboardInterrupt:
            registry.cancel_all()
            executor.shutdown(wait=True, cancel_futures=True)
            raise
        else:
            executor.shutdown(wait=True)

    return counts["processed"], counts["existing"], counts["skipped"], counts["failed"]


def apply_process_niceness(nice_level):
    """Lower scheduling priority on POSIX systems so long batches stay desktop-friendly."""
    if nice_level <= 0:
        return
    if hasattr(os, "nice"):
        try:
            os.nice(nice_level)
        except OSError:
            pass


def main(argv=None):
    """Validate prerequisites, process files, and return a shell exit code."""
    args = parse_arguments(argv)
    audio_dir = args.audio_dir.resolve()
    anon_dir = args.anon_dir.resolve()
    output_dir = args.output_dir.resolve()

    if not audio_dir.is_dir():
        print(f"Audio directory not found: {audio_dir}", file=sys.stderr)
        return 2
    if not anon_dir.is_dir():
        print(f"CSV directory not found: {anon_dir}", file=sys.stderr)
        return 2
    if audio_dir == output_dir:
        print(
            "Output directory must be different from input audio directory.",
            file=sys.stderr,
        )
        return 2
    for binary in ("ffmpeg", "ffprobe"):
        if shutil.which(binary) is None:
            print(f"{binary} was not found on PATH.", file=sys.stderr)
            return 2
    if args.ogg_quality is not None:
        print("--ogg-quality applied to Vorbis. Use --opus-bitrate for Opus.", file=sys.stderr)

    apply_process_niceness(args.nice)

    try:
        processed, existing, skipped, failed = process_audio_files(
            audio_dir,
            anon_dir,
            output_dir,
            opus_bitrate=args.opus_bitrate,
            opus_compression_level=args.opus_compression_level,
            workers=args.workers,
            ffmpeg_threads=args.ffmpeg_threads,
            chunk_frames=args.chunk_frames,
        )
    except KeyboardInterrupt:
        print("\nInterrupted by user.", file=sys.stderr, flush=True)
        return 130

    print(
        f"Summary: {processed} processed, {existing} already existed, "
        f"{skipped} skipped, {failed} failed.",
        flush=True,
    )
    if processed == 0 and existing == 0:
        return 1
    return 1 if (skipped or failed) else 0


if __name__ == "__main__":
    sys.exit(main())
