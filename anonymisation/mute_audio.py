#!/usr/bin/env python3
"""Mute OGG audio intervals described by timestamped anonymisation CSV files."""

from __future__ import annotations

import argparse
import csv
import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections import defaultdict
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


class ValidationError(ValueError):
    """Raised when an anonymisation CSV cannot be used safely."""


@dataclass(frozen=True)
class CsvCandidate:
    path: Path
    timestamp: datetime
    declared_path: str | None = None


@dataclass(frozen=True)
class CsvIndex:
    by_declared_path: dict[str, list[CsvCandidate]]
    by_stem: dict[str, list[CsvCandidate]]


def normalize_declared_path(raw_line):
    """Normalize the first-line relative audio path declared in a CSV."""
    return raw_line.rstrip("\r\n").replace("\\", "/").strip()


def parse_arguments(argv=None):
    """Parse command-line directory and encoding overrides."""
    base_dir = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description="Mute OGG intervals listed in timestamped CSV files."
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
            print(f"Ignoring CSV with unexpected filename: {csv_path.name}")
            continue

        stamp = f"{match.group('date')}_{match.group('time')}"
        try:
            timestamp = datetime.strptime(stamp, "%Y%m%d_%H%M%S")
        except ValueError:
            print(f"Ignoring CSV with invalid timestamp: {csv_path.name}")
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


def read_csv(csv_path, expected_audio_path):
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

        print(f"Path check successful: {expected_audio_path}")
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


def get_exact_duration_ms(audio):
    """Compute exact audio duration in milliseconds from decoded PCM frame count."""
    if audio.frame_width <= 0 or audio.frame_rate <= 0:
        raise ValidationError("invalid decoded audio stream parameters")
    total_frames = len(audio.raw_data) // audio.frame_width
    return (Decimal(total_frames) * Decimal(1000)) / Decimal(audio.frame_rate)


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


def mute_intervals(audio, intervals, audio_segment_class):
    """Zero PCM frames in-place inside a single bytearray without changing frame count."""
    if not intervals:
        return audio

    frame_width = audio.frame_width
    frame_rate = Decimal(audio.frame_rate)
    total_frames = len(audio.raw_data) // frame_width
    buffer = bytearray(audio.raw_data)

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
        start_byte = start_frame * frame_width
        end_byte = end_frame * frame_width
        if end_byte > start_byte:
            buffer[start_byte:end_byte] = b"\x00" * (end_byte - start_byte)

    return audio_segment_class(
        data=bytes(buffer),
        sample_width=audio.sample_width,
        frame_rate=audio.frame_rate,
        channels=audio.channels,
    )


def verify_opus_output(path, expected_frames, expected_channels):
    """Count decoded samples per channel after Opus pre-skip and end trimming."""
    result = subprocess.run(
        [
            "ffprobe", "-v", "error", "-select_streams", "a",
            "-show_frames", "-show_entries",
            "frame=nb_samples:stream=codec_name,sample_rate,channels",
            "-of", "default=noprint_wrappers=1", str(path),
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode or result.stderr.strip():
        raise ValidationError(f"could not verify {path.name}: {result.stderr.strip()}")

    frames = 0
    stream = {}
    expected_stream = {
        "codec_name": "opus",
        "sample_rate": str(OPUS_SAMPLE_RATE),
        "channels": str(expected_channels),
    }
    for line in result.stdout.splitlines():
        key, _, value = line.partition("=")
        if key == "nb_samples":
            frames += int(value)
        elif key in expected_stream:
            if key in stream:
                raise ValidationError("output contains multiple audio streams")
            stream[key] = value
    for key, expected in expected_stream.items():
        if stream.get(key) != expected:
            raise ValidationError(f"expected {key}={expected}, found {stream.get(key)!r}")
    if frames != expected_frames:
        raise ValidationError(f"expected {expected_frames} decoded frames, found {frames}")


def export_atomically(audio, output_path, expected_frames, opus_bitrate):
    """Encode Opus and verify the temporary Ogg before publishing it."""
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

        # Supply enough trailing samples to flush the resampler, then trim at
        # the exact sample boundary before libopus adds its own codec delay.
        filters = f"apad,aresample={OPUS_SAMPLE_RATE},atrim=end_sample={expected_frames}"
        audio.export(
            temporary_path,
            format="ogg",
            codec="libopus",
            bitrate=opus_bitrate,
            parameters=["-af", filters],
        ).close()
        verify_opus_output(temporary_path, expected_frames, audio.channels)
        os.replace(temporary_path, output_path)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


def choose_csv(candidates, audio_path):
    """Choose the newest matching CSV and report ignored older versions."""
    ordered = sorted(
        candidates,
        key=lambda candidate: candidate.timestamp,
        reverse=True,
    )
    selected = ordered[0]
    print(f"Found CSV for {audio_path}: {selected.path.name}")
    for ignored in ordered[1:]:
        print(f"Ignoring older matching CSV: {ignored.path.name}")
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


def process_audio_files(
    audio_dir,
    anon_dir,
    output_dir,
    audio_segment_class,
    decode_error_class,
    opus_bitrate=None,
):
    """Process every OGG recursively and return (processed, existing, skipped, failed)."""
    csv_index = index_csv_files(anon_dir)
    audio_paths = find_audio_files(audio_dir, output_dir)
    processed = 0
    existing = 0
    skipped = 0
    failed = 0

    if not audio_paths:
        print(f"No .ogg files found in {audio_dir}", file=sys.stderr)
        return processed, existing, skipped, failed

    for audio_path in audio_paths:
        relative_path = audio_path.relative_to(audio_dir)
        relative_posix_path = relative_path.as_posix()
        output_path = output_dir / relative_path
        matches = csv_index.by_declared_path.get(relative_posix_path)
        if not matches:
            matches = csv_index.by_stem.get(audio_path.stem, [])
        if not matches:
            print(
                f"Not processed: {relative_posix_path} "
                "(no matching timestamped CSV found)"
            )
            skipped += 1
            continue

        csv_path = choose_csv(matches, relative_posix_path)
        try:
            intervals = read_csv(csv_path, relative_posix_path)
            audio = audio_segment_class.from_ogg(audio_path)
            duration_ms = get_exact_duration_ms(audio)
            clamped_intervals = validate_audio_bounds(intervals, duration_ms)
            source_frames = len(audio.raw_data) // audio.frame_width
            expected_frames = (
                source_frames * OPUS_SAMPLE_RATE + audio.frame_rate // 2
            ) // audio.frame_rate
            if expected_frames == 0:
                raise ValidationError("source duration rounds to zero PCM frames at 48 kHz")

            if output_path.is_file():
                newest_input = max(
                    audio_path.stat().st_mtime_ns, csv_path.stat().st_mtime_ns
                )
                if output_path.stat().st_mtime_ns >= newest_input:
                    try:
                        verify_opus_output(output_path, expected_frames, audio.channels)
                    except ValidationError as error:
                        print(f"Rebuilding {relative_posix_path}: {error}")
                    else:
                        print(
                            f"Already verified, skipping: {relative_posix_path} "
                            f"({expected_frames} decoded frames)"
                        )
                        existing += 1
                        continue

            audio = mute_intervals(
                audio, merge_intervals(clamped_intervals), audio_segment_class
            )
            export_atomically(audio, output_path, expected_frames, opus_bitrate)
        except (ValidationError, OSError, UnicodeError, decode_error_class) as error:
            print(f"Not processed: {relative_posix_path} ({error})")
            failed += 1
            continue
        except Exception as error:
            print(f"Not processed: {relative_posix_path} (unexpected error: {error})")
            failed += 1
            continue

        print(
            f"Processed successfully: {relative_posix_path} -> {output_path} "
            f"({expected_frames} decoded frames at {OPUS_SAMPLE_RATE} Hz)"
        )
        processed += 1

    return processed, existing, skipped, failed


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

    try:
        from pydub import AudioSegment
        from pydub.exceptions import CouldntDecodeError as PydubCouldntDecodeError
    except ModuleNotFoundError as error:
        if error.name in ("audioop", "pyaudioop"):
            print(
                "Missing 'audioop' module required by pydub on Python 3.13+; "
                "run pip install -r requirements.txt.",
                file=sys.stderr,
            )
        else:
            print(
                "pydub is not installed; run pip install -r requirements.txt.",
                file=sys.stderr,
            )
        return 2
    except ImportError as error:
        print(f"Failed to import pydub: {error}", file=sys.stderr)
        return 2

    processed, existing, skipped, failed = process_audio_files(
        audio_dir,
        anon_dir,
        output_dir,
        AudioSegment,
        PydubCouldntDecodeError,
        opus_bitrate=args.opus_bitrate,
    )
    print(
        f"Summary: {processed} processed, {existing} already existed, "
        f"{skipped} skipped, {failed} failed."
    )
    if processed == 0 and existing == 0:
        return 1
    return 1 if (skipped or failed) else 0


if __name__ == "__main__":
    sys.exit(main())
