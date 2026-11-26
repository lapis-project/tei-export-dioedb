# Anonymisation

This directory contains two independent Python workflows for anonymising DiÖ (Deutsch in Österreich) interview data:

1. **Audio muting** ([`mute_audio.py`](./mute_audio.py)) — mutes the intervals listed in timestamped CSV files and writes Ogg Opus recordings with a verified decoded frame count.
2. **Database token anonymisation** ([`process_tokens.py`](./process_tokens.py) + [`select_tokens.sql`](./select_tokens.sql)) — updates bracket-annotated tokens in PostgreSQL `public.token` with standardized placeholders (`NAME`, `ZAHL`, `ORT`, `SONSTIGE`).

> Note: `process_tokens.py` does **not** generate the CSV files in `anon/`; those interval CSVs are exported externally.

---

## Directory Contents

| File / Folder | Purpose |
| --- | --- |
| [`mute_audio.py`](./mute_audio.py) | Mutes timestamped intervals in `.ogg` audio files and writes muted copies to `audio_muted/`. |
| [`process_tokens.py`](./process_tokens.py) | Scans PostgreSQL `token` rows for bracketed anonymisation tags and replaces PII fields in-place. |
| [`select_tokens.sql`](./select_tokens.sql) | Query loaded by `process_tokens.py` to fetch chronologically ordered tokens per transcript and speaker. |
| [`requirements.txt`](./requirements.txt) | Pinned Python dependencies for `process_tokens.py` (`psycopg2-binary`, `python-dotenv`); `mute_audio.py` uses only the Python standard library plus `ffmpeg`/`ffprobe`. |
| [`.env.example`](./.env.example) | Template for PostgreSQL credentials required by `process_tokens.py`. |
| `audio/` | Input directory for raw `.ogg` recordings (gitignored; must mirror the subfolders declared in `anon/*.csv`). |
| `anon/` | Input directory containing timestamped anonymisation interval CSVs (`<stem>_YYYYMMDD_HHMMSS.csv`, gitignored). |
| `audio_muted/` | Output directory for muted `.ogg` files (gitignored; mirrors `audio/` subfolder structure). |

---

## Setup

### System Requirement
- Install `ffmpeg` with the `libopus` encoder and `ffprobe`. Both programs must be on `PATH`.

### Python Environment
Create and activate a virtual environment inside `anonymisation/` and install the dependencies:

```bash
cd anonymisation
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Environment Variables (for `process_tokens.py` only)
Copy [`.env.example`](./.env.example) to `.env` and fill in the PostgreSQL connection details:

```bash
cp .env.example .env
```

Required variables: `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASS`. Never commit `.env`.

---

## 1. Audio Muting (`mute_audio.py`)

The output is Ogg Opus at 48,000 Hz. One PCM frame contains one sample per channel. For 48 kHz input, the decoded output has exactly as many frames as the decoded source. For any other input rate, the expected count is the source frame count multiplied by `48000 / source_rate`, rounded to the nearest whole frame (half up). This preserves decoded duration to within half a 48 kHz frame, about 10.4 microseconds. The comparison uses decoded audio because container duration can include codec delay.

### Input Expectations
- **Audio files (`audio/`)**: `.ogg` files (matched case-insensitively) placed in the subdirectory declared on line 1 of their matching CSV (for example `audio/int03/0001_PIR_alt_f_INT.ogg`). Known subfolders in `anon/` include `int03/`, `ai04/`, `gfg03/`, `caf04/`, `narr04/`, `bdint03/`, and `kurzint03/`.
- **Interval CSVs (`anon/`)**:
  - Filename format: `<audio_stem>_YYYYMMDD_HHMMSS.csv`. If multiple timestamped CSVs match the same relative audio path, the newest one is used.
  - Line 1: Relative path of the target audio file (e.g. `int03/0001_PIR_alt_f_INT.ogg`). Must match `audio_path.relative_to(audio_dir)` or validation fails.
  - Line 2+: Semicolon-delimited (`;`) table containing at least `start` and `end` columns with `H:MM:SS[.fraction]` timestamps (`beep` and `sync` columns, if present, must be `0:00:00`):
    ```csv
    int03/0001_PIR_alt_f_INT.ogg
    eventId;beep;sync;start;end
    1310470;0:00:00;0:00:00;0:00:04;0:00:06.733000
    ```

### How It Works
1. Recursively scans `--anon-dir` for valid `<stem>_YYYYMMDD_HHMMSS.csv` files and indexes them by declared relative path (with stem fallback for mismatch diagnostics).
2. Recursively scans `--audio-dir` for `.ogg` files (excluding `--output-dir` if nested).
3. Selects the CSV for each recording and flags recordings missing a CSV (`skipped`).
4. Validates line 1 against the file's relative path, parses `start`/`end` with exact `Decimal` millisecond math, validates bounds against exact PCM duration (clamping up to `100ms` of minor decoder drift at EOF), and merges overlapping/adjacent intervals.
5. On a rerun, reuses an output only if it is at least as recent as the source and selected CSV, and decodes as 48 kHz Opus with the expected channel and frame counts. Older outputs and legacy Vorbis files are rebuilt. Use a fresh output directory when changing bitrate or when input files have been replaced with preserved modification times.
6. Streams decoded 16-bit PCM in fixed-size chunks (`--chunk-frames`, ~64 KiB for stereo) directly from `ffmpeg` through Python into a second `ffmpeg` `libopus` encoder process, zeroing muted frame ranges in-place without loading whole recordings into RAM or writing temporary WAV files. A valid CSV with zero intervals produces an Opus recording with the full signal. Opus is lossy, so samples near mute boundaries may decode to small nonzero values.
7. Probes stream metadata with `ffprobe` and streams the temporary Ogg through `ffmpeg` in constant memory to verify the exact decoded frame count. Only a file with the expected codec, sample rate, channel count, and exact frame count replaces the destination. The CLI reports per-file progress and the verified frame count.

### Usage

```bash
python anonymisation/mute_audio.py
```

Override defaults if needed:

```bash
python anonymisation/mute_audio.py \
  --audio-dir ./anonymisation/audio \
  --anon-dir ./anonymisation/anon \
  --output-dir ./anonymisation/audio_muted \
  --opus-bitrate 96k \
  --opus-compression-level 5 \
  --workers 4 \
  --ffmpeg-threads 1 \
  --nice 10
```

### CLI Options
- `--audio-dir`: Input directory containing raw `.ogg` files (default: `anonymisation/audio`).
- `--anon-dir`: Directory containing timestamped interval CSVs (default: `anonymisation/anon`).
- `--output-dir`: Destination directory for muted `.ogg` files (default: `anonymisation/audio_muted`).
- `--opus-bitrate` sets the target bitrate for newly encoded files, for example `96k`. By default, libopus selects the bitrate based on the number of channels. The legacy `--ogg-quality` option is accepted with a warning and has no effect on Opus.
- `--opus-compression-level`: `libopus` encoding complexity `0`–`10` (default: `libopus` default `10`; `5`–`6` encodes speech ~1.5–2× faster with near-identical quality).
- `--workers`: Number of audio files processed concurrently (default: `1`). Each worker uses ~65 MB peak RAM; on an 8-core machine `--workers 4` keeps memory under ~300 MB while cutting total runtime ~3–3.5×.
- `--ffmpeg-threads`: Limits worker threads per `ffmpeg` process (default: `1`) to keep CPU usage and thermal load bounded during large multi-gigabyte runs.
- `--chunk-frames`: Number of PCM frames processed per streaming chunk (default: `16384`, ~64 KiB for stereo `s16le`).
- `--nice`: POSIX scheduling niceness increment `0`–`19` (default: `10`) so long batches yield smoothly to desktop processes (`0` disables).

### Exit Codes
- `0`: At least one `.ogg` file was processed or already existed, with zero missing-CSV skips and zero failures.
- `1`: No `.ogg` files were found, at least one `.ogg` file was missing a CSV, or at least one file failed CSV validation, decoding, encoding, or output verification.
- `2`: Startup prerequisite failed (missing/invalid directory, or `ffmpeg`/`ffprobe` missing from `PATH`).

---

## 2. Database Token Anonymisation (`process_tokens.py`)

### How It Works
1. Loads `.env` and [`select_tokens.sql`](./select_tokens.sql) relative to `anonymisation/` (or `--sql-file`), joining `public.token` with `public.event` and ordering by `(transcript_id_id, "ID_Inf_id", e.start_time, e.end_time, token_reihung, id)`.
2. Inspects `ortho`, `text`, and `text_in_ortho` in Python for bracketed anonymisation markers starting at index `0` (tokens where `[` appears after leading characters such as `([Gustav … Michel]N)` are left unchanged):
   - **Single-token entity**: e.g. `[Ludwig]N`, `[Neck]O/`, `[Tux]O?`, `[Unterpetersdorf]`
   - **Multi-token chain**: starts on `[` and collects all consecutive tokens (including plain middle words) within the same `(transcript_id_id, "ID_Inf_id")` until a closing `]<tag>` is reached; unclosed chains reset at speaker/transcript boundaries or before a new `[` with a warning
   - **Standalone closing marker**: orphaned `]<tag>` with an explicit `N`/`Z`/`O`/`S` suffix (e.g. `Dasein]N`, `siebter]Z/`)
3. Maps the tag suffix (case-insensitive) to a standardized placeholder:

   | Suffix | Replacement |
   | --- | --- |
   | `N` | `NAME` |
   | `Z` | `ZAHL` |
   | `O` | `ORT` |
   | `S` or empty (on opened `[...]`) | `SONSTIGE` |

4. Previews staged updates and prompts for confirmation (`y/n`, or `--yes` / `--dry-run`).
5. Updates `ortho`, `phon`, `text`, `text_in_ortho`, `splemma` (plus non-empty `ttlemma`, `ttcheckword`, `lautg`, `sonst`, and clears `transcomment` to `NULL`) in `public.token` using `psycopg2.extras.execute_values` batches inside a single transaction.

### Usage

```bash
python anonymisation/process_tokens.py
```

Optional flags: `--dry-run`, `--yes`, `--batch-size 500`, `--sql-file path/to/query.sql`.

---

## Operational Notes

- **Downstream LAPIS waveforms**: After copying muted `.ogg` files into the LAPIS backend `AUDIO_DIR`, regenerate waveform sidecars (`pnpm -F @lapis/backend generate:audio-waveforms`) so visual waveforms reflect the muted intervals.
