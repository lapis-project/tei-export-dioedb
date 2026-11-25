# Scripts Overview

This directory contains the Python scripts for exporting DiÖ (Deutsch in Österreich) PostgreSQL data to TEI XML, converting TEI transcripts into NoSketch Engine `.vert` corpus files, and syncing/migrating metadata to the LAPIS database.

> **Working directory:** Run exporter and metadata scripts **from inside `scripts/`**, as most scripts resolve SQL files (`../queries/*.sql`) and tag definitions (`../dioe-tags.tei.xml`) relative to `scripts/`.

---

## Quick Reference: When to Use Which Script

| Script                                                                                 | Category            | When to Use                                                                                                                               |
| -------------------------------------------------------------------------------------- | ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| [`generate-standoff-tags.py`](./generate-standoff-tags.py)                             | TEI Taxonomy        | Regenerate `dioe-tags.tei.xml` when `dioe-tags-tree.json` changes.                                                                        |
| [`create-tei-file-single-transcript.py`](./create-tei-file-single-transcript.py)       | TEI Export          | Export or debug a **single** transcript (`<id>.xml`) and update `standoff_informants.xml`.                                                |
| [`process-all-transcripts.py`](./process-all-transcripts.py)                           | TEI Export (Batch)  | Export the transcripts selected by `all-transcripts.sql` to TEI XML and write the metadata CSVs.                                          |
| [`generate-vertical.py`](./generate-vertical.py)                                       | Vertical Export     | Convert TEI XML transcript files + `standoff_informants.xml` into NoSketch Engine `.vert` files.                                          |
| [`merge_vert_metadata.py`](./merge_vert_metadata.py)                                   | Vertical Enrichment | Merge CSV metadata (`standoff_informants.csv`, `transcript_metadata.csv`) and parsed `age_lower`/`age_upper` into existing `.vert` files. |
| [`load-metadata-lapisdb.py`](./load-metadata-lapisdb.py)                               | LAPIS DB Sync       | Transfer transcript, survey, informant, and place metadata from DiÖ-DB (`SOURCE`) to LAPIS DB (`TARGET`).                                 |
| [`migrate-informant-survey-to-junction.py`](./migrate-informant-survey-to-junction.py) | DB Migration        | One-time migration moving `informant.survey_id` into `informant_survey_conducted`.                                                        |
| [`migrate-place-survey-junction.py`](./migrate-place-survey-junction.py)               | DB Migration        | One-time migration populating `place_survey_conducted` from `informant.survey_id` and `informant_lives_in_place`.                         |
| [`create-tei-file-v2.py`](./create-tei-file-v2.py)                                     | Legacy              | Old JSON-based TEI prototype (`output3.json`); superseded by `create-tei-file-single-transcript.py`.                                      |
| [`fetch-dioe-db-data.py`](./fetch-dioe-db-data.py)                                     | Stub / Unused       | Incomplete DB connection stub with an empty `get_transcript_data()` function.                                                             |

---

## Setup & Environment Variables

Install dependencies into the repository's virtual environment:

```bash
pip install -r requirements.txt
```

Dependencies ([`requirements.txt`](./requirements.txt)): `psycopg2-binary==2.9.11`, `python-dotenv==1.2.1`, `lxml==5.4.0`.

Create a `.env` file in `scripts/` (see [`.env.example`](./.env.example)). Note that different scripts expect slightly different variable names:

- **TEI export scripts** (`create-tei-file-single-transcript.py`, `process-all-transcripts.py`):
  - `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASS`
- **LAPIS sync** (`load-metadata-lapisdb.py`):
  - `SOURCE_DB_HOST`, `SOURCE_DB_PORT`, `SOURCE_DB_NAME`, `SOURCE_DB_USER`, `SOURCE_DB_PASS`
  - `TARGET_DB_HOST`, `TARGET_DB_PORT`, `TARGET_DB_NAME`, `TARGET_DB_USER`, `TARGET_DB_PASS`
- **LAPIS migrations** (`migrate-informant-survey-to-junction.py`, `migrate-place-survey-junction.py`):
  - `TARGET_DB_HOST`, `TARGET_DB_PORT`, `TARGET_DB_NAME`, `TARGET_DB_USER`, `TARGET_DB_PASS`

---

## 1. TEI & NoSketch Engine Export Pipeline

For a full corpus build, run the following stages from `scripts/`. The batch script calls the single-transcript exporter and uses the repository's existing `dioe-tags.tei.xml`. Regenerate the taxonomy only when the tag tree changes.

| Stage                  | Inputs                                                                            | Outputs                                                                                                             |
| ---------------------- | --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| 1. Batch TEI export    | PostgreSQL, tag taxonomy, optional cached transcript data                         | `tei-files/<id>.xml`, `tei-files/standoff_informants.xml`, and both metadata CSVs in `tei-files/`                   |
| 2. Vertical conversion | Transcript XML and standoff XML from the same batch                               | `verticals_raw/<id>.vert`                                                                                           |
| 3. Metadata enrichment | Raw verticals and both batch CSVs, with the speaker header adapted as shown below | `../verticals/<id>.vert`, the corpus input copied by the [NoSketch Engine Dockerfile](../nosketchengine/Dockerfile) |

### `generate-standoff-tags.py`

- **Purpose:** Builds a hierarchical TEI feature-structure declaration (`<standOff><fs xml:id="dioe-tags-features">`) from the DiÖ tag tree JSON.
- **Inputs / Outputs:** Hardcoded in `__main__` to read `dioe-tags-tree.json` and write `dioe-tags.tei.xml` in the current working directory.
- **How it works:** Processes `tag_gene == 0` tags recursively via `children_ids` (matching same `tag_ebene_id` first when multiple entries share a `tag_id`), then runs a second pass for any orphaned non-generation-0 tags.
- **Usage:** Run from the repository root (where `dioe-tags-tree.json` and `dioe-tags.tei.xml` live), or move the generated file to `../dioe-tags.tei.xml`:
  ```bash
  python scripts/generate-standoff-tags.py
  ```

### `create-tei-file-single-transcript.py`

- **Purpose:** Exports one transcript to `<transcript_id>.xml` in the current working directory and creates or appends to `standoff_informants.xml`.
- **When to use:** Testing changes on a single transcript (e.g., `161`), refreshing one file, or running with `--use-cache` to avoid repeated DB queries.
- **How it works:**
  1. Reads token and tag data, informant IDs, and tokenset definitions from `<cache_dir>/<transcript_id>.json` on a cache hit. On a miss, it queries PostgreSQL and writes the cache when `--use-cache` is enabled. Whenever the transcript has informant IDs, it queries PostgreSQL for their metadata using `../queries/informant_query.sql`, including on a cache hit.
  2. Validates tag names case-insensitively against `../dioe-tags.tei.xml`.
  3. Builds TEI `<text>` with speaker-grouped `<u>` utterances, `<w>`, `<pc>`, `<pause>`, `<incident>`, `<unclear>`, `<standOff type="feature-declarations">`, and `<standOff type="timestamps">`.
  4. Appends missing `<person xml:id="spk_{id}">` and `<place xml:id="loc_{osm_id}">` entries to `standoff_informants.xml`.
- **Usage:**
  ```bash
  python create-tei-file-single-transcript.py 161 --use-cache --cache-dir cache/
  ```
- **Options:**
  - `transcript_id`: Numeric transcript ID (positional, required).
  - `--use-cache`: Read/write JSON cache files in `--cache-dir`.
  - `--cache-dir`: Cache folder relative to `scripts/` (default: `cache`).
  - `--standoff-dir`: Target directory for `standoff_informants.xml` (default: current working directory).

### `process-all-transcripts.py`

- **Purpose:** Orchestrates a full corpus TEI export.
- **When to use:** Generating or regenerating the corpus for all transcripts with matching survey records in `../queries/all-transcripts.sql`.
- **How it works:**
  1. Queries PostgreSQL for the transcript list and survey metadata on every run, even with a populated cache, and opens `<xml_dir>/transcript_metadata.csv`.
  2. Invokes `create-tei-file-single-transcript.py <id> --use-cache --cache-dir <cache_dir>` in a subprocess for each transcript and moves `<id>.xml` into `<xml_dir>`.
  3. Copies `standoff_informants.xml` into `<xml_dir>`, parses `<person>` nodes into `<xml_dir>/standoff_informants.csv`, and deletes the temporary `standoff_informants.xml` from `scripts/`.
- **Usage:**
  ```bash
  python process-all-transcripts.py cache/ tei-files/
  ```

### `generate-vertical.py`

- **Purpose:** Converts a directory of TEI XML transcripts into NoSketch Engine `.vert` files.
- **When to use:** After TEI export, to produce vertical input for Manatee/NoSketch Engine compilation.
- **How it works:**
  1. Loads speaker and location attributes (`sex`, `age`, `name`, `subproject`, `dialect_competence`, `standard_competence`, `residence`, `loc_id`, `lat`, `lon`) from `<standoff_file.xml>`.
  2. For each `*.xml` in `<tei_dir>` (excluding `<standoff_file.xml>`), resolves `#fs_*` feature structures and `#TL_*` timeline timestamps and writes `<stem>.vert` in `<output_dir>`.
  3. Outputs `<doc>`, `<file>`, enriched `<u>` tags, `<pause duration="..."/>` tags, and 8 tab-separated token columns: `word`, `lemma`, `pos`, `morph|syntax` (merged `dioe_tokenset_tags` and `dioe_tags`), `t_start`, `t_end`, `phon`, `token_id`.
- **Usage:**
  ```bash
  python generate-vertical.py tei-files/ tei-files/standoff_informants.xml verticals_raw/
  ```

### `merge_vert_metadata.py`

- **Purpose:** Post-processes `.vert` files to inject additional metadata from CSV files without overwriting existing attributes on `<doc>` or `<u>`.
- **When to use:** When enriching `.vert` files with transcript-level attributes (`erhebung`, `erhebungsart`, etc.) from `transcript_metadata.csv` and speaker attributes (plus parsed numeric `age_lower` and `age_upper` bounds, defaulting to `1` and `999`) from `standoff_informants.csv`.
- **How it works:** Matches `<doc>` via the `.vert` filename stem against `--transcript-key-column` (default: `id`), and matches `<u>` via `--speaker-key` (default: `who`) against the `id` column in `--informants-csv`.
- **Usage:** The batch exporter writes `xml_id` as the first speaker CSV column. Create a copy with the `id` header required by the merger, then merge it with the raw verticals and transcript CSV from the same run.

  ```bash
  sed '1s/^xml_id,/id,/' tei-files/standoff_informants.csv > tei-files/standoff_informants_for_merge.csv

  python merge_vert_metadata.py \
      --informants-csv tei-files/standoff_informants_for_merge.csv \
      --transcripts-csv tei-files/transcript_metadata.csv \
      --vert-dir verticals_raw/ \
      --out-dir ../verticals/ \
      --speaker-key who
  ```

---

## 2. LAPIS Database Sync & Migrations

These scripts are separate from the TEI/vertical pipeline and operate on the LAPIS PostgreSQL database.

### `load-metadata-lapisdb.py`

- **Purpose:** Reads DiÖ metadata from `SOURCE` using `../queries/transcript-metadata.sql` and creates missing survey types, surveys, survey instances, informants, places, and their links in `TARGET` (LAPIS).
- **When to use:** Populate LAPIS, add newly available records and links, or refresh audio links. For an existing `survey_conducted` row matched by `instance_id` and `survey_id`, the loader updates only `audio_link`. It reuses informants matched by `comment` (sigle) and places matched by latitude and longitude.
- **Tables touched in `TARGET`:** `survey_type`, `survey`, `project` (lookup), `project_survey`, `survey_conducted`, `age_group` (lookup), `informant`, `informant_survey_conducted`, `place`, `informant_lives_in_place`, and `place_survey_conducted`.
- **Usage:**
  ```bash
  python load-metadata-lapisdb.py
  ```

### `migrate-informant-survey-to-junction.py`

- **Purpose:** Migrates legacy `informant.survey_id` foreign keys into the `informant_survey_conducted` m:n junction table in `TARGET`.
- **When to use:** One-time schema migration if `informant.survey_id` still exists and needs to be copied before dropping the column. Exits early if `informant.survey_id` does not exist.
- **Usage:**
  ```bash
  python migrate-informant-survey-to-junction.py
  ```

### `migrate-place-survey-junction.py`

- **Purpose:** Populates `place_survey_conducted` in `TARGET` by joining `informant.survey_id` with `informant_lives_in_place.place_id`.
- **When to use:** One-time schema migration alongside `migrate-informant-survey-to-junction.py` while `informant.survey_id` is still present. Skips informants without a `place_id` or existing pairs.
- **Usage:**
  ```bash
  python migrate-place-survey-junction.py
  ```

---

## 3. Legacy & Stub Files

- **`create-tei-file-v2.py`:** Early JSON-based TEI generator reading a hardcoded `output3.json` and `spacy-tags.tei.xml`. Superseded by `create-tei-file-single-transcript.py` and should not be used for current exports.
- **`fetch-dioe-db-data.py`:** Minimal DB connection test/stub (`get_transcript_data()` does not execute any queries).

---

## 4. Supporting Files & Working Directories

- [`configuration-files/dioedbcorpus`](./configuration-files/dioedbcorpus): Earlier Manatee/NoSketch Engine corpus configuration file (see [`../nosketchengine/dioedbcorpus`](../nosketchengine/dioedbcorpus) for the current image configuration).
- `cache/`: Cached per-transcript JSON dumps (`<id>.json`).
- `tei-files/` and `verticals_raw/` hold intermediate TEI, metadata, and vertical files. Use fresh intermediate directories for a new full export because the converters process all matching files in their input directories.
- `../verticals/` holds the enriched files for the corpus image. The merge command overwrites matching output filenames. Use a separate output directory when previewing a subset.

---

## Code Review Notes & Known Quirks

1. **Hardcoded paths in `generate-standoff-tags.py`:** Reads `dioe-tags-tree.json` and writes `dioe-tags.tei.xml` in the current working directory rather than accepting CLI path arguments.
