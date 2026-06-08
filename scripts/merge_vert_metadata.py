"""
Merge standoff metadata into .vert files for NoSketch Engine.

Assumes every .vert file already contains a <doc> tag at the top.
The script merges transcript metadata into the existing <doc> tag and
injects speaker metadata into every <u> tag.

Usage:
    python merge_vert_metadata.py \
        --informants-csv standoff_informants.csv \
        --transcripts-csv transcript_metadata.csv \
        --vert-dir ./verticals_raw \
        --out-dir ./verticals \
        --speaker-key who
"""

import argparse
import csv
import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# CSV loader
# ---------------------------------------------------------------------------


def load_csv_as_dict(csv_path, key_column):
    """Load a CSV file into a dictionary keyed by a specific column."""
    data = {}
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row.get(key_column)
            if key is None:
                raise ValueError(
                    f"Key column '{key_column}' not found in {csv_path}. "
                    f"Available columns: {', '.join(row.keys())}"
                )
            data[key] = {k: v for k, v in row.items() if k != key_column and v}
    return data


# ---------------------------------------------------------------------------
# Regex helpers
# ---------------------------------------------------------------------------

_U_TAG_RE = re.compile(r"<u\b([^>]*)>")
_DOC_TAG_RE = re.compile(r"<doc\b([^>]*)>")


def parse_attrs(attr_str: str) -> dict:
    """Parse a string of XML attributes into a dictionary."""
    return dict(re.findall(r'(\S+?)="([^"]*)"', attr_str))


def build_attrs(attrs: dict) -> str:
    """Build an XML attribute string from a dictionary."""
    return " ".join(f'{k}="{v}"' for k, v in attrs.items())


def escape_xml_attr(value: str) -> str:
    """Escape special characters for XML attribute values."""
    return (
        value.replace("&", "&amp;")
        .replace('"', "&quot;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


# ---------------------------------------------------------------------------
# Core merge logic
# ---------------------------------------------------------------------------


def merge_vert_file(
    vert_path: Path,
    out_path: Path,
    speakers: dict,
    transcripts: dict,
    speaker_key: str,
):
    """Merge metadata into a single .vert file.

    Assumes the file already contains a <doc> tag. Merges transcript
    metadata into the existing <doc> and speaker metadata into <u> tags.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Derive transcript ID from filename stem (e.g., "1.vert" → "1")
    transcript_id = vert_path.stem
    transcript_meta = transcripts.get(transcript_id, {})

    if transcript_meta:
        print(
            f"   [{transcript_id}] Merging transcript metadata: {', '.join(transcript_meta.keys())}"
        )
    else:
        print(f"   [{transcript_id}] ⚠️ No transcript metadata found.")

    with open(vert_path, "r", encoding="utf-8") as fin, open(
        out_path, "w", encoding="utf-8"
    ) as fout:

        for line in fin:
            # --- Merge transcript metadata into existing <doc> tag ---
            m_doc = _DOC_TAG_RE.match(line)
            if m_doc:
                attrs = parse_attrs(m_doc.group(1))
                # Merge transcript metadata, never overwrite existing attributes
                for meta_key, meta_value in transcript_meta.items():
                    if meta_key not in attrs:
                        attrs[meta_key] = escape_xml_attr(meta_value)
                line = f"<doc {build_attrs(attrs)}>\n"
                fout.write(line)
                continue

            # --- Merge speaker metadata into <u> tags ---
            m_u = _U_TAG_RE.match(line)
            if not m_u:
                fout.write(line)
                continue

            attrs = parse_attrs(m_u.group(1))
            speaker_id = attrs.get(speaker_key)

            if speaker_id and speaker_id in speakers:
                for meta_key, meta_value in speakers[speaker_id].items():
                    if meta_key not in attrs:
                        attrs[meta_key] = escape_xml_attr(meta_value)

            fout.write(f"<u {build_attrs(attrs)}>\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Merge speaker and transcript metadata into .vert files."
    )
    parser.add_argument(
        "--informants-csv",
        type=Path,
        required=True,
        help="Path to standoff_informants.csv (speaker metadata).",
    )
    parser.add_argument(
        "--transcripts-csv",
        type=Path,
        required=True,
        help="Path to transcript_metadata.csv (transcript metadata).",
    )
    parser.add_argument(
        "--vert-dir",
        type=Path,
        required=True,
        help="Directory containing raw .vert files (each already wrapped in <doc>).",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="Directory for merged .vert output.",
    )
    parser.add_argument(
        "--speaker-key",
        type=str,
        default="who",
        help="Attribute on <u> that holds the speaker ID (default: who).",
    )
    parser.add_argument(
        "--transcript-key-column",
        type=str,
        default="id",
        help="Column name in transcript CSV that holds the transcript ID (default: id).",
    )
    parser.add_argument(
        "--vert-ext",
        type=str,
        default=".vert",
        help="Extension of vertical files (default: .vert).",
    )

    args = parser.parse_args()

    # --- Load metadata ---
    print(f"📄 Loading speaker metadata from {args.informants_csv}...")
    speakers = load_csv_as_dict(args.informants_csv, key_column="id")
    print(f"   Loaded {len(speakers)} speakers.")

    print(f"📄 Loading transcript metadata from {args.transcripts_csv}...")
    transcripts = load_csv_as_dict(
        args.transcripts_csv, key_column=args.transcript_key_column
    )
    print(f"   Loaded {len(transcripts)} transcripts.")

    # --- Find .vert files ---
    vert_files = sorted(args.vert_dir.glob(f"*{args.vert_ext}"))
    if not vert_files:
        print(f"❌ No *{args.vert_ext} files found in {args.vert_dir}")
        sys.exit(1)

    print(f"\n🔄 Processing {len(vert_files)} .vert files...")
    for vf in vert_files:
        out = args.out_dir / vf.name
        merge_vert_file(
            vert_path=vf,
            out_path=out,
            speakers=speakers,
            transcripts=transcripts,
            speaker_key=args.speaker_key,
        )

    print(f"\n✅ Merged {len(vert_files)} files into {args.out_dir}")


if __name__ == "__main__":
    main()
