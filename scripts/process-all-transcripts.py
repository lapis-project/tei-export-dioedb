import os
import sys
import argparse
import subprocess
import shutil
import csv
import xml.etree.ElementTree as ET
import psycopg2
from dotenv import load_dotenv

# --- Load Environment Variables ---
load_dotenv()


def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"❌ Could not connect to the database: {e}")
        return None


def parse_standoff_informants(xml_path):
    """Parse the standoff_informants.xml file and extract speaker metadata.

    Returns a list of dictionaries, one per speaker, with all available fields.
    """
    speakers = []

    # Register namespaces to handle them properly
    namespaces = {
        "tei": "http://www.tei-c.org/ns/1.0",
        "xi": "http://www.w3.org/2001/XInclude",
        "xml": "http://www.w3.org/XML/1998/namespace",
    }

    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Find all person elements in the TEI document
    # Using the namespace-aware path
    for person in root.findall(".//tei:person", namespaces):
        speaker = {}

        # Extract xml:id (speaker identifier)
        xml_id = person.get("{http://www.w3.org/XML/1998/namespace}id")
        if xml_id:
            speaker["xml_id"] = xml_id

        # Extract all child elements as potential metadata fields
        for child in person:
            # Get the tag name without namespace
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag

            # Handle different element types
            if tag == "persName":
                speaker["persName"] = child.text.strip() if child.text else ""
            elif tag == "sex":
                # The sex value is in the 'value' attribute
                speaker["sex"] = child.get("value", "")
            elif tag == "age":
                speaker["age"] = child.text.strip() if child.text else ""
            elif tag == "occupation":
                speaker["occupation"] = child.text.strip() if child.text else ""
            elif tag == "education":
                speaker["education"] = child.text.strip() if child.text else ""
            elif tag == "residence":
                # residence might have placeName or other children
                place_name = child.find("tei:placeName", namespaces)
                if place_name is not None and place_name.text:
                    speaker["residence"] = place_name.text.strip()
                else:
                    speaker["residence"] = child.text.strip() if child.text else ""
            elif tag == "nationality":
                speaker["nationality"] = child.text.strip() if child.text else ""
            elif tag == "langKnowledge":
                # Language knowledge - might have multiple entries
                langs = []
                for lang in child.findall("tei:langKnown", namespaces):
                    lang_val = lang.get("level", "")
                    lang_tag = lang.tag.split("}")[-1] if "}" in lang.tag else lang.tag
                    if lang.text:
                        langs.append(f"{lang.text.strip()}:{lang_val}")
                if langs:
                    speaker["languages"] = "; ".join(langs)
            elif tag == "note":
                # Notes might have type attributes
                note_type = child.get("type", "")
                if note_type:
                    speaker[f"note_{note_type}"] = (
                        child.text.strip() if child.text else ""
                    )
                else:
                    speaker["note"] = child.text.strip() if child.text else ""
            elif tag == "affiliation":
                speaker["affiliation"] = child.text.strip() if child.text else ""
            elif tag == "state":
                # state elements with type attributes
                state_type = child.get("type", "")
                if state_type:
                    speaker[f"state_{state_type}"] = (
                        child.text.strip() if child.text else ""
                    )
                else:
                    speaker["state"] = child.text.strip() if child.text else ""
            elif tag == "trait":
                # trait elements with type attributes
                trait_type = child.get("type", "")
                if trait_type:
                    speaker[f"trait_{trait_type}"] = (
                        child.text.strip() if child.text else ""
                    )
                else:
                    speaker["trait"] = child.text.strip() if child.text else ""
            else:
                # For any other elements, use the tag name as the column
                # and handle attributes
                text_content = child.text.strip() if child.text else ""
                if text_content:
                    speaker[tag] = text_content
                # Also capture any attributes as separate columns
                for attr_name, attr_value in child.attrib.items():
                    attr_key = (
                        attr_name.split("}")[-1] if "}" in attr_name else attr_name
                    )
                    if attr_value:
                        speaker[f"{tag}_{attr_key}"] = attr_value

        speakers.append(speaker)

    return speakers


def write_speakers_csv(speakers, csv_path):
    """Write speaker metadata to a CSV file.

    Dynamically determines columns from all available fields across all speakers.
    """
    if not speakers:
        print("⚠️  No speakers found to write to CSV.")
        return

    # Collect all unique field names across all speakers
    all_fields = set()
    for speaker in speakers:
        all_fields.update(speaker.keys())

    # Define preferred column order (common fields first)
    preferred_order = [
        "xml_id",
        "persName",
        "sex",
        "age",
        "occupation",
        "education",
        "residence",
        "nationality",
        "languages",
        "affiliation",
        "note",
        "note_type",
    ]

    # Build final fieldnames: preferred first, then remaining alphabetically
    fieldnames = []
    for field in preferred_order:
        if field in all_fields:
            fieldnames.append(field)

    # Add remaining fields alphabetically
    remaining = sorted(all_fields - set(fieldnames))
    fieldnames.extend(remaining)

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(speakers)

    print(f"✅ Wrote {len(speakers)} speaker records to {csv_path}")
    print(f"   Columns: {', '.join(fieldnames)}")


def init_transcript_metadata_csv(csv_path, fieldnames):
    """Open a transcript metadata CSV for incremental writing.

    Returns the open file handle and the csv.DictWriter.  The caller
    **must** close the file handle when done.
    """
    f = open(csv_path, "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    return f, writer


def write_transcript_metadata_row(writer, row_dict):
    """Append a single transcript row to the metadata CSV.

    Parameters
    ----------
    writer : csv.DictWriter
        The writer returned by `init_transcript_metadata_csv`.
    row_dict : dict
        Dictionary of column names → values for this transcript.
        Extra keys are silently ignored.
    """
    writer.writerow(row_dict)


def main():
    parser = argparse.ArgumentParser(
        description="Process all transcripts and generate TEI XML files."
    )
    parser.add_argument(
        "cache_dir",
        type=str,
        help="Directory to store cache files (passed to the single transcript script).",
    )
    parser.add_argument(
        "xml_dir",
        type=str,
        help="Directory where the finished transcript files will be stored.",
    )

    args = parser.parse_args()

    # Ensure absolute paths
    cache_dir = os.path.abspath(args.cache_dir)
    xml_dir = os.path.abspath(args.xml_dir)

    # Create output directory if it doesn't exist
    if not os.path.exists(xml_dir):
        print(f"Creating output directory: {xml_dir}")
        os.makedirs(xml_dir)

    # Determine script directory (where this script and the single-transcript script reside)
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Change working directory to script_dir to ensure relative paths in the called script work
    # (e.g., ../queries/..., ../dioe-tags.tei.xml)
    original_cwd = os.getcwd()
    os.chdir(script_dir)
    print(f"Changed working directory to: {script_dir}")

    conn = get_db_connection()
    if not conn:
        print("Failed to connect to database. Exiting.")
        return

    try:
        with conn.cursor() as cur:
            # Load query from file
            query_path = os.path.join(script_dir, "../queries/all-transcripts.sql")
            if not os.path.exists(query_path):
                print(f"❌ Query file not found: {query_path}")
                return

            print(f"Reading query from {query_path}...")
            with open(query_path, "r") as f:
                query = f.read()

            print("Fetching transcript list...")
            cur.execute(query)
            transcripts = cur.fetchall()
            print(f"✅ Found {len(transcripts)} transcripts.")
            standoff_file = "standoff_informants.xml"

            col_names = [desc[0] for desc in cur.description]
            print(
                f"✅ Found {len(transcripts)} transcripts (columns: {', '.join(col_names)})."
            )

            transcript_csv_path = os.path.join(xml_dir, "transcript_metadata.csv")
            transcript_csv_file, transcript_csv_writer = init_transcript_metadata_csv(
                transcript_csv_path, col_names
            )
            print(f"   Initialised {transcript_csv_path} for incremental writing.\n")

            for i, row in enumerate(transcripts):

                row_dict = dict(zip(col_names, row))
                t_id = row_dict.get(col_names[0])  # assume first col is the id
                t_name = row_dict.get(col_names[1]) if len(col_names) > 1 else ""

                print(
                    f"[{i+1}/{len(transcripts)}] Processing Transcript ID: {t_id} ({t_name})..."
                )

                # Construct command
                # python3 create-tei-file-single-transcript.py <id> --use-cache --cache-dir <dir>
                cmd = [
                    sys.executable,
                    "create-tei-file-single-transcript.py",
                    str(t_id),
                    "--use-cache",
                    "--cache-dir",
                    cache_dir,
                ]

                try:
                    subprocess.run(cmd, check=True)

                    # Move generated files to xml_dir
                    transcript_file = f"{t_id}.xml"

                    if os.path.exists(transcript_file):
                        shutil.move(
                            transcript_file, os.path.join(xml_dir, transcript_file)
                        )
                        print(f"  Moved {transcript_file} to {xml_dir}")
                    else:
                        print(
                            f"⚠️  Warning: Expected output file {transcript_file} not found."
                        )

                except subprocess.CalledProcessError as e:
                    print(f"❌ Error processing transcript {t_id}: {e}")
                    continue

                write_transcript_metadata_row(transcript_csv_writer, row_dict)

            # generate CSV ---
            if os.path.exists(standoff_file):
                shutil.copy(standoff_file, os.path.join(xml_dir, standoff_file))

                print(f"\n📄 Parsing {standoff_file} for speaker metadata...")
                speakers = parse_standoff_informants(standoff_file)
                print(f"   Found {len(speakers)} speakers")

                csv_file = "standoff_informants.csv"
                csv_path = os.path.join(xml_dir, csv_file)
                write_speakers_csv(speakers, csv_path)

                os.remove(standoff_file)
                print(
                    f"  Updated {standoff_file} and generated {csv_file} in {xml_dir}"
                )
            else:
                print(f"⚠️  Warning: {standoff_file} not found in working directory.")

    except Exception as e:
        print(f"❌ An error occurred: {e}")
        import traceback

        traceback.print_exc()
    finally:
        if conn:
            conn.close()
        os.chdir(original_cwd)


if __name__ == "__main__":
    main()
