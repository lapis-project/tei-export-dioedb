import os
import sys
import argparse
import subprocess
import shutil
import psycopg2
from dotenv import load_dotenv

# --- Load Environment Variables ---
load_dotenv()


def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"❌ Could not connect to the database: {e}")
        return None


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

            for i, (t_id, t_name) in enumerate(transcripts):
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

            if os.path.exists(standoff_file):
                shutil.copy(standoff_file, os.path.join(xml_dir, standoff_file))
                # Remove the local copy of the standoff file
                os.remove(standoff_file)
                print(f"  Updated {standoff_file} in {xml_dir}")
    except Exception as e:
        print(f"❌ An error occurred: {e}")
    finally:
        if conn:
            conn.close()
        # Restore CWD
        os.chdir(original_cwd)


if __name__ == "__main__":
    main()
