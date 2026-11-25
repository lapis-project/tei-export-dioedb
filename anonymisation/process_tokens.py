#!/usr/bin/env python3
"""Identify bracket-annotated tokens in DiÖDB and replace PII columns."""

import argparse
import os
import re
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
load_dotenv()

REQUIRED_DB_ENV_VARS = ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASS")

TAG_MAP = {
    "N": "NAME",
    "Z": "ZAHL",
    "O": "ORT",
    "S": "SONSTIGE",
    "": "SONSTIGE",
}

# Matches a closing bracket for an opened '[' token or chain, allowing optional
# N/Z/O/S tag (case-insensitive) and trailing transcription punctuation.
CLOSING_CHAIN_PATTERN = re.compile(
    r"\]([NZOSnzos]?)[\s\)\"'”„“.,!?;:/\-=]*$"
)

# Matches an orphaned closing bracket without '[' (e.g. "Dasein]N", "siebter]Z/").
# Requires an explicit N/Z/O/S tag (ignoring plain "ambig]") and excludes ')'
# so spans whose '[' is not at index 0 (e.g. "([Gustav ... Michel]N)") stay untouched.
STANDALONE_CLOSING_PATTERN = re.compile(
    r"\]([NZOSnzos])[\s\"'”„“.,!?;:/\-=]*$"
)


def parse_arguments(argv=None):
    """Parse optional CLI overrides."""
    parser = argparse.ArgumentParser(
        description="Anonymise bracket-annotated tokens in public.token."
    )
    parser.add_argument(
        "--sql-file",
        type=Path,
        default=BASE_DIR / "select_tokens.sql",
        help="path to token selection SQL file (default: anonymisation/select_tokens.sql)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="number of token updates per SQL batch (default: 500)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="preview staged updates without modifying the database",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="commit staged updates without interactive confirmation",
    )
    return parser.parse_args(argv)


def validate_db_env():
    """Return a list of missing required database environment variable names."""
    return [name for name in REQUIRED_DB_ENV_VARS if not os.getenv(name)]


def get_db_connection():
    """Open a PostgreSQL connection using DB_* environment variables."""
    missing = validate_db_env()
    if missing:
        raise RuntimeError(
            f"Missing required environment variable(s): {', '.join(missing)}"
        )
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
    )


def find_closing_tag(candidates, pattern):
    """Return the matched closing tag across candidate columns, preferring explicit tags."""
    matched_empty = False
    for value in candidates:
        match = pattern.search(value)
        if match:
            tag = match.group(1)
            if tag:
                return tag
            matched_empty = True
    return "" if matched_empty else None


def analyze_tokens(rows):
    """Pure Python token analysis returning (token_id, replacement) pairs."""
    chain_ids = []
    in_chain = False
    current_scope = None
    unclosed_count = 0
    updates = []

    for row in rows:
        token_id = row[0]
        transcript_id = row[1]
        inf_id = row[2]
        ortho = row[3]
        text_column = row[4]
        text_in_ortho = row[5]

        scope = (transcript_id, inf_id)
        if in_chain and scope != current_scope:
            unclosed_count += len(chain_ids)
            chain_ids = []
            in_chain = False
        current_scope = scope

        candidates = [v for v in (ortho, text_column, text_in_ortho) if v]
        if not candidates:
            if in_chain:
                chain_ids.append(token_id)
            continue

        opens_chain = any(v.startswith("[") for v in candidates)

        if in_chain:
            if opens_chain:
                # Previous '[' chain was never closed; discard it before starting a new one.
                unclosed_count += len(chain_ids)
                chain_ids = []
                in_chain = False
            else:
                chain_ids.append(token_id)
                closing_tag = find_closing_tag(candidates, CLOSING_CHAIN_PATTERN)
                if closing_tag is not None:
                    replacement = TAG_MAP.get(closing_tag.upper(), "SONSTIGE")
                    for cid in chain_ids:
                        updates.append((cid, replacement))
                    chain_ids = []
                    in_chain = False
                continue

        if opens_chain:
            closing_tag = find_closing_tag(candidates, CLOSING_CHAIN_PATTERN)
            if closing_tag is not None:
                replacement = TAG_MAP.get(closing_tag.upper(), "SONSTIGE")
                updates.append((token_id, replacement))
            else:
                chain_ids = [token_id]
                in_chain = True
        elif not any("[" in v for v in candidates):
            closing_tag = find_closing_tag(candidates, STANDALONE_CLOSING_PATTERN)
            if closing_tag is not None:
                replacement = TAG_MAP.get(closing_tag.upper(), "SONSTIGE")
                updates.append((token_id, replacement))

    if chain_ids:
        unclosed_count += len(chain_ids)

    if unclosed_count:
        print(
            f"Warning: {unclosed_count} token(s) found in unclosed bracket chain(s) and were skipped."
        )

    return updates


def process_tokens(argv=None):
    """Fetch tokens, stage anonymisation updates, and write them in one transaction."""
    args = parse_arguments(argv)
    if args.batch_size <= 0:
        print("Error: --batch-size must be a positive integer.", file=sys.stderr)
        return 2

    try:
        select_query = args.sql_file.read_text(encoding="utf-8")
    except OSError as error:
        print(f"Error reading SQL file '{args.sql_file}': {error}", file=sys.stderr)
        return 1

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(select_query)
        rows = cursor.fetchall()
    except Exception as error:
        print(f"Database connection/query failed: {error}", file=sys.stderr)
        return 1
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    updates = analyze_tokens(rows)

    if not updates:
        print("No matching bracket patterns found. No updates performed.")
        return 0

    print(f"\nFound {len(updates)} token(s) staged for update.")
    print("Preview (first 5):")
    for token_id, replacement in updates[:5]:
        print(f"  Token ID {token_id} -> {replacement}")
    if len(updates) > 5:
        print(f"  ... and {len(updates) - 5} more")

    if args.dry_run:
        print("Dry run complete. No changes were made.")
        return 0

    if not args.yes:
        choice = input("\nCommit all changes to the database? (y/n): ").strip().lower()
        if choice != "y":
            print("Aborted. No changes were made.")
            return 1

    update_query = """
        UPDATE public.token AS t
        SET
            ortho = v.replacement,
            phon = v.replacement,
            text = v.replacement,
            text_in_ortho = v.replacement,
            splemma = v.replacement,
            ttlemma = CASE WHEN t.ttlemma IS NOT NULL AND t.ttlemma <> '' THEN v.replacement ELSE t.ttlemma END,
            ttcheckword = CASE WHEN t.ttcheckword IS NOT NULL AND t.ttcheckword <> '' THEN v.replacement ELSE t.ttcheckword END,
            lautg = CASE WHEN t.lautg IS NOT NULL AND t.lautg <> '' THEN v.replacement ELSE t.lautg END,
            sonst = CASE WHEN t.sonst IS NOT NULL AND t.sonst <> '' THEN v.replacement ELSE t.sonst END,
            transcomment = NULL
        FROM (VALUES %s) AS v(id, replacement)
        WHERE t.id = v.id
    """

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        conn.autocommit = False
        cursor = conn.cursor()

        for i in range(0, len(updates), args.batch_size):
            batch = updates[i : i + args.batch_size]
            execute_values(
                cursor,
                update_query,
                batch,
                template="(%s::integer, %s)",
                page_size=len(batch),
            )
            print(
                f"Executed batch {(i // args.batch_size) + 1} ({len(batch)} rows)..."
            )

        conn.commit()
        print("All updates committed successfully.")
        return 0
    except Exception as error:
        if conn:
            conn.rollback()
        print(
            f"An error occurred during the update. All changes were rolled back: {error}",
            file=sys.stderr,
        )
        return 1
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    sys.exit(process_tokens())
