import os
import re
import psycopg2
from dotenv import load_dotenv

load_dotenv()

TAG_MAP = {"N": "NAME", "Z": "ZAHL", "O": "ORT", "S": "SONSTIGE", "": "SONSTIGE"}

SEARCH_PATTERN = r"\]([\w]?)\s*$"


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
    )


def has_bracket_pattern(value):
    """Return True if the string looks like an annotated token."""
    if not value:
        return False
    if value.startswith("["):
        return True
    return re.search(SEARCH_PATTERN, value) is not None


def analyze_tokens(rows):
    """Phase 1: Pure Python analysis. No DB transaction risk."""
    chain_ids = []
    in_chain = False
    updates = []

    for row in rows:
        token_id = row[0]
        ortho = row[1]
        text_column = row[2]

        token = None
        if has_bracket_pattern(ortho):
            token = ortho
        elif has_bracket_pattern(text_column):
            token = text_column

        if not token:
            continue

        closing_match = re.search(SEARCH_PATTERN, token)

        if not in_chain:
            if token.startswith("["):
                chain_ids = [token_id]  # Start fresh chain
                if closing_match:
                    # Single-token entity: e.g. "[Ludwig]N"
                    tag = closing_match.group(1)
                    replacement = TAG_MAP.get(tag, "SONSTIGE")
                    for cid in chain_ids:
                        updates.append(
                            (
                                replacement,
                                replacement,
                                replacement,
                                replacement,
                                replacement,
                                cid,
                            )
                        )
                    chain_ids = []
                else:
                    # Multi-token entity starts: e.g. "[Ludwig"
                    in_chain = True
            elif closing_match:
                # EDGE CASE: Closing bracket without an opening one
                # e.g. "Dasein]N" standing alone
                tag = closing_match.group(1)
                replacement = TAG_MAP.get(tag, "SONSTIGE")
                updates.append(
                    (
                        replacement,
                        replacement,
                        replacement,
                        replacement,
                        replacement,
                        token_id,
                    )
                )

        else:
            chain_ids.append(token_id)
            closing_match = None
            if ortho:
                closing_match = re.search(SEARCH_PATTERN, ortho)
            if not closing_match and text_column:
                closing_match = re.search(SEARCH_PATTERN, text_column)

            if closing_match:
                tag = closing_match.group(1)
                replacement = TAG_MAP.get(tag, "SONSTIGE")
                for cid in chain_ids:
                    updates.append(
                        (
                            replacement,
                            replacement,
                            replacement,
                            replacement,
                            replacement,
                            cid,
                        )
                    )
                chain_ids = []
                in_chain = False

    if chain_ids:
        print(
            f"Warning: {len(chain_ids)} token(s) found in an unclosed bracket chain and were skipped."
        )

    return updates


def process_tokens():
    # --- Read SQL file ---
    try:
        with open("select_tokens.sql", "r") as file:
            select_query = file.read()
    except FileNotFoundError:
        print("Error: 'select_tokens.sql' file not found.")
        return

    rows = []
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        conn.autocommit = True  #
        cursor = conn.cursor()
        cursor.execute(select_query)
        rows = cursor.fetchall()
    except Exception as e:
        print(f"Database connection/query failed: {e}")
        return
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    updates = analyze_tokens(rows)

    if not updates:
        print("No matching bracket patterns found. No updates performed.")
        return

    print(f"\nFound {len(updates)} token(s) staged for update.")
    print("Preview (first 5):")
    for u in updates[:5]:
        print(f"  Token ID {u[-1]} -> {u[0]}")
    if len(updates) > 5:
        print(f"  ... and {len(updates) - 5} more")

    choice = input("\nCommit all changes to the database? (y/n): ").strip().lower()
    if choice != "y":
        print("Aborted. No changes were made.")
        return

    # Execute in one focused write transaction ---
    update_query = """
        UPDATE public.token 
        SET ortho = %s, phon = %s, text = %s, text_in_ortho = %s, splemma = %s 
        WHERE id = %s
    """
    batch_size = 100

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        conn.autocommit = False  # Explicit transaction control
        cursor = conn.cursor()

        for i in range(0, len(updates), batch_size):
            batch = updates[i : i + batch_size]
            cursor.executemany(update_query, batch)
            print(f"Executed batch {(i // batch_size) + 1} ({len(batch)} rows)...")

        conn.commit()
        print("All updates committed successfully.")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"An error occurred during the update. All changes were rolled back: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    process_tokens()
