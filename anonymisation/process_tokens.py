import os
import re
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

# Tag mapping definitions
TAG_MAP = {"N": "NAME", "Z": "ZAHL", "O": "ORT", "S": "SONSTIGE", "": "SONSTIGE"}


def process_tokens():
    try:
        with open("select_tokens.sql", "r") as file:
            select_query = file.read()
    except FileNotFoundError:
        print("Error: 'select_tokens.sql' file not found.")
        return

    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        cursor = conn.cursor()
    except Exception as e:
        print(f"Database connection failed: {e}")
        return

    try:
        # Fetch the tokens
        cursor.execute(select_query)
        rows = cursor.fetchall()

        chain_ids = []
        in_chain = False
        updates = []

        for row in rows:
            token_id = row[0]
            ortho = row[1]

            # Skip if ortho is NULL
            if not ortho:
                continue

            # Look for a closing bracket followed optionally by N, Z, O, or S at the end of the string
            closing_match = re.search(r"\]([NZOS]?)\s*$", ortho)

            if not in_chain:
                # Check if a new chain starts
                if ortho.startswith("["):
                    chain_ids.append(token_id)

                    if closing_match:
                        # Single-token entity (e.g., "[Ludwig]N")
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
                        # Multi-token entity starts (e.g., "[Ludwig")
                        in_chain = True
            else:
                chain_ids.append(token_id)

                if closing_match:
                    # Chain ends here (e.g., "Dasein]N")
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

        if updates:
            total_updates = len(updates)
            print(f"Found {total_updates} tokens to update. Executing in batches...")

            update_query = """
                UPDATE public.token 
                SET ortho = %s, phon = %s, text = %s, text_in_ortho = %s, splemma = %s 
                WHERE id = %s
            """

            batch_size = 100

            # Loop through the updates list in increments of batch_size
            for i in range(0, total_updates, batch_size):
                # Slice the list to get the current batch
                batch = updates[i : i + batch_size]

                # Execute and commit the current batch
                cursor.executemany(update_query, batch)
                conn.commit()

                current_batch_num = (i // batch_size) + 1
                print(f"Processed batch {current_batch_num} ({len(batch)} rows)...")

            print("All updates completed successfully.")
        else:
            print("No matching bracket patterns found. No updates performed.")

    except Exception as e:
        conn.rollback()
        print(f"An error occurred during processing: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    process_tokens()
