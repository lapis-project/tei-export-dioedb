import json
import os
import time
import argparse
from datetime import timedelta

import psycopg2
from lxml import etree
from dotenv import load_dotenv

# --- Load Environment Variables ---
# Make sure you have a .env file in the same directory with your DB credentials
load_dotenv()

# --- Define XML Namespaces ---
TEI_NS = "http://www.tei-c.org/ns/1.0"
XML_NS = "http://www.w3.org/XML/1998/namespace"
XINCLUDE_NS = "http://www.w3.org/2001/XInclude"
NS_MAP = {None: TEI_NS, "xml": XML_NS, "xi": XINCLUDE_NS}


# --- Custom JSON Encoder for Caching ---
class CustomEncoder(json.JSONEncoder):
    """
    Custom JSON encoder to handle non-serializable types like datetime.timedelta.
    """

    def default(self, obj):
        if isinstance(obj, timedelta):
            # Convert timedelta to a string representation (e.g., "HH:MM:SS.ffffff")
            return str(obj)
        # Let the base class default method raise the TypeError for other types
        return super().default(obj)


# --- Database Connection ---
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


# --- NEW: Data Fetching Function ---
def fetch_transcript_data(transcript_id, use_cache=False, cache_dir="cache"):
    """
    Fetches and merges transcript data from the database using two queries.
    Optionally uses a file-based cache to speed up subsequent runs.
    """
    cache_file = os.path.join(cache_dir, f"transcript_{transcript_id}_data.json")
    if use_cache:
        if os.path.exists(cache_file):
            print(f"✅ Loading transcript data from cache: {cache_file}")
            try:
                with open(cache_file, "r") as f:
                    cached_data = json.load(f)
                # Ensure keys exist before returning
                if (
                    "transcript_data" in cached_data
                    and "unique_informant_ids" in cached_data
                    and "tokenset_definitions" in cached_data
                ):
                    return (
                        cached_data["transcript_data"],
                        cached_data["unique_informant_ids"],
                        cached_data["tokenset_definitions"],
                    )
                else:
                    print(f"⚠️ Cache file is missing required keys. Fetching from DB.")
            except (json.JSONDecodeError, KeyError) as e:
                print(f"⚠️ Cache file is invalid: {e}. Fetching from DB.")

        if not os.path.exists(cache_dir):
            print(f"Creating cache directory: {cache_dir}")
            os.makedirs(cache_dir)

    conn = get_db_connection()
    if not conn:
        return None, None, None

    transcript_data = []
    unique_informant_ids = []
    try:
        with conn.cursor() as cur:
            # --- Query 1: Fetch Tokens ---
            print(f"Executing token query for transcript ID: {transcript_id}...")
            with open("../queries/token_query.sql", "r") as f:
                token_query = f.read()

            start_time = time.time()
            cur.execute(token_query, (transcript_id,))
            tokens = cur.fetchall()
            end_time = time.time()
            print(
                f"✅ Token query finished in {end_time - start_time:.2f} seconds. Fetched {len(tokens)} tokens."
            )

            # Re-format tokens into a list of dictionaries
            columns = [desc[0] for desc in cur.description]
            tokens_as_dict = [dict(zip(columns, row)) for row in tokens]

            if not tokens_as_dict:
                return [], [], {}

            print(f"Executing tokenset query for all token IDs:...")
            with open("../queries/tokenset-fetch-query.sql", "r") as f:
                token_query = f.read()

            token_ids = list(token["token_id"] for token in tokens_as_dict)
            start_time = time.time()
            cur.execute(token_query, (token_ids,))
            tokenset_ids = cur.fetchall()
            end_time = time.time()
            print(
                f"✅ Tokenset query finished in {end_time - start_time:.2f} seconds. Fetched {len(tokens)} tokensets."
            )

            tokenset_ids_list = [row[0] for row in tokenset_ids]
            print(f"Executing tokenset answer query for all tokenset IDs:...")
            with open("../queries/tokenset-query.sql", "r") as f:
                tokenset_query_answers = f.read()
            start_time = time.time()
            cur.execute(tokenset_query_answers, (tokenset_ids_list,))
            tokensetquery_answers = cur.fetchall()
            end_time = time.time()
            print(
                f"✅ Tokenset query finished in {end_time - start_time:.2f} seconds. Fetched {len(tokens)} tokensets."
            )

            # --- Process Tokensets ---
            tokenset_definitions = {}
            token_to_tokensets = {}
            for row in tokensetquery_answers:
                # Row: ist_tokenset_id, tag_reihung, tag_name, tag_id, tag, tag_gene, token_id
                ts_id = row[0]
                tag_name = row[2]
                token_id = row[6]

                if ts_id not in tokenset_definitions:
                    tokenset_definitions[ts_id] = set()
                if tag_name:
                    tokenset_definitions[ts_id].add(tag_name)

                if token_id not in token_to_tokensets:
                    token_to_tokensets[token_id] = set()
                token_to_tokensets[token_id].add(ts_id)

            # Convert sets to sorted lists and keys to strings for consistency
            tokenset_definitions_serializable = {
                str(k): sorted(list(v)) for k, v in tokenset_definitions.items()
            }

            # Extract unique informant IDs
            unique_informant_ids = sorted(
                list(set(token["ID_Inf_id"] for token in tokens_as_dict))
            )
            print(f"Found unique informant IDs: {unique_informant_ids}")

            token_ids = [token["token_id"] for token in tokens_as_dict]
            # --- Query 2: Fetch Answers and Tags in Batches ---
            print("Executing answers and tags query in batches...")
            with open("../queries/tags_and_answers_query.sql", "r") as f:
                answers_query = f.read()

            BATCH_SIZE = 1000
            all_answers_data = []
            total_query_time = 0

            for i in range(0, 1000, BATCH_SIZE):
                batch_ids = token_ids[i : i + BATCH_SIZE]
                print(
                    f"  - Processing batch {i // BATCH_SIZE + 1}/{(len(token_ids) + BATCH_SIZE - 1) // BATCH_SIZE}..."
                )

                start_batch_time = time.time()
                cur.execute(answers_query, (batch_ids,))
                batch_answers = cur.fetchall()
                all_answers_data.extend(batch_answers)
                end_batch_time = time.time()

                batch_time = end_batch_time - start_batch_time
                total_query_time += batch_time
                print(f"Batch finished in {batch_time:.2f} seconds.")

            print(
                f"✅ All answer batches finished in {total_query_time:.2f} seconds. Fetched {len(all_answers_data)} answer sets in total."
            )

            # Create a dictionary for quick lookup of answers by token_id
            answers_map = {}
            for row in all_answers_data:
                token_id = row[0]
                answer_data = {
                    "tag_reihung": row[1],
                    "tag_name": row[2],
                    "tag_id": row[3],
                    "tag": row[4],
                    "tag_gene": row[5],
                }
                if token_id not in answers_map:
                    answers_map[token_id] = []
                answers_map[token_id].append(answer_data)

            # --- Merge Data ---
            print("Merging token and answer data...")
            for token in tokens_as_dict:
                token["tags"] = answers_map.get(token["token_id"], [])
                token["tokenset_ids"] = sorted(
                    list(token_to_tokensets.get(token["token_id"], []))
                )
                transcript_data.append(token)

            # --- Caching logic ---
            if use_cache:
                print(f"💾 Caching transcript data to: {cache_file}")
                data_to_cache = {
                    "transcript_data": transcript_data,
                    "unique_informant_ids": unique_informant_ids,
                    "tokenset_definitions": tokenset_definitions_serializable,
                }
                with open(cache_file, "w") as f:
                    json.dump(data_to_cache, f, cls=CustomEncoder)

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"❌ Database error: {error}")
        return None, None, None
    finally:
        if conn:
            conn.close()

    return transcript_data, unique_informant_ids, tokenset_definitions_serializable


def fetch_informants_data(informant_ids):
    """
    Fetches informant details from the database for a list of informant IDs.
    Processes the 'is_female' boolean into a 'gender' string.
    """
    conn = get_db_connection()
    if not conn:
        return None

    informants = []
    try:
        with conn.cursor() as cur:
            print(f"Executing informant query for IDs: {informant_ids}...")
            with open("../queries/informant_query.sql", "r") as f:
                informant_query = f.read()

            start_time = time.time()
            # Pass a tuple with a list for the ANY clause
            cur.execute(informant_query, (informant_ids,))
            informant_rows = cur.fetchall()
            end_time = time.time()
            print(
                f"✅ Informant query finished in {end_time - start_time:.2f} seconds. Fetched {len(informant_rows)} informants."
            )

            columns = [desc[0] for desc in cur.description]
            for row in informant_rows:
                informant_dict = dict(zip(columns, row))

                # Process 'is_female' to 'gender' string
                is_female = informant_dict.pop("is_female", None)
                if is_female is True:
                    informant_dict["gender"] = "female"
                elif is_female is False:
                    informant_dict["gender"] = "male"
                else:
                    informant_dict["gender"] = "not provided"

                # Map inf_id to 'id' and inf_sigle to 'sigle'
                informant_dict["id"] = informant_dict.pop("inf_id")
                informant_dict["sigle"] = informant_dict.pop("sigle")

                informants.append(informant_dict)

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"❌ Database error during informant fetch: {error}")
        return None
    finally:
        if conn:
            conn.close()

    return informants


def generate_standoff_informants_file(
    informants_data, output_filename="standoff_informants.xml"
):
    """
    Generates the standoff personography file from a list of informant dictionaries.
    """
    if not informants_data:
        print("Warning: No informant data to process for standoff file.")
        return

    personography_root = etree.Element("TEI", nsmap=NS_MAP)
    tei_header = etree.SubElement(personography_root, "teiHeader")
    file_desc = etree.SubElement(tei_header, "fileDesc")
    title_stmt = etree.SubElement(file_desc, "titleStmt")
    etree.SubElement(title_stmt, "title").text = "Standoff Personography for Project"
    pub_stmt = etree.SubElement(file_desc, "publicationStmt")
    etree.SubElement(pub_stmt, "p").text = "Not for publication."
    source_desc = etree.SubElement(file_desc, "sourceDesc")
    etree.SubElement(source_desc, "p").text = (
        "Data derived from project's informant database."
    )
    standoff = etree.SubElement(personography_root, "standOff")
    list_person = etree.SubElement(standoff, "listPerson")
    list_person.set(f"{{{XML_NS}}}id", "project_informants")

    for informant in informants_data:
        person = etree.SubElement(list_person, "person")
        person.set(f"{{{XML_NS}}}id", f'spk_{informant["id"]}')
        etree.SubElement(person, "persName").text = f"Informant {informant['sigle']}"
        etree.SubElement(person, "sex").set(
            "value", str(informant.get("gender", "unknown"))
        )
        if informant.get("age_group"):
            etree.SubElement(person, "age").text = informant.get("age_group")
        if informant.get("comment"):
            etree.SubElement(etree.SubElement(person, "note"), "p").text = (
                informant.get("comment")
            )
    tree = etree.ElementTree(personography_root)
    tree.write(
        output_filename, pretty_print=True, xml_declaration=True, encoding="UTF-8"
    )
    print(f"✅ Generated standoff file: {output_filename}")


def load_dioe_tags(filepath):
    """
    Parses the dioe-tags.tei.xml file with strict namespace handling.
    Expects root: <TEI xmlns="http://www.tei-c.org/ns/1.0">
    """
    try:
        tree = etree.parse(filepath)
        ns = {"tei": TEI_NS}

        # Select 'f' elements specifically within the TEI namespace
        elements = tree.xpath("//tei:f[@name]", namespaces=ns)

        # Fallback: if no namespaced elements found, try without namespace
        if not elements:
            print(
                f"⚠️  No namespaced 'f' elements found in {filepath}. Trying without namespace."
            )
            elements = tree.xpath("//f[@name]")

        tag_names = {f_el.get("name").lower() for f_el in elements if f_el.get("name")}

        print(f"✅ Loaded {len(tag_names)} unique DiÖ tag names from {filepath}")
        return tag_names
    except etree.XMLSyntaxError as e:
        print(f"❌ Error parsing dioe-tags.tei.xml: {e}")
        return set()
    except IOError as e:
        print(f"❌ Error reading dioe-tags.tei.xml: {e}")
        return set()


def generate_transcript_file(
    transcript_data,
    standoff_filename,
    tokenset_definitions,
    output_filename="transcript_0239.xml",
):
    """
    Creates the main TEI transcript file from the data fetched from the DB.
    """
    if not transcript_data:
        print("Warning: No transcript data to process.")
        return

    # Load the valid DiÖ tag names into a set for efficient, case-insensitive lookup
    dioe_tag_names = load_dioe_tags("../dioe-tags.tei.xml")

    def time_to_seconds(t):
        if isinstance(t, timedelta):
            return t.total_seconds()
        if isinstance(t, str):
            try:
                # Assuming format like "0:00:01.500000" or similar from str(timedelta)
                parts = t.split(":")
                if len(parts) == 3:
                    return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
            except (ValueError, IndexError):
                pass
        return 0.0

    # Sort tokens by their order in the transcript
    transcript_data.sort(
        key=lambda x: (
            time_to_seconds(x.get("start_time")),
            x.get("token_reihung") or 0,
            x.get("ID_Inf_id") or 0,
        )
    )

    transcript_name = (
        f"Transcript_{transcript_data[0].get('transcript_id_id', 'Unknown')}"
    )

    # --- NEW: Collect unique timestamps for timeline ---
    all_times = set()
    for token in transcript_data:
        st = token.get("start_time")
        et = token.get("end_time")
        if st is not None:
            all_times.add(st)
        if et is not None:
            all_times.add(et)

    sorted_times = sorted(list(all_times), key=time_to_seconds)
    time_to_id = {t: f"TL_{i}" for i, t in enumerate(sorted_times)}

    # Group tokens into utterances based on speaker changes
    utterances = []
    if transcript_data:
        current_utterance = {"speaker": transcript_data[0]["ID_Inf_id"], "tokens": []}
        for token in transcript_data:
            if token["ID_Inf_id"] != current_utterance["speaker"]:
                if current_utterance["tokens"]:
                    utterances.append(current_utterance)
                current_utterance = {"speaker": token["ID_Inf_id"], "tokens": []}
            current_utterance["tokens"].append(token)
        if current_utterance["tokens"]:
            utterances.append(current_utterance)

    # --- Build TEI XML Structure ---
    tei_root = etree.Element("TEI", nsmap=NS_MAP)
    tei_header = etree.SubElement(tei_root, "teiHeader")
    file_desc = etree.SubElement(tei_header, "fileDesc")
    title_stmt = etree.SubElement(file_desc, "titleStmt")
    etree.SubElement(title_stmt, "title").text = f"Transcript: {transcript_name}"
    publication_stmt = etree.SubElement(file_desc, "publicationStmt")
    etree.SubElement(publication_stmt, "p").text = "Digital Humanities Project"
    source_desc = etree.SubElement(file_desc, "sourceDesc")
    xinclude_element = etree.SubElement(source_desc, f"{{{XINCLUDE_NS}}}include")
    xinclude_element.set("href", standoff_filename)
    xinclude_element.set("xpointer", "project_informants")

    standoff_fv_el = etree.SubElement(tei_root, "standOff")
    standoff_fv_el.set("type", "feature-declarations")

    # --- Add Tokenset Feature Structures ---
    if tokenset_definitions:
        for ts_id, tag_names in tokenset_definitions.items():
            validated_tags = []
            for tag_name in tag_names:
                if tag_name.lower() in dioe_tag_names:
                    validated_tags.append("#" + tag_name)
                else:
                    print(
                        f"⚠️  Tokenset Tag '{tag_name}' not found in dioe-tags.tei.xml and will be skipped."
                    )

            if validated_tags:
                fs_id = f"fs_tokenset_{ts_id}"
                fs_el = etree.SubElement(standoff_fv_el, "fs")
                fs_el.set(f"{{{XML_NS}}}id", fs_id)
                f_el = etree.SubElement(fs_el, "f")
                f_el.set("name", "dioe_tokenset_tags")
                fs_feats_el = etree.SubElement(f_el, "fs")
                fs_feats_el.set("feats", " ".join(validated_tags))

    # NEW: StandOff for timestamps with timeline
    standoff_time_el = etree.SubElement(tei_root, "standOff")
    standoff_time_el.set("type", "timestamps")
    timeline_el = etree.SubElement(standoff_time_el, "timeline")
    timeline_el.set("unit", "s")
    for t in sorted_times:
        when_el = etree.SubElement(timeline_el, "when")
        when_el.set(f"{{{XML_NS}}}id", time_to_id[t])
        when_el.set("absolute", str(t))

    text_el = etree.SubElement(tei_root, "text")
    body_el = etree.SubElement(text_el, "body")
    div_el = etree.SubElement(body_el, "div")

    for utterance in utterances:
        u = etree.SubElement(div_el, "u")
        u.set("who", f"#spk_{utterance['speaker']}")

        # Set start/end for u based on first and last token
        if utterance["tokens"]:
            u_start = utterance["tokens"][0].get("start_time")
            u_end = utterance["tokens"][-1].get("end_time")
            if u_start is not None:
                u.set("start", f"#{time_to_id[u_start]}")
            if u_end is not None:
                u.set("end", f"#{time_to_id[u_end]}")

        for i, token in enumerate(utterance["tokens"]):
            token_text = token.get("text_in_ortho") or token.get("ortho") or ""
            el = None

            # Handle special token types like pauses or incidents
            if token_text.startswith("((") and token_text.endswith("))"):
                content = token_text[2:-2]
                if "s" in content:
                    el = etree.SubElement(u, "pause")
                    el.set("duration", content)
                else:
                    el = etree.SubElement(u, "incident")
                    etree.SubElement(el, "desc").text = content
            elif token_text == "(?)":
                el = etree.SubElement(u, "unclear")
            # Handle punctuation vs. words
            elif token.get("sppos") == "PUNCT":
                el = etree.SubElement(u, "pc")
                el.set(f"{{{XML_NS}}}id", f"t{token['token_id']}")
                el.text = token_text
            else:
                el = etree.SubElement(u, "w")
                el.set(f"{{{XML_NS}}}id", f"t{token['token_id']}")
                if token.get("splemma"):
                    el.set(
                        "lemma",
                        str(
                            token["splemma"][0]
                            if isinstance(token["splemma"], list)
                            else token["splemma"]
                        ),
                    )
                if token.get("sppos"):
                    el.set("type", token["sppos"])
                el.text = token_text.strip()
                # --- NEW SIMPLIFIED DiÖ TAG HANDLING ---
                ana_refs = []
                if el is not None and el.tag == "w":
                    # 1. Handle Single Token Tags
                    if token.get("tags"):
                        validated_tags = []
                        for tag_info in token["tags"]:
                            tag_name = tag_info.get("tag_name")
                            if tag_name:
                                # Case-insensitive check against the loaded tags
                                if tag_name.lower() in dioe_tag_names:
                                    validated_tags.append("#" + tag_name)
                                else:
                                    print(
                                        f"⚠️  Tag '{tag_name}' not found in dioe-tags.xml and will be skipped."
                                    )
                        if validated_tags:
                            # Create a feature structure for the DiÖ tags
                            fs_id = f"fs_{token['token_id']}_dioe"
                            fs_el = etree.SubElement(standoff_fv_el, "fs")
                            fs_el.set(f"{{{XML_NS}}}id", fs_id)

                            f_el = etree.SubElement(fs_el, "f")
                            f_el.set("name", "dioe_tags")

                            # The fs inside the f contains the feats
                            fs_feats_el = etree.SubElement(f_el, "fs")
                            fs_feats_el.set("feats", " ".join(validated_tags))

                            ana_refs.append(f"#{fs_id}")

                    # 2. Handle Tokenset Tags
                    if token.get("tokenset_ids"):
                        for ts_id in token["tokenset_ids"]:
                            if str(ts_id) in tokenset_definitions:
                                ana_refs.append(f"#fs_tokenset_{ts_id}")

                    if ana_refs:
                        el.set("ana", " ".join(ana_refs))

            # --- Timestamp Handling ---
            if el is not None:
                start_t = token.get("start_time")
                end_t = token.get("end_time")
                if start_t is not None:
                    el.set("start", f"#{time_to_id[start_t]}")
                if end_t is not None:
                    el.set("end", f"#{time_to_id[end_t]}")

            # Add trailing space after each element for readability
            if el is not None and i < len(utterance["tokens"]) - 1:
                el.tail = " "

    # Write the final XML to a file
    tree = etree.ElementTree(tei_root)
    tree.write(
        output_filename, pretty_print=True, xml_declaration=True, encoding="UTF-8"
    )
    print(f"✅ Generated transcript file: {output_filename}")


# --- Main Execution Block ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate TEI XML files from DiÖ-DB for a single transcript."
    )
    parser.add_argument(
        "transcript_id", type=int, help="The numeric ID of the transcript to process."
    )
    parser.add_argument(
        "--use-cache",
        action="store_true",
        help="Enable caching of the database queries to a JSON file.",
    )
    parser.add_argument(
        "--cache-dir",
        type=str,
        default="cache",
        help="The directory to store cache files. Default: 'cache' in the script's directory.",
    )
    args = parser.parse_args()

    # Make cache dir relative to script location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    cache_path = os.path.join(script_dir, args.cache_dir)

    # 1. DEFINE THE TRANSCRIPT ID YOU WANT to PROCESS
    TRANSCRIPT_ID_TO_PROCESS = args.transcript_id

    standoff_output_file = "standoff_informants.xml"
    transcript_output_file = f"transcript_{TRANSCRIPT_ID_TO_PROCESS}.xml"

    # 2. FETCH DATA FROM DATABASE OR CACHE
    transcript_data, unique_informant_ids, tokenset_definitions = fetch_transcript_data(
        TRANSCRIPT_ID_TO_PROCESS, args.use_cache, cache_path
    )

    # 3. FETCH INFORMANT DATA
    informants_data = []
    if unique_informant_ids:
        informants_data = fetch_informants_data(unique_informant_ids)

    # 4. GENERATE THE FILES
    if transcript_data is not None and informants_data is not None:
        generate_standoff_informants_file(informants_data, standoff_output_file)
        generate_transcript_file(
            transcript_data,
            standoff_output_file,
            tokenset_definitions,
            transcript_output_file,
        )
        print(f"\nProcess complete.")
    else:
        print("\nProcess failed due to data fetching errors.")
