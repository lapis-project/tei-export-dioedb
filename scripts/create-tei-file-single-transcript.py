import json
from lxml import etree
import os

# --- Define XML Namespaces (No changes here) ---
TEI_NS = "http://www.tei-c.org/ns/1.0"
XML_NS = "http://www.w3.org/XML/1998/namespace"
XINCLUDE_NS = "http://www.w3.org/2001/XInclude"
NS_MAP = {None: TEI_NS, "xml": XML_NS, "xi": XINCLUDE_NS}


# This function remains the same, assuming you also create informants.json
def generate_standoff_informants_file(
    informants_json_content, output_filename="standoff_informants.xml"
):
    try:
        data = json.loads(informants_json_content)
        informants = data.get("informants", [])
    except json.JSONDecodeError:
        print("Error: Invalid JSON data for informants.")
        return
    # (The rest of this function is unchanged)
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
    for informant in informants:
        person = etree.SubElement(list_person, "person")
        person.set(f"{{{XML_NS}}}id", f"spk_{informant['id']}")
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


# --- MODIFIED FUNCTION ---
def generate_transcript_file(
    transcript_filepath, standoff_filename, output_filename="transcript_0239.xml"
):
    """
    Creates the main TEI transcript file that links to a standoff file.
    This version loads the transcript data from a specified file path.
    """
    # ** NEW: Load transcript data from the JSON file **
    try:
        with open(transcript_filepath, "r", encoding="utf-8") as f:
            # Use json.load() to read directly from the file object
            data = json.load(f)
        transcript_data = data.get("queryresult", [])
    except FileNotFoundError:
        print(f"❌ Error: The transcript file was not found at '{transcript_filepath}'")
        return
    except json.JSONDecodeError:
        print(
            f"❌ Error: The file '{transcript_filepath}' is not a valid JSON file. Please check its content."
        )
        return

    # (The rest of the function logic is unchanged)
    transcript_data.sort(key=lambda x: x.get("reihung") or 0)
    if not transcript_data:
        print("Warning: No transcript data to process.")
        return
    transcript_name = transcript_data[0].get("transcriptname", "UnknownTranscript")
    utterances = []
    if transcript_data:
        current_utterance = {"speaker": transcript_data[0]["informantid"], "tokens": []}
        for token in transcript_data:
            if token["informantid"] != current_utterance["speaker"]:
                if current_utterance["tokens"]:
                    utterances.append(current_utterance)
                current_utterance = {"speaker": token["informantid"], "tokens": []}
            current_utterance["tokens"].append(token)
        if current_utterance["tokens"]:
            utterances.append(current_utterance)
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
    text_el = etree.SubElement(tei_root, "text")
    body_el = etree.SubElement(text_el, "body")
    div_el = etree.SubElement(body_el, "div")
    for utterance in utterances:
        u = etree.SubElement(div_el, "u")
        u.set("who", f"#spk_{utterance['speaker']}")
        for i, token in enumerate(utterance["tokens"]):
            token_text = token.get("text_ortho") or token.get("ortho") or ""
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
            elif token.get("sppos") == "PUNCT":
                el = etree.SubElement(u, "pc")
                el.set(f"{{{XML_NS}}}id", f"t{token['tokenid']}")
                el.text = token_text
            else:
                el = etree.SubElement(u, "w")
                el.set(f"{{{XML_NS}}}id", f"t{token['tokenid']}")
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
                if token.get("sptag"):
                    el.set("ana", f"#{token['sptag']}")
                el.text = (
                    token.get("text").strip()
                    if token.get("text", "").strip()
                    else token_text
                )
            if i < len(utterance["tokens"]) - 1:
                el.tail = " "
    tree = etree.ElementTree(tei_root)
    tree.write(
        output_filename, pretty_print=True, xml_declaration=True, encoding="UTF-8"
    )
    print(f"✅ Generated transcript file: {output_filename}")


# --- Main Execution Block ---

# 1. DEFINE YOUR FILE PATHS
#    ** This is the section you will edit. **
transcript_file_to_load = "sample-transcript.json"
informants_data_string = """
{
  "informants": [
    {"id": 164, "sigle": "0239", "gender": "1", "age_group": "young"},
    {"id": 165, "sigle": "0240", "gender": "2", "age_group": "young"}
  ]
}
"""
standoff_output_file = "standoff_informants.xml"
transcript_output_file = "transcript_0239.xml"

# 2. GENERATE THE FILES
# Generate the standoff file from the string (or you can load it from a file too)
generate_standoff_informants_file(informants_data_string, standoff_output_file)

# Generate the transcript file by passing the FILENAME
generate_transcript_file(
    transcript_file_to_load, standoff_output_file, transcript_output_file
)

print(f"\nProcess complete.")
