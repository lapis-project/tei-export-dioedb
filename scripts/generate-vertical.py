import sys
import os
import glob
from lxml import etree

NS = {
    "tei": "http://www.tei-c.org/ns/1.0",
    "xml": "http://www.w3.org/XML/1998/namespace",
}


def load_speaker_data(standoff_path):
    """
    Parses the informant XML and returns a dict:
    {'spk_6': {'sex': 'male', 'age': 'jung (18-35)', 'name': '0025'}}
    """
    data = {}
    if not os.path.exists(standoff_path):
        print(f"Warning: Standoff file not found at {standoff_path}")
        return data

    try:
        tree = etree.parse(standoff_path)
        # Iterate over all person entries
        for person in tree.xpath("//tei:person", namespaces=NS):
            pid = person.get(f"{{{NS['xml']}}}id")
            if not pid:
                continue

            # Extract fields safely
            sex_node = person.xpath("tei:sex/@value", namespaces=NS)
            age_node = person.xpath("tei:age/text()", namespaces=NS)
            name_node = person.xpath("tei:persName/text()", namespaces=NS)

            data[pid] = {
                "sex": sex_node[0] if sex_node else "UNK",
                "age": age_node[0].strip() if age_node else "UNK",
                "name": name_node[0].strip() if name_node else "UNK",
            }
    except Exception as e:
        print(f"Error reading standoff file: {e}")

    return data


def get_standoff_definitions(root):
    """Parses <standOff> for token-level annotations (ana references)."""
    mapping = {}
    for fs in root.xpath("//tei:standOff//tei:fs[@xml:id]", namespaces=NS):
        fs_id = fs.get(f"{{{NS['xml']}}}id")
        nested_fs = fs.xpath(".//tei:fs[@feats]", namespaces=NS)
        if fs_id and nested_fs:
            mapping[fs_id] = nested_fs[0].get("feats")
    return mapping


def get_timeline_definitions(root):
    """
    Parses <timeline> to create a map: 'TL_1' -> '0:00:00.273'
    """
    timeline = {}
    # Look for the timeline tag
    for when in root.xpath("//tei:standOff//tei:timeline//tei:when", namespaces=NS):
        w_id = when.get(f"{{{NS['xml']}}}id")
        # You can choose 'absolute' (00:00:00) or 'interval' depending on your XML
        w_time = when.get("absolute")

        if w_id and w_time:
            timeline[w_id] = w_time
    return timeline


def convert_to_vertical(tei_dir, standoff_file, output_file):
    # 1. Load Global Speaker DB
    speaker_db = load_speaker_data(standoff_file)

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as f_out:
        f_out.write('<doc id="lapis_corpus">\n')

        files = glob.glob(os.path.join(tei_dir, "*.xml"))
        files.sort()

        for file_path in files:
            try:
                # Skip the standoff file itself if it's in the same folder
                if os.path.abspath(file_path) == os.path.abspath(standoff_file):
                    continue

                print(f"Processing: {file_path}...")
                tree = etree.parse(file_path)
                root = tree.getroot()

                # Metadata
                title_node = root.xpath(
                    "//tei:teiHeader//tei:title/text()", namespaces=NS
                )
                title = title_node[0] if title_node else os.path.basename(file_path)

                # Token Standoff Map
                token_map = get_standoff_definitions(root)
                # Timeline map
                timeline_map = get_timeline_definitions(root)

                f_out.write(
                    f'<file id="{os.path.basename(file_path)}" title="{title}">\n'
                )

                # Iterate Utterances
                for u in root.xpath("//tei:text//tei:u", namespaces=NS):
                    # IDs
                    who_ref = u.get("who", "").replace("#", "")
                    start_ref = u.get("start", "").replace("#", "")
                    end_ref = u.get("end", "").replace("#", "")

                    u_start = timeline_map.get(start_ref, start_ref)
                    u_end = timeline_map.get(end_ref, end_ref)

                    # --- ENRICHMENT LOOKUP ---
                    spk_info = speaker_db.get(
                        who_ref, {"sex": "UNK", "age": "UNK", "name": "UNK"}
                    )

                    # Clean attributes (remove tabs/newlines which break Vertical format)
                    s_sex = spk_info["sex"].replace("\t", " ")
                    s_age = spk_info["age"].replace("\t", " ")
                    s_name = spk_info["name"].replace("\t", " ")

                    # Write enriched structural tag
                    f_out.write(
                        f'<u who="{who_ref}" sex="{s_sex}" age="{s_age}" name="{s_name}" start="{u_start}" end="{u_end}">\n'
                    )

                    # Process Tokens (w, pc, pause)
                    for node in u:
                        tag_name = etree.QName(node).localname

                        if tag_name in ["w", "pc"]:
                            word = node.text.strip() if node.text else ""
                            if not word:
                                continue

                            lemma = node.get("lemma", "-")
                            if lemma == " ":
                                lemma = "-"
                            pos = node.get("type", "-")
                            ana = node.get("ana", "").replace("#", "")
                            feats = token_map.get(ana, "-")

                            t_start_ref = node.get("start", "").replace("#", "")
                            t_end_ref = node.get("end", "").replace("#", "")
                            t_start = timeline_map.get(t_start_ref, "-")
                            t_end = timeline_map.get(t_end_ref, "-")

                            f_out.write(
                                f"{word}\t{lemma}\t{pos}\t{feats}\t{t_start}\t{t_end}\n"
                            )

                        elif tag_name == "pause":
                            dur = node.get("duration", "")
                            f_out.write(f'<pause duration="{dur}"/>\n')

                    f_out.write("</u>\n")

                f_out.write("</file>\n")

            except Exception as e:
                print(f"Error {file_path}: {e}", file=sys.stderr)

        f_out.write("</doc>\n")


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print(
            "Usage: python tei_to_vert.py <tei_dir> <standoff_file.xml> <output.vert>"
        )
        sys.exit(1)

    convert_to_vertical(sys.argv[1], sys.argv[2], sys.argv[3])
