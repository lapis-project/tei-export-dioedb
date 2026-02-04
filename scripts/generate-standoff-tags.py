import json
import xml.etree.ElementTree as ET
from xml.dom import minidom
import re
import os


def parse_children_ids(children_str):
    """Parses the '{1,2,3}' string into a list of integers."""
    if not children_str:
        return []
    # Use regex to find all sequences of digits
    ids = re.findall(r"\d+", children_str)
    return [int(id_str) for id_str in ids]


def find_tags_by_id(tag_list, tag_id):
    """Finds all tag entries in the list that match the given tag_id."""
    return [tag for tag in tag_list if tag.get("tag_id") == tag_id]


def generate_tei_from_json(json_file_path, output_xml_path):
    """
    Loads tag data from a JSON file, processes it into a hierarchical
    TEI feature structure with unique IDs, and writes the result to an XML file.
    """
    try:
        with open(json_file_path, "r", encoding="utf-8") as f:
            all_tags = json.load(f)
            if not isinstance(all_tags, list):
                raise ValueError("JSON content is not a list.")
    except (json.JSONDecodeError, FileNotFoundError, ValueError) as e:
        print(f"Error: Could not read or parse '{json_file_path}'. {e}")
        return

    all_tags = all_tags[0]["tags"]
    # Map to store occurrence count for each tag_id to ensure unique xml:id
    tag_id_occurrence_map = {}

    def create_feature_element(parent_xml_element, tag_data, all_tags_list):
        """
        Recursively creates <f> elements, ensuring each has a unique xml:id
        based on its tag_id and occurrence count.
        """
        current_tag_id = tag_data.get("tag_id")
        if current_tag_id not in tag_id_occurrence_map:
            tag_id_occurrence_map[current_tag_id] = 1
        else:
            tag_id_occurrence_map[current_tag_id] += 1

        f_element = ET.SubElement(parent_xml_element, "f")
        f_element.set("name", tag_data.get("tag_abbrev", "UNKNOWN"))

        # Construct xml:id using tag_id and its occurrence count
        xml_id = f"tag-{current_tag_id}_{tag_id_occurrence_map[current_tag_id]}"
        f_element.set("xml:id", xml_id)

        string_element = ET.SubElement(f_element, "string")
        string_element.text = tag_data.get("tag_name", "Unnamed Tag")

        children_ids = parse_children_ids(tag_data.get("children_ids"))
        if children_ids:
            fs_for_children = ET.SubElement(f_element, "fs", {"type": "tag"})
            for child_id in children_ids:
                # Find all potential matches for the child ID
                potential_children = find_tags_by_id(all_tags_list, child_id)
                if not potential_children:
                    print(
                        f"Warning: Child tag with ID {child_id} not found for parent {tag_data.get('tag_id')}."
                    )
                    continue

                # Heuristic: Find the best match. Often the one with the same ebene_id.
                parent_ebene_id = tag_data.get("tag_ebene_id")
                best_match = [
                    c
                    for c in potential_children
                    if c.get("tag_ebene_id") == parent_ebene_id
                ]

                child_tag_data = best_match[0] if best_match else potential_children[0]

                create_feature_element(fs_for_children, child_tag_data, all_tags_list)

    # --- Setup TEI XML structure ---
    TEI_NAMESPACE = "http://www.tei-c.org/ns/1.0"
    ET.register_namespace("", TEI_NAMESPACE)
    root = ET.Element("TEI")

    tei_header = ET.SubElement(root, "teiHeader")
    file_desc = ET.SubElement(tei_header, "fileDesc")
    title_stmt = ET.SubElement(file_desc, "titleStmt")
    ET.SubElement(title_stmt, "title").text = (
        "Feature Structure Declaration for DiÖ Annotation Tags"
    )
    publication_stmt = ET.SubElement(file_desc, "publicationStmt")
    ET.SubElement(publication_stmt, "p").text = (
        f"Generated from {os.path.basename(json_file_path)}."
    )
    source_desc = ET.SubElement(file_desc, "sourceDesc")
    ET.SubElement(source_desc, "p").text = (
        f"Source data is the JSON file: {os.path.basename(json_file_path)}."
    )

    standoff = ET.SubElement(root, "standOff")
    fs_root = ET.SubElement(
        standoff, "fs", {"xml:id": "dioe-tags-features", "type": "feature-system"}
    )

    # Identify top-level tags (generation 0) and start the recursive build
    top_level_tags = [tag for tag in all_tags if tag.get("tag_gene") == 0]
    for top_level_tag in top_level_tags:
        create_feature_element(fs_root, top_level_tag, all_tags)

    # --- Write to file ---
    xml_string = ET.tostring(root, "unicode")
    dom = minidom.parseString(xml_string)
    pretty_xml_string = dom.toprettyxml(indent="  ")

    try:
        with open(output_xml_path, "w", encoding="utf-8") as f:
            f.write(pretty_xml_string)
        print(f"✅ Successfully generated TEI file at '{output_xml_path}'")
    except IOError as e:
        print(f"Error: Could not write to file '{output_xml_path}'. {e}")


if __name__ == "__main__":
    # Changed to dioe-tags-tree.json as it seems to be the intended source
    generate_tei_from_json("dioe-tags.json", "dioe-tags.tei.xml")
