import requests
import xml.etree.ElementTree as ET
import json
import csv
from typing import Dict, List, Optional
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ChangesetObjectExtractor:
    def __init__(self, output_dir: str = "./output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.objects_file = self.output_dir / "objects.jsonl"
        self.queue_file = self.output_dir / "fetch_prev_queue.csv"

    def load_changeset_ids(self, filepath: str) -> List[int]:
        path = Path(filepath)
        if not path.exists():
            raise FileNotFoundError(f"Changeset list file not found: {path}")

        sample = path.read_text(encoding="utf-8-sig", errors="replace")[:4096]
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",\t;")
        except csv.Error:
            dialect = csv.excel_tab if path.suffix.lower() in [".tsv", ".tab"] else csv.excel

        try:
            has_header = csv.Sniffer().has_header(sample)
        except csv.Error:
            has_header = True

        ids: List[int] = []

        def normalize_header(s: str) -> str:
            return (s or "").strip().lower().replace(" ", "").replace("-", "").replace("_", "")

        with path.open("r", encoding="utf-8-sig", newline="") as f:
            if has_header:
                reader = csv.DictReader(f, dialect=dialect)
                if not reader.fieldnames:
                    return []

                fields = reader.fieldnames
                normalized = {normalize_header(h): h for h in fields}
                candidates = [
                    "changesetid", "changeset", "id", "changeset_id", "changeset-id", "changeset id"
                ]
                id_key = None
                for c in candidates:
                    k = normalize_header(c)
                    if k in normalized:
                        id_key = normalized[k]
                        break

                if id_key is None:
                    id_key = fields[0]

                for row in reader:
                    raw = (row.get(id_key) or "").strip()
                    if not raw:
                        continue
                    try:
                        ids.append(int(raw))
                    except ValueError:
                        continue
            else:
                reader = csv.reader(f, dialect=dialect)
                for row in reader:
                    if not row:
                        continue
                    raw = (row[0] or "").strip()
                    if not raw:
                        continue
                    try:
                        ids.append(int(raw))
                    except ValueError:
                        continue

        deduped = list(dict.fromkeys(ids))
        logger.info(f"Loaded {len(deduped)} changeset ids from {path}")
        return deduped

    def download_changeset(self, changeset_id: int) -> Optional[str]:
        url = f"https://api.openstreetmap.org/api/0.6/changeset/{changeset_id}/download"
        try:
            logger.info(f"Downloading changeset {changeset_id}...")
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.error(f"Failed to download changeset {changeset_id}: {e}")
            return None

    def parse_node(self, node_elem: ET.Element, action: str, changeset_id: int) -> Dict:
        obj = {
            "changeset_id": changeset_id,
            "action": action,
            "obj_type": "node",
            "obj_id": int(node_elem.get("id")),
            "version": int(node_elem.get("version")),
            "timestamp": node_elem.get("timestamp"),
            "visible": node_elem.get("visible", "true") == "true",
            "user": node_elem.get("user"),
            "uid": int(node_elem.get("uid")) if node_elem.get("uid") else None
        }

        if node_elem.get("lat") and node_elem.get("lon"):
            obj["geom"] = {
                "lat": float(node_elem.get("lat")),
                "lon": float(node_elem.get("lon"))
            }

        tags = {}
        for tag in node_elem.findall("tag"):
            tags[tag.get("k")] = tag.get("v")
        if tags:
            obj["tags"] = tags

        return obj

    def parse_way(self, way_elem: ET.Element, action: str, changeset_id: int) -> Dict:
        obj = {
            "changeset_id": changeset_id,
            "action": action,
            "obj_type": "way",
            "obj_id": int(way_elem.get("id")),
            "version": int(way_elem.get("version")),
            "timestamp": way_elem.get("timestamp"),
            "visible": way_elem.get("visible", "true") == "true",
            "user": way_elem.get("user"),
            "uid": int(way_elem.get("uid")) if way_elem.get("uid") else None
        }

        node_refs = [nd.get("ref") for nd in way_elem.findall("nd")]
        if node_refs:
            obj["refs"] = {"node_refs": node_refs}

        tags = {}
        for tag in way_elem.findall("tag"):
            tags[tag.get("k")] = tag.get("v")
        if tags:
            obj["tags"] = tags

        return obj

    def parse_relation(self, rel_elem: ET.Element, action: str, changeset_id: int) -> Dict:
        obj = {
            "changeset_id": changeset_id,
            "action": action,
            "obj_type": "relation",
            "obj_id": int(rel_elem.get("id")),
            "version": int(rel_elem.get("version")),
            "timestamp": rel_elem.get("timestamp"),
            "visible": rel_elem.get("visible", "true") == "true",
            "user": rel_elem.get("user"),
            "uid": int(rel_elem.get("uid")) if rel_elem.get("uid") else None
        }

        members = []
        for member in rel_elem.findall("member"):
            members.append({
                "type": member.get("type"),
                "ref": member.get("ref"),
                "role": member.get("role", "")
            })
        if members:
            obj["refs"] = {"members": members}

        tags = {}
        for tag in rel_elem.findall("tag"):
            tags[tag.get("k")] = tag.get("v")
        if tags:
            obj["tags"] = tags

        return obj

    def extract_objects(self, changeset_id: int) -> List[Dict]:
        xml_data = self.download_changeset(changeset_id)
        if not xml_data:
            return []

        try:
            root = ET.fromstring(xml_data)
        except ET.ParseError as e:
            logger.error(f"Failed to parse XML for changeset {changeset_id}: {e}")
            return []

        objects = []

        for action in ["create", "modify", "delete"]:
            for action_elem in root.findall(action):
                for node in action_elem.findall("node"):
                    objects.append(self.parse_node(node, action, changeset_id))

                for way in action_elem.findall("way"):
                    objects.append(self.parse_way(way, action, changeset_id))

                for relation in action_elem.findall("relation"):
                    objects.append(self.parse_relation(relation, action, changeset_id))

        logger.info(f"Extracted {len(objects)} objects from changeset {changeset_id}")
        return objects

    def save_objects(self, objects: List[Dict]):
        with open(self.objects_file, 'a', encoding='utf-8') as f:
            for obj in objects:
                f.write(json.dumps(obj, ensure_ascii=False) + '\n')
        logger.info(f"Saved {len(objects)} objects to {self.objects_file}")

    def generate_fetch_queue(self):
        if not self.objects_file.exists():
            logger.warning(f"{self.objects_file} not found")
            return

        queue_items = []

        with open(self.objects_file, 'r', encoding='utf-8') as f:
            for line in f:
                obj = json.loads(line)

                if obj["action"] == "modify" and obj["version"] > 1:
                    prev_version = obj["version"] - 1
                    url = f"https://api.openstreetmap.org/api/0.6/{obj['obj_type']}/{obj['obj_id']}/{prev_version}"

                    queue_items.append({
                        "changeset_id": obj["changeset_id"],
                        "action": obj["action"],
                        "obj_type": obj["obj_type"],
                        "obj_id": obj["obj_id"],
                        "cur_version": obj["version"],
                        "prev_version": prev_version,
                        "reason": "need_before_state",
                        "url": url
                    })

        if queue_items:
            with open(self.queue_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=[
                    "changeset_id", "action", "obj_type", "obj_id",
                    "cur_version", "prev_version", "reason", "url"
                ])
                writer.writeheader()
                writer.writerows(queue_items)
            logger.info(f"Generated fetch queue with {len(queue_items)} items at {self.queue_file}")
        else:
            logger.info("No objects require previous version fetch")

    def process_changesets(self, changeset_ids: List[int]):
        if self.objects_file.exists():
            self.objects_file.unlink()

        total_objects = 0

        for cs_id in changeset_ids:
            objects = self.extract_objects(cs_id)
            if objects:
                self.save_objects(objects)
                total_objects += len(objects)

        logger.info(f"Total {total_objects} objects extracted from {len(changeset_ids)} changesets")
        self.generate_fetch_queue()


def main():
    changeset_list_path = "./test-data/changesets.csv"

    extractor = ChangesetObjectExtractor(output_dir="./output")

    all_changeset_ids = extractor.load_changeset_ids(changeset_list_path)
    changeset_ids = all_changeset_ids[:10]

    logger.info(f"Processing only {len(changeset_ids)} changesets (sample run)")

    extractor.process_changesets(changeset_ids)

    print("\n처리 완료")
    print(f"- objects.jsonl: {extractor.objects_file}")
    print(f"- fetch_prev_queue.csv: {extractor.queue_file}")


if __name__ == "__main__":
    main()