import requests
import xml.etree.ElementTree as ET
import json
import csv
from typing import Dict, Optional, Set, Tuple
from pathlib import Path
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# OSM 객체의 이전 버전 정보를 추출하는 클래스
class ObjectVersionExtractor:
    def __init__(self, input_dir: str = "./output", output_dir: str = "./output"):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        self.queue_file = self.input_dir / "fetch_prev_queue.csv"
        self.versions_file = self.output_dir / "object_versions.jsonl"

        # 누적 실행을 위한 처리 완료 버전 기록 파일
        self.processed_file = self.output_dir / "processed_versions.txt"

        self.request_delay = 0.5  

    def _make_key(self, obj_type: str, obj_id: int, version: int) -> Tuple[str, int, int]:
        return (obj_type, int(obj_id), int(version))

    def _load_processed_versions(self) -> Set[Tuple[str, int, int]]:
        if not self.processed_file.exists():
            return set()

        processed: Set[Tuple[str, int, int]] = set()
        with self.processed_file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = [p.strip() for p in line.split(",")]
                if len(parts) != 3:
                    continue
                obj_type, obj_id_s, ver_s = parts
                try:
                    processed.add((obj_type, int(obj_id_s), int(ver_s)))
                except ValueError:
                    continue
        return processed

    def _mark_processed(self, key: Tuple[str, int, int]) -> None:
        obj_type, obj_id, version = key
        with self.processed_file.open("a", encoding="utf-8") as f:
            f.write(f"{obj_type},{obj_id},{version}\n")

    def _load_existing_keys_from_versions_file(self) -> Set[Tuple[str, int, int]]:
        if not self.versions_file.exists():
            return set()

        keys: Set[Tuple[str, int, int]] = set()
        with self.versions_file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue

                obj_type = obj.get("obj_type")
                obj_id = obj.get("obj_id")
                version = obj.get("version")
                if obj_type is None or obj_id is None or version is None:
                    continue

                try:
                    keys.add((str(obj_type), int(obj_id), int(version)))
                except ValueError:
                    continue
        return keys

    # API 호출/파싱 로직 
    def fetch_object_version(self, obj_type: str, obj_id: int, version: int) -> Optional[str]:
        url = f"https://api.openstreetmap.org/api/0.6/{obj_type}/{obj_id}/{version}"

        try:
            logger.info(f"Fetching {obj_type}/{obj_id}/v{version}...")
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            # Rate limiting
            time.sleep(self.request_delay)

            return response.text
        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            if status == 404:
                logger.warning(f"{obj_type}/{obj_id}/v{version} not found (404)")
            elif status == 410:
                logger.warning(f"{obj_type}/{obj_id}/v{version} deleted (410)")
            else:
                logger.error(f"HTTP error fetching {obj_type}/{obj_id}/v{version}: {e}")
            return None
        except requests.RequestException as e:
            logger.error(f"Failed to fetch {obj_type}/{obj_id}/v{version}: {e}")
            return None

    def parse_node_version(self, node_elem: ET.Element) -> Dict:
        obj = {
            "obj_type": "node",
            "obj_id": int(node_elem.get("id")),
            "version": int(node_elem.get("version")),
            "timestamp": node_elem.get("timestamp"),
            "source": "prev_version_api",
        }

        if node_elem.get("lat") and node_elem.get("lon"):
            obj["geom"] = {
                "lat": float(node_elem.get("lat")),
                "lon": float(node_elem.get("lon")),
            }

        tags = {}
        for tag in node_elem.findall("tag"):
            tags[tag.get("k")] = tag.get("v")
        if tags:
            obj["tags"] = tags

        return obj

    def parse_way_version(self, way_elem: ET.Element) -> Dict:
        obj = {
            "obj_type": "way",
            "obj_id": int(way_elem.get("id")),
            "version": int(way_elem.get("version")),
            "timestamp": way_elem.get("timestamp"),
            "source": "prev_version_api",
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

    def parse_relation_version(self, rel_elem: ET.Element) -> Dict:
        obj = {
            "obj_type": "relation",
            "obj_id": int(rel_elem.get("id")),
            "version": int(rel_elem.get("version")),
            "timestamp": rel_elem.get("timestamp"),
            "source": "prev_version_api",
        }

        members = []
        for member in rel_elem.findall("member"):
            members.append(
                {
                    "type": member.get("type"),
                    "ref": member.get("ref"),
                    "role": member.get("role", ""),
                }
            )
        if members:
            obj["refs"] = {"members": members}

        tags = {}
        for tag in rel_elem.findall("tag"):
            tags[tag.get("k")] = tag.get("v")
        if tags:
            obj["tags"] = tags

        return obj

    def extract_version_info(self, xml_data: str, obj_type: str) -> Optional[Dict]:
        try:
            root = ET.fromstring(xml_data)
        except ET.ParseError as e:
            logger.error(f"Failed to parse XML: {e}")
            return None

        if obj_type == "node":
            elem = root.find("node")
            if elem is not None:
                return self.parse_node_version(elem)
        elif obj_type == "way":
            elem = root.find("way")
            if elem is not None:
                return self.parse_way_version(elem)
        elif obj_type == "relation":
            elem = root.find("relation")
            if elem is not None:
                return self.parse_relation_version(elem)

        logger.warning(f"No {obj_type} element found in XML")
        return None

    def save_version(self, version_obj: Dict):
        with self.versions_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(version_obj, ensure_ascii=False) + "\n")


    def process_queue(self, overwrite: bool = False):
        if not self.queue_file.exists():
            logger.error(f"{self.queue_file} not found")
            return

        # overwrite 모드면 누적 파일 초기화
        if overwrite:
            if self.versions_file.exists():
                self.versions_file.unlink()
            if self.processed_file.exists():
                self.processed_file.unlink()

        processed = self._load_processed_versions()
        processed |= self._load_existing_keys_from_versions_file()

        total_items = 0
        success_count = 0
        fail_count = 0
        skipped = 0

        with self.queue_file.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                total_items += 1

                obj_type = row["obj_type"]
                obj_id = int(row["obj_id"])
                prev_version = int(row["prev_version"])

                key = self._make_key(obj_type, obj_id, prev_version)

                # 이미 수집된 건 스킵
                if key in processed:
                    skipped += 1
                    continue

                xml_data = self.fetch_object_version(obj_type, obj_id, prev_version)

                if xml_data:
                    version_obj = self.extract_version_info(xml_data, obj_type)

                    if version_obj:
                        self.save_version(version_obj)

                        # 성공했을 때만 처리 완료 기록 (재시도 가능하게)
                        self._mark_processed(key)
                        processed.add(key)

                        success_count += 1
                        logger.info(f"Saved {obj_type}/{obj_id}/v{prev_version}")
                    else:
                        fail_count += 1
                        logger.warning(f"Failed to parse {obj_type}/{obj_id}/v{prev_version}")
                else:
                    fail_count += 1

        logger.info("\n" + "=" * 60)
        logger.info("Processing complete!")
        logger.info(f"Total items: {total_items}")
        logger.info(f"Skipped: {skipped}")
        logger.info(f"Success: {success_count}")
        logger.info(f"Failed: {fail_count}")
        logger.info(f"Output: {self.versions_file}")
        logger.info(f"Processed index: {self.processed_file}")
        logger.info("=" * 60)

    def set_rate_limit(self, requests_per_second: float):
        self.request_delay = 1.0 / requests_per_second
        logger.info(f"Rate limit set to {requests_per_second} requests/second")