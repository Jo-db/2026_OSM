import requests
import xml.etree.ElementTree as ET
import json
import csv
from typing import Dict, List, Optional, Set, Tuple
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ChangesetObjectExtractor:
    def __init__(self, output_dir: str = "./output"):
        # 출력 디렉토리 설정 및 생성
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        # 출력 파일 경로 설정
        self.objects_file = self.output_dir / "objects.jsonl"
        self.queue_file = self.output_dir / "fetch_prev_queue.csv"

        # 누적 실행을 위한 처리 완료 changeset 기록 파일
        self.processed_file = self.output_dir / "processed_changesets.txt"

    # 파일에서 changeset id 목록 로드
    def load_changeset_ids(self, path: Path) -> List[int]:
        # 파일 존재 확인
        if not path.exists():
            raise FileNotFoundError(f"Dataset file not found: {path}")

        # 확장자로 delimiter 결정
        delimiter = "\t" if path.suffix.lower() in [".tsv", ".tab"] else ","

        ids: List[int] = []

        # CSV 파일을 DictReader로 읽어서 'changeset' 컬럼에서 id 추출
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f, delimiter=delimiter)

            if not reader.fieldnames or "changeset" not in reader.fieldnames:
                raise ValueError(f"'changeset' column not found in {path}")

            for row in reader:
                raw = (row.get("changeset") or "").strip()
                if not raw:
                    continue
                try:
                    ids.append(int(raw))
                except ValueError:
                    continue

        # 중복 제거(순서 유지)
        return list(dict.fromkeys(ids))

    # 이미 처리된 changeset 목록 로드
    def _load_processed_changesets(self) -> Set[int]:
        if not self.processed_file.exists():
            return set()

        processed: Set[int] = set()
        with self.processed_file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    processed.add(int(line))
                except ValueError:
                    continue
        return processed

    # 처리 완료 changeset 기록(append)
    def _mark_processed(self, changeset_id: int) -> None:
        with self.processed_file.open("a", encoding="utf-8") as f:
            f.write(f"{changeset_id}\n")

    # changeset 다운로드
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

    # XML에서 node 요소 파싱
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
            "uid": int(node_elem.get("uid")) if node_elem.get("uid") else None,
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

    # XML에서 way 요소 파싱
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
            "uid": int(way_elem.get("uid")) if way_elem.get("uid") else None,
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

    # XML에서 relation 요소 파싱
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
            "uid": int(rel_elem.get("uid")) if rel_elem.get("uid") else None,
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

    # changeset에서 객체 추출
    def extract_objects(self, changeset_id: int) -> Optional[List[Dict]]:
        xml_data = self.download_changeset(changeset_id)
        if not xml_data:
            return None  # 다운로드 실패

        try:
            root = ET.fromstring(xml_data)
        except ET.ParseError as e:
            logger.error(f"Failed to parse XML for changeset {changeset_id}: {e}")
            return None  # 파싱 실패

        objects: List[Dict] = []

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

    # 추출된 객체를 JSONL 파일에 저장
    def save_objects(self, objects: List[Dict]):
        with self.objects_file.open("a", encoding="utf-8") as f:
            for obj in objects:
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")
        logger.info(f"Saved {len(objects)} objects to {self.objects_file}")

    # 이전 버전이 필요한 객체를 fetch queue에 추가
    def generate_fetch_queue(self):
        if not self.objects_file.exists():
            logger.warning(f"{self.objects_file} not found")
            return

        queue_items: List[Dict] = []
        seen: Set[Tuple[str, int, int]] = set()

        with self.objects_file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if obj.get("action") == "modify" and obj.get("version", 0) > 1:
                    prev_version = obj["version"] - 1

                    key = (obj.get("obj_type"), int(obj.get("obj_id")), int(prev_version))
                    if key in seen:
                        continue
                    seen.add(key)

                    url = f"https://api.openstreetmap.org/api/0.6/{obj['obj_type']}/{obj['obj_id']}/{prev_version}"

                    queue_items.append(
                        {
                            "changeset_id": obj["changeset_id"],
                            "action": obj["action"],
                            "obj_type": obj["obj_type"],
                            "obj_id": obj["obj_id"],
                            "cur_version": obj["version"],
                            "prev_version": prev_version,
                            "reason": "need_before_state",
                            "url": url,
                        }
                    )

        if queue_items:
            with self.queue_file.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "changeset_id",
                        "action",
                        "obj_type",
                        "obj_id",
                        "cur_version",
                        "prev_version",
                        "reason",
                        "url",
                    ],
                )
                writer.writeheader()
                writer.writerows(queue_items)
            logger.info(f"Generated fetch queue with {len(queue_items)} items at {self.queue_file}")
        else:
            logger.info("No objects require previous version fetch")

    # 이미 처리된 changeset 스킵 + 누적 저장
    def process_changesets(self, changeset_ids: List[int], overwrite: bool = False):
        # overwrite 모드면 누적 파일 초기화
        if overwrite:
            if self.objects_file.exists():
                self.objects_file.unlink()
            if self.queue_file.exists():
                self.queue_file.unlink()
            if self.processed_file.exists():
                self.processed_file.unlink()

        processed = self._load_processed_changesets()

        total_objects = 0
        skipped = 0
        done = 0
        failed = 0

        for cs_id in changeset_ids:
            # 이미 처리한 changeset이면 스킵
            if cs_id in processed:
                skipped += 1
                continue

            objects = self.extract_objects(cs_id)

            # 실패(None)면 processed에 기록하지 않음 (다음 실행에서 재시도 가능)
            if objects is None:
                failed += 1
                logger.warning(f"Failed changeset (will retry later): {cs_id}")
                continue

            if objects:
                self.save_objects(objects)
                total_objects += len(objects)

            # 성공했을 때만 처리 완료 changeset 기록
            self._mark_processed(cs_id)
            processed.add(cs_id)
            done += 1

        logger.info(
            f"Done={done}, Skipped={skipped}, Failed={failed}, "
            f"TotalObjectsAdded={total_objects}, InputChangesets={len(changeset_ids)}"
        )

        # queue는 전체 objects.jsonl 기준으로 재생성
        self.generate_fetch_queue()