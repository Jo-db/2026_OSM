import requests
import xml.etree.ElementTree as ET
import json
import csv
from typing import Dict, Optional
from pathlib import Path
import logging
import time
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
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

        self.request_delay = 0.5  # 초
    
    # 특정 버전의 객체 정보를 API에서 조회 
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
            if e.response.status_code == 404:
                logger.warning(f"{obj_type}/{obj_id}/v{version} not found (404)")
            elif e.response.status_code == 410:
                logger.warning(f"{obj_type}/{obj_id}/v{version} deleted (410)")
            else:
                logger.error(f"HTTP error fetching {obj_type}/{obj_id}/v{version}: {e}")
            return None
        except requests.RequestException as e:
            logger.error(f"Failed to fetch {obj_type}/{obj_id}/v{version}: {e}")
            return None
    
    # Node 버전 정보 파싱 
    def parse_node_version(self, node_elem: ET.Element) -> Dict:
        obj = {
            "obj_type": "node",
            "obj_id": int(node_elem.get("id")),
            "version": int(node_elem.get("version")),
            "source": "prev_version_api"
        }
        
        # 좌표 정보
        if node_elem.get("lat") and node_elem.get("lon"):
            obj["geom"] = {
                "lat": float(node_elem.get("lat")),
                "lon": float(node_elem.get("lon"))
            }
        
        # 태그 정보
        tags = {}
        for tag in node_elem.findall("tag"):
            tags[tag.get("k")] = tag.get("v")
        if tags:
            obj["tags"] = tags
            
        return obj
    
    # Way 버전 정보 파싱 
    def parse_way_version(self, way_elem: ET.Element) -> Dict:
        obj = {
            "obj_type": "way",
            "obj_id": int(way_elem.get("id")),
            "version": int(way_elem.get("version")),
            "source": "prev_version_api"
        }
        
        # Node references
        node_refs = [nd.get("ref") for nd in way_elem.findall("nd")]
        if node_refs:
            obj["refs"] = {"node_refs": node_refs}
        
        # 태그 정보
        tags = {}
        for tag in way_elem.findall("tag"):
            tags[tag.get("k")] = tag.get("v")
        if tags:
            obj["tags"] = tags
            
        return obj
    
    def parse_relation_version(self, rel_elem: ET.Element) -> Dict:
        """Relation 버전 정보를 파싱합니다."""
        obj = {
            "obj_type": "relation",
            "obj_id": int(rel_elem.get("id")),
            "version": int(rel_elem.get("version")),
            "source": "prev_version_api"
        }
        
        # Members
        members = []
        for member in rel_elem.findall("member"):
            members.append({
                "type": member.get("type"),
                "ref": member.get("ref"),
                "role": member.get("role", "")
            })
        if members:
            obj["refs"] = {"members": members}
        
        # 태그 정보
        tags = {}
        for tag in rel_elem.findall("tag"):
            tags[tag.get("k")] = tag.get("v")
        if tags:
            obj["tags"] = tags
            
        return obj
    
    # XML 데이터에서 객체 버전 정보 추출 
    def extract_version_info(self, xml_data: str, obj_type: str) -> Optional[Dict]:
        try:
            root = ET.fromstring(xml_data)
        except ET.ParseError as e:
            logger.error(f"Failed to parse XML: {e}")
            return None
        
        # 객체 타입에 따라 파싱
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
    
    # 버전 정보를 JSONL 파일에 추가 
    def save_version(self, version_obj: Dict):
        with open(self.versions_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(version_obj, ensure_ascii=False) + '\n')
    
    # fetch_prev_queue.csv를 읽어서 각 객체의 이전 버전 수집 
    def process_queue(self):
        if not self.queue_file.exists():
            logger.error(f"{self.queue_file} not found")
            return
        
        # 기존 파일 삭제
        if self.versions_file.exists():
            self.versions_file.unlink()
        
        total_items = 0
        success_count = 0
        fail_count = 0
        
        with open(self.queue_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                total_items += 1
                
                obj_type = row["obj_type"]
                obj_id = int(row["obj_id"])
                prev_version = int(row["prev_version"])
                
                # API에서 이전 버전 정보 가져오기
                xml_data = self.fetch_object_version(obj_type, obj_id, prev_version)
                
                if xml_data:
                    # XML 파싱 및 정보 추출
                    version_obj = self.extract_version_info(xml_data, obj_type)
                    
                    if version_obj:
                        self.save_version(version_obj)
                        success_count += 1
                        logger.info(f"Saved {obj_type}/{obj_id}/v{prev_version}")
                    else:
                        fail_count += 1
                        logger.warning(f"Failed to parse {obj_type}/{obj_id}/v{prev_version}")
                else:
                    fail_count += 1
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing complete!")
        logger.info(f"Total items: {total_items}")
        logger.info(f"Success: {success_count}")
        logger.info(f"Failed: {fail_count}")
        logger.info(f"Output: {self.versions_file}")
        logger.info(f"{'='*60}")
    
    # API 요청 속도 제한 설정 
    def set_rate_limit(self, requests_per_second: float):
        self.request_delay = 1.0 / requests_per_second
        logger.info(f"Rate limit set to {requests_per_second} requests/second")


def main():
    extractor = ObjectVersionExtractor(
        input_dir="./output",
        output_dir="./output"
    )
    
    # OSM API 정책에 맞게 요청 속도 조정 (기본: 2 req/sec)
    extractor.set_rate_limit(2)
    
    # Queue 처리
    extractor.process_queue()
    
    print("\n이전 버전 수집 완료")
    print(f"- object_versions.jsonl: {extractor.versions_file}")


if __name__ == "__main__":
    main()