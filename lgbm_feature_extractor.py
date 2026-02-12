import json
import pandas as pd
import os
import math
from datetime import datetime
from collections import Counter, defaultdict
from tqdm import tqdm


class LGBMFeatureExtractor:
    """
    objects.jsonl (현재 버전 레코드들) + object_versions.jsonl (이전 버전 레코드들)
    를 읽어서 LightGBM 학습용 피처 테이블을 만든다.

    결측 처리 정책:
    - 컬럼이 아예 없으면 0으로 생성
    - 값이 None/NaN/숫자형이 아니면 -> NaN으로 강제 -> 최종적으로 0 처리
    - inf/-inf도 최종적으로 0 처리
    """

    def __init__(self, data_dir="output"):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.data_dir = os.path.join(current_dir, data_dir)

        self.input_curr = os.path.join(self.data_dir, "objects.jsonl")
        self.input_prev = os.path.join(self.data_dir, "object_versions.jsonl")
        self.output_file = os.path.join(self.data_dir, "lgbm_features.csv")

        print(f"Working Directory: {self.data_dir}")

        # (obj_type, obj_id, version) -> prev record
        self.prev_cache = {}

        # node_id -> (lat, lon)
        self.coords_curr = {}
        self.coords_prev = {}

        # prev way geometry 계산 시 "prev에 없는 노드 좌표"를 curr로 보완하기 위한 cache
        self.coords_prev_fallback = {}

        # changeset / user 통계 (Context 피처)
        self.stats_cs_size = Counter()
        self.stats_user_edit = Counter()
        self.stats_user_div = defaultdict(set)

    # ----------------------------
    # Utility
    # ----------------------------
    def _parse_ts(self, ts_str):
        """OSM timestamp string -> unix seconds. 없거나 파싱 실패 시 0."""
        if not ts_str:
            return 0
        try:
            return int(datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ").timestamp())
        except:
            return 0

    def _haversine(self, lat1, lon1, lat2, lon2):
        """두 위경도 사이 거리(m)."""
        if lat1 is None or lat2 is None or lon1 is None or lon2 is None:
            return 0.0

        R = 6371000.0
        try:
            phi1, phi2 = math.radians(lat1), math.radians(lat2)
            dphi = math.radians(lat2 - lat1)
            dlambda = math.radians(lon2 - lon1)

            a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
            return R * c
        except:
            return 0.0

    def _calculate_way_metrics(self, node_refs, coord_cache):
        """
        way의 node_refs로부터
        - length_m: polyline length
        - area_m2: polygon area (폐곡선일 때만 의미)
        - centroid: 평균 중심점 (lat, lon)
        를 계산한다.

        좌표 매칭이 부족하면 length/area/centroid 자체가 0으로 떨어질 수 있음.
        """
        coords = []
        refs = node_refs or []

        for nid in refs:
            try:
                nid = int(nid)
            except:
                pass
            if nid in coord_cache:
                lat, lon = coord_cache[nid]
                # lat/lon이 None이면 버림 (NaN/None 방지)
                if lat is not None and lon is not None:
                    coords.append((lat, lon))

        if len(coords) < 2:
            return 0.0, 0.0, (0.0, 0.0)

        # length
        length_m = 0.0
        for i in range(len(coords) - 1):
            length_m += self._haversine(coords[i][0], coords[i][1], coords[i + 1][0], coords[i + 1][1])

        # centroid
        sum_lat = sum(c[0] for c in coords)
        sum_lon = sum(c[1] for c in coords)
        centroid = (sum_lat / len(coords), sum_lon / len(coords))

        # area: 폐곡선일 때만 계산 (open way의 area는 노이즈)
        area_m2 = 0.0
        is_closed = (len(refs) >= 3 and str(refs[0]) == str(refs[-1]))

        if is_closed and len(coords) >= 3:
            xy = []
            for lat, lon in coords:
                y = lat * 111320
                x = lon * (40075000 * math.cos(math.radians(lat)) / 360)
                xy.append((x, y))

            sum1 = sum(xy[i][0] * xy[(i + 1) % len(xy)][1] for i in range(len(xy)))
            sum2 = sum(xy[i][1] * xy[(i + 1) % len(xy)][0] for i in range(len(xy)))
            area_m2 = 0.5 * abs(sum1 - sum2)

        return length_m, area_m2, centroid

    # ----------------------------
    # Preprocess (scan & cache)
    # ----------------------------
    def preprocess(self):
        """
        1) prev 파일(object_versions.jsonl) 읽어서 prev_cache/coords_prev 구축
        2) curr 파일(objects.jsonl) 읽어서 coords_curr + changeset/user 통계 구축
        3) coords_prev_fallback 구축 (prev 없는 노드는 curr로 보완)
        """
        print("데이터 스캔 및 캐싱...")

        # 1) Prev scan
        if os.path.exists(self.input_prev):
            with open(self.input_prev, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        d = json.loads(line)
                        self.prev_cache[(d.get("obj_type"), d.get("obj_id"), d.get("version"))] = d

                        if d.get("obj_type") == "node" and "geom" in d and d["geom"]:
                            self.coords_prev[d.get("obj_id")] = (d["geom"].get("lat"), d["geom"].get("lon"))
                    except:
                        continue

        # 2) Curr scan
        if os.path.exists(self.input_curr):
            with open(self.input_curr, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        d = json.loads(line)

                        if d.get("obj_type") == "node" and "geom" in d and d["geom"]:
                            self.coords_curr[d.get("obj_id")] = (d["geom"].get("lat"), d["geom"].get("lon"))

                        self.stats_cs_size[d.get("changeset_id")] += 1
                        uid = d.get("uid", 0)
                        self.stats_user_edit[uid] += 1
                        self.stats_user_div[uid].add(d.get("obj_type"))
                    except:
                        continue

        # 3) prev_fallback cache
        # 기본은 curr 좌표, prev 좌표가 있으면 prev로 덮어서 "이전 상태" 우선
        self.coords_prev_fallback = dict(self.coords_curr)
        self.coords_prev_fallback.update(self.coords_prev)

    # ----------------------------
    # Feature extraction per record
    # ----------------------------
    def extract_row(self, curr):
        feat = {}

        action = (curr.get("action") or "").lower()
        version = curr.get("version", 1)

        # 이전 버전 찾기: version-1
        prev = None
        if action != "create" and version > 1:
            prev = self.prev_cache.get((curr.get("obj_type"), curr.get("obj_id"), version - 1))

        # --------------------------------
        # 0) Identifiers
        # --------------------------------
        feat["changeset_id"] = curr.get("changeset_id")
        feat["obj_id"] = curr.get("obj_id")

        # --------------------------------
        # 1) Basic Meta
        # --------------------------------
        type_map = {"node": 0, "way": 1, "relation": 2}
        feat["object_type"] = type_map.get(curr.get("obj_type"), -1)
        feat["version_count"] = version

        feat["is_delete"] = 1 if action == "delete" else 0
        feat["is_create"] = 1 if action == "create" else 0
        feat["is_modify"] = 1 if action == "modify" else 0

        curr_ts = self._parse_ts(curr.get("timestamp"))
        feat["last_modified_time"] = curr_ts
        feat["created_time"] = curr_ts if version == 1 else 0

        # --------------------------------
        # 2) Tag-based (diff)
        # --------------------------------
        c_tags = curr.get("tags", {}) or {}
        p_tags = (prev.get("tags", {}) if prev else {}) or {}

        c_keys, p_keys = set(c_tags.keys()), set(p_tags.keys())

        feat["tag_count"] = len(c_tags)
        feat["tag_add_count"] = len(c_keys - p_keys)
        feat["tag_remove_count"] = len(p_keys - c_keys)
        feat["tag_modify_count"] = sum(1 for k in (c_keys & p_keys) if c_tags.get(k) != p_tags.get(k))
        feat["name_changed"] = 1 if c_tags.get("name") != p_tags.get("name") else 0

        # --------------------------------
        # 3) Geometry-based (diff)
        # --------------------------------
        feat["length_change_ratio"] = 0.0
        feat["area_change_ratio"] = 0.0
        feat["node_count_change"] = 0
        feat["centroid_shift"] = 0.0

        obj_type = curr.get("obj_type")

        c_refs = (curr.get("refs", {}) or {}).get("node_refs", [])
        if not c_refs:
            c_refs = (curr.get("refs", {}) or {}).get("members", [])

        p_refs = []
        if prev:
            p_refs = (prev.get("refs", {}) or {}).get("node_refs", [])
            if not p_refs:
                p_refs = (prev.get("refs", {}) or {}).get("members", [])

        feat["node_count_change"] = len(c_refs) - len(p_refs)

        if obj_type == "node":
            if prev and "geom" in curr and "geom" in prev and curr["geom"] and prev["geom"]:
                feat["centroid_shift"] = self._haversine(
                    curr["geom"].get("lat"), curr["geom"].get("lon"),
                    prev["geom"].get("lat"), prev["geom"].get("lon"),
                )

        elif obj_type == "way":
            c_len, c_area, c_cent = self._calculate_way_metrics(c_refs, self.coords_curr)

            if prev:
                p_len, p_area, p_cent = self._calculate_way_metrics(p_refs, self.coords_prev_fallback)

                eps = 1e-6
                feat["length_change_ratio"] = (c_len - p_len) / max(p_len, eps)
                feat["area_change_ratio"] = (c_area - p_area) / max(p_area, eps)

                if p_len > 0:
                    feat["centroid_shift"] = self._haversine(c_cent[0], c_cent[1], p_cent[0], p_cent[1])

        # --------------------------------
        # 4) Changeset Derived (Context)
        # --------------------------------
        cid = curr.get("changeset_id")
        uid = curr.get("uid", 0)

        feat["changeset_size"] = self.stats_cs_size.get(cid, 0)
        feat["user_edit_count"] = self.stats_user_edit.get(uid, 0)
        feat["user_object_diversity"] = len(self.stats_user_div.get(uid, set()))

        # prev에 timestamp 추가
        prev_ts = self._parse_ts(prev.get("timestamp")) if prev else 0
        feat["time_gap_prev"] = (curr_ts - prev_ts) if prev_ts > 0 else 0

        return feat

    # ----------------------------
    # Run
    # ----------------------------
    def run(self):
        self.preprocess()
        results = []
        print("Feature 추출 중...")

        if not os.path.exists(self.input_curr):
            print(f"입력 파일 없음: {self.input_curr}")
            return

        with open(self.input_curr, "r", encoding="utf-8") as f:
            for line in tqdm(f):
                try:
                    curr = json.loads(line)
                    results.append(self.extract_row(curr))
                except:
                    continue

        df = pd.DataFrame(results)

        cols = [
            # 0) identifiers
            "changeset_id", "obj_id",

            # 1) meta
            "object_type", "version_count",
            "is_delete", "is_create", "is_modify",
            "last_modified_time", "created_time",

            # 2) tag
            "tag_count", "tag_add_count", "tag_remove_count", "tag_modify_count", "name_changed",

            # 3) geometry
            "length_change_ratio", "area_change_ratio", "node_count_change", "centroid_shift",

            # 4) changeset derived
            "changeset_size", "user_edit_count", "user_object_diversity", "time_gap_prev",
        ]

        # (1) 컬럼 자체가 없으면 0으로 생성
        for c in cols:
            if c not in df.columns:
                df[c] = 0

        # (2) 값이 None/문자열 등으로 섞여 NaN이 생기면 -> 숫자로 강제 변환 (실패는 NaN)
        #     그 후 NaN을 0으로 치환 (결측치 0처리 보장)
        df[cols] = df[cols].apply(pd.to_numeric, errors="coerce")
        df[cols] = df[cols].fillna(0)

        # (3) 혹시 모를 inf/-inf도 0 처리 (ratio 계산 보호)
        df[cols] = df[cols].replace([float("inf"), float("-inf")], 0)

        df[cols].to_csv(self.output_file, index=False)
        print(f"저장: {self.output_file}")
        print(f"   Shape: {df.shape}")


if __name__ == "__main__":
    extractor = LGBMFeatureExtractor()
    extractor.run()
