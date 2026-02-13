import json
import pandas as pd
import os
import math
from datetime import datetime
from collections import Counter, defaultdict
from tqdm import tqdm

class LGBMFeatureExtractor:
    def __init__(self, data_dir="output"):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.data_dir = os.path.join(current_dir, data_dir)
        
        self.input_curr = os.path.join(self.data_dir, "objects.jsonl")
        self.input_prev = os.path.join(self.data_dir, "object_versions.jsonl")
        self.output_file = os.path.join(self.data_dir, "lgbm_features.csv")
        
        print(f"Working Directory: {self.data_dir}")
        
        self.prev_cache = {}
        self.coords_curr = {}
        self.coords_prev = {}
        self.stats_cs_size = Counter()
        self.stats_user_edit = Counter()
        self.stats_user_div = defaultdict(set)

    def _parse_ts(self, ts_str):
        if not ts_str: return 0
        try: return int(datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ").timestamp())
        except: return 0

    def _haversine(self, lat1, lon1, lat2, lon2):
        if lat1 is None or lat2 is None: return 0.0
        R = 6371000
        try:
            phi1, phi2 = math.radians(lat1), math.radians(lat2)
            dphi = math.radians(lat2 - lat1)
            dlambda = math.radians(lon2 - lon1)
            a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2) * math.sin(dlambda/2)**2
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
            return R * c
        except: return 0.0

    def _calculate_way_metrics(self, node_refs, coord_cache):
        coords = []
        for nid in node_refs:
            try: nid = int(nid)
            except: pass
            if nid in coord_cache: coords.append(coord_cache[nid])
        
        if len(coords) < 2: return 0.0, 0.0, (0.0, 0.0)

        length_m = 0.0
        for i in range(len(coords) - 1):
            length_m += self._haversine(coords[i][0], coords[i][1], coords[i+1][0], coords[i+1][1])
        
        sum_lat = sum(c[0] for c in coords)
        sum_lon = sum(c[1] for c in coords)
        centroid = (sum_lat / len(coords), sum_lon / len(coords))

        area_m2 = 0.0
        if len(coords) >= 3:
            xy = []
            for lat, lon in coords:
                y = lat * 111320
                x = lon * (40075000 * math.cos(math.radians(lat)) / 360)
                xy.append((x, y))
            sum1 = sum(xy[i][0] * xy[(i+1)%len(xy)][1] for i in range(len(xy)))
            sum2 = sum(xy[i][1] * xy[(i+1)%len(xy)][0] for i in range(len(xy)))
            area_m2 = 0.5 * abs(sum1 - sum2)

        return length_m, area_m2, centroid

    def preprocess(self):
        print("데이터 스캔 및 캐싱...")
        if os.path.exists(self.input_prev):
            with open(self.input_prev, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        d = json.loads(line)
                        self.prev_cache[(d.get('obj_type'), d.get('obj_id'), d.get('version'))] = d
                        if d.get('obj_type') == 'node' and 'geom' in d:
                            self.coords_prev[d.get('obj_id')] = (d['geom']['lat'], d['geom']['lon'])
                    except: continue

        if os.path.exists(self.input_curr):
            with open(self.input_curr, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        d = json.loads(line)
                        if d.get('obj_type') == 'node' and 'geom' in d:
                            self.coords_curr[d.get('obj_id')] = (d['geom']['lat'], d['geom']['lon'])
                        self.stats_cs_size[d.get('changeset_id')] += 1
                        self.stats_user_edit[d.get('uid', 0)] += 1
                        self.stats_user_div[d.get('uid', 0)].add(d.get('obj_type'))
                    except: continue

    def extract_row(self, curr):
        feat = {}
        prev = None
        if curr['action'] != 'create' and curr['version'] > 1:
            prev = self.prev_cache.get((curr['obj_type'], curr['obj_id'], curr['version'] - 1))

        # ✅ 식별자 추가
        feat['changeset_id'] = curr.get('changeset_id')

        # 1) Meta
        type_map = {'node': 0, 'way': 1, 'relation': 2}
        feat['object_type'] = type_map.get(curr['obj_type'], -1)
        feat['version_count'] = curr.get('version', 1)
        feat['is_deleted'] = 1 if curr['action'] == 'delete' else 0
        curr_ts = self._parse_ts(curr.get('timestamp'))
        feat['last_modified_time'] = curr_ts
        feat['is_create'] = 1 if curr['action'] == 'create' else 0
        feat['created_time'] = curr_ts if curr.get('version') == 1 else 0

        # 2) Tag
        c_tags = curr.get('tags', {})
        p_tags = prev.get('tags', {}) if prev else {}
        c_keys, p_keys = set(c_tags.keys()), set(p_tags.keys())
        feat['tag_count'] = len(c_tags)
        feat['tag_add_count'] = len(c_keys - p_keys)
        feat['tag_remove_count'] = len(p_keys - c_keys)
        feat['tag_modify_count'] = sum(1 for k in c_keys & p_keys if c_tags[k] != p_tags[k])
        feat['name_changed'] = 1 if c_tags.get('name') != p_tags.get('name') else 0

        # 3) Geometry
        feat['length_change_ratio'] = 0.0
        feat['area_change_ratio'] = 0.0
        feat['node_count_change'] = 0
        feat['centroid_shift'] = 0.0
        
        c_refs = curr.get('refs', {}).get('node_refs', []) or curr.get('refs', {}).get('members', [])
        p_refs = prev.get('refs', {}).get('node_refs', []) or prev.get('refs', {}).get('members', []) if prev else []
        feat['node_count_change'] = len(c_refs) - len(p_refs)

        if curr['obj_type'] == 'node':
            if prev and 'geom' in curr and 'geom' in prev:
                feat['centroid_shift'] = self._haversine(curr['geom']['lat'], curr['geom']['lon'], prev['geom']['lat'], prev['geom']['lon'])
        elif curr['obj_type'] == 'way':
            c_len, c_area, c_cent = self._calculate_way_metrics(c_refs, self.coords_curr)
            if prev:
                p_len, p_area, p_cent = self._calculate_way_metrics(p_refs, self.coords_prev)
                eps = 1e-6
                feat['length_change_ratio'] = (c_len - p_len) / max(p_len, eps)
                feat['area_change_ratio'] = (c_area - p_area) / max(p_area, eps)
                if p_len > 0: feat['centroid_shift'] = self._haversine(c_cent[0], c_cent[1], p_cent[0], p_cent[1])

        # 4) Changeset Derived
        cid, uid = curr.get('changeset_id'), curr.get('uid', 0)
        feat['changeset_size'] = self.stats_cs_size.get(cid, 0)
        feat['user_edit_count'] = self.stats_user_edit.get(uid, 0)
        feat['user_object_diversity'] = len(self.stats_user_div.get(uid, set()))
        prev_ts = self._parse_ts(prev.get('timestamp')) if prev else 0
        feat['time_gap_prev'] = (curr_ts - prev_ts) if prev_ts > 0 else 0

        return feat

    def run(self):
        self.preprocess()
        results = []
        print("Feature 추출 중...")
        
        if not os.path.exists(self.input_curr): return

        with open(self.input_curr, 'r', encoding='utf-8') as f:
            for line in tqdm(f):
                try: results.append(self.extract_row(json.loads(line)))
                except: continue
        
        df = pd.DataFrame(results)
        
        # ✅ changeset_id를 맨 앞에 배치
        cols = ['changeset_id', 
                'object_type', 'version_count', 'is_deleted', 'last_modified_time', 'is_create', 'created_time',
                'tag_count', 'tag_add_count', 'tag_remove_count', 'tag_modify_count', 'name_changed',
                'length_change_ratio', 'area_change_ratio', 'node_count_change', 'centroid_shift',
                'changeset_size', 'user_edit_count', 'user_object_diversity', 'time_gap_prev']
        
        for c in cols:
            if c not in df.columns: df[c] = 0
            
        df[cols].to_csv(self.output_file, index=False)
        print(f"저장: {self.output_file}")
        print(f"   Shape: {df.shape}")

if __name__ == "__main__":
    extractor = LGBMFeatureExtractor()
    extractor.run()