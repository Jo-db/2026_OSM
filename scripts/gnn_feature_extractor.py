import json
import pandas as pd
from math import radians, cos, sin, sqrt, atan2
from itertools import combinations

# 헬퍼 함수

def haversine(lat1, lon1, lat2, lon2):
    """두 위도/경도 좌표 사이 거리(m) 계산"""
    R = 6371000
    phi1, phi2 = radians(lat1), radians(lat2)
    dphi = radians(lat2 - lat1)
    dlambda = radians(lon2 - lon1)
    a = sin(dphi/2)**2 + cos(phi1)*cos(phi2)*sin(dlambda/2)**2
    c = 2*atan2(sqrt(a), sqrt(1-a))
    return R * c

def geo_shift(node_before, node_after):
    return haversine(node_before['lat'], node_before['lon'], node_after['lat'], node_after['lon'])

def centroid(nodes):
    if not nodes:  
        return {'lat': 0.0, 'lon': 0.0}  
    lats = [n['lat'] for n in nodes]
    lons = [n['lon'] for n in nodes]
    return {'lat': sum(lats)/len(lats), 'lon': sum(lons)/len(lons)}

def way_length(node_map, node_refs):
    length = 0
    for i in range(len(node_refs)-1):
        n1 = node_map.get(node_refs[i])
        n2 = node_map.get(node_refs[i+1])
        if n1 and n2:
            length += haversine(n1['lat'], n1['lon'], n2['lat'], n2['lon'])
    return length

# JSONL 로드

def load_jsonl(file_path):
    data = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            data.append(json.loads(line))
    return data

# 노드 feature 

def build_node_features(objects, object_versions):
    nodes = []
    prev_map = {(obj['obj_type'], obj['obj_id'], obj['version']): obj for obj in object_versions}
    node_geom_map = {obj['obj_id']: obj['geom'] for obj in objects if obj['obj_type']=='node' and 'geom' in obj}

    for obj in objects:
        obj_type = obj['obj_type']
        obj_id = obj['obj_id']
        action = obj['action']
        version = obj['version']
        prev_obj = prev_map.get((obj_type, obj_id, version - 1), None)

        object_type_id = {"node":0, "way":1, "relation":2}[obj_type]
        is_created = 1 if action == "create" else 0
        is_deleted = 1 if action == "delete" else 0
        version_delta = version - (prev_obj['version'] if prev_obj else 0)

        tag_count_before = len(prev_obj['tags']) if prev_obj and 'tags' in prev_obj else 0
        tag_count_after = len(obj['tags']) if 'tags' in obj else 0
        tag_add_count = len(set(obj.get('tags', {}).keys()) - set(prev_obj.get('tags', {}).keys())) if prev_obj else tag_count_after
        tag_remove_count = len(set(prev_obj.get('tags', {}).keys()) - set(obj.get('tags', {}).keys())) if prev_obj else 0
        tag_modify_count = sum(1 for k in obj.get('tags', {}) if prev_obj and k in prev_obj.get('tags', {}) and prev_obj['tags'][k] != obj['tags'][k])

        geo_shift_distance = geo_shift(prev_obj['geom'], obj['geom']) if obj_type=="node" and prev_obj and 'geom' in prev_obj else 0
        length_change_ratio = 0
        centroid_shift = 0
        member_count_delta = 0

        # Way feature 
        if obj_type=="way" and 'refs' in obj and 'node_refs' in obj['refs']:
            refs = obj['refs']['node_refs']
            length_after = way_length(node_geom_map, refs)
            length_before = way_length(node_geom_map, prev_obj['refs']['node_refs']) if prev_obj and 'refs' in prev_obj else length_after
            length_change_ratio = (length_after - length_before)/length_before if length_before>0 else 0

            # centroid shift
            cent_after = centroid([node_geom_map[r] for r in refs if r in node_geom_map])
            if prev_obj and 'refs' in prev_obj:
                cent_before = centroid([node_geom_map[r] for r in prev_obj['refs']['node_refs'] if r in node_geom_map])
                centroid_shift = geo_shift(cent_before, cent_after)

        # Relation feature
        if obj_type=="relation" and 'refs' in obj:
            member_count_delta = len(obj['refs'].get('members', [])) - len(prev_obj['refs'].get('members', [])) if prev_obj else len(obj['refs'].get('members', []))

        nodes.append({
            "object_id": obj_id,
            "object_type_id": object_type_id,
            "is_created": is_created,
            "is_deleted": is_deleted,
            "version_delta": version_delta,
            "tag_count_before": tag_count_before,
            "tag_count_after": tag_count_after,
            "tag_add_count": tag_add_count,
            "tag_remove_count": tag_remove_count,
            "tag_modify_count": tag_modify_count,
            "geo_shift_distance": geo_shift_distance,
            "length_change_ratio": length_change_ratio,
            "centroid_shift": centroid_shift,
            "member_count_delta": member_count_delta
        })
    return pd.DataFrame(nodes)


# Edge 

def build_edges(objects):
    edges = []
    node_refs_map = {obj['obj_id']: obj for obj in objects if obj['obj_type']=='node'}
    way_map = {obj['obj_id']: obj for obj in objects if obj['obj_type']=='way'}
    relation_map = {obj['obj_id']: obj for obj in objects if obj['obj_type']=='relation'}

    # contains : way -> node
    for way_id, way in way_map.items():
        for node_ref in way.get('refs', {}).get('node_refs', []):
            if node_ref in node_refs_map:
                edges.append({'source': way_id, 'target': node_ref, 'edge_type':'contains'})

    # member_of : relation -> member
    for rel_id, rel in relation_map.items():
        for m in rel.get('refs', {}).get('members', []):
            edges.append({'source': rel_id, 'target': m['ref'], 'edge_type':'member_of'})

    # connected ways : 공유 node
    node_to_ways = {}
    for way_id, way in way_map.items():
        for node_ref in way.get('refs', {}).get('node_refs', []):
            node_to_ways.setdefault(node_ref, []).append(way_id)
    for node_ref, ways in node_to_ways.items():
        for w1, w2 in combinations(ways, 2):
            edges.append({'source': w1, 'target': w2, 'edge_type':'connected'})
            edges.append({'source': w2, 'target': w1, 'edge_type':'connected'})  # 양방향

    return pd.DataFrame(edges)


# Label 

def build_labels(nodes_df):
    labels = []

    for _, row in nodes_df.iterrows():
        anomaly = False

        # Rule-based anomaly detection
        if row["geo_shift_distance"] > 50:   # 50m 이상 이동
            anomaly = True
        if row["tag_add_count"] + row["tag_remove_count"] + row["tag_modify_count"] > 5:
            anomaly = True
        if row["is_deleted"] == 1:
            anomaly = True
        if abs(row["length_change_ratio"]) > 0.5:
            anomaly = True

        labels.append({
            "object_id": row["object_id"],
            "label": int(anomaly)  
        })

    return pd.DataFrame(labels)



# 메인

if __name__ == "__main__":
    objects = load_jsonl("output/objects.jsonl")
    object_versions = load_jsonl("output/object_versions.jsonl")

    nodes_df = build_node_features(objects, object_versions)
    nodes_df.to_csv("output/nodes.csv", index=False)

    edges_df = build_edges(objects)
    edges_df.to_csv("output/edges.csv", index=False)

    labels_df = build_labels(nodes_df)
    labels_df.to_csv("output/labels.csv", index=False)

    print(" nodes.csv, edges.csv, labels.csv 생성 완료!")
