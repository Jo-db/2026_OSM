import json
import csv
import math
import argparse
from collections import defaultdict

# Utils

def load_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        return [json.loads(line) for line in f]


def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))



# Feature Extraction

def extract_features(objects, before_map):
    nodes = []

    for obj in objects:
        obj_id = obj["obj_id"]
        obj_type = obj["obj_type"]
        version = obj["version"]
        action = obj["action"]

        before = before_map.get((obj_type, obj_id, version - 1))

        object_type_id = {"node": 0, "way": 1, "relation": 2}[obj_type]
        is_created = 1 if action == "create" else 0
        is_deleted = 1 if action == "delete" else 0
        version_delta = 1 if before else 0

        tags_after = obj.get("tags", {})
        tags_before = before.get("tags", {}) if before else {}

        tag_count_before = len(tags_before)
        tag_count_after = len(tags_after)

        tag_add = len(set(tags_after) - set(tags_before))
        tag_remove = len(set(tags_before) - set(tags_after))
        tag_modify = sum(
            1 for k in tags_after
            if k in tags_before and tags_after[k] != tags_before[k]
        )

       
        geo_shift = 0.0
        length_change_ratio = 0.0
        centroid_shift = 0.0
        member_count_delta = 0

        if obj_type == "node" and before and "geom" in obj and "geom" in before:
            geo_shift = haversine(
                before["geom"]["lat"], before["geom"]["lon"],
                obj["geom"]["lat"], obj["geom"]["lon"]
            )

        if obj_type == "relation" and before:
            member_count_delta = (
                len(obj.get("members", [])) -
                len(before.get("members", []))
            )

        nodes.append([
            obj_id,
            object_type_id,
            is_created,
            is_deleted,
            version_delta,
            tag_count_before,
            tag_count_after,
            tag_add,
            tag_remove,
            tag_modify,
            geo_shift,
            length_change_ratio,
            centroid_shift,
            member_count_delta
        ])

    return nodes



def build_edges(objects):
    edges = []
    for obj in objects:
        if obj["obj_type"] == "way":
            way_id = obj["obj_id"]
            for n in obj.get("refs", {}).get("node_refs", []):
                edges.append([n, way_id, "contains"])
    return edges



def write_nodes(nodes, out_path):
    header = [
        "object_id", "object_type_id",
        "is_created", "is_deleted", "version_delta",
        "tag_count_before", "tag_count_after",
        "tag_add_count", "tag_remove_count", "tag_modify_count",
        "geo_shift_distance", "length_change_ratio",
        "centroid_shift", "member_count_delta"
    ]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(nodes)


def write_edges(edges, out_path):
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["src_id", "dst_id", "edge_type"])
        writer.writerows(edges)


def write_labels(nodes, out_path):
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["object_id", "spam", "import", "tagging_error"])
        for n in nodes:
            writer.writerow([n[0], 0, 0, 0])



def main(args):
    objects = load_jsonl(args.objects)
    versions = load_jsonl(args.versions)

    before_map = {}
    for v in versions:
        before_map[(v["obj_type"], v["obj_id"], v["version"])] = v

    nodes = extract_features(objects, before_map)
    edges = build_edges(objects)

    write_nodes(nodes, f"{args.out}/nodes.csv")
    write_edges(edges, f"{args.out}/edges.csv")
    write_labels(nodes, f"{args.out}/labels.csv")

    print("✔ GNN feature extraction completed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--objects", required=True)
    parser.add_argument("--versions", required=True)
    parser.add_argument("--out", required=True)
    main(parser.parse_args())
