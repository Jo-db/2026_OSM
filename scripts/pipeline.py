import argparse
import csv
from pathlib import Path
from typing import List, Optional

from objects_extractor import ChangesetObjectExtractor
from object_version_extractor import ObjectVersionExtractor


DATASET_PATHS = {
    "changesets": Path("./test-data/changesets.csv"),
    "ovid": Path("./test-data/ovid_labels.tsv"),
    "training": Path("./training-data/labels.tsv"),
}


def load_changeset_ids(path: Path) -> List[int]:
    if not path.exists():
        raise FileNotFoundError(f"Dataset file not found: {path}")

    delimiter = "\t" if path.suffix.lower() in [".tsv", ".tab"] else ","
    ids: List[int] = []

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

    return list(dict.fromkeys(ids))


def slice_ids(ids: List[int], start: int, end: Optional[int]) -> List[int]:
    if start < 0:
        start = 0
    return ids[start:] if end is None else ids[start:end]


def parse_args():
    p = argparse.ArgumentParser()

    p.add_argument(
        "--dataset",
        required=True,
        choices=["changesets", "ovid", "training"],
        help="Which dataset to use",
    )
    p.add_argument("--start", type=int, default=0, help="Start index (0-based, inclusive)")
    p.add_argument("--end", type=int, default=None, help="End index (0-based, exclusive). Omit for 'to the end'")

    p.add_argument("--output-dir", type=str, default="./output", help="Output directory")
    p.add_argument("--overwrite", action="store_true", help="Reset outputs before running")

    # 기본은 prev 수집, 안 하고 싶을 때만 끄는 옵션
    p.add_argument(
        "--no-prev",
        action="store_true",
        help="Do NOT fetch previous versions (default: fetch prev versions)",
    )

    return p.parse_args()


def main():
    args = parse_args()

    path = DATASET_PATHS[args.dataset]
    ids = load_changeset_ids(path)
    subset = slice_ids(ids, args.start, args.end)

    print(f"[dataset] {args.dataset} -> {path}")
    print(f"[total] {len(ids)} ids")
    print(f"[range] start={args.start}, end={args.end}")
    print(f"[subset] {len(subset)} ids")
    print(f"[output] {args.output_dir}")
    print(f"[overwrite] {args.overwrite}")
    print(f"[prev] {'OFF' if args.no_prev else 'ON (default)'}")

    if subset:
        print(f"[first/last] {subset[0]} ... {subset[-1]}")

    # 1) changeset -> objects + queue (누적/스킵 로직 반영)
    extractor = ChangesetObjectExtractor(output_dir=args.output_dir)
    extractor.process_changesets(subset, overwrite=args.overwrite)

    print("\n✔ objects / queue 생성(또는 누적) 완료")
    print(f"  - objects: {extractor.objects_file}")
    print(f"  - queue  : {extractor.queue_file}")
    print(f"  - processed: {extractor.processed_file}")

    # 2) prev version 수집 (기본 ON)
    if not args.no_prev:
        version_extractor = ObjectVersionExtractor(
            input_dir=args.output_dir,
            output_dir=args.output_dir,
        )

        version_extractor.set_rate_limit(2)
        version_extractor.process_queue(overwrite=args.overwrite)

        print("\n✔ object_versions.jsonl 생성 완료")
        print(f"  - versions: {version_extractor.versions_file}")
    else:
        print("\n(스킵) --no-prev 옵션으로 이전 버전 수집을 비활성화했습니다.")


if __name__ == "__main__":
    main()