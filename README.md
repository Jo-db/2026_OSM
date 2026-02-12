## 📁 디렉토리 구조
```
scripts/
 ├── pipeline.py
 ├── objects_extractor.py
 ├── object_version_extractor.py
 ├── gnn_feature_extractor.py
 └── lgbm_feature_extractor.py

test-data/
 ├── changesets.csv
 └── ovid_labels.tsv

output/
 ├── objects.jsonl
 ├── object_versions.jsonl
 ├── nodes.csv
 ├── edges.csv
 ├── labels.csv
 └── lgbm_features.csv
```

## 🔄 전체 흐름
```
changesets.csv
   ↓
objects_extractor
   ↓
objects.jsonl
   ↓
object_version_extractor
   ↓
object_versions.jsonl
   ↓
feature extractor (GNN / LGBM)
```

## ▶ 실행
`python scripts/pipeline.py`

예시:
```
python scripts/pipeline.py \
  --changeset-list ./test-data/changesets.csv \
  --label-file ./test-data/ovid_labels.tsv \
  --start 0 \
  --end 100 \
  --mode both
```

## ⚙ 주요 옵션
| 옵션                  | 설명                      |
| ------------------- | ----------------------- |
| `--changeset-list`  | changeset ID 목록 파일      |
| `--label-file`      | 외부 라벨 파일 (선택)           |
| `--start` / `--end` | 처리 범위 인덱스               |
| `--mode`            | `gnn` / `lgbm` / `both` |
