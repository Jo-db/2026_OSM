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
 ├── ovid_labels.tsv
 └── training/labels.tsv

output/
 ├── objects.jsonl
 ├── fetch_prev_queue.csv
 ├── object_versions.jsonl
 ├── processed_changesets.txt
 ├── processed_versions.txt
 ├── nodes.csv
 ├── edges.csv
 ├── labels.csv
 └── lgbm_features.csv
```

---

## 🔄 전체 흐름

```
dataset (--dataset)
   ↓
changeset ID 추출
   ↓
objects_extractor
   ↓
objects.jsonl + fetch_prev_queue.csv
   ↓
object_version_extractor (기본 ON)
   ↓
object_versions.jsonl
```

* 이미 처리된 changeset / version은 자동 스킵 (누적 실행 가능)
* 기본적으로 이전 버전(prev)도 함께 수집
* `--no-prev` 옵션 사용 시 이전 버전 수집 생략

---

## ▶ 실행

기본 실행 예시:

```
python scripts/pipeline.py --dataset changesets
```

범위 지정:

```
python scripts/pipeline.py --dataset ovid --start 0 --end 100
```

이전 버전 수집 끄기:

```
python scripts/pipeline.py --dataset training --no-prev
```

output 초기화 후 다시 실행:

```
python scripts/pipeline.py --dataset changesets --overwrite
```

---

## ⚙ 주요 옵션

| 옵션                  | 설명                                          |
| ------------------- | ------------------------------------------- |
| `--dataset`         | 사용할 데이터셋 (`changesets`, `ovid`, `training`) |
| `--start` / `--end` | 처리할 ID 범위                                   |
| `--output-dir`      | 출력 디렉토리 (기본: `./output`)                    |
| `--overwrite`       | 기존 결과 초기화                                   |
| `--no-prev`         | 이전 버전 수집 비활성화 (기본은 ON)                      |