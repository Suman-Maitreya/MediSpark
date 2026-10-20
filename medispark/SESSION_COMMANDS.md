# MediSpark — Session Command Log

Every command run during the 500K-row pipeline re-run and the data-leakage
investigation/fix, in the order they were actually executed, with an
explanation of what each one does. Paths assume Git Bash on Windows from
`D:\SEM - 5\BigData` unless noted.

---

## 1. Check the environment before starting

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```
Lists running containers and their status. Used to confirm the Spark/HDFS
cluster (`namenode`, `datanode1-3`, `spark-master`, `spark-worker-1/2`) was
up before touching anything.

```bash
tasklist | grep -i "Docker Desktop.exe"
```
Windows process list filtered to Docker Desktop — used when `docker ps` hung,
to check whether the Docker Desktop engine/WSL backend was still starting up.

```bash
for i in $(seq 1 20); do
  docker inspect --format='{{.Name}}: {{.State.Health.Status}}' \
    namenode datanode1 datanode2 datanode3
  sleep 6
done
```
Polls the Docker healthcheck status of the 4 HDFS containers every 6 seconds,
up to 20 times, until all report `healthy`. Prevents running Spark jobs
against a cluster that isn't ready yet.

```powershell
Get-CimInstance Win32_OperatingSystem | Select-Object FreePhysicalMemory,TotalVisibleMemorySize
```
PowerShell — reports free vs. total system RAM (in KB). Used to diagnose a
`numpy._core._exceptions._ArrayMemoryError` during augmentation (system had
only ~1.8GB free) and to confirm more RAM was free before retrying.

```powershell
Get-Process | Sort-Object WorkingSet64 -Descending | Select-Object -First 12 Name, @{N='MB';E={[math]::Round($_.WorkingSet64/1MB)}}
```
Lists the top 12 processes by memory usage, so the heaviest RAM consumers
(OneDrive, WSL, Chrome, etc.) could be identified before asking to free memory.

---

## 2. Data augmentation (Python, first pass — 500K rows)

```bash
cd "datasets" && PYTHONIOENCODING=utf-8 python augment_datasets.py > augment_run.log 2>&1
```
Runs the SMOTE-style augmentation script that expands each of the 4 CSVs
(`heart_original.csv`, `diabetes_original.csv`, `stroke_original.csv`,
`lung_cancer_original.csv`) up to `TARGET_SIZE = 500000` rows via k-NN
interpolation. `PYTHONIOENCODING=utf-8` avoids console encoding errors from
the script's Unicode checkmarks. Output redirected to a log file so it could
be inspected without truncation. First run crashed partway (memory error on
`diabetes`); re-run after freeing RAM completed all 4 datasets.

---

## 3. Clearing and reloading HDFS

```bash
MSYS_NO_PATHCONV=1 docker exec namenode hdfs dfs -ls /medispark
```
Lists the top-level HDFS directory structure. `MSYS_NO_PATHCONV=1` stops Git
Bash from mangling the leading `/medispark` into a Windows path before it
reaches the container — without it you get `ls: No FileSystem for scheme "C"`.

```bash
for d in raw cleaned features splits models evaluation ingested; do
  docker exec namenode hdfs dfs -rm -r -skipTrash "/medispark/$d/*"
done
```
Deletes the contents of every pipeline-stage directory in HDFS (but keeps the
directories themselves), so stale output from the previous (50K-row) run
doesn't mix with the new 500K-row run. `-skipTrash` deletes immediately
instead of moving to HDFS trash.

```bash
docker cp "heart_original.csv" namenode:/tmp/heart_original.csv
docker cp "diabetes_original.csv" namenode:/tmp/diabetes_original.csv
docker cp "stroke_original.csv" namenode:/tmp/stroke_original.csv
docker cp "lung_cancer_original.csv" namenode:/tmp/lung_cancer_original.csv
```
Copies each augmented CSV from the local `datasets/originals/` folder into
the `namenode` container's local filesystem (`/tmp`), since `hdfs dfs -put`
needs the source file to already be reachable from inside the container.

```bash
docker exec namenode hdfs dfs -put -f "/tmp/heart_original.csv" "/medispark/raw/heart_original.csv"
# ...repeated for the other 3 datasets
```
Uploads each CSV from the container's local `/tmp` into HDFS proper, under
`/medispark/raw/`. `-f` overwrites if a file already exists at that path.

```bash
docker exec namenode hdfs dfs -ls /medispark/raw
```
Confirms all 4 files landed in HDFS with the expected sizes.

---

## 4. Building the Spark job jar

```bash
cd "medispark" && sbt assembly
```
Compiles the Scala pipeline and bundles it with all dependencies into a
single fat JAR (`target/scala-2.12/medispark-assembly-1.0.jar`), which is
what `spark-submit` actually runs. Re-run every time a `.scala` file changes.

---

## 5. Running the pipeline stages

Each stage is a `spark-submit` call against the same jar, targeting a
different Scala `object`'s `main`:

```bash
docker exec spark-master spark-submit \
  --class medispark.ingestion.DataIngestion \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  /jars/medispark-assembly-1.0.jar
```
**Stage 1 — Ingestion.** Reads the 4 raw CSVs from HDFS with an explicit
schema, validates row/column counts, writes them out as Parquet under
`/medispark/ingested/`.

```bash
docker exec spark-master spark-submit \
  --class medispark.cleaning.DataCleaning \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  /jars/medispark-assembly-1.0.jar
```
**Stage 2 — Cleaning.** Drops nulls, removes outliers (e.g. cholesterol,
blood pressure ranges), renames target columns to `label`, writes cleaned
Parquet to `/medispark/cleaned/`.

```bash
docker exec spark-master spark-submit \
  --class medispark.features.FeatureEngineering \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  /jars/medispark-assembly-1.0.jar
```
**Stage 3 — Feature Engineering.** String-indexes categorical columns
(stroke's `gender`, `work_type`, etc.; lung cancer's `Level` target),
assembles feature columns into a vector, applies `StandardScaler`, writes to
`/medispark/features/`.

```bash
docker exec spark-master spark-submit \
  --class medispark.training.ModelTraining \
  --master spark://spark-master:7077 \
  --driver-memory 2g \
  --executor-memory 2g \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  /jars/medispark-assembly-1.0.jar
```
**Stage 4 — Model Training.** Splits each dataset into train/test, trains a
`RandomForestClassifier` per disease (100 trees, 150 for lung cancer's
multi-class problem), evaluates on the test split, saves the model to
`/medispark/models/` and the test/prediction Parquet to `/medispark/splits/`.
Given extra driver/executor memory since this is the heaviest stage.

```bash
docker exec spark-master spark-submit \
  --class medispark.evaluation.ModelEvaluation \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  /jars/medispark-assembly-1.0.jar
```
**Stage 6 — Evaluation.** Loads each saved model and its predictions,
computes accuracy/F1/precision/recall/AUC-ROC, confusion matrix, top-10
feature importances, and per-class precision/recall. Saves results as JSON
under `/medispark/evaluation/`. (Stage 5, the optional live streaming demo,
was skipped.)

```bash
docker exec namenode hdfs dfs -cat "/medispark/evaluation/heart/*.json"
# ...repeated for diabetes, stroke, lung_cancer
```
Reads the saved JSON evaluation summaries straight out of HDFS, rather than
parsing them back out of the verbose Spark console log.

---

## 6. Running and testing the web app

```bash
pip show flask reportlab
```
Confirms the Python dependencies the Flask app needs are installed.

```bash
cd "webapp" && python app.py > "webapp_run.log" 2>&1 &
```
Starts the Flask development server in the background, with all
stdout/stderr captured to a log file for later inspection.

```bash
curl -s -o /dev/null -w "%{http_code}" "http://localhost:5000/api/metrics"
# ...and similarly for /, /api/status, /api/metrics/<disease>
```
Hits each API route and prints only the HTTP status code, to quickly confirm
nothing was throwing a 500 error.

```bash
curl -s -X POST http://localhost:5000/api/predict -H "Content-Type: application/json" -d '{...}'
curl -s -X POST http://localhost:5000/api/whatif -H "Content-Type: application/json" -d '{...}'
curl -s -X POST http://localhost:5000/api/explain -H "Content-Type: application/json" -d '{...}'
```
Sends realistic sample payloads to the prediction, what-if, and explanation
endpoints and inspects the JSON response for correctness/errors.

```bash
grep -in "error\|exception\|traceback" "webapp_run.log"
```
Scans the running Flask server's log for any stack traces surfaced by real
usage (as opposed to just checking HTTP status codes).

---

## 7. Investigating the suspiciously high accuracy

```bash
python -c "
import pandas as pd, numpy as np
df = pd.read_csv('heart_original.csv')
... # normalize numeric columns, sample 1500 rows, find nearest neighbor
... # distance for each in a 30,000-row reference pool
"
```
A standalone nearest-neighbor check: for a random sample of rows, finds the
closest other row in the dataset (normalized Euclidean distance) and reports
what fraction are near-duplicates (distance < 0.0001). First run on the
unmodified data found an 87% near-duplicate rate for heart — the smoking gun
for data leakage (synthetic siblings of test rows sitting in the training
set). Repeated for diabetes and stroke with the same result pattern.

```bash
grep -n "randomSplit" "medispark/src/main/scala/medispark/training/ModelTraining.scala"
```
Confirmed the actual leak mechanism in code: `data.randomSplit(Array(0.8, 0.2))`
was called *after* all synthetic augmentation, so near-duplicate synthetic
rows could land on both sides of the split.

```bash
grep -n "^  Columns:" "stage2_cleaning.log"
```
Pulled the exact post-cleaning column order for each dataset straight from
the Stage 2 log, needed to map the model's numeric `Feature[i]` importance
indices back to real column names for the webapp.

```bash
git log --all --oneline -- "datasets/originals/*.csv"
git check-ignore -v datasets/originals/heart_original.csv ...
```
Checked whether git history held an earlier, un-augmented version of the
source CSVs to restart from cleanly. Result: `datasets/` is gitignored, no
history exists — true raw data for heart/diabetes/lung_cancer is
unrecoverable.

```bash
find "/d" -iname "*diabetes*.csv" -o -iname "*heart*original*.csv" -o -iname "*lung_cancer*.csv"
find "/d" -iname "*.csv" | xargs wc -l | awk '$1>250 && $1<2000'
```
Searched the whole D: drive for any leftover small (~300-2000 row) CSV that
might be a genuine raw original. None found.

---

## 8. Fixing the leakage — first attempt (subsampling, did not work)

```bash
cd "datasets" && python -c "import ast; ast.parse(open('augment_datasets.py').read())"
```
Quick syntax check on the rewritten Python script before running it for
real (`ast.parse` will raise on any syntax error without executing anything).

```bash
cd "datasets" && PYTHONIOENCODING=utf-8 python augment_datasets.py > augment_run3.log 2>&1
```
First fix attempt: randomly subsample each dataset to 10% before
re-augmenting with a new split-before-synthesize `smote_augment`. Verified
with the same nearest-neighbor check afterward — **leakage was still severe**
(cross-split near-dup rate still 50-62%), because random subsampling thins
out dense duplicate clusters without actually separating them.

---

## 9. Fixing the leakage — second attempt (grid deduplication, worked)

```bash
cd "datasets" && PYTHONIOENCODING=utf-8 python augment_datasets.py > augment_run4.log 2>&1
```
Second fix attempt: replaced subsampling with grid-based deduplication
(collapse near-identical rows to one representative per coarse feature-space
bucket) *before* splitting into train/test and re-synthesizing. This run
crashed partway through on `lung_cancer` (a leftover `_split` string column
from the previous run got treated as a numeric feature).

```bash
cd "datasets" && PYTHONIOENCODING=utf-8 python -c "
import augment_datasets as ad
ad.augment_lung_cancer()
" > augment_lung_fix.log 2>&1
```
After patching the bug, re-ran just the `lung_cancer` augmentation function
directly (via Python import, bypassing the `if __name__ == '__main__'` guard)
rather than re-running all 4 datasets, since heart/diabetes/stroke had
already completed successfully in the previous invocation.

```bash
python -c "
import pandas as pd, numpy as np
# ... load heart/diabetes/stroke, split by _split column,
# ... check nearest-neighbor distance from test rows to train rows only
"
```
Re-ran the empirical leakage check, this time specifically measuring
*cross-split* near-duplicates (test row vs. nearest train row) rather than
dataset-wide near-duplicates. Confirmed substantial improvement: heart's
near-dup rate dropped from 87% to 50%, diabetes 74%→39%, stroke 62%→23%, with
median nearest-neighbor distance increasing by roughly 2-3 orders of
magnitude across the board.

---

## 10. Re-running the full pipeline against the fixed data

```bash
for f in heart_original.csv diabetes_original.csv stroke_original.csv lung_cancer_original.csv; do
  echo "$f: $(wc -l < "$f") lines"
done
docker ps --format "table {{.Names}}\t{{.Status}}"
```
Sanity-checked the new CSV row counts and that the Docker cluster was still
up before starting the reload.

```bash
for d in raw cleaned features splits models evaluation ingested; do
  docker exec namenode hdfs dfs -rm -r -skipTrash "/medispark/$d/*"
done
```
Same HDFS clear as step 3, repeated for the corrected data.

```bash
docker cp heart_original.csv namenode:/tmp/heart_original.csv
# ...and the other 3
docker exec namenode hdfs dfs -put -f "/tmp/heart_original.csv" "/medispark/raw/heart_original.csv"
# ...and the other 3
docker exec namenode hdfs dfs -ls /medispark/raw
```
Same copy-into-container-then-upload-to-HDFS sequence as step 3. This time
only 4 files were needed — `DataIngestion.scala` was fixed to read
`stroke_original.csv` directly, removing the old `stroke_augmented.csv`
duplicate-upload workaround.

```bash
cd "medispark" && sbt assembly
```
Rebuilt the jar to pick up the 3 changed Scala files (`DataIngestion.scala`,
`FeatureEngineering.scala`, `ModelTraining.scala` — now split-aware via a new
`_split` column instead of Spark's random 80/20 split).

```bash
docker exec spark-master spark-submit --class medispark.ingestion.DataIngestion ...
docker exec spark-master spark-submit --class medispark.cleaning.DataCleaning ...
docker exec spark-master spark-submit --class medispark.features.FeatureEngineering ...
docker exec spark-master spark-submit --class medispark.training.ModelTraining --driver-memory 2g --executor-memory 2g ...
docker exec spark-master spark-submit --class medispark.evaluation.ModelEvaluation ...
```
Re-ran Stages 1 through 4 and 6 exactly as in step 5, now against the
leakage-fixed data and the split-aware jar.

```bash
docker exec namenode hdfs dfs -cat "/medispark/evaluation/heart/*.json"
# ...and the other 3
```
Pulled the corrected evaluation metrics: heart 99.67%, diabetes 97.79%,
stroke 93.15% (down from 95.97%), lung_cancer 94.60% (down from 97.04%).

```bash
grep -n "Accuracy:\|Confusion Matrix\|Feature Importances\|Per-class\|Class [0-9]:" "stage6_v2.log"
```
Extracted confusion matrices, per-class precision/recall, and feature
importances from the full evaluation log to feed back into the webapp.

---

## 11. Verifying the webapp after the fix

```bash
curl -s -o /dev/null -w "%{http_code}" "http://localhost:5000/"
curl -s http://localhost:5000/api/metrics/stroke
grep -in "error\|exception\|traceback" "webapp_run.log"
```
Final check: confirmed the auto-reloading Flask dev server picked up the
`app.py`/`app.js` metric updates, all routes still returned 200, and no
errors appeared in the server log.

---

## What changed in the code (not shell commands, but part of the same fix)

- **`datasets/augment_datasets.py`** — added `dedup_anchor()` (grid-based
  near-duplicate collapse) and rewrote `smote_augment()` to split real anchor
  rows into train/test *before* generating any synthetic neighbors, tagging
  every output row with a `_split` column.
- **`medispark/.../ingestion/DataIngestion.scala`** — added a `_split`
  `StringType` field to all 4 CSV schemas; pointed the stroke path at
  `stroke_original.csv` instead of the old `stroke_augmented.csv` workaround.
- **`medispark/.../features/FeatureEngineering.scala`** — final column
  selection now keeps `_split` alongside `label`/`features`; excluded
  `_split` from the auto-derived feature-column lists for heart/diabetes/lung.
- **`medispark/.../training/ModelTraining.scala`** — replaced
  `data.randomSplit(Array(0.8, 0.2))` with a filter on the `_split` column.
- **`medispark/webapp/app.py`, `static/js/app.js`, `templates/index.html`**
  — fixed hardcoded 50,000/200,000 stale dataset-size numbers, and updated
  all 4 diseases' accuracy/F1/confusion-matrix/feature-importance data twice
  (once after the initial 500K re-run, once after the leakage fix).
