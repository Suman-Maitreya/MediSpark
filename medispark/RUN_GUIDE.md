# MediSpark — Complete Execution Guide

## Prerequisites
- Docker Desktop running on Windows with WSL2
- Java 8+ and sbt installed (for building the JAR)
- Python 3 with `flask`, `reportlab` (for webapp)

---

## STEP 0: Clean Up Old Files (ONE TIME)

Before starting, delete old augmented datasets and build artifacts:

```powershell
# In PowerShell, from D:\SEM - 5\BigData\
# Delete old augmented CSVs (300MB+ each — not needed anymore)
Remove-Item datasets\heart.csv, datasets\diabetes.csv, datasets\stroke.csv, datasets\lung_cancer.csv -ErrorAction SilentlyContinue
Remove-Item datasets\augment_all.py, datasets\augment_lung.py, datasets\augment_stroke.py, datasets\gen_heart.py -ErrorAction SilentlyContinue

# Delete old build artifacts
Remove-Item -Recurse medispark\target -ErrorAction SilentlyContinue
Remove-Item -Recurse medispark\project\target -ErrorAction SilentlyContinue
Remove-Item -Recurse medispark\project\project -ErrorAction SilentlyContinue
```

---

## STEP 1: Start Docker Cluster

```powershell
cd "D:\SEM - 5\BigData"
docker-compose -f docker-compose-medispark.yaml up -d
```

Wait 30 seconds, then verify:
```powershell
docker ps
# Should see: namenode, datanode1, datanode2, datanode3, spark-master, spark-worker-1, spark-worker-2
```

Check HDFS Web UI: http://localhost:9870
Check Spark Web UI: http://localhost:8080

---

## STEP 2: Upload Raw Datasets to HDFS

```powershell
# Create HDFS directories
docker exec -it namenode hdfs dfs -mkdir -p /medispark/raw

# Upload the 4 original datasets (mounted at /datasets in namenode)
docker exec -it namenode hdfs dfs -put /datasets/heart_original.csv /medispark/raw/
docker exec -it namenode hdfs dfs -put /datasets/diabetes_original.csv /medispark/raw/
docker exec -it namenode hdfs dfs -put /datasets/stroke_original.csv /medispark/raw/
docker exec -it namenode hdfs dfs -put /datasets/lung_cancer_original.csv /medispark/raw/

# Verify
docker exec -it namenode hdfs dfs -ls /medispark/raw/
```

You should see all 4 CSV files listed.

---

## STEP 3: Build the JAR

```powershell
cd "D:\SEM - 5\BigData\medispark"
sbt clean assembly
```

This creates: `target/scala-2.12/medispark-assembly-1.0.jar`
The JAR is automatically available in the spark-master container at `/jars/` (mounted via docker-compose).

---

## STEP 4: Run Stage 1 — Data Ingestion

```powershell
docker exec -it spark-master spark-submit ^
  --class medispark.ingestion.DataIngestion ^
  --master spark://spark-master:7077 ^
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 ^
  /jars/medispark-assembly-1.0.jar
```

**Expected output:** Reads 4 CSVs from HDFS, prints row counts and schemas, writes Parquet to `/medispark/ingested/`.

Verify:
```powershell
docker exec -it namenode hdfs dfs -ls /medispark/ingested/
```

---

## STEP 5: Run Stage 2 — Data Cleaning

```powershell
docker exec -it spark-master spark-submit ^
  --class medispark.cleaning.DataCleaning ^
  --master spark://spark-master:7077 ^
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 ^
  /jars/medispark-assembly-1.0.jar
```

**Expected output:** Handles nulls (fills BMI median for stroke, drops id/index columns), removes outliers, prints before/after row counts.

---

## STEP 6: Run Stage 3 — Feature Engineering

```powershell
docker exec -it spark-master spark-submit ^
  --class medispark.features.FeatureEngineering ^
  --master spark://spark-master:7077 ^
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 ^
  /jars/medispark-assembly-1.0.jar
```

**Expected output:** StringIndexes categorical columns (stroke), indexes multi-class target (lung cancer Level), assembles features into vectors, applies StandardScaler.

---

## STEP 7: Run Stage 4 — Model Training

```powershell
docker exec -it spark-master spark-submit ^
  --class medispark.training.ModelTraining ^
  --master spark://spark-master:7077 ^
  --driver-memory 2g ^
  --executor-memory 2g ^
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 ^
  /jars/medispark-assembly-1.0.jar
```

**Expected output:** Trains RandomForest for each disease (100 trees for binary, 150 for lung cancer), prints accuracy/F1 on test set, saves models to HDFS.

⏱️ This may take 5-15 minutes depending on your machine.

---

## STEP 8: Run Stage 5 — Streaming (Optional Demo)

**Terminal 1** — Start the stream generator (in namenode):
```powershell
docker cp "D:\SEM - 5\BigData\medispark\src\main\python\generate_stream.py" namenode:/tmp/
docker exec -it namenode python3 /tmp/generate_stream.py
```

**Terminal 2** — Start the Spark streaming job:
```powershell
docker exec -it spark-master spark-submit ^
  --class medispark.streaming.StreamScoring ^
  --master spark://spark-master:7077 ^
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 ^
  /jars/medispark-assembly-1.0.jar
```

The streaming job runs for 2 minutes, scoring micro-batches as they arrive. Press Ctrl+C in both terminals when done.

---

## STEP 9: Run Stage 6 — Evaluation

```powershell
docker exec -it spark-master spark-submit ^
  --class medispark.evaluation.ModelEvaluation ^
  --master spark://spark-master:7077 ^
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 ^
  /jars/medispark-assembly-1.0.jar
```

**Expected output:** Prints accuracy, F1, precision, recall, AUC-ROC, confusion matrix, and feature importances for all 4 diseases. Saves results as JSON to HDFS.

**IMPORTANT:** After this stage, copy the printed metrics into:
- `webapp/app.py` → `EVALUATION_DATA` dict
- `webapp/static/js/app.js` → `EVAL` object

This updates the webapp with real model performance numbers.

---

## STEP 10: Run the Web Application

```powershell
cd "D:\SEM - 5\BigData\medispark\webapp"
pip install flask reportlab
python app.py
```

Open: **http://localhost:5000**

The webapp has 6 sections:
1. **Dashboard** — Pipeline overview, dataset sizes, 5V mapping
2. **Live Stream** — Real-time patient scoring simulation
3. **Risk Calculator** — Individual prediction with SHAP explainability
4. **Model Performance** — Stage 6 metrics, confusion matrix, feature importance
5. **What-If Analysis** — Interactive parameter sliders with radar chart
6. **Batch Predict** — CSV upload for bulk predictions

---

## STEP 11: Generate Visualization Charts (Optional)

```powershell
cd "D:\SEM - 5\BigData\medispark\src\main\python"
pip install matplotlib numpy
python visualize_results.py
```

Charts saved to `D:\SEM - 5\BigData\visualizations\`.

---

## Project File Structure

```
BigData/
├── docker-compose-medispark.yaml
├── datasets/
│   └── originals/
│       ├── heart_original.csv       (253,681 rows)
│       ├── diabetes_original.csv    (253,681 rows)
│       ├── stroke_original.csv      (500,001 rows)
│       └── lung_cancer_original.csv (1,000,001 rows)
└── medispark/
    ├── build.sbt
    ├── project/
    │   ├── build.properties
    │   └── plugins.sbt
    ├── src/main/
    │   ├── scala/medispark/
    │   │   ├── ingestion/DataIngestion.scala      (Stage 1)
    │   │   ├── cleaning/DataCleaning.scala         (Stage 2)
    │   │   ├── features/FeatureEngineering.scala   (Stage 3)
    │   │   ├── training/ModelTraining.scala         (Stage 4)
    │   │   ├── streaming/StreamScoring.scala        (Stage 5)
    │   │   └── evaluation/ModelEvaluation.scala     (Stage 6)
    │   └── python/
    │       ├── generate_stream.py
    │       └── visualize_results.py
    └── webapp/
        ├── app.py
        ├── templates/index.html
        └── static/
            ├── css/style.css
            └── js/app.js
```

## Tech Stack
- **Processing:** Apache Spark 3.5.1 (Scala 2.12.18)
- **Storage:** Hadoop HDFS 3.2.1 (3 DataNodes)
- **ML:** Spark MLlib RandomForest
- **Streaming:** Spark Structured Streaming
- **Web:** Flask + Chart.js
- **Infrastructure:** Docker Compose (7 containers)

## Datasets (No Augmentation)
| Dataset | Records | Source |
|---------|---------|--------|
| Heart Disease | 253,681 | BRFSS 2015 |
| Diabetes | 253,681 | BRFSS 2015 |
| Stroke | 500,001 | Kaggle |
| Lung Cancer | 1,000,001 | Kaggle |
| **Total** | **2,007,364** | |
