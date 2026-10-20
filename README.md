# MediSpark — Distributed Multi-Disease Clinical Risk Prediction Engine

> **Course:** 23AID302 — Big Data Analytics  
> **Team:** Suman Maitreya &middot; Rajkumar R &middot; Harish K &middot; GRS Kartheek

MediSpark is a distributed clinical risk prediction system built on Apache Spark and Hadoop HDFS. It ingests multi-format healthcare datasets, processes them through a six-stage pipeline, trains RandomForest classifiers for four diseases, and serves predictions through a real-time dashboard — all running on a 7-container Docker cluster.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)  
2. [The 5 V's of Big Data](#the-5-vs-of-big-data)  
3. [Tech Stack](#tech-stack)  
4. [Project Structure](#project-structure)  
5. [Pipeline Stages](#pipeline-stages)  
6. [Datasets](#datasets)  
7. [Model Performance](#model-performance)  
8. [Prerequisites](#prerequisites)  
9. [Setup & Installation](#setup--installation)  
10. [Running the Pipeline](#running-the-pipeline)  
11. [Web Application](#web-application)  
12. [Screenshots](#screenshots)  
13. [License](#license)

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                         Docker Compose Cluster                       │
│                                                                      │
│   ┌───────────┐   ┌───────────┐   ┌───────────┐   ┌───────────┐    │
│   │ datanode1  │   │ datanode2  │   │ datanode3  │   │  namenode  │    │
│   │  (HDFS)   │   │  (HDFS)   │   │  (HDFS)   │   │  (HDFS)   │    │
│   └─────┬─────┘   └─────┬─────┘   └─────┬─────┘   └─────┬─────┘    │
│         └────────────────┴───────────────┴───────────────┘           │
│                              HDFS Layer                              │
│                          (3× replication)                            │
│                                                                      │
│   ┌──────────────┐   ┌────────────────┐   ┌────────────────┐        │
│   │ spark-master │   │ spark-worker-1 │   │ spark-worker-2 │        │
│   │  (Driver)    │   │  (2G, 2 cores) │   │  (2G, 2 cores) │        │
│   └──────┬───────┘   └───────┬────────┘   └───────┬────────┘        │
│          └───────────────────┴────────────────────┘                  │
│                         Spark Compute Layer                          │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
                      ┌─────────────────────┐
                      │   Flask Dashboard   │
                      │   (localhost:5000)   │
                      └─────────────────────┘
```

---

## The 5 V's of Big Data

| V            | How MediSpark Demonstrates It                                                                 |
|--------------|-----------------------------------------------------------------------------------------------|
| **Volume**   | 2 million patient records (500K per disease) + ~11,000 chest X-ray images stored across HDFS  |
| **Velocity** | Spark Structured Streaming ingests a live JSON patient-data feed for real-time scoring         |
| **Variety**  | Multi-format ingestion — CSV, JSON, Parquet, and PNG images in one unified pipeline            |
| **Veracity** | Stage 2 cleaning: null imputation, outlier capping, type validation, duplicate removal         |
| **Value**    | Trained RandomForest classifiers deliver per-patient risk predictions via an interactive webapp |

---

## Tech Stack

| Layer          | Technology                                      |
|----------------|------------------------------------------------ |
| Distributed FS | Hadoop HDFS 3.2.1 (1 NameNode + 3 DataNodes)   |
| Processing     | Apache Spark 3.5.1 + Spark MLlib               |
| Language       | Scala 2.12.18 (all pipeline stages)             |
| ML Algorithm   | RandomForest Classifier (per-disease)           |
| Orchestration  | Docker Compose (7 containers)                   |
| Streaming      | Spark Structured Streaming                      |
| Web App        | Python Flask + Chart.js v4.4.0                  |
| Augmentation   | Custom SMOTE with k-NN interpolation (Python)   |
| Platform       | Windows 11 + WSL2 + Docker Desktop              |

---

## Project Structure

```
BigData/
├── docker-compose-medispark.yaml       # 7-container cluster definition
├── README.md
│
├── datasets/
│   ├── augment_datasets.py             # SMOTE augmentation → 500K rows/dataset
│   ├── convert_formats.py              # CSV → JSON, CSV → Parquet converter
│   ├── extract_xray_features.py        # X-ray pixel feature extraction → Parquet
│   └── originals/
│       ├── heart_original.csv          # Heart Disease UCI (CSV)
│       ├── diabetes_original.json      # Pima Diabetes (JSON Lines)
│       ├── stroke_original.csv         # Stroke Prediction (CSV)
│       ├── lung_cancer_original.parquet # Lung Cancer (Parquet)
│       └── chest_xray/images/          # NIH Chest X-rays (~11K PNGs)
│
├── medispark/
│   ├── build.sbt                       # Scala/Spark build config
│   ├── src/main/scala/medispark/
│   │   ├── ingestion/DataIngestion.scala       # Stage 1 — Multi-format ingest
│   │   ├── cleaning/DataCleaning.scala         # Stage 2 — Veracity checks
│   │   ├── features/FeatureEngineering.scala   # Stage 3 — Feature pipelines
│   │   ├── training/ModelTraining.scala        # Stage 4 — RandomForest training
│   │   ├── streaming/StreamScoring.scala       # Stage 5 — Real-time scoring
│   │   ├── evaluation/ModelEvaluation.scala    # Stage 6 — Full evaluation
│   │   └── imaging/ImageClassification.scala   # Stage 7 — X-ray classification
│   │
│   ├── target/scala-2.12/
│   │   └── medispark-assembly-1.0.jar  # Fat JAR (mounted into Spark container)
│   │
│   └── webapp/
│       ├── app.py                      # Flask backend + risk engine
│       ├── templates/index.html        # Dashboard UI
│       └── static/
│           ├── css/style.css           # Glassmorphic dark theme
│           └── js/app.js              # Chart.js visualizations + dock nav
```

---

## Pipeline Stages

```
  CSV / JSON / Parquet / PNG
            │
            ▼
  ┌─────────────────────┐
  │  Stage 1: Ingestion  │   Reads CSV, JSON, Parquet into unified DataFrames
  │  DataIngestion.scala │   Stores raw data in /medispark/ingested/ on HDFS
  └──────────┬──────────┘
             ▼
  ┌─────────────────────┐
  │  Stage 2: Cleaning   │   Null imputation, outlier capping, type casting,
  │  DataCleaning.scala  │   duplicate removal → /medispark/cleaned/
  └──────────┬──────────┘
             ▼
  ┌─────────────────────┐
  │  Stage 3: Features   │   StringIndexer, VectorAssembler, StandardScaler
  │  FeatureEngineering  │   per disease → /medispark/features/
  └──────────┬──────────┘
             ▼
  ┌─────────────────────┐
  │  Stage 4: Training   │   RandomForest classifiers (one per disease)
  │  ModelTraining.scala │   80/20 train-test split → /medispark/models/
  └──────────┬──────────┘
             ▼
  ┌─────────────────────┐
  │  Stage 5: Streaming  │   Structured Streaming reads a live JSON feed,
  │  StreamScoring.scala │   applies saved models for real-time predictions
  └──────────┬──────────┘
             ▼
  ┌─────────────────────┐
  │  Stage 6: Evaluation │   Accuracy, F1, Precision, Recall, AUC-ROC,
  │  ModelEvaluation     │   confusion matrices, feature importances
  └──────────┬──────────┘
             ▼
  ┌──────────────────────────┐
  │  Stage 7: X-ray Classify  │   Reads extracted pixel features from Parquet,
  │  ImageClassification      │   trains Normal vs Abnormal RandomForest
  └──────────────────────────┘
```

Each stage is a standalone Scala `object` with its own `main` method, submitted independently via `spark-submit`. Stage 7 uses a Python preprocessing script (`extract_xray_features.py`) to extract pixel statistics from chest X-ray images before Spark training.

---

## Datasets

| Disease       | Source                    | Original Size | Augmented Size | Format  |
|---------------|---------------------------|---------------|----------------|---------|
| Heart Disease | UCI Heart Disease         | 303           | 500,000        | CSV     |
| Diabetes      | Pima Indians Diabetes     | 768           | 500,000        | JSON    |
| Stroke        | Kaggle Stroke Prediction  | 5,110         | 500,000        | CSV     |
| Lung Cancer   | Kaggle Lung Cancer Survey | 1,000         | 500,000        | Parquet |
| Chest X-rays  | NIH ChestX-ray14         | 5,606 labeled | —              | PNG     |

Augmentation uses a custom SMOTE implementation with k-NN interpolation (k=5) to scale each tabular dataset to 500K records while preserving class distributions. Chest X-ray images (5,606 labeled from NIH ChestX-ray14) are preprocessed into pixel-level statistical features and fed through a Spark-trained RandomForest for binary classification (Normal vs Abnormal).

**Total HDFS storage:** ~13.3 GB (with 3× replication across 3 DataNodes)

---

## Model Performance

Results from Stage 6 evaluation on 500K rows per dataset (80/20 leakage-safe split):

| Disease       | Accuracy | F1 Score | Precision | Recall  | AUC-ROC |
|---------------|----------|----------|-----------|---------|---------|
| Heart Disease | 99.60%   | 99.60%   | 99.60%    | 99.60%  | 99.99%  |
| Diabetes      | 97.45%   | 97.44%   | 97.47%    | 97.45%  | 99.70%  |
| Stroke        | 92.13%   | 89.55%   | 92.75%    | 92.13%  | 94.57%  |
| Lung Cancer   | 93.57%   | 93.57%   | 93.58%    | 93.57%  | —       |

**Note on Stroke recall:** The Stroke class has a recall of 17.89% due to severe class imbalance (~95% non-stroke in the original dataset). The model heavily favors the majority class, which is a known characteristic of tree-based classifiers on highly imbalanced datasets.

### Chest X-ray Classification (Stage 7)

Binary classification on 5,606 labeled NIH ChestX-ray14 images (Normal vs Abnormal). Features are extracted from 64×64 grayscale images: pixel statistics (mean, std, skewness, kurtosis), quadrant means, histogram bins, edge density, entropy, and contrast. A RandomForest classifier (100 trees, maxDepth=10) is trained on these 27 features. Metrics are updated after running Stage 7.

---

## Prerequisites

- **Docker Desktop** (with WSL2 backend on Windows)
- **WSL2** with Ubuntu (Windows)
- **sbt** (Scala Build Tool) — for compiling the fat JAR
- **Python 3.8+** — for augmentation scripts and the Flask webapp
- **~15 GB free disk space** — for HDFS storage with 3× replication

### Python Dependencies

```bash
pip install flask pandas numpy scikit-learn pyarrow Pillow
```

---

## Setup & Installation

### 1. Clone the repository

```bash
git clone <repository-url>
cd BigData
```

### 2. Start the Docker cluster

```bash
docker compose -f docker-compose-medispark.yaml up -d
```

Wait ~30 seconds for all 7 containers to initialize. Verify with:

```bash
docker ps    # Should show: namenode, datanode1-3, spark-master, spark-worker-1, spark-worker-2
```

### 3. Prepare datasets

```bash
cd datasets
python augment_datasets.py          # Augments all 4 datasets to 500K rows
python convert_formats.py           # Converts diabetes → JSON, lung cancer → Parquet
python extract_xray_features.py     # Extracts pixel features from X-ray images → Parquet
```

### 4. Upload datasets to HDFS

```bash
docker exec -it namenode hdfs dfs -mkdir -p /medispark/raw
docker exec -it namenode hdfs dfs -put /datasets/heart_original.csv /medispark/raw/
docker exec -it namenode hdfs dfs -put /datasets/diabetes_original.json /medispark/raw/
docker exec -it namenode hdfs dfs -put /datasets/stroke_original.csv /medispark/raw/
docker exec -it namenode hdfs dfs -put /datasets/lung_cancer_original.parquet /medispark/raw/
```

### 5. Build the fat JAR

```bash
cd medispark
sbt assembly
```

This produces `target/scala-2.12/medispark-assembly-1.0.jar`, which is volume-mounted into the Spark container at `/jars/`.

---

## Running the Pipeline

Execute each stage sequentially. Each stage depends on the output of the previous one.

```bash
# Stage 1 — Ingestion
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g --executor-memory 1g --executor-cores 2 \
  --class medispark.ingestion.DataIngestion /jars/medispark-assembly-1.0.jar

# Stage 2 — Cleaning
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g --executor-memory 1g --executor-cores 2 \
  --class medispark.cleaning.DataCleaning /jars/medispark-assembly-1.0.jar

# Stage 3 — Feature Engineering
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g --executor-memory 1g --executor-cores 2 \
  --class medispark.features.FeatureEngineering /jars/medispark-assembly-1.0.jar

# Stage 4 — Model Training (longest: 10-20 min)
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g --executor-memory 1g --executor-cores 2 \
  --class medispark.training.ModelTraining /jars/medispark-assembly-1.0.jar

# Stage 5 — Streaming (requires generate_stream.py running in parallel)
# In a separate terminal:
#   cd medispark/webapp && python generate_stream.py
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g --executor-memory 1g --executor-cores 2 \
  --class medispark.streaming.StreamScoring /jars/medispark-assembly-1.0.jar

# Stage 6 — Evaluation
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g --executor-memory 1g --executor-cores 2 \
  --class medispark.evaluation.ModelEvaluation /jars/medispark-assembly-1.0.jar

# Stage 7 — X-ray Image Classification
# (requires extract_xray_features.py output uploaded to HDFS first)
docker exec -it namenode hdfs dfs -mkdir -p /medispark/xray_features
docker exec -it namenode hdfs dfs -put /datasets/xray_features.parquet /medispark/xray_features/
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g --executor-memory 1g --executor-cores 2 \
  --class medispark.imaging.ImageClassification /jars/medispark-assembly-1.0.jar
```

Each stage prints `STAGE X COMPLETE` on success. If a stage runs out of memory, reduce executor memory to `512m` or restart the Docker containers.

---

## Web Application

The Flask dashboard provides an interactive interface to the trained models.

### Start the webapp

```bash
cd medispark/webapp
python app.py
```

Open **http://localhost:5000** in your browser.

### Dashboard Features

| Section              | Description                                                        |
|----------------------|--------------------------------------------------------------------|
| **Dashboard**        | Overview cards: total records, diseases, pipeline stages, HDFS size|
| **Streaming**        | Live feed visualization of simulated patient data                  |
| **Risk Calculator**  | Interactive form — enter patient vitals, get per-disease risk %    |
| **Model Performance**| Per-disease accuracy, feature importance, confusion matrices       |
| **What-If Analysis** | Adjust patient parameters and see risk change in real time         |
| **X-ray Analysis**   | Upload a chest X-ray image for Normal/Abnormal classification      |
| **Batch Processing** | Upload a CSV of patients for bulk risk scoring                     |

The webapp runs in two modes: **LIVE** (connects to Spark/HDFS when Docker is running) and **STANDALONE** (uses pre-computed Stage 6 metrics when the cluster is down).

---

## Monitoring & Admin UIs

| Service        | URL                    | Purpose                                |
|----------------|------------------------|----------------------------------------|
| HDFS NameNode  | http://localhost:9870  | File browser, DataNode health, storage |
| Spark Master   | http://localhost:8080  | Running/completed jobs, worker status  |
| MediSpark App  | http://localhost:5000  | Clinical risk prediction dashboard     |

---

## Troubleshooting

| Problem                           | Solution                                                                       |
|-----------------------------------|--------------------------------------------------------------------------------|
| Stage gets killed / OOM           | Lower memory: `--executor-memory 512m`; restart Docker containers              |
| "Connection refused" to NameNode  | `docker restart namenode datanode1 datanode2 datanode3`, wait 30s               |
| spark-submit hangs                | Check `docker ps`; restart Spark: `docker compose restart spark-master`        |
| Webapp shows "STANDALONE" mode    | Docker cluster not running; start it with `docker compose up -d`               |
| HDFS "No such file or directory"  | Previous pipeline stage didn't complete — rerun it                             |

---

## License

This project was developed for academic purposes as part of the Big Data Analytics course (23AID302). Not intended for clinical use.
