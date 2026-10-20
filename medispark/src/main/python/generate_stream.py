#!/usr/bin/env python3
"""
MediSpark — Stream Generator
Produces synthetic patient CSV micro-batches for Spark Structured Streaming.
Run inside the namenode container to write directly to HDFS.
"""

import subprocess
import random
import time
import os
from datetime import datetime

HDFS_STREAM_DIR = "/medispark/stream_input"
BATCH_SIZE = 5
INTERVAL_SEC = 10

FIRST_NAMES = [
    "Aarav","Vivaan","Aditya","Vihaan","Arjun","Sai","Reyansh","Krishna",
    "Ishaan","Shaurya","Ananya","Diya","Priya","Riya","Kavya","Meera",
    "Neha","Pooja","Shreya","Tanvi","Rahul","Amit","Suresh","Vikram","Kiran"
]
LAST_NAMES = [
    "Sharma","Patel","Kumar","Singh","Reddy","Nair","Gupta","Joshi",
    "Iyer","Menon","Das","Rao","Chopra","Mishra","Verma","Agarwal"
]

def generate_patient():
    pid = f"PAT-{random.choice(FIRST_NAMES)}-{random.choice(LAST_NAMES)}-{random.randint(1000,9999)}"
    age = random.randint(18, 85)
    gender = random.choice([0.0, 1.0])
    bmi = round(random.uniform(16.0, 45.0), 1)
    bp = round(random.uniform(80, 200), 0)
    chol = round(random.uniform(120, 320), 0)
    glucose = round(random.uniform(60, 250), 1)
    smoking = random.choice([0.0, 1.0])
    exercise = round(random.uniform(0, 10), 1)
    hypertension = random.choice([0.0, 1.0])
    heart_hist = random.choice([0.0, 1.0])
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return f"{pid},{ts},{age},{gender},{bmi},{bp},{chol},{glucose},{smoking},{exercise},{hypertension},{heart_hist}"

def write_to_hdfs(filename, content):
    """Write content to HDFS via hdfs dfs -put"""
    local_path = f"/tmp/{filename}"
    with open(local_path, "w") as f:
        f.write(content)
    subprocess.run(
        ["hdfs", "dfs", "-put", "-f", local_path, f"{HDFS_STREAM_DIR}/{filename}"],
        capture_output=True
    )
    os.remove(local_path)

def main():
    print("=" * 60)
    print("  MediSpark — Stream Generator")
    print(f"  Batch size: {BATCH_SIZE} patients every {INTERVAL_SEC}s")
    print(f"  HDFS path:  {HDFS_STREAM_DIR}")
    print("=" * 60)

    # Create HDFS directory
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", HDFS_STREAM_DIR], capture_output=True)

    batch_num = 0
    try:
        while True:
            batch_num += 1
            header = "patient_id,timestamp,age,gender,bmi,blood_pressure,cholesterol,glucose,smoking,exercise,hypertension,heart_disease_history"
            rows = [header] + [generate_patient() for _ in range(BATCH_SIZE)]
            content = "\n".join(rows) + "\n"

            filename = f"batch_{batch_num:04d}_{int(time.time())}.csv"
            write_to_hdfs(filename, content)

            print(f"  Batch {batch_num}: {BATCH_SIZE} patients -> {filename}")
            time.sleep(INTERVAL_SEC)

    except KeyboardInterrupt:
        print(f"\n  Stopped after {batch_num} batches.")

if __name__ == "__main__":
    main()
