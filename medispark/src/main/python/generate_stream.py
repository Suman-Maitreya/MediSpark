"""
generate_stream.py
Stage 5 of MediSpark — Synthetic Patient Stream Generator

What this does:
  Every 5 seconds, generates a random patient record with realistic
  medical values for all 4 diseases and writes it as a JSON file
  to HDFS at /medispark/stream/input/

  The PatientStreamScorer (Scala) watches this directory and scores
  each patient against all 4 disease models in real time.

Usage:
  Run this INSIDE the namenode container:
    python3 /tmp/generate_stream.py

  Or from your host machine:
    docker exec -it namenode python3 /tmp/generate_stream.py
"""

import json
import random
import time
import subprocess
import os
from datetime import datetime

# Patient name pools for realistic IDs
FIRST_NAMES = [
    "Arun", "Priya", "Rahul", "Sneha", "Vikram", "Ananya", "Karthik", "Divya",
    "Suresh", "Meera", "Arjun", "Lakshmi", "Ravi", "Deepa", "Sanjay", "Kavitha",
    "Amit", "Pooja", "Rajesh", "Nandini", "Mohan", "Swathi", "Ganesh", "Revathi",
    "Harish", "Anjali", "Prasad", "Bhavana", "Manoj", "Sangeetha"
]

LAST_NAMES = [
    "Sharma", "Patel", "Kumar", "Reddy", "Nair", "Iyer", "Gupta", "Singh",
    "Menon", "Pillai", "Rao", "Das", "Joshi", "Verma", "Mehta", "Shah",
    "Mishra", "Choudhury", "Pandey", "Agarwal"
]


def generate_patient():
    """
    Generate a single synthetic patient record with realistic medical values.

    The record contains fields for ALL 4 diseases:
    - Heart disease / Diabetes (shared BRFSS survey fields)
    - Stroke (demographic + clinical fields)
    - Lung cancer (symptom severity scores 1-10)
    """

    # Generate a patient ID like "PAT-Arun-Sharma-1234"
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    patient_id = f"PAT-{first}-{last}-{random.randint(1000, 9999)}"

    # Current timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # --- Shared fields for Heart Disease & Diabetes (BRFSS format) ---
    # These are binary (0/1) or integer-coded values from health surveys
    age_category = random.randint(1, 13)       # 1=18-24, 13=80+
    sex = random.choice([0.0, 1.0])            # 0=Female, 1=Male
    bmi = round(random.uniform(15.0, 50.0), 1) # Body Mass Index

    patient = {
        "patient_id": patient_id,
        "timestamp": timestamp,

        # --- Heart/Diabetes shared fields ---
        "HighBP": random.choice([0.0, 1.0]),           # High blood pressure
        "HighChol": random.choice([0.0, 1.0]),         # High cholesterol
        "CholCheck": random.choice([0.0, 1.0]),        # Cholesterol check in last 5 years
        "BMI": bmi,
        "Smoker": random.choice([0.0, 1.0]),           # Smoked 100+ cigarettes lifetime
        "Stroke": random.choice([0.0, 1.0]),           # Ever had a stroke
        "Diabetes": random.choice([0.0, 1.0, 2.0]),    # 0=No, 1=Yes, 2=Pre-diabetes
        "HeartDiseaseorAttack": random.choice([0.0, 1.0]),
        "PhysActivity": random.choice([0.0, 1.0]),     # Physical activity in last 30 days
        "Fruits": random.choice([0.0, 1.0]),           # Eats fruit daily
        "Veggies": random.choice([0.0, 1.0]),          # Eats veggies daily
        "HvyAlcoholConsump": random.choice([0.0, 1.0]),
        "AnyHealthcare": random.choice([0.0, 1.0]),    # Has health insurance
        "NoDocbcCost": random.choice([0.0, 1.0]),      # Couldn't see doc due to cost
        "GenHlth": float(random.randint(1, 5)),        # General health 1=Excellent to 5=Poor
        "MentHlth": float(random.randint(0, 30)),      # Days of poor mental health
        "PhysHlth": float(random.randint(0, 30)),      # Days of poor physical health
        "DiffWalk": random.choice([0.0, 1.0]),         # Difficulty walking
        "Sex": sex,
        "Age": float(age_category),
        "Education": float(random.randint(1, 6)),      # 1=Never attended to 6=College grad
        "Income": float(random.randint(1, 8)),         # 1=<$10k to 8=>$75k

        # --- Stroke-specific fields ---
        "gender": random.choice(["Male", "Female"]),
        "age_stroke": round(random.uniform(20.0, 85.0), 1),
        "hypertension": random.choice([0.0, 1.0]),
        "heart_disease": random.choice([0.0, 1.0]),
        "ever_married": random.choice(["Yes", "No"]),
        "work_type": random.choice(["Private", "Self-employed", "Govt_job", "children"]),
        "Residence_type": random.choice(["Urban", "Rural"]),
        "avg_glucose_level": round(random.uniform(55.0, 280.0), 2),
        "bmi_stroke": round(random.uniform(15.0, 50.0), 1),
        "smoking_status": random.choice(["formerly smoked", "never smoked", "smokes", "Unknown"]),

        # --- Lung cancer fields (symptom severity 1-10) ---
        "Gender_lung": random.choice([1.0, 2.0]),      # 1=Male, 2=Female
        "Air_Pollution": float(random.randint(1, 8)),
        "Alcohol_use": float(random.randint(1, 8)),
        "Dust_Allergy": float(random.randint(1, 8)),
        "OccuPational_Hazards": float(random.randint(1, 8)),
        "Genetic_Risk": float(random.randint(1, 8)),
        "chronic_Lung_Disease": float(random.randint(1, 7)),
        "Balanced_Diet": float(random.randint(1, 7)),
        "Obesity": float(random.randint(1, 7)),
        "Smoking_lung": float(random.randint(1, 8)),
        "Passive_Smoker": float(random.randint(1, 8)),
        "Chest_Pain": float(random.randint(1, 9)),
        "Coughing_of_Blood": float(random.randint(1, 9)),
        "Fatigue": float(random.randint(1, 9)),
        "Weight_Loss": float(random.randint(1, 8)),
        "Shortness_of_Breath": float(random.randint(1, 9)),
        "Wheezing": float(random.randint(1, 8)),
        "Swallowing_Difficulty": float(random.randint(1, 8)),
        "Clubbing_of_Finger_Nails": float(random.randint(1, 9)),
        "Frequent_Cold": float(random.randint(1, 7)),
        "Dry_Cough": float(random.randint(1, 7)),
        "Snoring": float(random.randint(1, 7)),
    }

    return patient


def write_to_hdfs(patient, file_number):
    """
    Write a patient JSON record to HDFS.
    Uses hdfs dfs -put to upload the file.
    """
    # Write to a temporary local file first
    local_path = f"/tmp/patient_{file_number}.json"
    hdfs_path = f"/medispark/stream/input/patient_{file_number}.json"

    with open(local_path, "w") as f:
        json.dump(patient, f)

    # Upload to HDFS (overwrite if exists)
    subprocess.run(
        ["hdfs", "dfs", "-put", "-f", local_path, hdfs_path],
        capture_output=True, text=True
    )

    # Clean up local temp file
    os.remove(local_path)

    return hdfs_path


def main():
    print("=" * 60)
    print("  MediSpark — Patient Stream Generator")
    print("=" * 60)
    print("  Generating a new patient record every 5 seconds")
    print("  Writing to: /medispark/stream/input/")
    print("  Press Ctrl+C to stop")
    print("=" * 60)

    file_number = 1

    try:
        while True:
            # Generate a patient
            patient = generate_patient()

            # Write to HDFS
            hdfs_path = write_to_hdfs(patient, file_number)

            # Print confirmation
            print(f"\n  [{patient['timestamp']}] Patient #{file_number}")
            print(f"    ID: {patient['patient_id']}")
            print(f"    Written to: {hdfs_path}")

            file_number += 1

            # Wait 5 seconds before generating the next patient
            time.sleep(5)

    except KeyboardInterrupt:
        print(f"\n\n  Stream generator stopped. Generated {file_number - 1} patients.")


if __name__ == "__main__":
    main()
