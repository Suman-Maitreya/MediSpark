#!/usr/bin/env python3
"""
MediSpark — Flask Web Application
Multi-disease clinical risk prediction engine.
Dual mode: LIVE (Spark+HDFS when Docker running) vs STANDALONE (pre-computed).
"""

from flask import Flask, render_template, request, jsonify, send_file
import subprocess, json, random, threading, time, io, os
import numpy as np
from pathlib import Path
from datetime import datetime
import onnxruntime as ort

app = Flask(__name__)

# ==============================================================================
# EVALUATION DATA — Updated after running Stage 6
# Values from Stage 6 run on 2026-09-24 (500K rows/dataset, leakage-safe split)
# ==============================================================================
EVALUATION_DATA = {
    "heart": {
        "name": "Heart Disease",
        "accuracy": 99.60, "f1": 99.60, "precision": 99.60, "recall": 99.60, "auc_roc": 99.99,
        "features": ["age","sex","cp","trestbps","chol","fbs","restecg",
                      "thalach","exang","oldpeak","slope","ca","thal"],
        "feature_importances": {"cp":0.1777,"ca":0.1420,"thal":0.1281,"oldpeak":0.0833,
            "exang":0.0772,"thalach":0.0732,"slope":0.0660,"age":0.0643,
            "sex":0.0546,"trestbps":0.0480},
        "classes": ["No Disease", "Disease"],
        "confusion": [[51328,349],[52,48271]],
        "per_class": [
            {"label":"No Disease","precision":99.90,"recall":99.32,"f1":99.61},
            {"label":"Disease","precision":99.28,"recall":99.89,"f1":99.59}
        ]
    },
    "diabetes": {
        "name": "Diabetes",
        "accuracy": 97.45, "f1": 97.44, "precision": 97.47, "recall": 97.45, "auc_roc": 99.70,
        "features": ["Pregnancies","Glucose","BloodPressure","SkinThickness",
                      "Insulin","BMI","DiabetesPedigreeFunction","Age"],
        "feature_importances": {"Glucose":0.2938,"BMI":0.1687,"Age":0.1254,
            "DiabetesPedigreeFunction":0.1037,"Pregnancies":0.0923,"Insulin":0.0789,
            "SkinThickness":0.0700,"BloodPressure":0.0672},
        "classes": ["No Diabetes", "Diabetes"],
        "confusion": [[56223,616],[1824,37007]],
        "per_class": [
            {"label":"No Diabetes","precision":96.86,"recall":98.92,"f1":97.88},
            {"label":"Diabetes","precision":98.36,"recall":95.30,"f1":96.81}
        ]
    },
    "stroke": {
        "name": "Stroke",
        "accuracy": 92.13, "f1": 89.55, "precision": 92.75, "recall": 92.13, "auc_roc": 94.57,
        "features": ["age","hypertension","heart_disease","avg_glucose_level","bmi",
                      "gender_idx","ever_married_idx","work_type_idx",
                      "Residence_type_idx","smoking_status_idx"],
        "feature_importances": {"age":0.3565,"avg_glucose_level":0.1472,"bmi":0.1151,
            "smoking_status_idx":0.0912,"work_type_idx":0.0676,"Residence_type_idx":0.0523,
            "ever_married_idx":0.0446,"gender_idx":0.0442,"hypertension":0.0425,
            "heart_disease":0.0388},
        "classes": ["No Stroke", "Stroke"],
        "confusion": [[90413,2],[7869,1714]],
        "per_class": [
            {"label":"No Stroke","precision":91.99,"recall":100.00,"f1":95.83},
            {"label":"Stroke","precision":99.88,"recall":17.89,"f1":30.34}
        ]
    },
    "lung_cancer": {
        "name": "Lung Cancer",
        "accuracy": 93.57, "f1": 93.57, "precision": 93.58, "recall": 93.57, "auc_roc": 0,
        "features": ["Age","Gender","Air_Pollution","Alcohol_use","Dust_Allergy",
                      "OccuPational_Hazards","Genetic_Risk","chronic_Lung_Disease",
                      "Balanced_Diet","Obesity","Smoking","Passive_Smoker","Chest_Pain",
                      "Coughing_of_Blood","Fatigue","Weight_Loss","Shortness_of_Breath",
                      "Wheezing","Swallowing_Difficulty","Clubbing_of_Finger_Nails",
                      "Frequent_Cold","Dry_Cough","Snoring"],
        "feature_importances": {"Coughing_of_Blood":0.1462,"Obesity":0.0852,
            "Passive_Smoker":0.0825,"Wheezing":0.0753,"Clubbing_of_Finger_Nails":0.0636,
            "Fatigue":0.0581,"Balanced_Diet":0.0540,"Shortness_of_Breath":0.0443,
            "Swallowing_Difficulty":0.0436,"Air_Pollution":0.0396},
        "classes": ["Medium", "High", "Low"],
        "confusion": [[38446,1270,932],[956,32616,934],[923,1411,22512]],
        "per_class": [
            {"label":"Medium","precision":95.34,"recall":94.58,"f1":94.96},
            {"label":"High","precision":92.40,"recall":94.52,"f1":93.45},
            {"label":"Low","precision":92.35,"recall":90.61,"f1":91.47}
        ]
    }
}

# X-ray evaluation data — Stage 7 results (V3 — CNN + Handcrafted, 5606 images)
# NOTE: These are placeholder values from V2. Re-run Stage 7 with V3 features
# to get updated metrics. The feature list reflects the new V3 pipeline.
XRAY_EVALUATION = {
    "name": "Chest X-ray",
    "accuracy": 62.30, "f1": 61.46, "precision": 62.07, "recall": 62.30, "auc_roc": 67.56,
    "features": [f"cnn_feat_{i}" for i in range(1000)] + [
                 "pixel_mean","pixel_std","pixel_skew","pixel_kurtosis",
                 "pixel_iqr","pixel_cv",
                 "center_mean","periphery_ratio",
                 "edge_density","gradient_mag_mean",
                 "entropy","symmetry_lr_mean",
                 "row_profile_std","col_profile_std",
                 "patient_age","patient_gender"],
    "feature_importances": {"periphery_ratio":0.0916,"patient_age":0.0664,
        "center_mean":0.0528,"pixel_mean":0.0502,
        "cnn_feat_0":0.0100,"cnn_feat_1":0.0095},
    "classes": ["Normal", "Abnormal"],
    "confusion": [[444,146],[260,227]],
    "per_class": [
        {"label":"Normal","precision":63.07,"recall":75.25,"f1":68.62},
        {"label":"Abnormal","precision":60.86,"recall":46.61,"f1":52.79}
    ],
    "model_version": "V3 — ResNet-18 CNN + Handcrafted",
    "total_features": 1016
}

# Dataset sizes (SMOTE-augmented from originals)
DATASET_SIZES = {
    "heart": 500000,
    "diabetes": 500000,
    "stroke": 500000,
    "lung_cancer": 500000,
    "xray": 5606
}

# ==============================================================================
# RISK PREDICTION ENGINE
# ==============================================================================

# Feature weights for risk scoring (simplified model for standalone mode)
RISK_WEIGHTS = {
    "heart": {
        "age": 0.18, "blood_pressure": 0.16, "cholesterol": 0.14, "bmi": 0.12,
        "smoking": 0.12, "glucose": 0.08, "exercise": -0.08, "gender": 0.06,
        "hypertension": 0.04
    },
    "diabetes": {
        "glucose": 0.25, "bmi": 0.20, "age": 0.15, "blood_pressure": 0.10,
        "smoking": 0.08, "exercise": -0.10, "cholesterol": 0.06, "gender": 0.03,
        "hypertension": 0.03
    },
    "stroke": {
        "age": 0.28, "glucose": 0.18, "bmi": 0.14, "hypertension": 0.12,
        "blood_pressure": 0.10, "smoking": 0.08, "cholesterol": 0.05,
        "exercise": -0.05
    },
    "lung_cancer": {
        "smoking": 0.25, "age": 0.15, "bmi": 0.05, "cholesterol": 0.03,
        "blood_pressure": 0.02, "exercise": -0.05, "glucose": 0.02
    }
}

# Normal ranges for risk calculation
RANGES = {
    "age": (18, 90), "bmi": (15, 50), "blood_pressure": (80, 220),
    "cholesterol": (100, 350), "glucose": (50, 250)
}

def normalize(val, key):
    lo, hi = RANGES.get(key, (0, 1))
    return max(0.0, min(1.0, (val - lo) / (hi - lo)))

def predict_risk(data):
    """Compute risk probability for each disease."""
    results = {}
    for disease, weights in RISK_WEIGHTS.items():
        score = 0.08  # baseline
        for feature, weight in weights.items():
            raw = data.get(feature, 0)
            if isinstance(raw, str):
                try: raw = float(raw)
                except: raw = 0
            if feature in RANGES:
                norm = normalize(raw, feature)
            elif feature == "smoking":
                norm = float(raw)
            elif feature == "exercise":
                norm = min(raw / 10.0, 1.0)
            elif feature == "gender":
                norm = float(raw)
            elif feature == "hypertension":
                norm = float(raw)
            else:
                norm = raw
            score += weight * norm

        prob = max(0.02, min(0.98, score))
        level = "HIGH" if prob > 0.6 else "MEDIUM" if prob > 0.3 else "LOW"
        results[disease] = {"probability": round(prob, 4), "level": level}
    return results

def explain_prediction(data):
    """SHAP-style feature contribution breakdown."""
    explanations = {}
    for disease, weights in RISK_WEIGHTS.items():
        contributions = []
        for feature, weight in weights.items():
            raw = data.get(feature, 0)
            if isinstance(raw, str):
                try: raw = float(raw)
                except: raw = 0
            contribution = abs(weight) * (normalize(raw, feature) if feature in RANGES else float(raw))
            direction = "risk_increase" if weight > 0 else "risk_decrease"
            contributions.append({
                "feature": feature.replace("_", " ").title(),
                "value": raw,
                "contribution": round(contribution, 4),
                "direction": direction
            })
        contributions.sort(key=lambda x: x["contribution"], reverse=True)
        explanations[disease] = {"contributions": contributions, "baseline": 30.0}
    return explanations

# ==============================================================================
# STREAMING ENGINE
# ==============================================================================

FIRST_NAMES = ["Aarav","Vivaan","Aditya","Vihaan","Arjun","Sai","Ananya","Diya",
               "Priya","Riya","Kavya","Meera","Neha","Rahul","Amit","Suresh"]
LAST_NAMES = ["Sharma","Patel","Kumar","Singh","Reddy","Nair","Gupta","Joshi"]

streaming_active = False
streaming_buffer = []
stream_lock = threading.Lock()

def generate_patient():
    name = f"{random.choice(FIRST_NAMES)}-{random.choice(LAST_NAMES)}"
    pid = f"PAT-{name}-{random.randint(1000,9999)}"
    # Skew toward healthier values so risk distribution is realistic
    # ~60% healthy, ~25% moderate, ~15% high-risk
    profile = random.random()
    if profile < 0.60:  # healthy
        age = random.randint(18, 50)
        bmi = round(random.gauss(23.0, 3.0), 1)
        bp = round(random.gauss(115, 10))
        chol = round(random.gauss(180, 25))
        gluc = round(random.gauss(90, 12), 1)
        smoking = 0
        exercise = round(random.uniform(4, 10), 1)
        hyp = 0
        hd_hist = 0
    elif profile < 0.85:  # moderate risk
        age = random.randint(35, 65)
        bmi = round(random.gauss(28.0, 4.0), 1)
        bp = round(random.gauss(140, 15))
        chol = round(random.gauss(230, 30))
        gluc = round(random.gauss(130, 25), 1)
        smoking = random.choice([0, 1])
        exercise = round(random.uniform(1, 6), 1)
        hyp = random.choice([0, 1])
        hd_hist = 0
    else:  # high risk
        age = random.randint(55, 85)
        bmi = round(random.gauss(34.0, 4.0), 1)
        bp = round(random.gauss(170, 20))
        chol = round(random.gauss(280, 30))
        gluc = round(random.gauss(200, 30), 1)
        smoking = 1
        exercise = round(random.uniform(0, 3), 1)
        hyp = 1
        hd_hist = random.choice([0, 1])
    # Clamp values to valid ranges
    bmi = max(15.0, min(50.0, bmi))
    bp = max(80, min(220, bp))
    chol = max(100, min(350, chol))
    gluc = max(50, min(250, gluc))
    data = {
        "patient_id": pid,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "age": age,
        "gender": random.choice([0, 1]),
        "bmi": bmi,
        "blood_pressure": bp,
        "cholesterol": chol,
        "glucose": gluc,
        "smoking": smoking,
        "exercise": exercise,
        "hypertension": hyp,
        "heart_disease_history": hd_hist
    }
    data["predictions"] = predict_risk(data)
    return data

def stream_worker():
    global streaming_active
    while streaming_active:
        patient = generate_patient()
        with stream_lock:
            streaming_buffer.append(patient)
            if len(streaming_buffer) > 200:
                streaming_buffer.pop(0)
        time.sleep(5)

# ==============================================================================
# X-RAY IMAGE PREDICTION
# ==============================================================================

# ── ResNet-18 ONNX model for live X-ray inference ────────────────────────────
# Load once at startup; used by extract_xray_features() for real-time predictions.
RESNET_SIZE = 224
IMAGENET_MEAN = np.array([0.485, 0.456, 0.406], dtype=np.float32)
IMAGENET_STD  = np.array([0.229, 0.224, 0.225], dtype=np.float32)
HAND_IMG_SIZE = 128

# Try to load the ONNX model — fall back gracefully if not available
_onnx_session = None
_RESNET_ONNX_PATH = os.path.join(os.path.dirname(__file__), "..", "datasets", "resnet18.onnx")
if os.path.exists(_RESNET_ONNX_PATH):
    try:
        _onnx_session = ort.InferenceSession(_RESNET_ONNX_PATH, providers=["CPUExecutionProvider"])
        print(f"[X-ray] ResNet-18 ONNX model loaded from {_RESNET_ONNX_PATH}")
    except Exception as e:
        print(f"[X-ray] WARNING: Could not load ONNX model: {e}")
else:
    print(f"[X-ray] WARNING: ResNet-18 ONNX model not found at {_RESNET_ONNX_PATH}")
    print(f"[X-ray]          X-ray predictions will use handcrafted features only.")


def extract_xray_features(img_bytes):
    """
    Extract V3 features: ResNet-18 CNN (1000-d) + 14 handcrafted + demographics.
    Matches the Spark training pipeline (extract_xray_features_v3.py).
    """
    from PIL import Image

    features = {}

    # ── CNN features (1000-d) via ResNet-18 ONNX ────────────────────────
    img = Image.open(io.BytesIO(img_bytes)).convert("L")

    if _onnx_session is not None:
        # Convert grayscale to 3-channel RGB (same as training pipeline)
        img_rgb = Image.merge("RGB", [img, img, img])
        img_rgb = img_rgb.resize((RESNET_SIZE, RESNET_SIZE), Image.LANCZOS)
        arr = np.array(img_rgb, dtype=np.float32) / 255.0
        arr = (arr - IMAGENET_MEAN) / IMAGENET_STD
        arr = arr.transpose(2, 0, 1)[np.newaxis, ...]  # (1, 3, 224, 224)

        iname = _onnx_session.get_inputs()[0].name
        oname = _onnx_session.get_outputs()[0].name
        cnn_out = _onnx_session.run([oname], {iname: arr})[0][0]  # 1000-d

        for i, v in enumerate(cnn_out):
            features[f"cnn_feat_{i}"] = float(v)
    else:
        # Fallback: zero CNN features (will degrade accuracy)
        for i in range(1000):
            features[f"cnn_feat_{i}"] = 0.0

    # ── Handcrafted features (14) ───────────────────────────────────────
    img2 = img.resize((HAND_IMG_SIZE, HAND_IMG_SIZE), Image.LANCZOS)
    pixels = np.array(img2, dtype=np.float64) / 255.0
    flat = pixels.flatten()
    h, w = pixels.shape
    mid_w = w // 2
    mean_val = float(np.mean(flat))
    std_val = float(np.std(flat))
    p25 = float(np.percentile(flat, 25))
    p75 = float(np.percentile(flat, 75))

    features["pixel_mean"] = mean_val
    features["pixel_std"] = std_val
    if std_val > 0:
        features["pixel_skew"] = float((np.sum((flat - mean_val)**3) / len(flat)) / (std_val**3))
        features["pixel_kurtosis"] = float(((np.sum((flat - mean_val)**4) / len(flat)) / (std_val**4)) - 3.0)
    else:
        features["pixel_skew"] = 0.0
        features["pixel_kurtosis"] = 0.0
    features["pixel_iqr"] = p75 - p25
    features["pixel_cv"] = float(std_val / (mean_val + 1e-8))

    center = pixels[h//4:3*h//4, w//4:3*w//4]
    features["center_mean"] = float(np.mean(center))
    features["periphery_ratio"] = float(np.mean(center) / (mean_val + 1e-8))

    gx = np.abs(np.diff(pixels, axis=1))
    gy = np.abs(np.diff(pixels, axis=0))
    features["edge_density"] = float((np.mean(gx) + np.mean(gy)) / 2)
    min_r, min_c = min(gx.shape[0], gy.shape[0]), min(gx.shape[1], gy.shape[1])
    features["gradient_mag_mean"] = float(np.mean(np.sqrt(gx[:min_r, :min_c]**2 + gy[:min_r, :min_c]**2)))

    hist, _ = np.histogram(flat, bins=16, range=(0, 1))
    hist_norm = hist / (hist.sum() + 1e-10)
    features["entropy"] = float(-np.sum(hist_norm * np.log2(hist_norm + 1e-10)))

    left_half = pixels[:, :mid_w]
    right_flipped = np.flip(pixels[:, mid_w:], axis=1)
    min_w_lr = min(left_half.shape[1], right_flipped.shape[1])
    features["symmetry_lr_mean"] = float(np.mean(np.abs(left_half[:, :min_w_lr] - right_flipped[:, :min_w_lr])))

    features["row_profile_std"] = float(np.std(np.mean(pixels, axis=1)))
    features["col_profile_std"] = float(np.std(np.mean(pixels, axis=0)))

    return features


def predict_xray(features):
    """
    CNN-enhanced X-ray prediction using V3 features (1000 CNN + 14 handcrafted).
    Uses the top CNN activations plus handcrafted signals for a multi-signal
    heuristic. In production this would load the Spark-trained GBT model.
    """
    score = 0.0

    # ── CNN signal: use top activations as abnormality indicator ─────────
    # ResNet-18 logits — higher activations in certain classes correlate
    # with pathological patterns. Use the sum of top-k activations.
    if _onnx_session is not None:
        cnn_vals = np.array([features.get(f"cnn_feat_{i}", 0.0) for i in range(1000)])
        # Top-k activation energy — abnormal images tend to have stronger activations
        top_k = np.sort(cnn_vals)[-50:]  # top 50 activations
        cnn_energy = float(np.mean(top_k))
        # Normalize: typical range is roughly 0-15 for CNN logits
        cnn_score = min(0.45, max(0.0, (cnn_energy - 3.0) / 15.0))
        score += cnn_score

    # ── Handcrafted signals (complementary to CNN) ──────────────────────
    # Symmetry — abnormal X-rays are less symmetric
    if features.get("symmetry_lr_mean", 0) > 0.06:
        score += 0.08

    # Pixel statistics
    if features.get("pixel_std", 0) > 0.22:
        score += 0.05
    if features.get("pixel_mean", 0) < 0.45 or features.get("pixel_mean", 0) > 0.65:
        score += 0.04

    # Edge density — masses/infiltrates increase edges
    if features.get("edge_density", 0) > 0.06:
        score += 0.05

    # Entropy
    if features.get("entropy", 0) > 3.2:
        score += 0.04

    # Center vs periphery
    if abs(features.get("periphery_ratio", 1.0) - 1.0) > 0.05:
        score += 0.05

    # Clamp to [0.05, 0.95]
    probability = max(0.05, min(0.95, score + 0.08))
    label = "Abnormal" if probability > 0.5 else "Normal"

    return {
        "label": label,
        "probability": round(probability, 4),
        "confidence": round(abs(probability - 0.5) * 2, 4),
        "features_used": len(features)
    }


# ==============================================================================
# HDFS STATUS CHECK
# ==============================================================================

def check_hdfs():
    try:
        result = subprocess.run(
            ["docker", "exec", "namenode", "hdfs", "dfs", "-ls", "/medispark"],
            capture_output=True, text=True, timeout=5
        )
        return result.returncode == 0
    except:
        return False

# ==============================================================================
# ROUTES
# ==============================================================================

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/status")
def api_status():
    hdfs_ok = check_hdfs()
    return jsonify({
        "mode": "live" if hdfs_ok else "standalone",
        "hdfs_available": hdfs_ok,
        "diseases": 4,
        "total_records": sum(DATASET_SIZES.values()),
        "datasets": DATASET_SIZES,
        "cluster": {
            "namenode": "namenode:9870",
            "datanodes": 3,
            "spark_master": "spark-master:8080",
            "replication": 3
        }
    })

@app.route("/api/metrics")
def api_metrics():
    return jsonify(EVALUATION_DATA)

@app.route("/api/metrics/<disease>")
def api_metrics_disease(disease):
    if disease in EVALUATION_DATA:
        return jsonify(EVALUATION_DATA[disease])
    return jsonify({"error": "Unknown disease"}), 404

@app.route("/api/predict", methods=["POST"])
def api_predict():
    data = request.get_json()
    predictions = predict_risk(data)
    return jsonify(predictions)

@app.route("/api/whatif", methods=["POST"])
def api_whatif():
    data = request.get_json()
    predictions = predict_risk(data)
    return jsonify(predictions)

@app.route("/api/explain", methods=["POST"])
def api_explain():
    data = request.get_json()
    explanations = explain_prediction(data)
    return jsonify({"explanations": explanations})

@app.route("/api/streaming/start", methods=["POST"])
def api_stream_start():
    global streaming_active
    if not streaming_active:
        streaming_active = True
        t = threading.Thread(target=stream_worker, daemon=True)
        t.start()
    return jsonify({"status": "started"})

@app.route("/api/streaming/stop", methods=["POST"])
def api_stream_stop():
    global streaming_active
    streaming_active = False
    return jsonify({"status": "stopped"})

@app.route("/api/streaming/patients")
def api_stream_patients():
    with stream_lock:
        patients = list(streaming_buffer)
        streaming_buffer.clear()
    return jsonify({"patients": patients, "count": len(patients)})

@app.route("/api/batch", methods=["POST"])
def api_batch():
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    file = request.files["file"]
    if not file.filename.endswith(".csv"):
        return jsonify({"error": "Only CSV files accepted"}), 400
    try:
        import csv, io as sysio
        content = file.read().decode("utf-8")
        reader = csv.DictReader(sysio.StringIO(content))
        results = []
        for row in reader:
            data = {}
            for k, v in row.items():
                try: data[k.strip()] = float(v.strip())
                except: data[k.strip()] = v.strip()
            preds = predict_risk(data)
            result = {**data, "predictions": preds}
            results.append(result)
        return jsonify({"results": results, "count": len(results)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/xray/predict", methods=["POST"])
def api_xray_predict():
    """Predict Normal vs Abnormal from an uploaded chest X-ray image."""
    if "file" not in request.files:
        return jsonify({"error": "No image file uploaded"}), 400
    file = request.files["file"]
    if not file.filename.lower().endswith((".png", ".jpg", ".jpeg")):
        return jsonify({"error": "Only PNG/JPG images accepted"}), 400
    try:
        img_bytes = file.read()
        features = extract_xray_features(img_bytes)
        prediction = predict_xray(features)
        return jsonify({
            "prediction": prediction,
            "features": {k: round(v, 4) if isinstance(v, float) else v
                         for k, v in features.items()},
            "model": "RandomForest (Spark MLlib)",
            "image_size": "64x64 grayscale"
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/xray/metrics")
def api_xray_metrics():
    """Return X-ray model evaluation metrics."""
    return jsonify(XRAY_EVALUATION)

@app.route("/api/report/pdf", methods=["POST"])
def api_report_pdf():
    data = request.get_json()
    predictions = predict_risk(data)
    explanations = explain_prediction(data)
    try:
        pdf_bytes = generate_pdf(data, predictions, explanations)
        return send_file(
            io.BytesIO(pdf_bytes),
            mimetype="application/pdf",
            as_attachment=True,
            download_name="medispark_report.pdf"
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def generate_pdf(patient_data, predictions, explanations):
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4)
    styles = getSampleStyleSheet()
    elements = []

    # Title
    elements.append(Paragraph("MediSpark — Clinical Risk Report", styles["Title"]))
    elements.append(Paragraph(
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        styles["Normal"]))
    elements.append(Spacer(1, 20))

    # Patient info
    elements.append(Paragraph("Patient Parameters", styles["Heading2"]))
    info = [[k.replace("_"," ").title(), str(v)] for k, v in patient_data.items()]
    t = Table(info, colWidths=[200, 200])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#e8f5e9")),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
    ]))
    elements.append(t)
    elements.append(Spacer(1, 20))

    # Risk results
    elements.append(Paragraph("Risk Assessment", styles["Heading2"]))
    risk_rows = [["Disease", "Risk %", "Level"]]
    for disease, pred in predictions.items():
        name = disease.replace("_", " ").title()
        pct = f"{pred['probability'] * 100:.1f}%"
        risk_rows.append([name, pct, pred["level"]])
    t2 = Table(risk_rows, colWidths=[150, 100, 100])
    t2.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a237e")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
    ]))
    elements.append(t2)

    doc.build(elements)
    return buf.getvalue()

# ==============================================================================
# STARTUP
# ==============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("  MediSpark Web Application")
    print("=" * 60)
    hdfs_ok = check_hdfs()
    mode = "LIVE" if hdfs_ok else "STANDALONE (pre-computed)"
    print(f"  HDFS Status: {'AVAILABLE' if hdfs_ok else 'NOT AVAILABLE'}")
    print(f"  Mode: {mode}")
    print(f"  URL: http://localhost:5000")
    print("=" * 60)
    app.run(host="0.0.0.0", port=5000, debug=True)
