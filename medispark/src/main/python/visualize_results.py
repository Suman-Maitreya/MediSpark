#!/usr/bin/env python3
"""
MediSpark — Visualization Script
Reads Stage 6 evaluation results from HDFS and generates charts.
Run on the host machine after evaluation is complete.
"""

import subprocess
import json
import os

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
except ImportError:
    print("Install matplotlib + numpy: pip install matplotlib numpy")
    exit(1)

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "..", "..", "..", "..", "visualizations")

COLORS = {
    "heart": "#ff2d55",
    "diabetes": "#ff8c00",
    "stroke": "#bf5af2",
    "lung_cancer": "#34c759"
}

# Hardcoded fallback — replaced with real values after Stage 6
RESULTS = {
    "heart":       {"accuracy": 0, "f1": 0, "precision": 0, "recall": 0, "auc_roc": 0},
    "diabetes":    {"accuracy": 0, "f1": 0, "precision": 0, "recall": 0, "auc_roc": 0},
    "stroke":      {"accuracy": 0, "f1": 0, "precision": 0, "recall": 0, "auc_roc": 0},
    "lung_cancer": {"accuracy": 0, "f1": 0, "precision": 0, "recall": 0, "auc_roc": 0}
}

def load_from_hdfs():
    """Try to load results from HDFS via docker exec"""
    for disease in RESULTS:
        try:
            result = subprocess.run(
                ["docker", "exec", "namenode", "hdfs", "dfs", "-cat",
                 f"/medispark/evaluation/{disease}/part-*.json"],
                capture_output=True, text=True, timeout=10
            )
            if result.returncode == 0 and result.stdout.strip():
                data = json.loads(result.stdout.strip().split("\n")[0])
                RESULTS[disease] = data
                print(f"  Loaded {disease}: accuracy={data.get('accuracy',0):.2f}%")
        except Exception as e:
            print(f"  Warning: Could not load {disease} from HDFS: {e}")

def plot_accuracy_comparison():
    fig, ax = plt.subplots(figsize=(10, 6))
    diseases = list(RESULTS.keys())
    labels = [d.replace("_", " ").title() for d in diseases]
    acc = [RESULTS[d]["accuracy"] for d in diseases]
    colors = [COLORS[d] for d in diseases]

    bars = ax.bar(labels, acc, color=colors, edgecolor="white", linewidth=0.5)
    ax.set_ylabel("Accuracy (%)", fontsize=12)
    ax.set_title("Model Accuracy Comparison", fontsize=14, fontweight="bold")
    ax.set_ylim(0, 105)
    for bar, val in zip(bars, acc):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                f"{val:.1f}%", ha="center", va="bottom", fontweight="bold")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "accuracy_comparison.png"), dpi=150)
    plt.close()

def plot_metrics_radar():
    diseases = list(RESULTS.keys())
    metrics = ["accuracy", "f1", "precision", "recall"]
    fig, axes = plt.subplots(2, 2, figsize=(12, 10), subplot_kw=dict(polar=True))

    for idx, disease in enumerate(diseases):
        ax = axes[idx // 2][idx % 2]
        values = [RESULTS[disease].get(m, 0) for m in metrics]
        values.append(values[0])  # close the radar

        angles = np.linspace(0, 2 * np.pi, len(metrics), endpoint=False).tolist()
        angles.append(angles[0])

        ax.plot(angles, values, color=COLORS[disease], linewidth=2)
        ax.fill(angles, values, color=COLORS[disease], alpha=0.15)
        ax.set_xticks(angles[:-1])
        ax.set_xticklabels([m.title() for m in metrics], size=9)
        ax.set_ylim(0, 105)
        ax.set_title(disease.replace("_", " ").title(),
                     fontsize=12, fontweight="bold", pad=15)

    plt.suptitle("Per-Disease Metrics Radar", fontsize=14, fontweight="bold")
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "metrics_radar.png"), dpi=150)
    plt.close()

def plot_metrics_grouped_bar():
    fig, ax = plt.subplots(figsize=(12, 6))
    diseases = list(RESULTS.keys())
    metrics = ["accuracy", "f1", "precision", "recall"]
    x = np.arange(len(diseases))
    width = 0.18

    for i, metric in enumerate(metrics):
        values = [RESULTS[d].get(metric, 0) for d in diseases]
        bars = ax.bar(x + i * width, values, width, label=metric.title(),
                      alpha=0.85)

    ax.set_ylabel("Score (%)")
    ax.set_title("All Metrics by Disease", fontsize=14, fontweight="bold")
    ax.set_xticks(x + width * 1.5)
    ax.set_xticklabels([d.replace("_", " ").title() for d in diseases])
    ax.legend()
    ax.set_ylim(0, 110)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "metrics_grouped_bar.png"), dpi=150)
    plt.close()

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print("=" * 50)
    print("  MediSpark — Visualization Generator")
    print("=" * 50)

    print("\n  Loading results from HDFS...")
    load_from_hdfs()

    if all(RESULTS[d]["accuracy"] == 0 for d in RESULTS):
        print("\n  No results loaded. Run Stage 6 first.")
        print("  Generating placeholder charts...")

    print("\n  Generating charts...")
    plot_accuracy_comparison()
    print("  -> accuracy_comparison.png")
    plot_metrics_radar()
    print("  -> metrics_radar.png")
    plot_metrics_grouped_bar()
    print("  -> metrics_grouped_bar.png")

    print(f"\n  Charts saved to: {OUTPUT_DIR}")
    print("  Done!")

if __name__ == "__main__":
    main()
