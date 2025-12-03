import os
import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

BASE_DIR = "outputs_100k"

# ============================================================
# Helper: parse stats.txt
# ============================================================
def parse_stats(file_path):
    stats = {
        "Average": None,
        "Min": None,
        "Max": None,
        "P99": None,
        "IndexMemKB": 0,
        "IndexMemMB": 0.0,
    }
    if not os.path.exists(file_path):
        return stats

    with open(file_path, "r") as f:
        for line in f:
            if "Average" in line:
                stats["Average"] = float(line.split(":")[1].strip())
            elif "Min" in line:
                stats["Min"] = float(line.split(":")[1].strip())
            elif "Max" in line:
                stats["Max"] = float(line.split(":")[1].strip())
            elif "P99" in line:
                stats["P99"] = float(line.split(":")[1].strip())
            elif "Index Memory (KB)" in line:
                stats["IndexMemKB"] = float(line.split(":")[1].strip())
            elif "Index Memory (MB)" in line:
                stats["IndexMemMB"] = float(line.split(":")[1].strip())

    return stats


# ============================================================
# Helper: parse accuracy.txt
# ============================================================
def parse_accuracy(file_path):
    acc = {"Hits": None, "Misses": None, "HitRate": None, "MissRate": None}
    if not os.path.exists(file_path):
        return acc

    with open(file_path, "r") as f:
        for line in f:
            if "Hits:" in line:
                acc["Hits"] = int(line.split(":")[1].strip())
            elif "Misses:" in line:
                acc["Misses"] = int(line.split(":")[1].strip())
            elif "Hit Rate" in line:
                acc["HitRate"] = float(line.split(":")[1].strip())
            elif "Miss Rate" in line:
                acc["MissRate"] = float(line.split(":")[1].strip())
    return acc


# ============================================================
# Helper: parse index_memory.txt (for models that separate it)
# ============================================================
def parse_index_memory(mem_file):
    kb = 0
    mb = 0.0
    if not os.path.exists(mem_file):
        return kb, mb

    with open(mem_file, "r") as f:
        for line in f:
            if "Index Memory (KB)" in line:
                kb = float(line.split(":")[1].strip())
            elif "Index Memory (MB)" in line:
                mb = float(line.split(":")[1].strip())
    return kb, mb


# ============================================================
# Parse whole outputs directory
# ============================================================
rows = []

for model in os.listdir(BASE_DIR):
    model_dir = os.path.join(BASE_DIR, model)
    if not os.path.isdir(model_dir):
        continue

    for dist in os.listdir(model_dir):  # linear/poly/random
        dist_dir = os.path.join(model_dir, dist)
        if not os.path.isdir(dist_dir):
            continue

        stats_file = os.path.join(dist_dir, "stats.txt")
        acc_file = os.path.join(dist_dir, "accuracy.txt")
        mem_file = os.path.join(dist_dir, "index_memory.txt")

        stats = parse_stats(stats_file)
        acc = parse_accuracy(acc_file)

        # stats file may NOT contain memory for some models
        if stats["IndexMemKB"] in (None, 0):
            kb, mb = parse_index_memory(mem_file)
            stats["IndexMemKB"] = kb
            stats["IndexMemMB"] = mb

        rows.append({
            "Model": model,
            "Dataset": dist,
            **stats,
            **acc
        })

df = pd.DataFrame(rows)

print("\n================ SUMMARY TABLE ================\n")
print(df.to_string(index=False))

# Save CSV
df.to_csv("benchmark_summary.csv", index=False)
print("\nSaved summary to benchmark_summary.csv\n")


# ============================================================
# PLOTTING SECTION
# ============================================================
sns.set(style="whitegrid")

# ------------------------------
# Average Latency Plot
# ------------------------------
plt.figure(figsize=(12, 6))
sns.barplot(data=df, x="Dataset", y="Average", hue="Model", errorbar=None)
plt.title("Average Latency Comparison (ms)")
plt.ylabel("Average latency (ms)")
plt.savefig("avg_latency_comparison.png")
print("Saved avg_latency_comparison.png")

# ------------------------------
# P99 Latency
# ------------------------------
plt.figure(figsize=(12, 6))
sns.barplot(data=df, x="Dataset", y="P99", hue="Model", errorbar=None)
plt.title("P99 Latency Comparison (ms)")
plt.ylabel("P99 latency (ms)")
plt.savefig("p99_latency_comparison.png")
print("Saved p99_latency_comparison.png")

# ------------------------------
# Hit Rate
# ------------------------------
plt.figure(figsize=(12, 6))
sns.barplot(data=df, x="Dataset", y="HitRate", hue="Model", errorbar=None)
plt.title("Hit Rate (%)")
plt.ylabel("Hit Rate (%)")
plt.savefig("hit_rate_comparison.png")
print("Saved hit_rate_comparison.png")

# ------------------------------
# Index Memory (KB)
# ------------------------------
plt.figure(figsize=(12, 6))
sns.barplot(data=df, x="Dataset", y="IndexMemKB", hue="Model", errorbar=None)
plt.title("Index Memory Usage (KB)")
plt.ylabel("Memory (KB)")
plt.savefig("index_memory_comparison.png")
print("Saved index_memory_comparison.png")

# ------------------------------
# Heatmap: Latency
# ------------------------------
pivot_latency = df.pivot(index="Model", columns="Dataset", values="Average")

plt.figure(figsize=(8, 6))
sns.heatmap(pivot_latency, annot=True, cmap="viridis")
plt.title("Heatmap: Average Latency Across Models & Datasets")
plt.savefig("avg_latency_heatmap.png")
print("Saved avg_latency_heatmap.png")

# ------------------------------
# Heatmap: Index Memory
# ------------------------------
pivot_mem = df.pivot(index="Model", columns="Dataset", values="IndexMemKB")

plt.figure(figsize=(8, 6))
sns.heatmap(pivot_mem, annot=True, cmap="magma")
plt.title("Heatmap: Index Memory (KB)")
plt.savefig("index_memory_heatmap.png")
print("Saved index_memory_heatmap.png")

print("\nAll plots and summary table generated!\n")
