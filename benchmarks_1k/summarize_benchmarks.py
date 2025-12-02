import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

BASE_DIR = "outputs"

# ---------------------------------------------------------
# Helper function: parse stats.txt (now includes memory)
# ---------------------------------------------------------
def parse_stats(file_path):
    stats = {
        "Average": None,
        "Min": None,
        "Max": None,
        "P99": None,
        "IndexMemKB": None,
        "IndexMemMB": None,
    }

    if not os.path.exists(file_path):
        return stats

    with open(file_path, "r") as f:
        for line in f:
            if "Average" in line:
                stats["Average"] = float(line.split(":")[1].strip())
            elif "Min" in line and "Memory" not in line:
                stats["Min"] = float(line.split(":")[1].strip())
            elif "Max" in line and "Memory" not in line:
                stats["Max"] = float(line.split(":")[1].strip())
            elif "P99" in line:
                stats["P99"] = float(line.split(":")[1].strip())
            elif "Index Memory (KB)" in line:
                stats["IndexMemKB"] = float(line.split(":")[1].strip())
            elif "Index Memory (MB)" in line:
                stats["IndexMemMB"] = float(line.split(":")[1].strip())

    return stats


# ---------------------------------------------------------
# Helper function: parse accuracy.txt
# ---------------------------------------------------------
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


# ---------------------------------------------------------
# Aggregate all benchmark results
# ---------------------------------------------------------
rows = []

for model in os.listdir(BASE_DIR):  # art, rmi_linear, ...
    model_dir = os.path.join(BASE_DIR, model)
    if not os.path.isdir(model_dir):
        continue

    for dist in os.listdir(model_dir):  # linear, poly, random
        dist_dir = os.path.join(model_dir, dist)
        if not os.path.isdir(dist_dir):
            continue

        stats_file = os.path.join(dist_dir, "stats.txt")
        acc_file = os.path.join(dist_dir, "accuracy.txt")

        stats = parse_stats(stats_file)
        acc = parse_accuracy(acc_file)

        row = {
            "Model": model,
            "Dataset": dist,
            **stats,
            **acc,
        }
        rows.append(row)

# Create DataFrame
df = pd.DataFrame(rows)

print("\n================ SUMMARY TABLE ================\n")
print(df.to_string(index=False))

df.to_csv("benchmark_summary.csv", index=False)
print("\nSaved summary to benchmark_summary.csv\n")


# ---------------------------------------------------------
# Visualization Section (NEW MEMORY PLOTS ADDED)
# ---------------------------------------------------------
sns.set(style="whitegrid")

# ---- 1. Average Latency ----
plt.figure(figsize=(12,6))
sns.barplot(data=df, x="Dataset", y="Average", hue="Model")
plt.title("Average Latency Comparison (ms)")
plt.ylabel("Latency (ms)")
plt.savefig("avg_latency_comparison.png")
print("Saved avg_latency_comparison.png")

# ---- 2. Hit Rate ----
plt.figure(figsize=(12,6))
sns.barplot(data=df, x="Dataset", y="HitRate", hue="Model")
plt.title("Hit Rate Comparison (%)")
plt.ylabel("Hit Rate (%)")
plt.savefig("hit_rate_comparison.png")
print("Saved hit_rate_comparison.png")

# ---- 3. P99 Latency ----
plt.figure(figsize=(12,6))
sns.barplot(data=df, x="Dataset", y="P99", hue="Model")
plt.title("P99 Latency Comparison")
plt.ylabel("Latency (ms)")
plt.savefig("p99_latency_comparison.png")
print("Saved p99_latency_comparison.png")

# ---- 4. Heatmap of Average Latency ----
pivot_df = df.pivot(index="Model", columns="Dataset", values="Average")
plt.figure(figsize=(8,6))
sns.heatmap(pivot_df, annot=True, cmap="viridis")
plt.title("Heatmap: Average Latency Across Models & Datasets")
plt.savefig("avg_latency_heatmap.png")
print("Saved avg_latency_heatmap.png")


# ---------------------------------------------------------
# NEW PLOTS: MEMORY METRICS
# ---------------------------------------------------------

# ---- 5. Index Memory (KB) ----
plt.figure(figsize=(12,6))
sns.barplot(data=df, x="Dataset", y="IndexMemKB", hue="Model")
plt.title("Index Memory Usage (KB) by Model and Dataset")
plt.ylabel("Memory (KB)")
plt.savefig("index_memory_kb_comparison.png")
print("Saved index_memory_kb_comparison.png")

# ---- 6. Index Memory (MB) ----
plt.figure(figsize=(12,6))
sns.barplot(data=df, x="Dataset", y="IndexMemMB", hue="Model")
plt.title("Index Memory Usage (MB) by Model and Dataset")
plt.ylabel("Memory (MB)")
plt.savefig("index_memory_mb_comparison.png")
print("Saved index_memory_mb_comparison.png")

# ---- 7. Scatter: Latency vs Memory ----
plt.figure(figsize=(10,6))
sns.scatterplot(
    data=df,
    x="IndexMemKB",
    y="Average",
    hue="Model",
    style="Dataset",
    s=120
)
plt.title("Average Latency vs Index Memory (KB)")
plt.xlabel("Index Memory (KB)")
plt.ylabel("Average Latency (ms)")
plt.savefig("latency_vs_memory_scatter.png")
print("Saved latency_vs_memory_scatter.png")

print("\nAll plots and summary files generated successfully!\n")
