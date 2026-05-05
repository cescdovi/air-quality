"""Job 4 · Plot

Genera la gráfica final: últimos 7 días observados + predicción 24 h
+ banda ± MAE.

Input:  s3://$AQ_BUCKET/{dataset.parquet, predictions.csv, metrics.json}
Output: s3://$AQ_BUCKET/forecast.png
"""
import json
import os

import fsspec
import matplotlib

matplotlib.use("Agg")  # backend sin display, necesario en contenedor
import matplotlib.pyplot as plt
import pandas as pd

TARGET = "NO2"
STATION = "Pista Silla"
TAIL_HOURS = 24 * 7

BUCKET = os.environ["AQ_BUCKET"]
S3_OPTS = {"client_kwargs": {"endpoint_url": os.environ["AWS_ENDPOINT_URL"]}}
IN_DATASET = f"s3://{BUCKET}/dataset.parquet"
IN_PREDS = f"s3://{BUCKET}/predictions.csv"
IN_METRICS = f"s3://{BUCKET}/metrics.json"
OUTPUT = f"s3://{BUCKET}/forecast.png"


def main() -> None:
    df = pd.read_parquet(IN_DATASET, storage_options=S3_OPTS)
    preds = pd.read_csv(IN_PREDS, parse_dates=["timestamp"], storage_options=S3_OPTS)
    with fsspec.open(IN_METRICS, "r", **S3_OPTS) as f:
        mae = json.load(f)["mae"]
    print(f"Histórico: {len(df)} filas · Predicción: {len(preds)} filas · MAE: {mae:.2f}")

    tail = df.tail(TAIL_HOURS)

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(tail["timestamp"], tail[TARGET], label="Observado (últimos 7 días)", linewidth=1)
    ax.plot(preds["timestamp"], preds["prediction"], label="Predicción 24 h", linewidth=2, color="tab:red")
    ax.fill_between(
        preds["timestamp"],
        preds["prediction"] - mae,
        preds["prediction"] + mae,
        alpha=0.2, color="tab:red", label=f"± MAE ({mae:.1f})",
    )
    ax.set_title(f"NO₂ · {STATION} · predicción a 24 h")
    ax.set_ylabel("NO₂ (µg/m³)")
    ax.legend()
    fig.tight_layout()
    with fsspec.open(OUTPUT, "wb", **S3_OPTS) as f:
        fig.savefig(f, dpi=120)
    print(f"Escrito {OUTPUT}")


if __name__ == "__main__":
    main()
