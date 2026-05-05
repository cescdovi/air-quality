"""Job 3 · Train + Predict

Calcula features (hour, dayofweek, no2_lag24), recorta el buffer inicial,
hace split temporal 80/20, entrena LinearRegression, mide MAE en test
y predice las próximas 24 horas.

Input:  s3://$AQ_BUCKET/clean.parquet
Output: s3://$AQ_BUCKET/{dataset.parquet, predictions.csv, metrics.json}
"""
import json
import os

import fsspec
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error

TARGET = "NO2"
FEATURES = ["hour", "dayofweek", "no2_lag24"]
WINDOW_START = "2021-07-01"

BUCKET = os.environ["AQ_BUCKET"]
S3_OPTS = {"client_kwargs": {"endpoint_url": os.environ["AWS_ENDPOINT_URL"]}}
INPUT = f"s3://{BUCKET}/clean.parquet"
OUT_DATASET = f"s3://{BUCKET}/dataset.parquet"
OUT_PREDS = f"s3://{BUCKET}/predictions.csv"
OUT_METRICS = f"s3://{BUCKET}/metrics.json"


def main() -> None:
    df = pd.read_parquet(INPUT, storage_options=S3_OPTS)
    print(f"Leído {INPUT}: {len(df)} filas")

    df["hour"] = df["timestamp"].dt.hour
    df["dayofweek"] = df["timestamp"].dt.dayofweek
    df["no2_lag24"] = df[TARGET].shift(24)
    df = df[df["timestamp"] >= WINDOW_START].reset_index(drop=True)
    assert df[FEATURES].notna().all().all(), "NaN en features tras recortar buffer"

    df.to_parquet(OUT_DATASET, index=False, storage_options=S3_OPTS)
    print(f"Escrito {OUT_DATASET} ({len(df)} filas)")

    split = int(len(df) * 0.8)
    train, test = df.iloc[:split], df.iloc[split:]

    model = LinearRegression()
    model.fit(train[FEATURES], train[TARGET])
    mae = float(mean_absolute_error(test[TARGET], model.predict(test[FEATURES])))

    print(f"Train: {len(train)} ({train['timestamp'].min()} → {train['timestamp'].max()})")
    print(f"Test:  {len(test)} ({test['timestamp'].min()} → {test['timestamp'].max()})")
    print(f"MAE en test: {mae:.2f} µg/m³")

    last_ts = df["timestamp"].iloc[-1]
    future_ts = pd.date_range(last_ts + pd.Timedelta(hours=1), periods=24, freq="h")
    history = df.set_index("timestamp")[TARGET]
    future = pd.DataFrame({"timestamp": future_ts})
    future["hour"] = future["timestamp"].dt.hour
    future["dayofweek"] = future["timestamp"].dt.dayofweek
    future["no2_lag24"] = [history.get(ts - pd.Timedelta(hours=24)) for ts in future["timestamp"]]
    assert future[FEATURES].notna().all().all(), "NaN en features de predicción"
    future["prediction"] = model.predict(future[FEATURES])

    future[["timestamp", "prediction"]].to_csv(OUT_PREDS, index=False, storage_options=S3_OPTS)
    with fsspec.open(OUT_METRICS, "w", **S3_OPTS) as f:
        json.dump({"mae": mae, "n_train": len(train), "n_test": len(test)}, f, indent=2)
    print(f"Escrito {OUT_PREDS} y {OUT_METRICS}")


if __name__ == "__main__":
    main()
