"""Job 1 · Ingest

Descarga el CSV de calidad del aire de Valencia, filtra por estación
y recorta a la ventana 2021-07-01 → 2021-12-31 con un buffer de 24 h
(necesario para que el lag-24 esté definido en la primera hora útil).

Input:  API de datos abiertos de Valencia.
Output: s3://$AQ_BUCKET/raw.parquet con columnas [timestamp, NO2].
"""
import os
from io import BytesIO

import pandas as pd
import requests

CSV_URL = (
    "https://opendata.vlci.valencia.es/es/dataset/"
    "b5c2656c-6c1c-413d-a56e-549e52220502/resource/"
    "4be7248b-9597-4017-89af-82a9b6e2382f/download/"
    "rvvcca.-datos-horarios-valencia-2016-2021-curt-cas.csv"
)
STATION = "Pista Silla"
TARGET = "NO2"
WINDOW_START = "2021-07-01"
WINDOW_END = "2021-12-31"
BUFFER_HOURS = 24

BUCKET = os.environ["AQ_BUCKET"]
S3_OPTS = {"client_kwargs": {"endpoint_url": os.environ["AWS_ENDPOINT_URL"]}}
OUTPUT = f"s3://{BUCKET}/raw.parquet"


def main() -> None:
    print(f"Descargando CSV ({CSV_URL[:70]}...)")
    response = requests.get(CSV_URL, timeout=120)
    response.raise_for_status()
    df = pd.read_csv(BytesIO(response.content), sep=";", encoding="utf-8-sig")
    print(f"Filas descargadas: {len(df)}")

    df = df[df["Estación"] == STATION].copy()
    df["timestamp"] = pd.to_datetime(
        df["Fecha"].str.slice(0, 10) + " " + df["Hora"],
        format="%Y-%m-%d %H:%M:%S",
    )

    buffer_start = pd.Timestamp(WINDOW_START) - pd.Timedelta(hours=BUFFER_HOURS)
    window_end_inclusive = pd.Timestamp(WINDOW_END) + pd.Timedelta(hours=23)
    df = df[(df["timestamp"] >= buffer_start) & (df["timestamp"] <= window_end_inclusive)]
    df = df[["timestamp", TARGET]].sort_values("timestamp").reset_index(drop=True)

    print(f"Filas tras filtrado: {len(df)}")
    print(f"Rango: {df['timestamp'].min()} → {df['timestamp'].max()}")
    print(f"Nulls en {TARGET}: {df[TARGET].isna().sum()} ({df[TARGET].isna().mean():.1%})")

    df.to_parquet(OUTPUT, index=False, storage_options=S3_OPTS)
    print(f"Escrito {OUTPUT}")


if __name__ == "__main__":
    main()
