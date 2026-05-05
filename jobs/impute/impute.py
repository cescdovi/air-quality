"""Job 2 · Impute

Rellena cada NaN de NO2 con la media de los 4 valores no nulos
inmediatamente anteriores. Iteramos para que rachas largas de NaN
se rellenen progresivamente usando los valores ya imputados.

Input:  s3://$AQ_BUCKET/raw.parquet
Output: s3://$AQ_BUCKET/clean.parquet (sin NaN en NO2).
"""
import os

import pandas as pd

TARGET = "NO2"
WINDOW = 4

BUCKET = os.environ["AQ_BUCKET"]
S3_OPTS = {"client_kwargs": {"endpoint_url": os.environ["AWS_ENDPOINT_URL"]}}
INPUT = f"s3://{BUCKET}/raw.parquet"
OUTPUT = f"s3://{BUCKET}/clean.parquet"


def main() -> None:
    df = pd.read_parquet(INPUT, storage_options=S3_OPTS)
    print(f"Leído {INPUT}: {len(df)} filas")

    n_before = df[TARGET].isna().sum()
    print(f"NaN antes: {n_before}")

    while df[TARGET].isna().any():
        prev_nan = df[TARGET].isna().sum()
        df[TARGET] = df[TARGET].fillna(
            df[TARGET].shift(1).rolling(window=WINDOW, min_periods=1).mean()
        )
        # NaN al inicio de la serie (sin previos donde mirar) — irrellenables
        if df[TARGET].isna().sum() == prev_nan:
            break

    n_after = df[TARGET].isna().sum()
    print(f"NaN después: {n_after}")
    print(f"Imputados: {n_before - n_after}")
    assert n_after == 0, "Quedan NaN tras la imputación — revisar la serie"

    df.to_parquet(OUTPUT, index=False, storage_options=S3_OPTS)
    print(f"Escrito {OUTPUT}")


if __name__ == "__main__":
    main()
