import argparse
import yaml
import pandas as pd
import numpy as np
import json
import logging
import time
import sys
import os
import csv


def setup_logger(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


def write_metrics(output_path, data):
    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)


def load_config(path):
    if not os.path.exists(path):
        raise Exception("Config file not found")

    with open(path, "r") as f:
        config = yaml.safe_load(f)

    required_keys = ["seed", "window", "version"]
    for key in required_keys:
        if key not in config:
            raise Exception(f"Missing config key: {key}")

    return config


def load_data(path):
    if not os.path.exists(path):
        raise Exception("Input file not found")

    try:
        # Handle weird CSV (quotes + delimiter issues)
        df = pd.read_csv(path, sep=",", quoting=csv.QUOTE_NONE, engine="python")
    except Exception:
        raise Exception("Invalid CSV format")

    # If still single column → force split manually
    if len(df.columns) == 1:
        logging.warning("CSV parsed as single column, applying manual split fix")

        df = pd.read_csv(path, header=None)
        df = df[0].astype(str).str.split(",", expand=True)

        # Assign expected columns (based on your dataset)
        df.columns = [
            "timestamp", "open", "high", "low",
            "close", "volume_btc", "volume_usd"
        ]

    if df.empty:
        raise Exception("Dataset is empty")

    # Normalize column names
    df.columns = df.columns.str.strip().str.lower()

    logging.info(f"Columns detected: {df.columns.tolist()}")

    # Ensure close column exists
    if "close" not in df.columns:
        raise Exception("Dataset validation failed: 'close' column not found")

    # Convert close to numeric (important)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    return df


def process_data(df, window):
    # Rolling mean
    df["rolling_mean"] = df["close"].rolling(window=window).mean()

    # Drop NaNs
    df_valid = df.dropna().copy()

    # Generate signal
    df_valid["signal"] = np.where(
        df_valid["close"] > df_valid["rolling_mean"], 1, 0
    )

    return df_valid


def compute_metrics(df_valid, version, seed, start_time):
    rows_processed = len(df_valid)
    signal_rate = df_valid["signal"].mean()

    latency_ms = int((time.time() - start_time) * 1000)

    return {
        "version": version,
        "rows_processed": rows_processed,
        "metric": "signal_rate",
        "value": round(float(signal_rate), 4),
        "latency_ms": latency_ms,
        "seed": seed,
        "status": "success"
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--log-file", required=True)

    args = parser.parse_args()

    setup_logger(args.log_file)

    start_time = time.time()

    try:
        logging.info("Job started")

        # CONFIG
        config = load_config(args.config)
        seed = config["seed"]
        window = config["window"]
        version = config["version"]

        np.random.seed(seed)

        logging.info(f"Config loaded: seed={seed}, window={window}, version={version}")

        # DATA
        df = load_data(args.input)
        logging.info(f"Rows loaded: {len(df)}")

        # PROCESS
        df_valid = process_data(df, window)
        logging.info(f"Processed rows: {len(df_valid)}")

        # METRICS
        metrics = compute_metrics(df_valid, version, seed, start_time)

        logging.info(f"Signal rate: {metrics['value']}")
        logging.info(f"Metrics: {metrics}")
        logging.info("Job completed successfully")

        write_metrics(args.output, metrics)

        print(json.dumps(metrics, indent=2))
        sys.exit(0)

    except Exception as e:
        logging.error(str(e))

        error_metrics = {
            "version": version if 'version' in locals() else "v1",
            "status": "error",
            "error_message": str(e)
        }

        write_metrics(args.output, error_metrics)

        print(json.dumps(error_metrics, indent=2))
        sys.exit(1)


if __name__ == "__main__":
    main()