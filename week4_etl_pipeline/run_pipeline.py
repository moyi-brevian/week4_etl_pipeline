"""
run_pipeline.py
----------------
Modular ETL pipeline for the plant's operational sensor data.

    Extract  -> read the raw sensor log CSV
    Transform-> clean it (Week 2 logic: standardize categoricals, dedupe,
                interpolate small gaps, drop physically impossible readings)
    Validate -> run the Great Expectations suite from setup_quality_suite.py;
                HALT before loading anything if validation fails
    Load     -> idempotently write the clean data into a SQLite target table

Usage:
    python run_pipeline.py

Configuration is read from a `.env` file (see `.env.example`) via
python-dotenv, and every run appends a structured entry to pipeline.log.
"""

import logging
import os
import sys
import time
from datetime import datetime

import great_expectations as gx
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text

# ---------------------------------------------------------------------------
# Configuration (python-dotenv)
# ---------------------------------------------------------------------------
load_dotenv()

SOURCE_CSV_PATH = os.getenv("SOURCE_CSV_PATH", "data/ops_sensor_log_dirty.csv")
DATABASE_PATH = os.getenv("DATABASE_PATH", "data/ops_pipeline.db")
TARGET_TABLE = os.getenv("TARGET_TABLE", "ops_readings_clean")
GX_PROJECT_ROOT = os.getenv("GX_PROJECT_ROOT", ".")
LOG_FILE = os.getenv("LOG_FILE", "pipeline.log")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

PRESSURE_MIN = float(os.getenv("PRESSURE_MIN", 0))
PRESSURE_MAX = float(os.getenv("PRESSURE_MAX", 500))
TEMP_MIN = float(os.getenv("TEMP_MIN", -50))
TEMP_MAX = float(os.getenv("TEMP_MAX", 200))
FLOW_MIN = float(os.getenv("FLOW_MIN", 0))
FLOW_MAX = float(os.getenv("FLOW_MAX", 2000))

VALID_SHIFTS = ["Morning", "Afternoon", "Night"]
SUITE_NAME = "ops_quality_suite"

# ---------------------------------------------------------------------------
# Logging -- comprehensive, written to LOG_FILE (append) and echoed to console
# ---------------------------------------------------------------------------
logger = logging.getLogger("ops_pipeline")
logger.setLevel(LOG_LEVEL)
logger.handlers.clear()

_formatter = logging.Formatter(
    "%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)

_file_handler = logging.FileHandler(LOG_FILE)
_file_handler.setFormatter(_formatter)
logger.addHandler(_file_handler)

_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(_formatter)
logger.addHandler(_console_handler)


# ---------------------------------------------------------------------------
# EXTRACT
# ---------------------------------------------------------------------------
def extract(csv_path: str) -> pd.DataFrame:
    """Read the raw sensor log from disk."""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Source file not found: {csv_path}")

    df = pd.read_csv(csv_path)
    logger.info(f"EXTRACT: read {len(df)} rows from '{csv_path}'")
    return df


# ---------------------------------------------------------------------------
# TRANSFORM
# ---------------------------------------------------------------------------
def _standardize_zone(raw_zone):
    """Collapse spelling/case/whitespace variants of a zone name to one canonical label."""
    if pd.isna(raw_zone):
        return np.nan
    cleaned = str(raw_zone).strip().upper().replace("-", "_").replace(" ", "_")
    for direction in ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"]:
        if direction in cleaned:
            return f"Zone_{direction.capitalize()}"
    return raw_zone


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the raw sensor log:
      - parse timestamps
      - standardize Zone / Shift labels
      - drop exact duplicates
      - interpolate short gaps in sensor readings (per zone)
      - drop rows outside physically plausible sensor ranges
    """
    rows_in = len(df)
    out = df.copy()

    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce")
    out["Zone"] = out["Zone"].apply(_standardize_zone)
    out["Shift"] = out["Shift"].astype(str).str.strip().str.capitalize()
    out.loc[~out["Shift"].isin(VALID_SHIFTS), "Shift"] = np.nan

    out = out.drop_duplicates()
    out = out.dropna(subset=["timestamp", "Zone", "Shift"]).sort_values("timestamp")

    metric_cols = ["Pressure_PSI", "Temperature_C", "Flow_Rate_LPM"]
    out[metric_cols] = (
        out.groupby("Zone")[metric_cols]
        .apply(lambda g: g.interpolate(method="linear", limit_direction="both"))
        .reset_index(level=0, drop=True)
    )
    out = out.dropna(subset=metric_cols)

    out = out[
        out["Pressure_PSI"].between(PRESSURE_MIN, PRESSURE_MAX)
        & out["Temperature_C"].between(TEMP_MIN, TEMP_MAX)
        & out["Flow_Rate_LPM"].between(FLOW_MIN, FLOW_MAX)
    ]
    out = out.reset_index(drop=True)

    logger.info(f"TRANSFORM: {rows_in} rows in -> {len(out)} rows out ({rows_in - len(out)} dropped)")
    return out


# ---------------------------------------------------------------------------
# VALIDATE (Great Expectations)
# ---------------------------------------------------------------------------
def validate(df: pd.DataFrame) -> bool:
    """
    Run the ops_quality_suite (built by setup_quality_suite.py) against the
    transformed data. Returns True if every expectation passes, False
    otherwise. Individual rule results are logged either way.
    """
    context = gx.get_context(mode="file", project_root_dir=GX_PROJECT_ROOT)

    try:
        suite = context.suites.get(SUITE_NAME)
    except Exception as exc:
        raise RuntimeError(
            f"Expectation suite '{SUITE_NAME}' not found under {GX_PROJECT_ROOT}/gx. "
            f"Run `python setup_quality_suite.py` first."
        ) from exc

    data_source = context.data_sources.add_pandas(f"pandas_{int(time.time())}")
    data_asset = data_source.add_dataframe_asset(name="ops_batch_asset")
    batch_def = data_asset.add_batch_definition_whole_dataframe("ops_batch_def")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    result = batch.validate(suite)

    for expectation_result in result.results:
        cfg = expectation_result.expectation_config
        status = "PASS" if expectation_result.success else "FAIL"
        logger.info(f"VALIDATE: [{status}] {cfg.type} {cfg.kwargs}")

    if result.success:
        logger.info("VALIDATE: all data quality checks passed.")
    else:
        failed = [r.expectation_config.type for r in result.results if not r.success]
        logger.error(f"VALIDATE: {len(failed)} check(s) failed: {failed}")

    return bool(result.success)


# ---------------------------------------------------------------------------
# LOAD (idempotent)
# ---------------------------------------------------------------------------
def load(df: pd.DataFrame, db_path: str, table_name: str) -> int:
    """
    Idempotent load: clear the target table before writing, so re-running the
    pipeline (e.g. after a re-triggered cron job, or reprocessing the same
    day) never duplicates rows. `to_sql` creates the table automatically on
    the first run if it doesn't exist yet.
    """
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    engine = create_engine(f"sqlite:///{db_path}")

    with engine.begin() as conn:
        if inspect(engine).has_table(table_name):
            result = conn.execute(text(f"DELETE FROM {table_name}"))
            logger.info(f"LOAD: cleared {result.rowcount} existing row(s) from '{table_name}' (idempotency step)")
        else:
            logger.info(f"LOAD: target table '{table_name}' does not exist yet, will be created")

    df.to_sql(table_name, engine, if_exists="append", index=False)
    logger.info(f"LOAD: inserted {len(df)} row(s) into '{table_name}' at '{db_path}'")
    return len(df)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    start_time = datetime.now()
    logger.info("=" * 70)
    logger.info(f"PIPELINE START: {start_time.isoformat()}")

    try:
        raw_df = extract(SOURCE_CSV_PATH)
        clean_df = transform(raw_df)

        is_valid = validate(clean_df)
        if not is_valid:
            logger.critical(
                "PIPELINE HALTED: data quality validation failed. "
                "No rows were loaded into the target table."
            )
            sys.exit(1)

        rows_loaded = load(clean_df, DATABASE_PATH, TARGET_TABLE)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(
            f"PIPELINE SUCCESS: extracted={len(raw_df)} rows, "
            f"transformed={len(clean_df)} rows, loaded={rows_loaded} rows, "
            f"duration={duration:.2f}s"
        )
        logger.info(f"PIPELINE END: {end_time.isoformat()}")
        logger.info("=" * 70)

    except Exception:
        logger.exception("PIPELINE FAILED with an unhandled exception:")
        logger.info(f"PIPELINE END (FAILURE): {datetime.now().isoformat()}")
        logger.info("=" * 70)
        sys.exit(1)


if __name__ == "__main__":
    main()
