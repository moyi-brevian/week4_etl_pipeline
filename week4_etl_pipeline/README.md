# Week 4 — Ops Sensor ETL Pipeline

A small, modular ETL pipeline that extracts the plant's raw sensor log,
cleans it, validates it against a data-quality suite, and loads it into a
local SQLite table — with logging, idempotent loads, and scheduled
automation.

```
data/ops_sensor_log_dirty.csv --> extract() --> transform() --> validate() --> load()
                                                                      |
                                                          halts pipeline if any
                                                          quality rule fails
```

## Repo contents

| File / folder                          | Purpose                                                             |
|-----------------------------------------|-----------------------------------------------------------------------|
| `run_pipeline.py`                       | Main ETL script (extract, transform, validate, load)                |
| `setup_quality_suite.py`                | One-time script that builds the Great Expectations config (`gx/`)   |
| `.env.example` / `.env`                 | Configuration template / your local config (not committed)          |
| `requirements.txt`                      | Python dependencies                                                  |
| `data/ops_sensor_log_dirty.csv`         | Raw source data                                                      |
| `data/ops_pipeline.db`                  | SQLite target database (created on first run)                        |
| `gx/`                                   | Great Expectations project + `ops_quality_suite` (data quality rules)|
| `automation/cron_entry.txt`             | Proof of a cron schedule (Linux/macOS)                                |
| `automation/windows_task_scheduler.txt` | Proof of a Windows Task Scheduler schedule                           |
| `pipeline.log`                          | Log output (sample from a real run is committed)                     |

## 1. Install dependencies

```bash
git clone <this-repo-url> week4_etl_pipeline
cd week4_etl_pipeline

python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

pip install -r requirements.txt
```

## 2. Set up your `.env`

Copy the template and adjust paths/thresholds if needed (the defaults work
out of the box against the sample data in `data/`):

```bash
cp .env.example .env
```

`.env` variables:

| Variable                          | Meaning                                                         |
|------------------------------------|-------------------------------------------------------------------|
| `SOURCE_CSV_PATH`                  | Path to the raw sensor log CSV                                   |
| `DATABASE_PATH`                    | Path to the SQLite target database                               |
| `TARGET_TABLE`                     | Name of the table the clean data is loaded into                  |
| `GX_PROJECT_ROOT`                  | Folder containing the Great Expectations project (`gx/`)         |
| `LOG_FILE`                         | Path to the pipeline's log file                                  |
| `LOG_LEVEL`                        | Logging verbosity (`INFO`, `DEBUG`, etc.)                        |
| `PRESSURE_MIN` / `PRESSURE_MAX`    | Physically plausible pressure range (PSI)                        |
| `TEMP_MIN` / `TEMP_MAX`            | Physically plausible temperature range (°C)                      |
| `FLOW_MIN` / `FLOW_MAX`            | Physically plausible flow-rate range (LPM)                       |

`.env` is gitignored — never commit real credentials or machine-specific
paths. Only `.env.example` (a template with no secrets) is checked in.

## 3. Build the data quality suite (one-time step)

```bash
python setup_quality_suite.py
```

This creates a `gx/` folder containing a real [Great Expectations](https://greatexpectations.io/)
project and an `ops_quality_suite` with 7 validation rules:

1. `Pressure_PSI` between `PRESSURE_MIN` and `PRESSURE_MAX`
2. `Temperature_C` between `TEMP_MIN` and `TEMP_MAX`
3. `Flow_Rate_LPM` between `FLOW_MIN` and `FLOW_MAX`
4. `timestamp` is never null
5. `Zone` is one of the 5 standardized plant zones
6. `Shift` is one of the 3 defined shifts
7. The batch is never empty (row count >= 1)

Re-run this script any time you want to change the rules — it rebuilds the
suite from scratch (idempotent), so it's safe to run repeatedly.

## 4. Run the pipeline

```bash
python run_pipeline.py
```

What happens, in order:

1. **Extract** — reads `SOURCE_CSV_PATH` into memory, logs the row count.
2. **Transform** — parses timestamps, standardizes `Zone`/`Shift` labels,
   drops duplicates, interpolates short gaps in sensor readings per zone,
   and drops physically impossible readings.
3. **Validate** — runs the `ops_quality_suite` against the transformed data.
   **If any rule fails, the pipeline logs the failure and exits immediately
   — nothing is loaded.**
4. **Load** — idempotently writes the clean data into `TARGET_TABLE`: the
   table is cleared first, then the fresh rows are inserted. Running the
   pipeline twice in a row leaves the table with the same row count as
   running it once (no duplicates), which you can verify yourself:

   ```bash
   python run_pipeline.py
   python run_pipeline.py   # same row count in the target table both times
   ```

Every run appends a timestamped entry to `pipeline.log` recording the start
time, end time, extract/transform/load row counts, every validation rule's
pass/fail result, and the full traceback of any unhandled error.

## 5. Automation

See `automation/cron_entry.txt` (Linux/macOS) and
`automation/windows_task_scheduler.txt` (Windows) for the exact schedule
entries used to run this pipeline unattended every day at 02:00. Because
`run_pipeline.py` exits with a non-zero status code on any failure
(validation failure or unhandled exception), either scheduler will correctly
report a failed run without any extra wiring.

## Design notes

- **Idempotency**: implemented as "clear target table, then insert" in
  `load()`, rather than checking unique IDs row-by-row — simpler to reason
  about for a full daily refresh of this table, and just as safe against
  double-runs.
- **Fail-fast data quality**: `validate()` runs independently of
  `transform()`'s own filtering, as a safety net — if a future code change
  to `transform()` ever lets a bad row slip through, `validate()` still
  catches it and halts the pipeline before it reaches the target table.
