"""
setup_quality_suite.py
-----------------------
Run this ONCE (or whenever validation rules change) to (re)build the Great
Expectations project folder (`gx/`) and the expectation suite used by
run_pipeline.py.

    python setup_quality_suite.py

The resulting `gx/` folder is committed to the repo -- run_pipeline.py just
loads it read-only on every run, it does not recreate it. This keeps the
"data quality configuration" versioned and reviewable like any other code,
separate from the pipeline logic that *uses* it.
"""

import os

import great_expectations as gx
from dotenv import load_dotenv

load_dotenv()

GX_PROJECT_ROOT = os.getenv("GX_PROJECT_ROOT", ".")
SUITE_NAME = "ops_quality_suite"

# Thresholds come from .env so the same numbers used here are the ones
# documented (and tunable) in one place, rather than hardcoded twice.
PRESSURE_MIN = float(os.getenv("PRESSURE_MIN", 0))
PRESSURE_MAX = float(os.getenv("PRESSURE_MAX", 500))
TEMP_MIN = float(os.getenv("TEMP_MIN", -50))
TEMP_MAX = float(os.getenv("TEMP_MAX", 200))
FLOW_MIN = float(os.getenv("FLOW_MIN", 0))
FLOW_MAX = float(os.getenv("FLOW_MAX", 2000))

VALID_ZONES = ["Zone_North", "Zone_South", "Zone_East", "Zone_West", "Zone_Central"]
VALID_SHIFTS = ["Morning", "Afternoon", "Night"]


def build_suite():
    context = gx.get_context(mode="file", project_root_dir=GX_PROJECT_ROOT)

    # Re-running this script should be idempotent too: drop any existing
    # suite of the same name before rebuilding it, instead of erroring out
    # or silently duplicating expectations.
    if SUITE_NAME in [s.name for s in context.suites.all()]:
        context.suites.delete(SUITE_NAME)

    suite = context.suites.add(gx.ExpectationSuite(name=SUITE_NAME))

    # --- 5+ validation rules relevant to the ops sensor domain ---------
    # 1. Pressure must fall within the physically plausible operating range.
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="Pressure_PSI", min_value=PRESSURE_MIN, max_value=PRESSURE_MAX
        )
    )
    # 2. Temperature must fall within the physically plausible operating range.
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="Temperature_C", min_value=TEMP_MIN, max_value=TEMP_MAX
        )
    )
    # 3. Flow rate must fall within the physically plausible operating range.
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="Flow_Rate_LPM", min_value=FLOW_MIN, max_value=FLOW_MAX
        )
    )
    # 4. Every reading must have a timestamp -- an un-timestamped row is
    #    useless for time-series analysis and shouldn't reach the target table.
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="timestamp")
    )
    # 5. Zone must be one of the five known, standardized plant zones (catches
    #    anything that slipped through the transform step's standardization).
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(column="Zone", value_set=VALID_ZONES)
    )
    # 6. Shift must be one of the three defined shifts.
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(column="Shift", value_set=VALID_SHIFTS)
    )
    # 7. The batch shouldn't be empty -- an empty load is almost always a sign
    #    the extract step silently failed upstream, not a legitimately quiet day.
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1)
    )

    print(f"Suite '{SUITE_NAME}' built with {len(suite.expectations)} expectations.")
    print(f"Saved under: {os.path.join(GX_PROJECT_ROOT, 'gx', 'expectations')}")


if __name__ == "__main__":
    build_suite()
