from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Data
DATA_DIR = PROJECT_ROOT / "data"
INPUT_GLOB = str(DATA_DIR / "*.parquet")

# SQLite DB file 
SQLITE_DB = DATA_DIR / "etl_db.sqlite"

# Outputs (CSV/parquet)
OUTPUT_DIR = PROJECT_ROOT / "outputs"
HOURLY_CSV = OUTPUT_DIR / "meter_hourly.csv"
DAILY_CSV = OUTPUT_DIR / "meter_daily_agg.csv"
DAILY_ROLLING_CSV = OUTPUT_DIR / "meter_daily_rolling_7d.csv"
MONTHLY_CSV = OUTPUT_DIR / "meter_monthly_agg.csv"
LOG_CSV = OUTPUT_DIR / "etl_log.csv"
FEATURES_PARQUET = OUTPUT_DIR / "features.parquet"

# SQL outputs under src/sql/outputs
SQL_OUTPUT_DIR = PROJECT_ROOT / "src" / "sql" / "outputs"

# Timezone
LOCAL_TZNAME = "Europe/Vilnius"

