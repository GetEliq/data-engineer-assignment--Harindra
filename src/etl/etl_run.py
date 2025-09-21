import argparse
from pathlib import Path
from ..config import settings
from ..data_io.reader import read_all_parquets, expand_daily_array_to_hourly
from ..features.feature_generator import (
    add_time_features_hourly, add_lag_features, add_rolling_features_hourly,
    aggregate_daily_from_hourly, add_rolling_aggregates_daily, categorize_daily_consumption,
    add_season_and_holiday_flags, derive_monthly_from_daily
)
from ..utils.db_utils import save_to_sqlite_table
from ..utils.logger import get_logger

logger = get_logger(__name__, csv_path=settings.LOG_CSV)

def run_etl(input_glob: str | None = None, out_folder: Path | None = None, sqlite_path: Path | None = None):
    input_glob = input_glob or settings.INPUT_GLOB
    out_folder = Path(out_folder or settings.OUTPUT_DIR)
    sqlite_path = Path(sqlite_path or settings.SQLITE_DB)

    out_folder.mkdir(parents=True, exist_ok=True)
    settings.SQL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Starting ETL: reading input %s", input_glob)
    raw = read_all_parquets(input_glob)
    if raw.empty:
        logger.error("No input data found; exiting.")
        return

    hourly = expand_daily_array_to_hourly(raw)
    hourly.to_csv(out_folder / "meter_hourly_raw.csv", index=False)
    logger.info("Wrote hourly raw csv: %s", out_folder / "meter_hourly_raw.csv")

    hourly = add_time_features_hourly(hourly)
    hourly = add_lag_features(hourly, lags=[1, 24])
    hourly = add_rolling_features_hourly(hourly, window_hours=24)
    hourly.to_csv(out_folder / "meter_hourly_features.csv", index=False)
    logger.info("Wrote hourly features csv: %s", out_folder / "meter_hourly_features.csv")

    save_to_sqlite_table(hourly, "meter_hourly", sqlite_path)

    daily = aggregate_daily_from_hourly(hourly)
    daily = add_season_and_holiday_flags(daily)
    daily = add_rolling_aggregates_daily(daily, windows=[7, 30])
    daily = categorize_daily_consumption(daily)
    daily.to_csv(out_folder / "meter_daily_agg.csv", index=False)
    logger.info("Wrote daily csv: %s", out_folder / "meter_daily_agg.csv")
    save_to_sqlite_table(daily, "meter_daily", sqlite_path)
    save_to_sqlite_table(daily, "meter_daily_rolling_7d", sqlite_path)

    monthly = derive_monthly_from_daily(daily)
    monthly.to_csv(out_folder / "meter_monthly_agg.csv", index=False)
    logger.info("Wrote monthly csv: %s", out_folder / "meter_monthly_agg.csv")
    save_to_sqlite_table(monthly, "meter_monthly", sqlite_path)

    logger.info("ETL finished. SQLite DB at %s", sqlite_path)

def _cli():
    p = argparse.ArgumentParser(description="Run ETL: expand daily arrays -> hourly, features, save to sqlite")
    p.add_argument("--input-glob", default=None, help="Parquet glob pattern")
    p.add_argument("--out-folder", default=None, help="Folder for CSV outputs")
    p.add_argument("--sqlite-path", default=None, help="Path to sqlite DB")
    args = p.parse_args()
    run_etl(args.input_glob, args.out_folder, args.sqlite_path)

if __name__ == "__main__":
    _cli()
