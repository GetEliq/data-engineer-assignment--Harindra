from pathlib import Path
from typing import List
import pandas as pd
import numpy as np
import pytz
from datetime import datetime, time, timedelta
from ..utils.logger import get_logger
from ..config import settings
import glob

logger = get_logger(__name__, csv_path=settings.LOG_CSV)
LOCAL_TZ = pytz.timezone(settings.LOCAL_TZNAME)

def read_all_parquets(glob_pattern: str) -> pd.DataFrame:
    files = glob.glob(glob_pattern)
    if not files:
        logger.warning("No parquet files found for pattern: %s", glob_pattern)
        return pd.DataFrame()
    parts = []
    for p in sorted(files):
        logger.info("Reading parquet %s", p)
        parts.append(pd.read_parquet(p))
    df = pd.concat(parts, ignore_index=True)
    logger.info("Total rows read: %d", len(df))
    return df

def expand_daily_array_to_hourly(df: pd.DataFrame,
                                 date_col: str = "date",
                                 arr_col: str = "energy_consumption",
                                 timezone: pytz.BaseTzInfo = LOCAL_TZ) -> pd.DataFrame:

    rows = []
    required = {"client_id", "ext_dev_ref", date_col, arr_col}
    if not required.issubset(df.columns):
        raise ValueError(f"Input dataframe must contain columns: {required}")

    for idx, row in df.iterrows():
        arr = row[arr_col]
        if not isinstance(arr, (list, tuple, np.ndarray)) or len(arr) != 24:
            logger.warning("Skipping row idx=%s ext_dev_ref=%s date=%s invalid array", idx, row.get("ext_dev_ref"), row.get(date_col))
            continue

        raw_date = row[date_col]
        if isinstance(raw_date, pd.Timestamp):
            day_date = raw_date.date()
        elif isinstance(raw_date, str):
            day_date = pd.to_datetime(raw_date).date()
        else:
            day_date = raw_date

        local_midnight = timezone.localize(datetime.combine(day_date, time(0, 0)))
        for h, v in enumerate(arr):
            ts_local = local_midnight + timedelta(hours=int(h))
            ts_utc = ts_local.astimezone(pytz.UTC)
            rows.append({
                "client_id": row["client_id"],
                "ext_dev_ref": row["ext_dev_ref"],
                "resolution": row.get("resolution"),
                "date_local": day_date.isoformat(),
                "hour": int(h),
                "timestamp_local": ts_local.isoformat(),
                "timestamp_utc": ts_utc.isoformat(),
                "consumption_kwh": float(v)
            })
    df_hourly = pd.DataFrame(rows)
    if df_hourly.empty:
        logger.warning("Resulting hourly dataframe is empty.")
        return df_hourly
    df_hourly = df_hourly.sort_values(["ext_dev_ref", "timestamp_local"]).reset_index(drop=True)
    logger.info("Expanded into %d hourly rows", len(df_hourly))
    return df_hourly

