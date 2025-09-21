from typing import List, Optional, Sequence, Union
import re
import pandas as pd
import numpy as np
import pytz
import holidays
from sklearn.metrics.pairwise import cosine_similarity

from ..utils.logger import get_logger
from ..config import settings

logger = get_logger(__name__, csv_path=settings.LOG_CSV)

_LT_HOLIDAYS = holidays.CountryHoliday("LT")

_LOCAL_TZ = pytz.timezone(settings.LOCAL_TZNAME)

_TZ_DETECT_RE = re.compile(r'([+-]\d{2}:\d{2}|[+-]\d{4}|Z)$')

def _series_has_tz_offset(s: pd.Series) -> bool:
    """Return True if any non-null string in series ends with a timezone offset token."""
    non_null = s.dropna().astype(str)
    if non_null.empty:
        return False
    return non_null.str.contains(_TZ_DETECT_RE, regex=True).any()

def _parse_timestamp_local_to_tz(
    series: pd.Series,
    local_tz: pytz.BaseTzInfo = _LOCAL_TZ
) -> pd.Series:
    """
    Robustly parse timestamp_local values into a tz-aware pandas Series in `local_tz`.
    """
    # If already tz-aware dtype, return as-is
    if pd.api.types.is_datetime64tz_dtype(series):
        return series

    # If series contains actual pd.Timestamp objects with tzinfo:
    if series.apply(lambda x: getattr(x, "tzinfo", None) is not None if not pd.isna(x) else False).any():
        def _ensure_tz(x):
            if pd.isna(x):
                return pd.NaT
            if isinstance(x, pd.Timestamp) and x.tzinfo is not None:
                return x.tz_convert(local_tz)
            return pd.to_datetime(x, utc=True, errors="coerce").tz_convert(local_tz) if _TZ_DETECT_RE.search(str(x)) else pd.to_datetime(x, errors="coerce").tz_localize(local_tz)

        parsed = series.map(_ensure_tz)
        return pd.to_datetime(parsed)

    has_tz = _series_has_tz_offset(series)

    if has_tz:
        # parse with utc=True (normalizes offsets), then convert to local tz
        parsed = pd.to_datetime(series, utc=True, errors="coerce")
        parsed = parsed.dt.tz_convert(local_tz)
    else:
        # parse naive datetimes and localize to local tz
        parsed = pd.to_datetime(series, errors="coerce")
        if pd.api.types.is_datetime64tz_dtype(parsed):
            parsed = parsed.dt.tz_convert(local_tz)
        else:
            parsed = parsed.dt.tz_localize(local_tz)
    return parsed

def add_time_features_hourly(df: pd.DataFrame, timestamp_col: str = "timestamp_local") -> pd.DataFrame:
    """
    Parse `timestamp_local` into a tz-aware `timestamp` column (Europe/Vilnius),
    and add common time features: hour, day_of_week, is_weekend, date_local, month, year.
    """
    if timestamp_col not in df.columns:
        raise KeyError(f"{timestamp_col} is required in dataframe")

    df = df.copy()

    parsed = _parse_timestamp_local_to_tz(df[timestamp_col], local_tz=_LOCAL_TZ)

    n_bad = int(parsed.isna().sum())
    if n_bad:
        logger.warning("add_time_features_hourly: %d timestamp_local values could not be parsed and will be dropped", n_bad)
        df = df.loc[~parsed.isna()].copy()
        parsed = parsed.loc[~parsed.isna()].copy()

    df["timestamp"] = parsed.values
    df["hour"] = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.dayofweek
    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)
    df["date_local"] = df["timestamp"].dt.date.astype(str)
    df["month"] = df["timestamp"].dt.month
    df["year"] = df["timestamp"].dt.year

    return df


def add_lag_features(df: pd.DataFrame, lags: Sequence[int] = (1, 24), ts_col: str = "timestamp") -> pd.DataFrame:
    """
    Add lag_{h} columns (hour-based) per ext_dev_ref.
    """
    df = df.copy()
    if ts_col not in df.columns:
        raise KeyError(f"{ts_col} required for add_lag_features")

    # ensure sorted for shifting
    df = df.sort_values(["ext_dev_ref", ts_col]).reset_index(drop=True)

    for lag in lags:
        col = f"lag_{lag}h"
        df[col] = df.groupby("ext_dev_ref")["consumption_kwh"].shift(lag)

    return df


def add_rolling_features_hourly(df: pd.DataFrame, window_hours: int = 24, ts_col: str = "timestamp") -> pd.DataFrame:
    """
    Add rolling mean/std over the previous `window_hours` excluding current point.
    Output columns:
      rolling_mean_{window_hours}h, rolling_std_{window_hours}h
    """
    df = df.copy()
    if ts_col not in df.columns:
        raise KeyError(f"{ts_col} required for rolling features")

    df = df.sort_values(["ext_dev_ref", ts_col]).reset_index(drop=True)

    mean_col = f"rolling_mean_{window_hours}h"
    std_col = f"rolling_std_{window_hours}h"

    df[mean_col] = df.groupby("ext_dev_ref")["consumption_kwh"].transform(
        lambda s: s.shift(1).rolling(window_hours, min_periods=1).mean()
    )
    df[std_col] = df.groupby("ext_dev_ref")["consumption_kwh"].transform(
        lambda s: s.shift(1).rolling(window_hours, min_periods=1).std().fillna(0)
    )
    return df


def aggregate_daily_from_hourly(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "date_local" not in df.columns:
        if "timestamp" in df.columns:
            df["date_local"] = df["timestamp"].dt.date.astype(str)
        else:
            raise KeyError("Either 'date_local' or 'timestamp' must be present in hourly df")
    agg = df.groupby(["client_id", "ext_dev_ref", "date_local"], as_index=False).agg(
        total_kwh=("consumption_kwh", "sum"),
        mean_kwh=("consumption_kwh", "mean"),
        max_kwh=("consumption_kwh", "max"),
        std_kwh=("consumption_kwh", "std"),
        hourly_count=("consumption_kwh", "count")
    )
    idx = df.groupby(["client_id", "ext_dev_ref", "date_local"])["consumption_kwh"].idxmax()
    peak = df.loc[idx, ["client_id", "ext_dev_ref", "date_local", "hour", "consumption_kwh"]].rename(
        columns={"hour": "peak_hour", "consumption_kwh": "peak_kwh"}
    )
    res = agg.merge(peak, on=["client_id", "ext_dev_ref", "date_local"], how="left")

    # Peak-to-mean ratio
    res["peak_to_mean_ratio"] = (res["peak_kwh"] / res["mean_kwh"]).replace(np.inf, 0).fillna(0)

    # Night vs day consumption
    night_df = df[df["hour"].isin(range(0,7))].groupby(["client_id","ext_dev_ref","date_local"])["consumption_kwh"].sum().rename("night_kwh")
    day_df = df[df["hour"].isin(range(18,24))].groupby(["client_id","ext_dev_ref","date_local"])["consumption_kwh"].sum().rename("day_kwh")
    res = res.merge(night_df.reset_index(), on=["client_id","ext_dev_ref","date_local"], how="left")
    res = res.merge(day_df.reset_index(), on=["client_id","ext_dev_ref","date_local"], how="left")
    res[["night_kwh","day_kwh"]] = res[["night_kwh","day_kwh"]].fillna(0)

    # Cumulative consumption
    res = res.sort_values(["client_id","ext_dev_ref","date_local"])
    res["cumulative_total_kwh"] = res.groupby(["client_id","ext_dev_ref"])["total_kwh"].cumsum()

    # Weekend vs weekday ratio  
    res["weekend_vs_weekday"] = res["total_kwh"] / (res.groupby(["client_id","ext_dev_ref"])["total_kwh"].transform("mean") + 1e-6)

    return res


def add_rolling_aggregates_daily(df_daily: pd.DataFrame, windows: Sequence[int] = (7, 30)) -> pd.DataFrame:
    """
    Add rolling means of daily total_kwh for windows (days).
    """
    df = df_daily.copy()
    if "date_local" not in df.columns:
        raise KeyError("'date_local' required for add_rolling_aggregates_daily")

    df["date_local_dt"] = pd.to_datetime(df["date_local"])
    df = df.sort_values(["client_id", "ext_dev_ref", "date_local_dt"]).reset_index(drop=True)

    for w in windows:
        col = f"rolling_{w}d_total_kwh"
        df[col] = df.groupby(["client_id", "ext_dev_ref"])["total_kwh"].transform(
            lambda s: s.rolling(window=w, min_periods=1).mean()
        ).round(6)

    return df.drop(columns=["date_local_dt"])


def categorize_daily_consumption(df_daily: pd.DataFrame, bins: Optional[List[float]] = None, labels: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Add a categorical band for daily total_kwh.
    """
    df = df_daily.copy()
    if bins is None:
        bins = [-1.0, 5.0, 15.0, 50.0, 999999.0]
    if labels is None:
        labels = ["very_low", "low", "medium", "high"]

    df["consumption_category"] = pd.cut(df["total_kwh"], bins=bins, labels=labels)
    return df


def add_season_and_holiday_flags(df_daily: pd.DataFrame) -> pd.DataFrame:
    """
    Add season (winter/spring/summer/fall), is_holiday (Lithuania), is_summer_vacation flag.
    """
    df = df_daily.copy()
    if "date_local" in df.columns:
        dts = pd.to_datetime(df["date_local"])
    elif "timestamp" in df.columns:
        dts = df["timestamp"].dt.normalize()
    else:
        raise KeyError("'date_local' or 'timestamp' required for add_season_and_holiday_flags")

    df["date_local_dt"] = dts
    df["month"] = df["date_local_dt"].dt.month
    df["season"] = df["month"].map({
        12: "winter", 1: "winter", 2: "winter",
        3: "spring", 4: "spring", 5: "spring",
        6: "summer", 7: "summer", 8: "summer",
        9: "fall", 10: "fall", 11: "fall"
    })
    df["is_holiday"] = df["date_local_dt"].dt.date.apply(lambda d: 1 if d in _LT_HOLIDAYS else 0)
    df["is_summer_vacation"] = df["month"].isin([7, 8]).astype(int)

    return df.drop(columns=["date_local_dt"])


def derive_monthly_from_daily(df_daily: pd.DataFrame) -> pd.DataFrame:
    """
    Compute monthly aggregates (monthly_total_kwh, monthly_mean_kwh, monthly_max_kwh, days_with_data).
    """
    df = df_daily.copy()
    if "date_local" in df.columns:
        dts = pd.to_datetime(df["date_local"])
    elif "timestamp" in df.columns:
        dts = df["timestamp"]
    else:
        raise KeyError("'date_local' or 'timestamp' required for derive_monthly_from_daily")

    df["year"] = dts.dt.year
    df["month"] = dts.dt.month

    m = df.groupby(["client_id", "ext_dev_ref", "year", "month"], as_index=False).agg(
        monthly_total_kwh=("total_kwh", "sum"),
        monthly_mean_kwh=("total_kwh", "mean"),
        monthly_max_kwh=("total_kwh", "max"),
        days_with_data=("total_kwh", "count")
    )
    m["monthly_total_kwh"] = m["monthly_total_kwh"].astype(float).round(6)
    m["monthly_mean_kwh"] = m["monthly_mean_kwh"].astype(float).round(6)
    m["monthly_max_kwh"] = m["monthly_max_kwh"].astype(float).round(6)
    return m

def add_daily_load_shape_similarity(df_hourly: pd.DataFrame, df_daily: pd.DataFrame) -> pd.DataFrame:
    """
    Compute cosine similarity per meter per day and merge into daily dataframe
    """
    df_hourly = df_hourly.copy()
    df_hourly = df_hourly.sort_values(["ext_dev_ref","date_local","hour"])
    daily_vectors = df_hourly.pivot_table(index=["ext_dev_ref","date_local"],columns="hour",values="consumption_kwh",fill_value=0)
    
    sims = []
    for ext_dev_ref in daily_vectors.index.get_level_values(0).unique():
        meter_data = daily_vectors.loc[ext_dev_ref]
        sim_list = [np.nan]
        for i in range(1,len(meter_data)):
            sim = cosine_similarity([meter_data.iloc[i-1].values],[meter_data.iloc[i].values])[0,0]
            sim_list.append(sim)
        sims.extend(sim_list)
    daily_vectors["daily_load_similarity"] = sims
    sim_df = daily_vectors.reset_index()[["ext_dev_ref","date_local","daily_load_similarity"]]
    
    df_daily = df_daily.merge(sim_df,on=["ext_dev_ref","date_local"],how="left")
    return df_daily
