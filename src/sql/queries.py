QUERIES = {
    "yearly_consumption": """
        SELECT CAST(strftime('%Y', date_local) AS INTEGER) AS year,
               SUM(total_kwh) AS yearly_kwh
        FROM meter_daily
        GROUP BY year
        ORDER BY year;
    """,
    "top_meters_by_year": """
        SELECT ext_dev_ref,
               CAST(strftime('%Y', date_local) AS INTEGER) AS year,
               SUM(total_kwh) AS yearly_kwh
        FROM meter_daily
        GROUP BY ext_dev_ref, year
        ORDER BY year DESC, yearly_kwh DESC
        LIMIT 100;
    """,
    "hourly_profile_avg": """
        SELECT hour,
               AVG(consumption_kwh) AS avg_kwh,
               COUNT(*) AS samples
        FROM meter_hourly
        GROUP BY hour
        ORDER BY hour;
    """,
    "peak_hours_distribution": """
        SELECT peak_hour,
               COUNT(*) AS occurrences,
               AVG(peak_kwh) AS avg_peak_kwh
        FROM meter_daily
        WHERE peak_hour IS NOT NULL
        GROUP BY peak_hour
        ORDER BY occurrences DESC;
    """,
    "holiday_vs_nonholiday": """
        SELECT is_holiday,
               COUNT(*) AS days,
               AVG(total_kwh) AS avg_daily_kwh
        FROM meter_daily
        GROUP BY is_holiday
        ORDER BY is_holiday DESC;
    """,
    "vacation_effect": """
        SELECT is_summer_vacation,
               COUNT(*) AS days,
               AVG(total_kwh) AS avg_daily_kwh
        FROM meter_daily
        GROUP BY is_summer_vacation
        ORDER BY is_summer_vacation DESC;
    """,
    "monthly_trends": """
        SELECT year,
               month,
               SUM(monthly_total_kwh) AS total_kwh
        FROM meter_monthly
        GROUP BY year, month
        ORDER BY year, month;
    """,
    "consumption_categories": """
        SELECT consumption_category,
               COUNT(*) AS cnt,
               AVG(total_kwh) AS avg_daily_kwh
        FROM meter_daily
        GROUP BY consumption_category
        ORDER BY cnt DESC;
    """,
    "similar_meters": """
        WITH avg_per_meter AS (
            SELECT ext_dev_ref,
                   AVG(total_kwh) AS avg_daily_kwh
            FROM meter_daily
            GROUP BY ext_dev_ref
        ),
        target AS (
            SELECT avg_daily_kwh FROM avg_per_meter LIMIT 1
        )
        SELECT a.ext_dev_ref,
               a.avg_daily_kwh
        FROM avg_per_meter a, target
        WHERE a.avg_daily_kwh BETWEEN target.avg_daily_kwh * 0.9
                                  AND target.avg_daily_kwh * 1.1
        ORDER BY ABS(a.avg_daily_kwh - target.avg_daily_kwh) ASC
        LIMIT 50;
    """,
    "night_day_consumption": """
        SELECT SUM(night_kwh) AS night_kwh,
               SUM(day_kwh) AS day_kwh
        FROM meter_daily;
    """,
    "peak_to_mean_outliers": """
        SELECT *
        FROM meter_daily
        WHERE peak_to_mean_ratio > 2.5
        ORDER BY peak_to_mean_ratio DESC;
    """ 
}
