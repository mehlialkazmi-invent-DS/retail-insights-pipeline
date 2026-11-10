# Writes Tecovas' calendar as the fiscal_cal Delta table tecovas_config.py reads (path_segments.fiscal).
#
# Tecovas weeks run Monday-Sunday and noob/daily-data's year / week are ISO 8601 (2025-12-29 is 2026 week 1,
# 2023-01-01 is 2022 week 52), so Year / Week here are datetime.date.isocalendar(). Tecovas has no fiscal month
# of its own: a week belongs to the month (and quarter) of its Thursday, the day that also fixes its ISO year,
# so every month and quarter is whole weeks and a year is 52 or 53 of them.
#
# How to use (Databricks, Tecovas workspace, next to tecovas_config.py), once and whenever the range runs out:
#     %run ./tecovas_fiscal_cal
#     write_tecovas_fiscal_cal(spark, fund.paste)

import datetime
from typing import Callable

FIRST_DAY = datetime.date(2022, 1, 3)  # Monday, ISO 2022 week 1; before daily-data's first day (2023-01-01)
LAST_DAY = datetime.date(2028, 1, 2)  # Sunday, ISO 2027 week 52; the upload must extend past every window
PATH_SEGMENTS = ["temp", "firatg", "retail_insights_kpi", "fiscal_cal"]  # = tecovas_config path_segments.fiscal


def tecovas_calendar_rows():
    """(date, Year, Week, Quarter, Month, month_name) for every day of FIRST_DAY..LAST_DAY."""
    rows = []
    day = FIRST_DAY
    while day <= LAST_DAY:
        iso_year, iso_week, iso_weekday = day.isocalendar()
        thursday = day + datetime.timedelta(days=4 - iso_weekday)
        rows.append((day, iso_year, iso_week, (thursday.month - 1) // 3 + 1, thursday.month, thursday.strftime("%B")))
        day += datetime.timedelta(days=1)
    return rows


def write_tecovas_fiscal_cal(spark, fund_paste: Callable[..., str]) -> str:
    path = fund_paste("/mnt/invent-tecovas-datastore", *PATH_SEGMENTS)
    frame = spark.createDataFrame(
        tecovas_calendar_rows(),
        "date date, Year int, Week int, Quarter int, Month int, month_name string",
    )
    frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    print(f"fiscal_cal: {frame.count()} days {FIRST_DAY}..{LAST_DAY} -> {path}")
    return path
