from __future__ import annotations

import pandas as pd

from src.utils.date_utils import monday_of_week

CALENDAR_FEATURES = [
    "day_of_week",
    "week_of_month",
    "month",
    "iso_week",
    "is_month_end",
    "is_any_holiday",
    "is_pre_holiday",
    "is_post_holiday",
    "is_bridge_day",
    "easter_week_relative",
    "is_post_easter_window_1_6",
]


def build_daily_calendar_features(dim_date: pd.DataFrame) -> pd.DataFrame:
    return dim_date[["date", *CALENDAR_FEATURES]].rename(columns={"date": "fecha"}).copy()


def build_weekly_calendar_features(dim_date: pd.DataFrame) -> pd.DataFrame:
    calendar = dim_date.copy()
    calendar["fecha"] = monday_of_week(calendar["date"])
    grouped = calendar.groupby("fecha")
    out = grouped.agg(
        day_of_week=("day_of_week", "first"),
        week_of_month=("week_of_month", "first"),
        month=("month", "first"),
        iso_week=("iso_week", "first"),
        is_month_end=("is_month_end", "max"),
        is_any_holiday=("is_any_holiday", "sum"),
        is_pre_holiday=("is_pre_holiday", "sum"),
        is_post_holiday=("is_post_holiday", "sum"),
        is_bridge_day=("is_bridge_day", "sum"),
        easter_week_relative=("easter_week_relative", "mean"),
        is_post_easter_window_1_6=("is_post_easter_window_1_6", "max"),
    ).reset_index()
    return out
