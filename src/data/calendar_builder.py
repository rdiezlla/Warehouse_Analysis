from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import holidays
import numpy as np
import pandas as pd
from dateutil.easter import easter

from src.paths import EXTERNAL_DIR
from src.utils.date_utils import week_of_month
from src.utils.io_utils import save_dataframe

LOGGER = logging.getLogger(__name__)

MADRID_CITY_LOCAL = {
    2021: [date(2021, 5, 15), date(2021, 11, 9)],
    2022: [date(2022, 5, 16), date(2022, 11, 9)],
    2023: [date(2023, 5, 15), date(2023, 11, 9)],
    2024: [date(2024, 5, 15), date(2024, 11, 9)],
    2025: [date(2025, 5, 15), date(2025, 11, 10)],
    2026: [date(2026, 5, 15), date(2026, 11, 9)],
}
CITY_SOURCE = {
    2021: "https://www.comunidad.madrid/sites/default/files/doc/municipios/bocm20201222_fiestas.pdf",
    2022: "https://www.comunidad.madrid/sites/default/files/doc/municipios/bocm20211220_fiestas.pdf",
    2023: "https://www.comunidad.madrid/sites/default/files/doc/municipios/bocm20221221_fiestasmadrid.pdf",
    2024: "https://www.bocm.es/boletin/CM_Orden_BOCM/2023/12/15/BOCM-20231215-27.PDF",
    2025: "https://www.bocm.es/boletin/CM_Orden_BOCM/2024/12/13/BOCM-20241213-23.PDF",
    2026: "https://www.bocm.es/boletin/CM_Orden_BOCM/2025/12/12/BOCM-20251212-34.PDF",
}


def ensure_holiday_reference_csv(settings: dict) -> Path:
    target = Path(settings["calendar"]["external_csv"])
    if not target.is_absolute():
        target = EXTERNAL_DIR.parent.parent / target
    if target.exists():
        return target

    years = range(pd.Timestamp(settings["calendar"]["start_date"]).year, pd.Timestamp(settings["calendar"]["end_date"]).year + 1)
    national = holidays.country_holidays("ES", years=years)
    madrid_region = holidays.country_holidays("ES", years=years, subdiv="MD")
    rows = []
    for year in years:
        for holiday_date in MADRID_CITY_LOCAL[year]:
            rows.append(
                {
                    "date": pd.Timestamp(holiday_date),
                    "holiday_name_national": national.get(holiday_date),
                    "holiday_name_region": madrid_region.get(holiday_date) if holiday_date in madrid_region else None,
                    "holiday_name_city": "Festivo local Madrid capital",
                    "source_url": CITY_SOURCE[year],
                }
            )
    national_dates = {pd.Timestamp(key) for key in national.keys()}
    region_dates = {pd.Timestamp(key) for key in madrid_region.keys()}
    all_dates = sorted(national_dates | region_dates | {pd.Timestamp(day) for values in MADRID_CITY_LOCAL.values() for day in values})
    frame = pd.DataFrame({"date": all_dates}).drop_duplicates()
    frame["holiday_name_national"] = frame["date"].map(lambda value: national.get(value.date()))
    frame["holiday_name_region"] = frame["date"].map(lambda value: madrid_region.get(value.date()) if value.date() in madrid_region else None)
    city_lookup = {pd.Timestamp(day): "Festivo local Madrid capital" for values in MADRID_CITY_LOCAL.values() for day in values}
    frame["holiday_name_city"] = frame["date"].map(city_lookup)
    frame["source_url"] = frame["date"].dt.year.map(CITY_SOURCE)
    save_dataframe(frame.sort_values("date"), target.with_suffix(""), index=False)
    LOGGER.info("Generated holiday reference CSV at %s", target)
    return target


def build_dim_date(settings: dict) -> pd.DataFrame:
    csv_path = ensure_holiday_reference_csv(settings)
    holidays_df = pd.read_csv(csv_path.with_suffix(".csv"), parse_dates=["date"])
    holidays_df = holidays_df.drop_duplicates("date")
    frame = pd.DataFrame({"date": pd.date_range(settings["calendar"]["start_date"], settings["calendar"]["end_date"], freq="D")})
    frame = frame.merge(holidays_df, on="date", how="left")
    frame["year"] = frame["date"].dt.year
    frame["month"] = frame["date"].dt.month
    frame["day"] = frame["date"].dt.day
    frame["day_of_week"] = frame["date"].dt.dayofweek
    frame["day_name"] = frame["date"].dt.day_name()
    frame["iso_week"] = frame["date"].dt.isocalendar().week.astype(int)
    frame["iso_year"] = frame["date"].dt.isocalendar().year.astype(int)
    frame["week_of_month"] = week_of_month(frame["date"]).astype(int)
    frame["is_month_end"] = frame["date"].dt.is_month_end.astype(int)
    frame["is_weekend"] = frame["day_of_week"].isin([5, 6]).astype(int)
    frame["is_holiday_national"] = frame["holiday_name_national"].notna().astype(int)
    frame["is_holiday_madrid_region"] = frame["holiday_name_region"].notna().astype(int)
    frame["is_holiday_madrid_city"] = frame["holiday_name_city"].notna().astype(int)
    frame["is_any_holiday"] = frame[["is_holiday_national", "is_holiday_madrid_region", "is_holiday_madrid_city"]].max(axis=1)
    frame["is_pre_holiday"] = frame["is_any_holiday"].shift(-1, fill_value=0)
    frame["is_post_holiday"] = frame["is_any_holiday"].shift(1, fill_value=0)
    prev_nonwork = frame["is_any_holiday"].shift(1, fill_value=0) | frame["is_weekend"].shift(1, fill_value=0)
    next_nonwork = frame["is_any_holiday"].shift(-1, fill_value=0) | frame["is_weekend"].shift(-1, fill_value=0)
    frame["is_bridge_day"] = ((frame["is_any_holiday"] == 0) & (frame["is_weekend"] == 0) & prev_nonwork.astype(bool) & next_nonwork.astype(bool)).astype(int)

    easter_sunday = pd.Series({year: pd.Timestamp(easter(year)) for year in frame["year"].unique()})
    frame["easter_sunday"] = frame["year"].map(easter_sunday)
    frame["easter_week_relative"] = (frame["date"] - frame["easter_sunday"]).dt.days
    frame["is_post_easter_window_1_6"] = frame["easter_week_relative"].between(1, 6).astype(int)
    frame = frame.drop(columns=["easter_sunday"])
    return frame
