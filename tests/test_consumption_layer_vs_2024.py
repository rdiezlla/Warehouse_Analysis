from __future__ import annotations

import unittest

import pandas as pd

from src.modeling.forecast_tracking import build_actuals_weekly
from src.reporting.consumption_layer import (
    MAIN_KPIS,
    _build_validations,
    _build_vs_2024_daily,
    _build_vs_2024_weekly,
    _load_dim_kpi,
)


def _build_actuals_daily_fixture() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for kpi_index, kpi in enumerate(MAIN_KPIS, start=1):
        for current_date in pd.date_range("2024-01-01", "2024-12-31", freq="D"):
            rows.append(
                {
                    "target_date": current_date,
                    "kpi": kpi,
                    "actual_value": float(100 * kpi_index + current_date.dayofyear),
                    "actual_source": "fixture",
                    "actual_truth_status": "observado_final",
                }
            )
        for current_date in pd.date_range("2026-01-01", "2026-03-31", freq="D"):
            rows.append(
                {
                    "target_date": current_date,
                    "kpi": kpi,
                    "actual_value": float(200 * kpi_index + current_date.dayofyear),
                    "actual_source": "fixture",
                    "actual_truth_status": "observado_final",
                }
            )
    return pd.DataFrame(rows)


def _build_forecast_daily_fixture() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for kpi_index, kpi in enumerate(MAIN_KPIS, start=1):
        for current_date in pd.date_range("2026-03-15", "2026-04-11", freq="D"):
            rows.append(
                {
                    "fecha": current_date,
                    "kpi": kpi,
                    "forecast_value": float(300 * kpi_index + current_date.dayofyear),
                    "actual_value": None,
                }
            )
    return pd.DataFrame(rows)


class ConsumptionLayerVs2024RegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.actuals_daily = _build_actuals_daily_fixture()
        self.actuals_weekly = build_actuals_weekly(self.actuals_daily)
        self.forecast_daily = _build_forecast_daily_fixture()
        self.forecast_weekly = (
            self.forecast_daily.assign(
                week_start_date=lambda df: pd.to_datetime(df["fecha"])
                - pd.to_timedelta(pd.to_datetime(df["fecha"]).dt.weekday, unit="D")
            )
            .groupby(["week_start_date", "kpi"], as_index=False)
            .agg(
                forecast_value=("forecast_value", "sum"),
                actual_value=("actual_value", "sum"),
            )
            .assign(forecast_source="daily_aggregated")
        )

    def test_daily_vs_2024_keeps_picking_reference_for_future_quarters(self) -> None:
        vs_daily = _build_vs_2024_daily(self.actuals_daily, self.forecast_daily)
        q3_row = vs_daily[
            (pd.to_datetime(vs_daily["fecha"]).eq(pd.Timestamp("2026-07-15")))
            & (vs_daily["kpi"] == "picking_lines_pi")
        ]

        self.assertEqual(len(q3_row), 1)
        self.assertTrue(pd.isna(q3_row.iloc[0]["actual_value_current"]))
        self.assertEqual(q3_row.iloc[0]["actual_value_2024"], 597.0)

    def test_weekly_vs_2024_keeps_picking_reference_for_future_quarters(self) -> None:
        vs_weekly = _build_vs_2024_weekly(self.actuals_weekly, self.forecast_weekly)
        q3_row = vs_weekly[
            (pd.to_datetime(vs_weekly["week_start_date"]).eq(pd.Timestamp("2026-07-13")))
            & (vs_weekly["kpi"] == "picking_units_pi")
        ]

        self.assertEqual(len(q3_row), 1)
        self.assertTrue(pd.isna(q3_row.iloc[0]["actual_value_current"]))
        self.assertGreater(q3_row.iloc[0]["actual_value_2024"], 0)

    def test_validations_report_picking_vs_2024_coverage_per_kpi(self) -> None:
        vs_daily = _build_vs_2024_daily(self.actuals_daily, self.forecast_daily)
        vs_weekly = _build_vs_2024_weekly(self.actuals_weekly, self.forecast_weekly)
        validations = _build_validations(
            self.forecast_daily.rename(columns={"fecha": "fecha"}),
            self.forecast_weekly,
            vs_daily,
            vs_weekly,
            pd.DataFrame(columns=["kpi"]),
            _load_dim_kpi(),
            self.actuals_daily,
            self.actuals_weekly,
        )

        check_names = set(validations["check_name"].tolist())
        self.assertIn("daily_vs_2024_reference_coverage_picking_lines_pi", check_names)
        self.assertIn("daily_vs_2024_reference_coverage_picking_units_pi", check_names)
        self.assertIn("weekly_vs_2024_reference_coverage_picking_lines_pi", check_names)
        self.assertIn("weekly_vs_2024_reference_coverage_picking_units_pi", check_names)

        focus = validations[
            validations["check_name"].isin(
                [
                    "daily_vs_2024_reference_coverage_picking_lines_pi",
                    "daily_vs_2024_reference_coverage_picking_units_pi",
                    "weekly_vs_2024_reference_coverage_picking_lines_pi",
                    "weekly_vs_2024_reference_coverage_picking_units_pi",
                ]
            )
        ]
        self.assertTrue((focus["status"] == "ok").all())


if __name__ == "__main__":
    unittest.main()
