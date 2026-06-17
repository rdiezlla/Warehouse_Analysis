from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


ACTION_KEEP_ACCESSIBLE = "Conviene mantener accesible"
ACTION_REVIEW_SECONDARY = "Candidato a revisar para almacen secundario"
ACTION_MOVE_OFF_SEASON = "Candidato a mover fuera de temporada"
ACTION_MANUAL_REVIEW = "Revisar manualmente"


@dataclass(frozen=True)
class ABCLegacyOutputs:
    datasets: dict[str, pd.DataFrame]
    kpis: dict[str, object]
    temporality_kpis: dict[str, object]


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    num = pd.to_numeric(numerator, errors="coerce")
    den = pd.to_numeric(denominator, errors="coerce")
    return num.div(den.where(den != 0))


def _first_non_empty(series: pd.Series) -> object:
    valid = series.dropna()
    return valid.iloc[0] if not valid.empty else pd.NA


def _prepare_movements(movimientos: pd.DataFrame) -> pd.DataFrame:
    if movimientos.empty:
        return pd.DataFrame()
    df = movimientos.copy()
    df["movement_type"] = df["movement_type"].astype("string").str.upper()
    df["operational_date"] = pd.to_datetime(df["operational_date"], errors="coerce")
    df["sku"] = df["sku"].astype("string")
    df["owner"] = df["owner"].astype("string")
    df["location"] = df["location"].astype("string")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)
    return df


def _prepare_stock(stock: pd.DataFrame | None) -> tuple[pd.DataFrame, pd.Timestamp | pd.NaT]:
    if stock is None or stock.empty:
        return pd.DataFrame(), pd.NaT
    df = stock.copy()
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"], errors="coerce")
    df["sku"] = df["sku"].astype("string")
    df["owner"] = df["owner_code"].fillna(df["owner"]).astype("string")
    df["location"] = df["location"].astype("string")
    df["stock_quantity"] = pd.to_numeric(df["stock_quantity"], errors="coerce").fillna(0)
    snapshot_date = df["snapshot_date"].dropna().max()
    return df, snapshot_date


def _add_abc(frame: pd.DataFrame, line_col: str, qty_col: str, group_sort_cols: list[str], abc_col: str) -> pd.DataFrame:
    if frame.empty:
        frame[abc_col] = pd.Series(dtype="object")
        frame[f"pct_{line_col}"] = pd.Series(dtype="float64")
        frame[f"pct_acum_{line_col}"] = pd.Series(dtype="float64")
        return frame
    ranked = frame.sort_values([line_col, qty_col, *group_sort_cols], ascending=[False, False, *([True] * len(group_sort_cols))]).reset_index(drop=True)
    total = float(ranked[line_col].sum())
    if total <= 0:
        ranked[f"pct_{line_col}"] = 0.0
        ranked[f"pct_acum_{line_col}"] = 0.0
        ranked[abc_col] = "D"
        return ranked
    ranked[f"pct_{line_col}"] = ranked[line_col] / total
    ranked[f"pct_acum_{line_col}"] = ranked[f"pct_{line_col}"].cumsum()
    before = ranked[f"pct_acum_{line_col}"] - ranked[f"pct_{line_col}"]
    ranked[abc_col] = "C"
    ranked.loc[before < 0.80, abc_col] = "A"
    ranked.loc[(before >= 0.80) & (before < 0.95), abc_col] = "B"
    ranked["ranking_abc"] = ranked.index + 1
    return ranked


def _stock_summary(stock: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    if stock.empty:
        return pd.DataFrame(columns=[*group_cols, "stock_actual", "ubicaciones_con_stock"])
    return (
        stock.groupby(group_cols, dropna=False)
        .agg(
            stock_actual=("stock_quantity", "sum"),
            ubicaciones_con_stock=("location", "nunique"),
            descripcion_stock=("sku_description", _first_non_empty),
        )
        .reset_index()
    )


def _aggregate_pi(pi: pd.DataFrame, cr: pd.DataFrame, stock: pd.DataFrame, group_cols: list[str], reference_date: pd.Timestamp) -> pd.DataFrame:
    if pi.empty:
        return pd.DataFrame(columns=[*group_cols, "lineas_pi", "cantidad_pi"])
    grouped = (
        pi.groupby(group_cols, dropna=False)
        .agg(
            descripcion_articulo=("sku_description", _first_non_empty),
            lineas_pi=("sku", "size"),
            cantidad_pi=("quantity", "sum"),
            pedidos_pi=("external_order_id", "nunique"),
            ubicaciones_movimiento=("location", "nunique"),
            primera_salida_pi=("operational_date", "min"),
            ultima_salida_pi=("operational_date", "max"),
            dias_con_salida_pi=("operational_date", lambda values: values.dt.normalize().nunique()),
        )
        .reset_index()
    )
    if not cr.empty:
        cr_summary = (
            cr.groupby(group_cols, dropna=False)
            .agg(lineas_cr=("sku", "size"), ultima_entrada_cr=("operational_date", "max"))
            .reset_index()
        )
        grouped = grouped.merge(cr_summary, on=group_cols, how="left")
    else:
        grouped["lineas_cr"] = 0
        grouped["ultima_entrada_cr"] = pd.NaT
    grouped["lineas_cr"] = grouped["lineas_cr"].fillna(0).astype(int)
    stock_group_cols = [column for column in group_cols if column in stock.columns]
    if stock_group_cols:
        grouped = grouped.merge(_stock_summary(stock, stock_group_cols), on=stock_group_cols, how="left")
    else:
        grouped["stock_actual"] = 0
        grouped["ubicaciones_con_stock"] = 0
    grouped["stock_actual"] = grouped["stock_actual"].fillna(0)
    grouped["ubicaciones_con_stock"] = grouped["ubicaciones_con_stock"].fillna(0).astype(int)
    grouped["dias_desde_ultima_salida"] = (reference_date.normalize() - pd.to_datetime(grouped["ultima_salida_pi"]).dt.normalize()).dt.days
    grouped["cobertura_lineas"] = _safe_divide(grouped["stock_actual"], grouped["lineas_pi"])
    grouped["cobertura_cantidad"] = _safe_divide(grouped["stock_actual"], grouped["cantidad_pi"])
    grouped["densidad_stock"] = _safe_divide(grouped["stock_actual"], grouped["ubicaciones_con_stock"])
    return grouped


def _article_from_owner(owner_frame: pd.DataFrame, reference_date: pd.Timestamp) -> pd.DataFrame:
    if owner_frame.empty:
        return pd.DataFrame()
    grouped = (
        owner_frame.groupby("sku", dropna=False)
        .agg(
            descripcion_articulo=("descripcion_articulo", _first_non_empty),
            lineas_pi=("lineas_pi", "sum"),
            cantidad_pi=("cantidad_pi", "sum"),
            pedidos_pi=("pedidos_pi", "sum"),
            ubicaciones_movimiento=("ubicaciones_movimiento", "sum"),
            primera_salida_pi=("primera_salida_pi", "min"),
            ultima_salida_pi=("ultima_salida_pi", "max"),
            dias_con_salida_pi=("dias_con_salida_pi", "sum"),
            lineas_cr=("lineas_cr", "sum"),
            ultima_entrada_cr=("ultima_entrada_cr", "max"),
            stock_actual=("stock_actual", "sum"),
            ubicaciones_con_stock=("ubicaciones_con_stock", "sum"),
        )
        .reset_index()
    )
    grouped["dias_desde_ultima_salida"] = (reference_date.normalize() - pd.to_datetime(grouped["ultima_salida_pi"]).dt.normalize()).dt.days
    grouped["cobertura_lineas"] = _safe_divide(grouped["stock_actual"], grouped["lineas_pi"])
    grouped["cobertura_cantidad"] = _safe_divide(grouped["stock_actual"], grouped["cantidad_pi"])
    grouped["densidad_stock"] = _safe_divide(grouped["stock_actual"], grouped["ubicaciones_con_stock"])
    return grouped


def _class_summary(frame: pd.DataFrame, abc_col: str, name: str) -> pd.DataFrame:
    if frame.empty or abc_col not in frame.columns:
        return pd.DataFrame(columns=["dataset", "abc_class", "articulos", "lineas_pi", "cantidad_pi", "stock_actual"])
    return (
        frame.groupby(abc_col, dropna=False)
        .agg(
            articulos=("sku", "nunique"),
            lineas_pi=("lineas_pi", "sum"),
            cantidad_pi=("cantidad_pi", "sum"),
            stock_actual=("stock_actual", "sum"),
        )
        .reset_index()
        .rename(columns={abc_col: "abc_class"})
        .assign(dataset=name)
    )


def _quarterly(pi: pd.DataFrame, cr: pd.DataFrame, stock: pd.DataFrame, group_cols: list[str], reference_date: pd.Timestamp) -> pd.DataFrame:
    if pi.empty:
        return pd.DataFrame()
    pi_period = pi.copy()
    pi_period["periodo_trimestre"] = pi_period["operational_date"].dt.to_period("Q").astype("string")
    cr_period = cr.copy()
    if not cr_period.empty:
        cr_period["periodo_trimestre"] = cr_period["operational_date"].dt.to_period("Q").astype("string")
    return _aggregate_pi(pi_period, cr_period, stock, ["periodo_trimestre", *group_cols], reference_date)


def _quarterly_changes(article_quarterly: pd.DataFrame) -> pd.DataFrame:
    if article_quarterly.empty or "abc_class_trimestre" not in article_quarterly.columns:
        return pd.DataFrame()
    ordered = article_quarterly.sort_values(["sku", "periodo_trimestre"])
    ordered["abc_anterior"] = ordered.groupby("sku")["abc_class_trimestre"].shift(1)
    ordered["lineas_pi_anterior"] = ordered.groupby("sku")["lineas_pi"].shift(1)
    changes = ordered[ordered["abc_anterior"].notna()].copy()
    changes["cambio_abc"] = changes["abc_anterior"].astype("string") + " -> " + changes["abc_class_trimestre"].astype("string")
    changes["delta_lineas_pi"] = changes["lineas_pi"] - changes["lineas_pi_anterior"].fillna(0)
    return changes[
        [
            "periodo_trimestre",
            "sku",
            "descripcion_articulo",
            "abc_anterior",
            "abc_class_trimestre",
            "cambio_abc",
            "lineas_pi_anterior",
            "lineas_pi",
            "delta_lineas_pi",
            "stock_actual",
        ]
    ]


def _temporality(pi: pd.DataFrame, article_current: pd.DataFrame, reference_date: pd.Timestamp) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, object]]:
    if pi.empty:
        empty = pd.DataFrame()
        return empty, empty, empty, empty, {}
    work = pi.copy()
    work["month"] = work["operational_date"].dt.to_period("M").astype("string")
    work["quarter"] = work["operational_date"].dt.to_period("Q").astype("string")
    monthly = (
        work.groupby(["sku", "month"], dropna=False)
        .agg(lineas_pi_mes=("sku", "size"), cantidad_pi_mes=("quantity", "sum"))
        .reset_index()
    )
    quarterly = (
        work.groupby(["sku", "quarter"], dropna=False)
        .agg(lineas_pi_trimestre=("sku", "size"), cantidad_pi_trimestre=("quantity", "sum"))
        .reset_index()
    )
    stats = (
        monthly.groupby("sku", dropna=False)
        .agg(
            meses_con_salida=("month", "nunique"),
            media_lineas_mes=("lineas_pi_mes", "mean"),
            desviacion_lineas_mes=("lineas_pi_mes", "std"),
            max_lineas_mes=("lineas_pi_mes", "max"),
        )
        .reset_index()
    )
    stats["coef_variacion_lineas_mes"] = _safe_divide(stats["desviacion_lineas_mes"].fillna(0), stats["media_lineas_mes"])
    current = article_current[["sku", "descripcion_articulo", "abc_class_actual", "lineas_pi", "stock_actual", "dias_desde_ultima_salida"]].copy()
    summary = current.merge(stats, on="sku", how="left")
    summary["meses_con_salida"] = summary["meses_con_salida"].fillna(0).astype(int)
    summary["temporalidad_clase"] = "Regular"
    summary.loc[summary["meses_con_salida"].lt(3), "temporalidad_clase"] = "Nuevo / sin historico suficiente"
    summary.loc[summary["dias_desde_ultima_salida"].fillna(9999).gt(180), "temporalidad_clase"] = "Dormido"
    summary.loc[summary["coef_variacion_lineas_mes"].fillna(0).gt(1.5), "temporalidad_clase"] = "Intermitente"
    summary.loc[summary["max_lineas_mes"].fillna(0).gt(summary["media_lineas_mes"].fillna(0) * 4), "temporalidad_clase"] = "Estacional"
    decision = summary.copy()
    decision["accion_recomendada"] = ACTION_KEEP_ACCESSIBLE
    decision.loc[
        decision["abc_class_actual"].eq("C") & decision["stock_actual"].fillna(0).gt(0),
        "accion_recomendada",
    ] = ACTION_REVIEW_SECONDARY
    decision.loc[
        decision["temporalidad_clase"].isin(["Dormido", "Estacional"]) & decision["stock_actual"].fillna(0).gt(0),
        "accion_recomendada",
    ] = ACTION_MOVE_OFF_SEASON
    decision.loc[
        decision["temporalidad_clase"].eq("Intermitente") & decision["stock_actual"].fillna(0).gt(0),
        "accion_recomendada",
    ] = ACTION_MANUAL_REVIEW
    decision["es_candidato_mover"] = decision["accion_recomendada"].isin([ACTION_REVIEW_SECONDARY, ACTION_MOVE_OFF_SEASON])
    decision["es_candidato_mantener"] = decision["accion_recomendada"].eq(ACTION_KEEP_ACCESSIBLE)
    kpis = {
        "fecha_referencia": str(reference_date.date()) if pd.notna(reference_date) else None,
        "conteo_temporalidad_clase": summary["temporalidad_clase"].value_counts(dropna=False).to_dict(),
        "conteo_acciones_recomendadas": decision["accion_recomendada"].value_counts(dropna=False).to_dict(),
        "numero_candidatos_mover": int(decision["es_candidato_mover"].sum()),
        "numero_candidatos_mantener": int(decision["es_candidato_mantener"].sum()),
    }
    return summary, monthly, quarterly, decision, kpis


def build_legacy_abc_outputs(movimientos: pd.DataFrame, stock: pd.DataFrame | None = None) -> ABCLegacyOutputs:
    movements = _prepare_movements(movimientos)
    stock_df, snapshot_date = _prepare_stock(stock)
    if movements.empty:
        return ABCLegacyOutputs(datasets={}, kpis={}, temporality_kpis={})

    reference_date = snapshot_date if pd.notna(snapshot_date) else movements["operational_date"].dropna().max()
    pi_all = movements[movements["movement_type"].eq("PI") & movements["sku"].notna()].copy()
    cr_all = movements[movements["movement_type"].eq("CR") & movements["sku"].notna()].copy()
    if pd.isna(reference_date):
        reference_date = pd.Timestamp.today().normalize()

    recent_start = reference_date.normalize() - pd.Timedelta(days=30)
    ytd_start = pd.Timestamp(year=reference_date.year, month=1, day=1)

    owner_30d = _aggregate_pi(pi_all[pi_all["operational_date"].ge(recent_start)], cr_all[cr_all["operational_date"].ge(recent_start)], stock_df, ["owner", "sku"], reference_date)
    owner_30d = _add_abc(owner_30d, "lineas_pi", "cantidad_pi", ["owner", "sku"], "abc_class_30d")
    article_30d = _article_from_owner(owner_30d, reference_date)
    article_30d = _add_abc(article_30d, "lineas_pi", "cantidad_pi", ["sku"], "abc_class_30d")

    owner_ytd = _aggregate_pi(pi_all[pi_all["operational_date"].ge(ytd_start)], cr_all[cr_all["operational_date"].ge(ytd_start)], stock_df, ["owner", "sku"], reference_date)
    owner_ytd = _add_abc(owner_ytd, "lineas_pi", "cantidad_pi", ["owner", "sku"], "abc_class_ytd")
    article_ytd = _article_from_owner(owner_ytd, reference_date)
    article_ytd = _add_abc(article_ytd, "lineas_pi", "cantidad_pi", ["sku"], "abc_class_actual")

    quarterly_owner = _quarterly(pi_all, cr_all, stock_df, ["owner", "sku"], reference_date)
    quarterly_owner = _add_abc(quarterly_owner, "lineas_pi", "cantidad_pi", ["periodo_trimestre", "owner", "sku"], "abc_class_trimestre")
    quarterly_article = _quarterly(pi_all, cr_all, stock_df, ["sku"], reference_date)
    quarterly_article = _add_abc(quarterly_article, "lineas_pi", "cantidad_pi", ["periodo_trimestre", "sku"], "abc_class_trimestre")
    quarterly_changes = _quarterly_changes(quarterly_article)

    temporality_article, temporality_monthly, temporality_quarterly, decision_article, temporality_kpis = _temporality(pi_all, article_ytd, reference_date)

    current_summary = pd.concat(
        [
            _class_summary(owner_ytd, "abc_class_ytd", "owner_article_ytd"),
            _class_summary(article_ytd, "abc_class_actual", "article_ytd"),
            _class_summary(article_30d, "abc_class_30d", "article_30d"),
            _class_summary(quarterly_article, "abc_class_trimestre", "article_quarterly"),
        ],
        ignore_index=True,
    )
    kpis = {
        "snapshot_date": str(snapshot_date.date()) if pd.notna(snapshot_date) else None,
        "fecha_referencia": str(reference_date.date()) if pd.notna(reference_date) else None,
        "articulos_actuales": int(article_ytd["sku"].nunique()) if not article_ytd.empty else 0,
        "owner_articulos_actuales": int(owner_ytd[["owner", "sku"]].drop_duplicates().shape[0]) if not owner_ytd.empty else 0,
        "stock_total": float(stock_df["stock_quantity"].sum()) if not stock_df.empty else 0.0,
        "lineas_pi_ytd": int(article_ytd["lineas_pi"].sum()) if not article_ytd.empty else 0,
        "conteo_abc_actual": article_ytd["abc_class_actual"].value_counts(dropna=False).to_dict() if "abc_class_actual" in article_ytd else {},
    }
    datasets = {
        "stock_abc_actual_owner_article": owner_ytd,
        "stock_abc_actual_article": article_ytd,
        "stock_abc_historico_trimestral_owner_article": quarterly_owner,
        "stock_abc_historico_trimestral_article": quarterly_article,
        "stock_abc_cambios_trimestrales": quarterly_changes,
        "stock_abc_temporalidad_article": temporality_article,
        "stock_abc_temporalidad_monthly_article": temporality_monthly,
        "stock_abc_temporalidad_quarterly_article": temporality_quarterly,
        "stock_abc_decision_almacen_article": decision_article,
        "stock_abc_resumen_clases": current_summary,
        "stock_abc_30d_owner_article": owner_30d,
        "stock_abc_30d_article": article_30d,
    }
    return ABCLegacyOutputs(datasets=datasets, kpis=kpis, temporality_kpis=temporality_kpis)
