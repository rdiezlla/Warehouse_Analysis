from __future__ import annotations

from collections import Counter
from itertools import combinations

import pandas as pd


def prepare_basket_movements(movimientos: pd.DataFrame) -> pd.DataFrame:
    if movimientos.empty:
        return pd.DataFrame(columns=["transaction_id", "sku", "sku_description", "quantity", "owner", "external_order_id", "basket_date"])

    df = movimientos.copy()
    df["movement_type"] = df["movement_type"].astype("string").str.upper()
    df = df[df["movement_type"].eq("PI")].copy()
    df = df[df["sku"].notna() & df["external_order_id"].notna()].copy()
    df["owner"] = df["owner"].fillna("UNKNOWN").astype("string")
    df["transaction_id"] = df["external_order_id"].astype("string") + "|" + df["owner"].astype("string")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)
    return df[["transaction_id", "sku", "sku_description", "quantity", "owner", "external_order_id", "basket_date"]]


def compute_market_basket_outputs(movimientos: pd.DataFrame, min_pair_transactions: int = 2) -> tuple[pd.DataFrame, pd.DataFrame]:
    basket_df = prepare_basket_movements(movimientos)
    if basket_df.empty:
        empty_pairs = pd.DataFrame(
            columns=[
                "article_a",
                "article_b",
                "shared_transactions",
                "support",
                "confidence_a_to_b",
                "confidence_b_to_a",
                "lift",
            ]
        )
        empty_rules = pd.DataFrame(
            columns=["antecedent", "consequent", "support", "confidence", "lift", "shared_transactions"]
        )
        return empty_pairs, empty_rules

    transaction_items = basket_df.groupby("transaction_id")["sku"].agg(lambda values: tuple(sorted(set(values.astype(str)))))
    transaction_count = int(len(transaction_items))
    item_counts = Counter()
    pair_counts = Counter()

    for items in transaction_items:
        if not items:
            continue
        item_counts.update(items)
        for article_a, article_b in combinations(items, 2):
            pair_counts[(article_a, article_b)] += 1

    pair_rows = []
    rule_rows = []
    for (article_a, article_b), shared in pair_counts.items():
        if shared < min_pair_transactions:
            continue
        support = shared / transaction_count if transaction_count else 0.0
        support_a = item_counts[article_a] / transaction_count if transaction_count else 0.0
        support_b = item_counts[article_b] / transaction_count if transaction_count else 0.0
        confidence_a_to_b = shared / item_counts[article_a] if item_counts[article_a] else 0.0
        confidence_b_to_a = shared / item_counts[article_b] if item_counts[article_b] else 0.0
        lift = support / (support_a * support_b) if support_a and support_b else 0.0
        pair_rows.append(
            {
                "article_a": article_a,
                "article_b": article_b,
                "shared_transactions": shared,
                "support": support,
                "confidence_a_to_b": confidence_a_to_b,
                "confidence_b_to_a": confidence_b_to_a,
                "lift": lift,
            }
        )
        rule_rows.extend(
            [
                {
                    "antecedent": article_a,
                    "consequent": article_b,
                    "support": support,
                    "confidence": confidence_a_to_b,
                    "lift": lift,
                    "shared_transactions": shared,
                },
                {
                    "antecedent": article_b,
                    "consequent": article_a,
                    "support": support,
                    "confidence": confidence_b_to_a,
                    "lift": lift,
                    "shared_transactions": shared,
                },
            ]
        )

    pairs = pd.DataFrame(pair_rows)
    rules = pd.DataFrame(rule_rows)
    if not pairs.empty:
        pairs = pairs.sort_values(["shared_transactions", "lift", "support"], ascending=[False, False, False]).reset_index(drop=True)
    if not rules.empty:
        rules = rules.sort_values(["lift", "confidence", "shared_transactions"], ascending=[False, False, False]).reset_index(drop=True)
    return pairs, rules
