from __future__ import annotations

from functools import lru_cache
from typing import Iterable

import pandas as pd

from src.paths import CONFIG_DIR
from src.utils.io_utils import load_yaml


@lru_cache(maxsize=1)
def load_feature_policy() -> dict:
    return load_yaml(CONFIG_DIR / "feature_policy.yaml")


def is_blocked_feature(column: str, policy: dict | None = None) -> bool:
    policy = policy or load_feature_policy()
    if column in set(policy.get("block_exact", [])):
        return True
    return any(column.startswith(prefix) for prefix in policy.get("block_prefixes", []))


def is_allowed_feature(column: str, policy: dict | None = None) -> bool:
    policy = policy or load_feature_policy()
    if is_blocked_feature(column, policy):
        return False
    if column in set(policy.get("allow_exact", [])):
        return True
    return any(column.startswith(prefix) for prefix in policy.get("allow_prefixes", []))


def validate_feature_columns(columns: Iterable[str], *, policy: dict | None = None) -> None:
    policy = policy or load_feature_policy()
    columns = list(columns)
    blocked = [column for column in columns if is_blocked_feature(column, policy)]
    not_allowed = [column for column in columns if not is_allowed_feature(column, policy)]
    if blocked:
        raise ValueError(f"Forbidden training/prediction columns detected: {blocked}")
    if not_allowed:
        raise ValueError(f"Columns outside allowlist detected: {not_allowed}")


def select_training_features(df: pd.DataFrame, *, extra_drop: Iterable[str] = ()) -> list[str]:
    policy = load_feature_policy()
    candidates = [
        column
        for column in df.columns
        if column not in set(extra_drop) and pd.api.types.is_numeric_dtype(df[column]) and is_allowed_feature(column, policy)
    ]
    validate_feature_columns(candidates, policy=policy)
    return candidates
